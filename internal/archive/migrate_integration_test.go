package archive

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestMigrateVerifyInspectHydrateIntegration(t *testing.T) {
	ctx := context.Background()
	dbDir := filepath.Join(t.TempDir(), "db")
	createBlockStoreFixture(t, dbDir, 5)
	reader, err := OpenCometBlockStore(dbDir, "goleveldb")
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()

	objectRoot := filepath.Join(t.TempDir(), "objects")
	store, err := NewLocalObjectStore(objectRoot)
	if err != nil {
		t.Fatal(err)
	}
	result, err := Migrate(ctx, reader, store, MigrationOptions{
		ChainID:       testChainID,
		StartHeight:   1,
		EndHeight:     5,
		SegmentBlocks: 2,
		Prefix:        "archive",
		Compression:   CompressionGzip,
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Segments != 3 || result.Uploaded != 3 {
		t.Fatalf("unexpected migration result: %+v", result)
	}
	for _, segment := range result.Manifest.Segments {
		for _, indexed := range segment.Blocks {
			source := reader.LoadBlock(indexed.Height)
			if source == nil {
				t.Fatalf("source block %d missing", indexed.Height)
			}
			if got := hex.EncodeToString(source.Hash()); got != indexed.Hash {
				t.Fatalf("height %d hash mismatch: manifest %s source %s", indexed.Height, indexed.Hash, got)
			}
		}
	}
	verifyResult, err := Verify(ctx, store, VerifyOptions{ManifestKey: result.ManifestKey})
	if err != nil {
		t.Fatal(err)
	}
	if verifyResult.SegmentsChecked != 3 || verifyResult.BlocksChecked != 5 {
		t.Fatalf("unexpected verify result: %+v", verifyResult)
	}
	summary, err := Inspect(ctx, store, result.ManifestKey)
	if err != nil {
		t.Fatal(err)
	}
	if summary.Blocks != 5 || summary.FirstHeight != 1 || summary.LastHeight != 5 {
		t.Fatalf("unexpected inspect summary: %+v", summary)
	}
	cacheDir := filepath.Join(t.TempDir(), "cache")
	hydrateResult, err := Hydrate(ctx, store, HydrateOptions{
		ManifestKey: result.ManifestKey,
		CacheDir:    cacheDir,
		StartHeight: 2,
		EndHeight:   4,
		MaxBytes:    1 << 20,
	})
	if err != nil {
		t.Fatal(err)
	}
	if hydrateResult.BlocksWritten != 3 {
		t.Fatalf("got %d hydrated blocks, want 3", hydrateResult.BlocksWritten)
	}
	for _, height := range []string{"000000000002.block", "000000000003.block", "000000000004.block"} {
		if _, err := os.Stat(filepath.Join(cacheDir, "chains", testChainID, "blocks", height)); err != nil {
			t.Fatal(err)
		}
	}
}

func TestMigrateHonorsManifestKeyOverride(t *testing.T) {
	ctx := context.Background()
	dbDir := filepath.Join(t.TempDir(), "db")
	createBlockStoreFixture(t, dbDir, 2)
	reader, err := OpenCometBlockStore(dbDir, "goleveldb")
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	store, err := NewLocalObjectStore(filepath.Join(t.TempDir(), "objects"))
	if err != nil {
		t.Fatal(err)
	}
	manifestKey := "custom/manifests/archive.json"
	result, err := Migrate(ctx, reader, store, MigrationOptions{
		ChainID:       testChainID,
		StartHeight:   1,
		EndHeight:     2,
		SegmentBlocks: 1,
		Prefix:        "archive",
		ManifestKey:   manifestKey,
		Compression:   CompressionGzip,
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.ManifestKey != manifestKey {
		t.Fatalf("manifest key = %q, want %q", result.ManifestKey, manifestKey)
	}
	if _, err := LoadManifest(ctx, store, manifestKey); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadManifest(ctx, store, ManifestKey("archive", testChainID, DefaultManifest)); !errors.Is(err, ErrObjectNotFound) {
		t.Fatalf("default manifest err=%v, want ErrObjectNotFound", err)
	}
	if _, err := store.Stat(ctx, manifestKey+".state.json"); err != nil {
		t.Fatalf("custom migration state missing: %v", err)
	}
}

func TestHydrateReplacesCacheSymlinkWithoutOverwritingTarget(t *testing.T) {
	ctx := context.Background()
	store, err := NewLocalObjectStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	record := makeTestRecord(t, 1)
	data, segment, err := EncodeSegment([]BlockRecord{record}, CompressionGzip)
	if err != nil {
		t.Fatal(err)
	}
	segment.Key = SegmentKey("archive", testChainID, segment)
	if putErr := store.Put(ctx, segment.Key, data); putErr != nil {
		t.Fatal(putErr)
	}
	manifest, err := NewManifest(testChainID, []SegmentManifest{segment})
	if err != nil {
		t.Fatal(err)
	}
	manifestKey := ManifestKey("archive", testChainID, DefaultManifest)
	if saveErr := SaveManifest(ctx, store, manifestKey, manifest); saveErr != nil {
		t.Fatal(saveErr)
	}
	cacheDir := filepath.Join(t.TempDir(), "cache")
	blockDir := filepath.Join(cacheDir, "chains", testChainID, "blocks")
	if mkdirErr := os.MkdirAll(blockDir, 0o755); mkdirErr != nil {
		t.Fatal(mkdirErr)
	}
	outside := filepath.Join(t.TempDir(), "outside.block")
	if writeErr := os.WriteFile(outside, []byte("outside"), 0o600); writeErr != nil {
		t.Fatal(writeErr)
	}
	cachePath := filepath.Join(blockDir, "000000000001.block")
	if symlinkErr := os.Symlink(outside, cachePath); symlinkErr != nil {
		t.Skipf("symlink unavailable: %v", symlinkErr)
	}
	if _, hydrateErr := Hydrate(ctx, store, HydrateOptions{
		ManifestKey: manifestKey,
		CacheDir:    cacheDir,
		StartHeight: 1,
		EndHeight:   1,
		MaxBytes:    1 << 20,
	}); hydrateErr != nil {
		t.Fatal(hydrateErr)
	}
	outsideData, err := os.ReadFile(outside)
	if err != nil {
		t.Fatal(err)
	}
	if string(outsideData) != "outside" {
		t.Fatalf("outside symlink target was overwritten with %q", outsideData)
	}
	info, err := os.Lstat(cachePath)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode()&os.ModeSymlink != 0 {
		t.Fatal("cache path is still a symlink after hydrate")
	}
	cacheData, err := os.ReadFile(cachePath)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(cacheData, record.Bytes) {
		t.Fatal("cache file does not contain hydrated block bytes")
	}
}

func TestHydrateEnforcesCacheLimitIncrementally(t *testing.T) {
	ctx := context.Background()
	store, err := NewLocalObjectStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	records := []BlockRecord{
		makeTestRecord(t, 1),
		makeTestRecord(t, 2),
		makeTestRecord(t, 3),
	}
	data, segment, err := EncodeSegment(records, CompressionGzip)
	if err != nil {
		t.Fatal(err)
	}
	segment.Key = SegmentKey("archive", testChainID, segment)
	if putErr := store.Put(ctx, segment.Key, data); putErr != nil {
		t.Fatal(putErr)
	}
	manifest, err := NewManifest(testChainID, []SegmentManifest{segment})
	if err != nil {
		t.Fatal(err)
	}
	manifestKey := ManifestKey("archive", testChainID, DefaultManifest)
	if saveErr := SaveManifest(ctx, store, manifestKey, manifest); saveErr != nil {
		t.Fatal(saveErr)
	}

	cacheDir := filepath.Join(t.TempDir(), "cache")
	maxBytes := int64(len(records[2].Bytes))
	result, err := Hydrate(ctx, store, HydrateOptions{
		ManifestKey: manifestKey,
		CacheDir:    cacheDir,
		StartHeight: 1,
		EndHeight:   3,
		MaxBytes:    maxBytes,
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.BlocksWritten != 3 {
		t.Fatalf("blocks written = %d, want 3", result.BlocksWritten)
	}
	blockDir := filepath.Join(cacheDir, "chains", testChainID, "blocks")
	entries, err := os.ReadDir(blockDir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || entries[0].Name() != "000000000003.block" {
		t.Fatalf("cache entries = %v, want only latest block", entries)
	}
	info, err := entries[0].Info()
	if err != nil {
		t.Fatal(err)
	}
	if info.Size() > maxBytes {
		t.Fatalf("retained cache size = %d, want <= %d", info.Size(), maxBytes)
	}
}

func TestHydrateCacheLimitIgnoresUnmanagedFiles(t *testing.T) {
	ctx := context.Background()
	store, err := NewLocalObjectStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	records := []BlockRecord{
		makeTestRecord(t, 1),
		makeTestRecord(t, 2),
	}
	data, segment, err := EncodeSegment(records, CompressionGzip)
	if err != nil {
		t.Fatal(err)
	}
	segment.Key = SegmentKey("archive", testChainID, segment)
	if putErr := store.Put(ctx, segment.Key, data); putErr != nil {
		t.Fatal(putErr)
	}
	manifest, err := NewManifest(testChainID, []SegmentManifest{segment})
	if err != nil {
		t.Fatal(err)
	}
	manifestKey := ManifestKey("archive", testChainID, DefaultManifest)
	if saveErr := SaveManifest(ctx, store, manifestKey, manifest); saveErr != nil {
		t.Fatal(saveErr)
	}
	cacheDir := filepath.Join(t.TempDir(), "cache")
	blockDir := filepath.Join(cacheDir, "chains", testChainID, "blocks")
	if mkdirErr := os.MkdirAll(blockDir, 0o755); mkdirErr != nil {
		t.Fatal(mkdirErr)
	}
	keepPath := filepath.Join(blockDir, "operator-note.txt")
	if writeErr := os.WriteFile(keepPath, []byte("do not remove"), 0o600); writeErr != nil {
		t.Fatal(writeErr)
	}
	maxBytes := int64(len(records[1].Bytes))
	if _, err := Hydrate(ctx, store, HydrateOptions{
		ManifestKey: manifestKey,
		CacheDir:    cacheDir,
		StartHeight: 1,
		EndHeight:   2,
		MaxBytes:    maxBytes,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(keepPath); err != nil {
		t.Fatalf("unmanaged file was removed: %v", err)
	}
	if _, err := os.Stat(filepath.Join(blockDir, "000000000001.block")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("old hydrated block stat err=%v, want not exist", err)
	}
	if _, err := os.Stat(filepath.Join(blockDir, "000000000002.block")); err != nil {
		t.Fatalf("latest hydrated block missing: %v", err)
	}
}

func TestHydrateRejectsManifestWithUnsafeChainID(t *testing.T) {
	ctx := context.Background()
	store, err := NewLocalObjectStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	manifestKey := ManifestKey("archive", testChainID, "unsafe-chain.json")
	badManifest := `{
  "version": 1,
  "chain_id": "../escape",
  "first_height": 0,
  "last_height": 0,
  "created_at": "0001-01-01T00:00:00Z",
  "updated_at": "0001-01-01T00:00:00Z",
  "segments": []
}`
	if putErr := store.Put(ctx, manifestKey, []byte(badManifest)); putErr != nil {
		t.Fatal(putErr)
	}
	cacheDir := filepath.Join(t.TempDir(), "cache")
	_, err = Hydrate(ctx, store, HydrateOptions{
		ManifestKey: manifestKey,
		CacheDir:    cacheDir,
	})
	if err == nil || !strings.Contains(err.Error(), "unsafe chain ID") {
		t.Fatalf("hydrate err=%v, want unsafe chain ID", err)
	}
	if _, statErr := os.Stat(filepath.Join(cacheDir, "chains")); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("cache chains stat err=%v, want not exist", statErr)
	}
}

func TestArchiveCommandsHonorCanceledContext(t *testing.T) {
	ctx := context.Background()
	dbDir := filepath.Join(t.TempDir(), "db")
	createBlockStoreFixture(t, dbDir, 3)
	reader, err := OpenCometBlockStore(dbDir, "goleveldb")
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	store, err := NewLocalObjectStore(filepath.Join(t.TempDir(), "objects"))
	if err != nil {
		t.Fatal(err)
	}
	result, err := Migrate(ctx, reader, store, MigrationOptions{
		ChainID:       testChainID,
		StartHeight:   1,
		EndHeight:     3,
		SegmentBlocks: 1,
		Prefix:        "archive",
		Compression:   CompressionGzip,
	})
	if err != nil {
		t.Fatal(err)
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := Migrate(canceled, reader, store, MigrationOptions{
		ChainID:       testChainID,
		StartHeight:   1,
		EndHeight:     3,
		SegmentBlocks: 1,
		Prefix:        "canceled",
		Compression:   CompressionGzip,
	}); !errors.Is(err, context.Canceled) {
		t.Fatalf("migrate err=%v, want context.Canceled", err)
	}
	if _, err := Verify(canceled, store, VerifyOptions{ManifestKey: result.ManifestKey}); !errors.Is(err, context.Canceled) {
		t.Fatalf("verify err=%v, want context.Canceled", err)
	}
	if _, err := Hydrate(canceled, store, HydrateOptions{
		ManifestKey: result.ManifestKey,
		CacheDir:    filepath.Join(t.TempDir(), "cache"),
	}); !errors.Is(err, context.Canceled) {
		t.Fatalf("hydrate err=%v, want context.Canceled", err)
	}
}

func TestMigrationResumeReusesCompletedSegments(t *testing.T) {
	ctx := context.Background()
	dbDir := filepath.Join(t.TempDir(), "db")
	createBlockStoreFixture(t, dbDir, 3)
	reader, err := OpenCometBlockStore(dbDir, "goleveldb")
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	store, err := NewLocalObjectStore(filepath.Join(t.TempDir(), "objects"))
	if err != nil {
		t.Fatal(err)
	}
	opts := MigrationOptions{
		ChainID:       testChainID,
		StartHeight:   1,
		EndHeight:     3,
		SegmentBlocks: 1,
		Prefix:        "archive",
		Compression:   CompressionGzip,
	}
	first, err := Migrate(ctx, reader, store, opts)
	if err != nil {
		t.Fatal(err)
	}
	if first.Uploaded != 3 {
		t.Fatalf("first migration uploaded %d segments, want 3", first.Uploaded)
	}
	state, err := loadMigrationState(ctx, store, StateKey(opts.Prefix, opts.ChainID, opts.ManifestName))
	if err != nil {
		t.Fatal(err)
	}
	state.NextHeight = 2
	state.Segments = state.Segments[:1]
	if saveErr := saveMigrationState(ctx, store, StateKey(opts.Prefix, opts.ChainID, opts.ManifestName), state); saveErr != nil {
		t.Fatal(saveErr)
	}
	second, err := Migrate(ctx, reader, store, opts)
	if err != nil {
		t.Fatal(err)
	}
	if second.Uploaded != 0 || second.Reused != 2 || second.Segments != 3 {
		t.Fatalf("unexpected resumed migration result: %+v", second)
	}
}

func TestMigrationResumesLegacyDefaultManifestState(t *testing.T) {
	ctx := context.Background()
	dbDir := filepath.Join(t.TempDir(), "db")
	createBlockStoreFixture(t, dbDir, 3)
	reader, err := OpenCometBlockStore(dbDir, "goleveldb")
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	objectRoot := filepath.Join(t.TempDir(), "objects")
	store, err := NewLocalObjectStore(objectRoot)
	if err != nil {
		t.Fatal(err)
	}
	opts := MigrationOptions{
		ChainID:       testChainID,
		StartHeight:   1,
		EndHeight:     3,
		SegmentBlocks: 1,
		Prefix:        "archive",
		Compression:   CompressionGzip,
	}
	first, err := Migrate(ctx, reader, store, opts)
	if err != nil {
		t.Fatal(err)
	}
	if first.Uploaded != 3 {
		t.Fatalf("first migration uploaded %d segments, want 3", first.Uploaded)
	}
	stateKey := StateKey(opts.Prefix, opts.ChainID, opts.ManifestName)
	state, err := loadMigrationState(ctx, store, stateKey)
	if err != nil {
		t.Fatal(err)
	}
	state.NextHeight = 2
	state.Segments = state.Segments[:1]
	legacyStateKey := legacyMigrationStateKey(opts.Prefix, opts.ChainID)
	if saveErr := saveMigrationState(ctx, store, legacyStateKey, state); saveErr != nil {
		t.Fatal(saveErr)
	}
	if removeErr := os.Remove(filepath.Join(objectRoot, filepath.FromSlash(stateKey))); removeErr != nil {
		t.Fatal(removeErr)
	}

	second, err := Migrate(ctx, reader, store, opts)
	if err != nil {
		t.Fatal(err)
	}
	if second.Uploaded != 0 || second.Reused != 2 || second.Segments != 3 {
		t.Fatalf("unexpected resumed migration result: %+v", second)
	}
	if _, statErr := store.Stat(ctx, stateKey); statErr != nil {
		t.Fatalf("new migration state missing after legacy resume: %v", statErr)
	}
}

func TestMigrationStateIsScopedByManifestName(t *testing.T) {
	ctx := context.Background()
	dbDir := filepath.Join(t.TempDir(), "db")
	createBlockStoreFixture(t, dbDir, 4)
	reader, err := OpenCometBlockStore(dbDir, "goleveldb")
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	store, err := NewLocalObjectStore(filepath.Join(t.TempDir(), "objects"))
	if err != nil {
		t.Fatal(err)
	}
	firstOpts := MigrationOptions{
		ChainID:       testChainID,
		StartHeight:   1,
		EndHeight:     2,
		SegmentBlocks: 1,
		Prefix:        "archive",
		ManifestName:  "first.json",
		Compression:   CompressionGzip,
	}
	if _, migrateErr := Migrate(ctx, reader, store, firstOpts); migrateErr != nil {
		t.Fatal(migrateErr)
	}
	secondOpts := firstOpts
	secondOpts.StartHeight = 3
	secondOpts.EndHeight = 4
	secondOpts.ManifestName = "second.json"
	second, err := Migrate(ctx, reader, store, secondOpts)
	if err != nil {
		t.Fatal(err)
	}
	if second.Manifest.FirstHeight != 3 || second.Manifest.LastHeight != 4 {
		t.Fatalf("second manifest range = %d-%d, want 3-4", second.Manifest.FirstHeight, second.Manifest.LastHeight)
	}
	if _, statErr := store.Stat(ctx, StateKey(firstOpts.Prefix, firstOpts.ChainID, firstOpts.ManifestName)); statErr != nil {
		t.Fatalf("first migration state missing: %v", statErr)
	}
	if _, statErr := store.Stat(ctx, StateKey(secondOpts.Prefix, secondOpts.ChainID, secondOpts.ManifestName)); statErr != nil {
		t.Fatalf("second migration state missing: %v", statErr)
	}
}

func TestMigrationRejectsInvalidResumeState(t *testing.T) {
	ctx := context.Background()
	dbDir := filepath.Join(t.TempDir(), "db")
	createBlockStoreFixture(t, dbDir, 3)
	reader, err := OpenCometBlockStore(dbDir, "goleveldb")
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()

	for _, tc := range []struct {
		name  string
		state func([]SegmentManifest) MigrationState
		want  string
	}{
		{
			name: "next-height-without-segments",
			state: func([]SegmentManifest) MigrationState {
				return MigrationState{
					ChainID:     testChainID,
					StartHeight: 1,
					EndHeight:   3,
					NextHeight:  2,
				}
			},
			want: "requires completed segments",
		},
		{
			name: "segment-gap",
			state: func(segments []SegmentManifest) MigrationState {
				return MigrationState{
					ChainID:     testChainID,
					StartHeight: 1,
					EndHeight:   3,
					NextHeight:  4,
					Segments:    []SegmentManifest{segments[0], segments[2]},
				}
			},
			want: "segment range gap",
		},
		{
			name: "unsafe-segment-key",
			state: func(segments []SegmentManifest) MigrationState {
				corrupt := segments[0]
				corrupt.Key = "../escape"
				return MigrationState{
					ChainID:     testChainID,
					StartHeight: 1,
					EndHeight:   3,
					NextHeight:  2,
					Segments:    []SegmentManifest{corrupt},
				}
			},
			want: "invalid object key",
		},
		{
			name: "duplicate-segment-key",
			state: func(segments []SegmentManifest) MigrationState {
				duplicate := segments[1]
				duplicate.Key = segments[0].Key
				return MigrationState{
					ChainID:     testChainID,
					StartHeight: 1,
					EndHeight:   3,
					NextHeight:  3,
					Segments:    []SegmentManifest{segments[0], duplicate},
				}
			},
			want: "duplicate segment key",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store, err := NewLocalObjectStore(filepath.Join(t.TempDir(), "objects"))
			if err != nil {
				t.Fatal(err)
			}
			opts := MigrationOptions{
				ChainID:       testChainID,
				StartHeight:   1,
				EndHeight:     3,
				SegmentBlocks: 1,
				Prefix:        "archive",
				Compression:   CompressionGzip,
			}
			first, err := Migrate(ctx, reader, store, opts)
			if err != nil {
				t.Fatal(err)
			}
			if saveErr := saveMigrationStateUnchecked(ctx, store, StateKey(opts.Prefix, opts.ChainID, opts.ManifestName), tc.state(first.Manifest.Segments)); saveErr != nil {
				t.Fatal(saveErr)
			}
			_, err = Migrate(ctx, reader, store, opts)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("migrate err=%v, want %q", err, tc.want)
			}
		})
	}
}

func TestSaveMigrationStateValidatesInputs(t *testing.T) {
	ctx := context.Background()
	store, err := NewLocalObjectStore(filepath.Join(t.TempDir(), "objects"))
	if err != nil {
		t.Fatal(err)
	}
	valid := MigrationState{
		ChainID:     testChainID,
		StartHeight: 1,
		EndHeight:   1,
		NextHeight:  1,
	}
	if err := saveMigrationState(ctx, store, "../escape", valid); err == nil || !strings.Contains(err.Error(), "migration state key") {
		t.Fatalf("saveMigrationState unsafe key err=%v", err)
	}
	if err := saveMigrationState(ctx, nil, StateKey("archive", testChainID, DefaultManifest), valid); err == nil || !strings.Contains(err.Error(), "object store is required") {
		t.Fatalf("saveMigrationState nil store err=%v", err)
	}
	invalid := valid
	invalid.NextHeight = 2
	if err := saveMigrationState(ctx, store, StateKey("archive", testChainID, DefaultManifest), invalid); err == nil || !strings.Contains(err.Error(), "requires completed segments") {
		t.Fatalf("saveMigrationState invalid state err=%v", err)
	}
}

func TestMigrationResumeRejectsUndecodableCompletedSegment(t *testing.T) {
	ctx := context.Background()
	dbDir := filepath.Join(t.TempDir(), "db")
	createBlockStoreFixture(t, dbDir, 1)
	reader, err := OpenCometBlockStore(dbDir, "goleveldb")
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	store, err := NewLocalObjectStore(filepath.Join(t.TempDir(), "objects"))
	if err != nil {
		t.Fatal(err)
	}
	badRecord := BlockRecord{
		Height: 1,
		Hash:   strings.Repeat("a", blockHashHexLen),
		Bytes:  []byte("not a comet block"),
	}
	data, segment, err := EncodeSegment([]BlockRecord{badRecord}, CompressionGzip)
	if err != nil {
		t.Fatal(err)
	}
	segment.Key = SegmentKey("archive", testChainID, segment)
	if putErr := store.Put(ctx, segment.Key, data); putErr != nil {
		t.Fatal(putErr)
	}
	state := MigrationState{
		ChainID:     testChainID,
		StartHeight: 1,
		EndHeight:   1,
		NextHeight:  2,
		Segments:    []SegmentManifest{segment},
	}
	opts := MigrationOptions{
		ChainID:       testChainID,
		StartHeight:   1,
		EndHeight:     1,
		SegmentBlocks: 1,
		Prefix:        "archive",
		Compression:   CompressionGzip,
	}
	if saveErr := saveMigrationState(ctx, store, StateKey(opts.Prefix, opts.ChainID, opts.ManifestName), state); saveErr != nil {
		t.Fatal(saveErr)
	}
	_, err = Migrate(ctx, reader, store, opts)
	if err == nil || !strings.Contains(err.Error(), "decode completed segment") {
		t.Fatalf("Migrate err=%v, want completed segment decode error", err)
	}
}

func saveMigrationStateUnchecked(ctx context.Context, store ObjectStore, key string, state MigrationState) error {
	data, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		return err
	}
	data = append(bytes.TrimSpace(data), '\n')
	return store.Put(ctx, key, data)
}

func TestMigrationRejectsTooManySegmentBlocks(t *testing.T) {
	_, err := Migrate(context.Background(), nil, nil, MigrationOptions{
		ChainID:       testChainID,
		SegmentBlocks: MaxSegmentBlocks + 1,
	})
	if err == nil || !strings.Contains(err.Error(), "exceeds maximum") {
		t.Fatalf("Migrate err=%v, want segment block maximum error", err)
	}
}

func TestMigrationRejectsExistingManifestRangeMismatchWithoutState(t *testing.T) {
	ctx := context.Background()
	dbDir := filepath.Join(t.TempDir(), "db")
	createBlockStoreFixture(t, dbDir, 5)
	reader, err := OpenCometBlockStore(dbDir, "goleveldb")
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	objectRoot := filepath.Join(t.TempDir(), "objects")
	store, err := NewLocalObjectStore(objectRoot)
	if err != nil {
		t.Fatal(err)
	}
	opts := MigrationOptions{
		ChainID:       testChainID,
		StartHeight:   1,
		EndHeight:     3,
		SegmentBlocks: 1,
		Prefix:        "archive",
		Compression:   CompressionGzip,
	}
	if _, migrateErr := Migrate(ctx, reader, store, opts); migrateErr != nil {
		t.Fatal(migrateErr)
	}
	statePath := filepath.Join(objectRoot, filepath.FromSlash(StateKey(opts.Prefix, opts.ChainID, opts.ManifestName)))
	if removeErr := os.Remove(statePath); removeErr != nil {
		t.Fatal(removeErr)
	}
	opts.EndHeight = 5
	_, err = Migrate(ctx, reader, store, opts)
	if err == nil || !strings.Contains(err.Error(), "existing manifest") || !strings.Contains(err.Error(), "requested 1-5") {
		t.Fatalf("migrate err=%v, want existing manifest range mismatch", err)
	}
}

func TestMigrationRerunsExistingManifestRangeWithoutState(t *testing.T) {
	ctx := context.Background()
	dbDir := filepath.Join(t.TempDir(), "db")
	createBlockStoreFixture(t, dbDir, 3)
	reader, err := OpenCometBlockStore(dbDir, "goleveldb")
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	objectRoot := filepath.Join(t.TempDir(), "objects")
	store, err := NewLocalObjectStore(objectRoot)
	if err != nil {
		t.Fatal(err)
	}
	opts := MigrationOptions{
		ChainID:       testChainID,
		StartHeight:   1,
		EndHeight:     3,
		SegmentBlocks: 1,
		Prefix:        "archive",
		Compression:   CompressionGzip,
	}
	if _, migrateErr := Migrate(ctx, reader, store, opts); migrateErr != nil {
		t.Fatal(migrateErr)
	}
	statePath := filepath.Join(objectRoot, filepath.FromSlash(StateKey(opts.Prefix, opts.ChainID, opts.ManifestName)))
	if removeErr := os.Remove(statePath); removeErr != nil {
		t.Fatal(removeErr)
	}
	result, err := Migrate(ctx, reader, store, opts)
	if err != nil {
		t.Fatal(err)
	}
	if result.Uploaded != 0 || result.Reused != 3 || result.Manifest.FirstHeight != 1 || result.Manifest.LastHeight != 3 {
		t.Fatalf("unexpected rerun result: %+v", result)
	}
}

func TestMigrationRerunDetectsMissingCompletedSegment(t *testing.T) {
	ctx := context.Background()
	dbDir := filepath.Join(t.TempDir(), "db")
	createBlockStoreFixture(t, dbDir, 2)
	reader, err := OpenCometBlockStore(dbDir, "goleveldb")
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	store, err := NewLocalObjectStore(filepath.Join(t.TempDir(), "objects"))
	if err != nil {
		t.Fatal(err)
	}
	opts := MigrationOptions{
		ChainID:       testChainID,
		StartHeight:   1,
		EndHeight:     2,
		SegmentBlocks: 1,
		Prefix:        "archive",
		Compression:   CompressionGzip,
	}
	first, err := Migrate(ctx, reader, store, opts)
	if err != nil {
		t.Fatal(err)
	}
	if first.Uploaded != 2 {
		t.Fatalf("first migration uploaded %d segments, want 2", first.Uploaded)
	}
	missing := first.Manifest.Segments[0].Key
	if removeErr := os.Remove(filepath.Join(store.root, filepath.FromSlash(missing))); removeErr != nil {
		t.Fatal(removeErr)
	}
	if _, err := Migrate(ctx, reader, store, opts); err == nil {
		t.Fatal("expected rerun to detect missing completed segment")
	}
}

func TestVerifyDetectsMissingAndCorruptObjects(t *testing.T) {
	ctx := context.Background()
	store, err := NewLocalObjectStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	data, segment, err := EncodeSegment([]BlockRecord{makeTestRecord(t, 1)}, CompressionGzip)
	if err != nil {
		t.Fatal(err)
	}
	segment.Key = SegmentKey("archive", testChainID, segment)
	manifest, err := NewManifest(testChainID, []SegmentManifest{segment})
	if err != nil {
		t.Fatal(err)
	}
	manifestKey := ManifestKey("archive", testChainID, DefaultManifest)
	if err := SaveManifest(ctx, store, manifestKey, manifest); err != nil {
		t.Fatal(err)
	}
	if _, err := Verify(ctx, store, VerifyOptions{ManifestKey: manifestKey}); err == nil {
		t.Fatal("expected missing object error")
	}
	if putErr := store.Put(ctx, segment.Key, data); putErr != nil {
		t.Fatal(putErr)
	}
	corrupt := append([]byte(nil), data...)
	corrupt[len(corrupt)-1] ^= 0xff
	if err := store.Put(ctx, segment.Key, corrupt); err != nil {
		t.Fatal(err)
	}
	if _, err := Verify(ctx, store, VerifyOptions{ManifestKey: manifestKey}); err == nil {
		t.Fatal("expected corrupt object error")
	}
}

func TestVerifyDetectsManifestInconsistency(t *testing.T) {
	ctx := context.Background()
	store, err := NewLocalObjectStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	data, segment, err := EncodeSegment([]BlockRecord{makeTestRecord(t, 1)}, CompressionGzip)
	if err != nil {
		t.Fatal(err)
	}
	segment.Key = SegmentKey("archive", testChainID, segment)
	if putErr := store.Put(ctx, segment.Key, data); putErr != nil {
		t.Fatal(putErr)
	}
	manifestKey := ManifestKey("archive", testChainID, DefaultManifest)
	badManifest := `{
  "version": 1,
  "chain_id": "` + testChainID + `",
  "first_height": 1,
  "last_height": 2,
  "segments": [
    {
      "key": "` + segment.Key + `",
      "first_height": 1,
      "last_height": 1,
      "compression": "` + segment.Compression + `",
      "size_bytes": ` + fmt.Sprint(segment.SizeBytes) + `,
      "sha256": "` + segment.SHA256 + `",
      "blocks": [
        {"height": 1, "hash": "` + segment.Blocks[0].Hash + `"}
      ]
    }
  ]
}`
	if putErr := store.Put(ctx, manifestKey, []byte(badManifest)); putErr != nil {
		t.Fatal(putErr)
	}
	_, err = Verify(ctx, store, VerifyOptions{ManifestKey: manifestKey})
	if err == nil || !strings.Contains(err.Error(), "manifest last height") {
		t.Fatalf("verify err=%v, want manifest inconsistency", err)
	}
}

func TestVerifySampleEveryDecodesOnlySampledBlocks(t *testing.T) {
	ctx := context.Background()
	store, err := NewLocalObjectStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	records := []BlockRecord{
		makeTestRecord(t, 1),
		{Height: 2, Hash: strings.Repeat("a", blockHashHexLen), Bytes: []byte("not a comet block")},
		makeTestRecord(t, 3),
		makeTestRecord(t, 4),
		makeTestRecord(t, 5),
	}
	data, segment, err := EncodeSegment(records, CompressionGzip)
	if err != nil {
		t.Fatal(err)
	}
	segment.Key = SegmentKey("archive", testChainID, segment)
	if putErr := store.Put(ctx, segment.Key, data); putErr != nil {
		t.Fatal(putErr)
	}
	manifest, err := NewManifest(testChainID, []SegmentManifest{segment})
	if err != nil {
		t.Fatal(err)
	}
	manifestKey := ManifestKey("archive", testChainID, DefaultManifest)
	if saveErr := SaveManifest(ctx, store, manifestKey, manifest); saveErr != nil {
		t.Fatal(saveErr)
	}
	if _, verifyErr := Verify(ctx, store, VerifyOptions{ManifestKey: manifestKey}); verifyErr == nil {
		t.Fatal("expected full verify to decode the invalid block")
	}
	result, err := Verify(ctx, store, VerifyOptions{ManifestKey: manifestKey, SampleEvery: 2})
	if err != nil {
		t.Fatal(err)
	}
	if result.SegmentsChecked != 1 || result.BlocksChecked != 3 {
		t.Fatalf("unexpected sampled verify result: %+v", result)
	}
}
