package archive

import (
	"bytes"
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"
)

func TestArchiveReadyArchivesIncrementalReadyRanges(t *testing.T) {
	ctx := context.Background()
	dbDir := filepath.Join(t.TempDir(), "db")
	createBlockStoreFixture(t, dbDir, 5)
	reader, err := OpenCometBlockStore(dbDir, "goleveldb")
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	store, err := NewLocalObjectStore(filepath.Join(t.TempDir(), "objects"))
	if err != nil {
		t.Fatal(err)
	}
	opts := LiveArchiveOptions{
		ChainID:       testChainID,
		Prefix:        "archive",
		ReadyHeight:   3,
		SegmentBlocks: 2,
		Compression:   CompressionGzip,
	}
	first, err := ArchiveReady(ctx, reader, store, opts)
	if err != nil {
		t.Fatal(err)
	}
	if first.Uploaded != 2 || first.BlocksArchived != 3 || first.FirstArchived != 1 || first.LastArchived != 3 {
		t.Fatalf("unexpected first live archive result: %+v", first)
	}
	if first.Manifest.FirstHeight != 1 || first.Manifest.LastHeight != 3 || len(first.Manifest.Segments) != 2 {
		t.Fatalf("unexpected first manifest: %+v", first.Manifest)
	}
	if _, verifyErr := Verify(ctx, store, VerifyOptions{ManifestKey: first.ManifestKey}); verifyErr != nil {
		t.Fatal(verifyErr)
	}
	opts.ReadyHeight = 5
	second, err := ArchiveReady(ctx, reader, store, opts)
	if err != nil {
		t.Fatal(err)
	}
	if second.Uploaded != 1 || second.BlocksArchived != 2 || second.FirstArchived != 4 || second.LastArchived != 5 {
		t.Fatalf("unexpected second live archive result: %+v", second)
	}
	if second.Manifest.FirstHeight != 1 || second.Manifest.LastHeight != 5 || len(second.Manifest.Segments) != 3 {
		t.Fatalf("unexpected second manifest: %+v", second.Manifest)
	}
	verifyResult, err := Verify(ctx, store, VerifyOptions{ManifestKey: second.ManifestKey})
	if err != nil {
		t.Fatal(err)
	}
	if verifyResult.BlocksChecked != 5 {
		t.Fatalf("verified %d blocks, want 5", verifyResult.BlocksChecked)
	}
}

func TestArchiveReadyHonorsManifestKeyOverride(t *testing.T) {
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
	manifestKey := "custom/live/archive.json"
	result, err := ArchiveReady(ctx, reader, store, LiveArchiveOptions{
		ChainID:       testChainID,
		Prefix:        "archive",
		ManifestKey:   manifestKey,
		ReadyHeight:   2,
		SegmentBlocks: 1,
		Compression:   CompressionGzip,
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.ManifestKey != manifestKey {
		t.Fatalf("manifest key = %q, want %q", result.ManifestKey, manifestKey)
	}
	if _, err := Verify(ctx, store, VerifyOptions{ManifestKey: manifestKey}); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadManifest(ctx, store, ManifestKey("archive", testChainID, DefaultManifest)); !errors.Is(err, ErrObjectNotFound) {
		t.Fatalf("default manifest err=%v, want ErrObjectNotFound", err)
	}
}

func TestArchiveReadyIsIdempotentAfterManifestProgress(t *testing.T) {
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
	opts := LiveArchiveOptions{
		ChainID:       testChainID,
		Prefix:        "archive",
		ReadyHeight:   3,
		SegmentBlocks: 1,
		Compression:   CompressionGzip,
	}
	first, err := ArchiveReady(ctx, reader, store, opts)
	if err != nil {
		t.Fatal(err)
	}
	if first.Uploaded != 3 {
		t.Fatalf("uploaded %d segments, want 3", first.Uploaded)
	}
	second, err := ArchiveReady(ctx, reader, store, opts)
	if err != nil {
		t.Fatal(err)
	}
	if second.Uploaded != 0 || second.Reused != 0 || second.BlocksArchived != 0 {
		t.Fatalf("unexpected no-op archive result: %+v", second)
	}
	if second.Manifest.LastHeight != 3 {
		t.Fatalf("manifest regressed: %+v", second.Manifest)
	}
}

func TestArchiveReadyHonorsCanceledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
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
	if _, err := ArchiveReady(ctx, reader, store, LiveArchiveOptions{
		ChainID:       testChainID,
		Prefix:        "archive",
		ReadyHeight:   3,
		SegmentBlocks: 1,
		Compression:   CompressionGzip,
	}); !errors.Is(err, context.Canceled) {
		t.Fatalf("archive-ready err=%v, want context.Canceled", err)
	}
}

func TestArchiveReadyRejectsTooManySegmentBlocks(t *testing.T) {
	_, err := ArchiveReady(context.Background(), nil, nil, LiveArchiveOptions{
		ChainID:       testChainID,
		ReadyHeight:   1,
		SegmentBlocks: MaxSegmentBlocks + 1,
	})
	if err == nil || !strings.Contains(err.Error(), "exceeds maximum") {
		t.Fatalf("ArchiveReady err=%v, want segment block maximum error", err)
	}
}

func TestArchiveReadyReusesExistingObjectAfterManifestLoss(t *testing.T) {
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
	opts := LiveArchiveOptions{
		ChainID:       testChainID,
		Prefix:        "archive",
		ManifestName:  "lost-manifest.json",
		ReadyHeight:   2,
		SegmentBlocks: 2,
		Compression:   CompressionGzip,
	}
	first, err := ArchiveReady(ctx, reader, store, opts)
	if err != nil {
		t.Fatal(err)
	}
	if first.Uploaded != 1 {
		t.Fatalf("uploaded %d segments, want 1", first.Uploaded)
	}
	// Rebuild the same archive under a new manifest key. The immutable segment
	// object should be reused instead of rewritten.
	opts.ManifestName = "rebuilt-manifest.json"
	second, err := ArchiveReady(ctx, reader, store, opts)
	if err != nil {
		t.Fatal(err)
	}
	if second.Uploaded != 0 || second.Reused != 1 || second.BlocksArchived != 2 {
		t.Fatalf("unexpected rebuilt archive result: %+v", second)
	}
}

func TestArchiveReadyFromHeadAppliesSafetyWindow(t *testing.T) {
	ctx := context.Background()
	dbDir := filepath.Join(t.TempDir(), "db")
	createBlockStoreFixture(t, dbDir, 5)
	reader, err := OpenCometBlockStore(dbDir, "goleveldb")
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	store, err := NewLocalObjectStore(filepath.Join(t.TempDir(), "objects"))
	if err != nil {
		t.Fatal(err)
	}
	result, err := ArchiveReadyFromHead(ctx, reader, store, LiveArchiveOptions{
		ChainID:       testChainID,
		Prefix:        "archive",
		SegmentBlocks: 2,
		Compression:   CompressionGzip,
	}, 2)
	if err != nil {
		t.Fatal(err)
	}
	if result.BlocksArchived != 3 || result.LastArchived != 3 {
		t.Fatalf("unexpected safety-window archive result: %+v", result)
	}
}

func TestArchiveReadyFromHeadNoopsBeforeSafetyWindow(t *testing.T) {
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
	result, err := ArchiveReadyFromHead(ctx, reader, store, LiveArchiveOptions{
		ChainID: testChainID,
		Prefix:  "archive",
	}, 10)
	if err != nil {
		t.Fatal(err)
	}
	if result.BlocksArchived != 0 || result.Manifest.LastHeight != 0 {
		t.Fatalf("unexpected no-op result: %+v", result)
	}
}

func TestArchiveReadyFromHeadPreflightsConfigBeforeNoop(t *testing.T) {
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

	_, err = ArchiveReadyFromHead(ctx, reader, store, LiveArchiveOptions{
		ChainID:     testChainID,
		Prefix:      "archive",
		Compression: "zstd",
	}, 10)
	if err == nil {
		t.Fatal("expected unsupported compression error")
	}

	_, err = ArchiveReadyFromHead(ctx, reader, store, LiveArchiveOptions{
		ChainID: "../../escape",
		Prefix:  "archive",
	}, 10)
	if err == nil {
		t.Fatal("expected unsafe chain ID error")
	}
}

func TestArchiveReadyDoesNotPublishUnverifiedUpload(t *testing.T) {
	ctx := context.Background()
	dbDir := filepath.Join(t.TempDir(), "db")
	createBlockStoreFixture(t, dbDir, 1)
	reader, err := OpenCometBlockStore(dbDir, "goleveldb")
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	localStore, err := NewLocalObjectStore(filepath.Join(t.TempDir(), "objects"))
	if err != nil {
		t.Fatal(err)
	}
	store := corruptingSegmentStore{ObjectStore: localStore}
	opts := LiveArchiveOptions{
		ChainID:       testChainID,
		Prefix:        "archive",
		ReadyHeight:   1,
		SegmentBlocks: 1,
		Compression:   CompressionGzip,
	}
	_, err = ArchiveReady(ctx, reader, store, opts)
	if err == nil || !strings.Contains(err.Error(), "differs from expected segment") {
		t.Fatalf("ArchiveReady err=%v, want segment verification error", err)
	}
	manifestKey := ManifestKey(opts.Prefix, opts.ChainID, opts.ManifestName)
	if _, err := LoadManifest(ctx, localStore, manifestKey); !errors.Is(err, ErrObjectNotFound) {
		t.Fatalf("manifest err=%v, want ErrObjectNotFound", err)
	}
}

func TestArchiveReadyFallsBackWhenETagIsOpaque(t *testing.T) {
	ctx := context.Background()
	dbDir := filepath.Join(t.TempDir(), "db")
	createBlockStoreFixture(t, dbDir, 1)
	reader, err := OpenCometBlockStore(dbDir, "goleveldb")
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	localStore, err := NewLocalObjectStore(filepath.Join(t.TempDir(), "objects"))
	if err != nil {
		t.Fatal(err)
	}
	store := opaqueETagSegmentStore{ObjectStore: localStore}
	result, err := ArchiveReady(ctx, reader, store, LiveArchiveOptions{
		ChainID:       testChainID,
		Prefix:        "archive",
		ReadyHeight:   1,
		SegmentBlocks: 1,
		Compression:   CompressionGzip,
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Uploaded != 1 {
		t.Fatalf("uploaded %d segments, want 1", result.Uploaded)
	}
	if _, err := Verify(ctx, localStore, VerifyOptions{ManifestKey: result.ManifestKey}); err != nil {
		t.Fatal(err)
	}
}

type corruptingSegmentStore struct {
	ObjectStore
}

func (s corruptingSegmentStore) Put(ctx context.Context, key string, data []byte) error {
	return s.ObjectStore.Put(ctx, key, corruptSegmentObject(key, data))
}

func (s corruptingSegmentStore) PutIfAbsent(ctx context.Context, key string, data []byte) error {
	immutableStore, ok := s.ObjectStore.(ImmutableObjectStore)
	if !ok {
		return s.Put(ctx, key, data)
	}
	return immutableStore.PutIfAbsent(ctx, key, corruptSegmentObject(key, data))
}

func corruptSegmentObject(key string, data []byte) []byte {
	corrupted := bytes.Clone(data)
	if strings.HasSuffix(key, ".cba") && len(corrupted) > 0 {
		corrupted[len(corrupted)-1] ^= 0xff
	}
	return corrupted
}

type opaqueETagSegmentStore struct {
	ObjectStore
}

func (s opaqueETagSegmentStore) PutReturningETag(ctx context.Context, key string, data []byte) (string, error) {
	if err := s.ObjectStore.Put(ctx, key, data); err != nil {
		return "", err
	}
	return "opaque-etag", nil
}

func (s opaqueETagSegmentStore) PutIfAbsent(ctx context.Context, key string, data []byte) error {
	_, err := s.PutIfAbsentReturningETag(ctx, key, data)
	return err
}

func (s opaqueETagSegmentStore) PutIfAbsentReturningETag(ctx context.Context, key string, data []byte) (string, error) {
	immutableStore, ok := s.ObjectStore.(ImmutableObjectStore)
	if !ok {
		exists, err := s.ObjectStore.Exists(ctx, key)
		if err != nil {
			return "", err
		}
		if exists {
			return "", ErrObjectAlreadyExists
		}
		return s.PutReturningETag(ctx, key, data)
	}
	if err := immutableStore.PutIfAbsent(ctx, key, data); err != nil {
		return "", err
	}
	return "opaque-etag", nil
}
