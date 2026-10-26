package archive

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/cometbft/cometbft/store"
)

func TestPruneVerifiedHotStorePrunesOnlyArchivedAndOutsideRetention(t *testing.T) {
	ctx := context.Background()
	dbDir := filepath.Join(t.TempDir(), "db")
	createBlockStoreFixture(t, dbDir, 6)
	reader, err := OpenCometBlockStore(dbDir, "goleveldb")
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	objectStore, err := NewLocalObjectStore(filepath.Join(t.TempDir(), "objects"))
	if err != nil {
		t.Fatal(err)
	}
	archived, err := ArchiveReady(ctx, reader, objectStore, LiveArchiveOptions{
		ChainID:       testChainID,
		Prefix:        "archive",
		ReadyHeight:   5,
		SegmentBlocks: 2,
		Compression:   CompressionGzip,
	})
	if err != nil {
		t.Fatal(err)
	}
	result, err := PruneVerifiedHotStore(ctx, reader, objectStore, PruneHotOptions{
		ManifestKey:            archived.ManifestKey,
		RetainBlocks:           2,
		EvidenceMaxAgeBlocks:   1,
		EvidenceMaxAgeDuration: time.Nanosecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.BaseBefore != 1 || result.PruneToHeight != 5 || result.BaseAfter != 5 || result.Pruned != 4 {
		t.Fatalf("unexpected prune result: %+v", result)
	}
	if reader.LoadBlock(4) != nil {
		t.Fatal("expected block 4 to be pruned")
	}
	if reader.LoadBlock(5) == nil || reader.LoadBlock(6) == nil {
		t.Fatal("expected retained hot blocks 5 and 6")
	}
}

func TestPruneVerifiedHotStoreDoesNotPruneUnverifiedArchive(t *testing.T) {
	ctx := context.Background()
	dbDir := filepath.Join(t.TempDir(), "db")
	createBlockStoreFixture(t, dbDir, 4)
	reader, err := OpenCometBlockStore(dbDir, "goleveldb")
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	objectStore, err := NewLocalObjectStore(filepath.Join(t.TempDir(), "objects"))
	if err != nil {
		t.Fatal(err)
	}
	archived, err := ArchiveReady(ctx, reader, objectStore, LiveArchiveOptions{
		ChainID:       testChainID,
		Prefix:        "archive",
		ReadyHeight:   3,
		SegmentBlocks: 3,
		Compression:   CompressionGzip,
	})
	if err != nil {
		t.Fatal(err)
	}
	segmentKey := archived.Manifest.Segments[0].Key
	data, err := objectStore.Get(ctx, segmentKey)
	if err != nil {
		t.Fatal(err)
	}
	data[len(data)-1] ^= 0xff
	if err := objectStore.Put(ctx, segmentKey, data); err != nil {
		t.Fatal(err)
	}
	if _, err := PruneVerifiedHotStore(ctx, reader, objectStore, PruneHotOptions{
		ManifestKey:  archived.ManifestKey,
		RetainBlocks: 1,
	}); err == nil {
		t.Fatal("expected corrupt archive to block pruning")
	}
	if reader.Base() != 1 {
		t.Fatalf("base changed after failed prune: %d", reader.Base())
	}
}

func TestPruneVerifiedHotStoreNoopsWhenRetentionProtectsRange(t *testing.T) {
	ctx := context.Background()
	dbDir := filepath.Join(t.TempDir(), "db")
	createBlockStoreFixture(t, dbDir, 4)
	reader, err := OpenCometBlockStore(dbDir, "goleveldb")
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	objectStore, err := NewLocalObjectStore(filepath.Join(t.TempDir(), "objects"))
	if err != nil {
		t.Fatal(err)
	}
	archived, err := ArchiveReady(ctx, reader, objectStore, LiveArchiveOptions{
		ChainID:       testChainID,
		Prefix:        "archive",
		ReadyHeight:   4,
		SegmentBlocks: 2,
		Compression:   CompressionGzip,
	})
	if err != nil {
		t.Fatal(err)
	}
	result, err := PruneVerifiedHotStore(ctx, reader, objectStore, PruneHotOptions{
		ManifestKey:  archived.ManifestKey,
		RetainBlocks: 10,
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Pruned != 0 || result.BaseAfter != 1 {
		t.Fatalf("unexpected retention-protected prune result: %+v", result)
	}
}

func pruneFixture(t *testing.T) (*store.BlockStore, *LocalObjectStore, LiveArchiveResult, PruneHotOptions) {
	t.Helper()
	dbDir := filepath.Join(t.TempDir(), "db")
	createBlockStoreFixture(t, dbDir, 6)
	hot, err := OpenCometBlockStore(dbDir, "goleveldb")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = hot.Close() })
	objects, err := NewLocalObjectStore(filepath.Join(t.TempDir(), "objects"))
	if err != nil {
		t.Fatal(err)
	}
	archived, err := ArchiveReady(t.Context(), hot, objects, LiveArchiveOptions{
		ChainID: testChainID, Prefix: "archive", ReadyHeight: 3,
		SegmentBlocks: 2, Compression: CompressionGzip,
	})
	if err != nil {
		t.Fatal(err)
	}
	return hot, objects, archived, PruneHotOptions{
		ManifestKey: archived.ManifestKey, RetainBlocks: 1,
		EvidenceMaxAgeBlocks: 1, EvidenceMaxAgeDuration: time.Nanosecond,
	}
}

func TestPruneRefusesForeignArchive(t *testing.T) {
	for _, mismatch := range []string{"chain ID", "block hash"} {
		t.Run(mismatch, func(t *testing.T) {
			hot, objects, archived, opts := pruneFixture(t)
			manifest := archived.Manifest
			if mismatch == "chain ID" {
				manifest.ChainID = "foreign-chain"
			} else {
				segment := manifest.Segments[len(manifest.Segments)-1]
				data, err := objects.Get(t.Context(), segment.Key)
				if err != nil {
					t.Fatal(err)
				}
				records, err := DecodeSegment(data, segment)
				if err != nil {
					t.Fatal(err)
				}
				block := makeTestBlock(t, segment.LastHeight)
				block.AppHash = []byte("another fork")
				records[len(records)-1], err = BlockToRecord(block)
				if err != nil {
					t.Fatal(err)
				}
				data, segment, err = EncodeSegment(records, CompressionGzip)
				if err != nil {
					t.Fatal(err)
				}
				segment.Key = SegmentKey("archive", testChainID, segment)
				if err := objects.Put(t.Context(), segment.Key, data); err != nil {
					t.Fatal(err)
				}
				manifest.Segments[len(manifest.Segments)-1] = segment
			}
			if err := SaveManifest(t.Context(), objects, opts.ManifestKey, manifest); err != nil {
				t.Fatal(err)
			}
			_, err := PruneVerifiedHotStore(t.Context(), hot, objects, opts)
			if err == nil || !strings.Contains(err.Error(), mismatch) {
				t.Fatalf("expected %s mismatch, got %v", mismatch, err)
			}
			if hot.Base() != 1 {
				t.Fatalf("base changed after refused prune: %d", hot.Base())
			}
		})
	}
}

type growingManifestStore struct {
	ObjectStore
	manifestKey  string
	manifestGets int
	grow         func() error
}

func (s *growingManifestStore) Get(ctx context.Context, key string) ([]byte, error) {
	data, err := s.ObjectStore.Get(ctx, key)
	if err == nil && key == s.manifestKey {
		s.manifestGets++
		if s.manifestGets == 1 {
			if growErr := s.grow(); growErr != nil {
				return nil, growErr
			}
		}
	}
	return data, err
}

func TestPruneUsesVerifiedManifestSnapshot(t *testing.T) {
	hot, objects, archived, opts := pruneFixture(t)
	growing := &growingManifestStore{ObjectStore: objects, manifestKey: opts.ManifestKey}
	growing.grow = func() error {
		_, err := ArchiveReady(t.Context(), hot, objects, LiveArchiveOptions{
			ChainID: testChainID, Prefix: "archive", ReadyHeight: 5,
			SegmentBlocks: 2, Compression: CompressionGzip,
		})
		return err
	}
	result, err := PruneVerifiedHotStore(t.Context(), hot, growing, opts)
	if err != nil {
		t.Fatal(err)
	}
	if result.ArchivedThrough != archived.Manifest.LastHeight || result.PruneToHeight != 4 || hot.Base() != 4 {
		t.Fatalf("pruned beyond verified snapshot: %+v", result)
	}
	if growing.manifestGets != 1 {
		t.Fatalf("manifest fetched %d times", growing.manifestGets)
	}
}

type countingGetStore struct {
	ObjectStore
	mu   sync.Mutex
	gets map[string]int
}

func (s *countingGetStore) Get(ctx context.Context, key string) ([]byte, error) {
	s.mu.Lock()
	s.gets[key]++
	s.mu.Unlock()
	return s.ObjectStore.Get(ctx, key)
}

func TestIncrementalVerifyFetchesOnlyNewSegments(t *testing.T) {
	hot, objects, archived, opts := pruneFixture(t)
	counting := &countingGetStore{ObjectStore: objects, gets: make(map[string]int)}
	verifier := NewIncrementalVerifier(counting, opts.ManifestKey)
	for cycle := 0; cycle < 2; cycle++ {
		result, err := verifier.Verify(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		if cycle == 1 && result.SegmentsChecked != 0 {
			t.Fatalf("reverified %d segments", result.SegmentsChecked)
		}
	}
	grown, err := ArchiveReady(t.Context(), hot, objects, LiveArchiveOptions{
		ChainID: testChainID, Prefix: "archive", ReadyHeight: 5,
		SegmentBlocks: 2, Compression: CompressionGzip,
	})
	if err != nil {
		t.Fatal(err)
	}
	result, err := verifier.Verify(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if result.SegmentsChecked != len(grown.Manifest.Segments)-len(archived.Manifest.Segments) {
		t.Fatalf("unexpected new segments checked: %+v", result)
	}
	for _, segment := range grown.Manifest.Segments {
		if counting.gets[segment.Key] != 1 {
			t.Fatalf("segment %s fetched %d times", segment.Key, counting.gets[segment.Key])
		}
	}
	if counting.gets[opts.ManifestKey] != 3 {
		t.Fatal("manifest was not reloaded each cycle")
	}
}

func TestIncrementalVerifyRechecksChangedDescriptor(t *testing.T) {
	for _, field := range []string{"key", "sha256", "size", "index"} {
		t.Run(field, func(t *testing.T) {
			_, objects, archived, opts := pruneFixture(t)
			counting := &countingGetStore{ObjectStore: objects, gets: make(map[string]int)}
			verifier := NewIncrementalVerifier(counting, opts.ManifestKey)
			if _, err := verifier.Verify(t.Context()); err != nil {
				t.Fatal(err)
			}
			manifest := archived.Manifest
			segment := &manifest.Segments[0]
			switch field {
			case "key":
				data, err := objects.Get(t.Context(), segment.Key)
				if err != nil {
					t.Fatal(err)
				}
				segment.Key += ".new"
				if err := objects.Put(t.Context(), segment.Key, data); err != nil {
					t.Fatal(err)
				}
			case "sha256":
				segment.SHA256 = strings.Repeat("a", 64)
			case "size":
				segment.SizeBytes++
			case "index":
				segment.Blocks[0].Hash = strings.Repeat("a", 64)
			default:
				t.Fatalf("unexpected field %q", field)
			}
			if err := SaveManifest(t.Context(), objects, opts.ManifestKey, manifest); err != nil {
				t.Fatal(err)
			}
			result, err := verifier.Verify(t.Context())
			if field == "key" {
				if err != nil || result.SegmentsChecked != 1 {
					t.Fatalf("changed key not verified: %+v, %v", result, err)
				}
			} else if err == nil {
				t.Fatalf("changed %s passed verification", field)
			}
		})
	}
}

func TestPruneRefusesUnarchivedHotPrefix(t *testing.T) {
	hot, objects, archived, opts := pruneFixture(t)
	manifest := archived.Manifest
	manifest.Segments = manifest.Segments[1:]
	manifest.FirstHeight = manifest.Segments[0].FirstHeight
	if err := SaveManifest(t.Context(), objects, opts.ManifestKey, manifest); err != nil {
		t.Fatal(err)
	}
	_, err := PruneVerifiedHotStore(t.Context(), hot, objects, opts)
	if err == nil || !strings.Contains(err.Error(), "after hot store base") {
		t.Fatalf("expected coverage error, got %v", err)
	}
	if hot.Base() != 1 {
		t.Fatalf("base changed: %d", hot.Base())
	}
}

func TestVerifyDistinguishesMissingManifestAndSegment(t *testing.T) {
	_, objects, archived, opts := pruneFixture(t)
	if _, err := Verify(t.Context(), objects, VerifyOptions{ManifestKey: "missing.json"}); !errors.Is(err, ErrManifestNotFound) {
		t.Fatalf("missing manifest error: %v", err)
	}
	manifest := archived.Manifest
	manifest.Segments[0].Key += ".missing"
	if err := SaveManifest(t.Context(), objects, opts.ManifestKey, manifest); err != nil {
		t.Fatal(err)
	}
	_, err := NewIncrementalVerifier(objects, opts.ManifestKey).Verify(t.Context())
	if !errors.Is(err, ErrObjectNotFound) || errors.Is(err, ErrManifestNotFound) {
		t.Fatalf("missing segment must fail verification: %v", err)
	}
}
