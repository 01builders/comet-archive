package archive

import (
	"context"
	"path/filepath"
	"testing"
	"time"
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
