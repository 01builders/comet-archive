package archive

import (
	"context"
	"path/filepath"
	"strings"
	"testing"

	ctypes "github.com/cometbft/cometbft/types"
)

type mapBlockReader map[int64]*ctypes.Block

func (r mapBlockReader) Base() int64 {
	var base int64
	for height := range r {
		if base == 0 || height < base {
			base = height
		}
	}
	return base
}

func (r mapBlockReader) Height() int64 {
	var head int64
	for height := range r {
		head = max(head, height)
	}
	return head
}

func (r mapBlockReader) LoadBlock(height int64) *ctypes.Block { return r[height] }

func (mapBlockReader) Close() error { return nil }

func forkedTestBlock(t *testing.T, height int64) *ctypes.Block {
	t.Helper()
	return makeUnlinkedTestBlock(t, height, ctypes.BlockID{
		Hash:          bytesOf(0xee, 32),
		PartSetHeader: ctypes.PartSetHeader{Total: 1, Hash: bytesOf(0xef, 32)},
	})
}

func TestArchiveReadyRefusesUnlinkedContinuation(t *testing.T) {
	ctx := context.Background()
	reader := mapBlockReader{}
	for height := int64(1); height <= 5; height++ {
		reader[height] = makeTestBlock(t, height)
	}
	reader[4] = forkedTestBlock(t, 4)
	store, err := NewLocalObjectStore(filepath.Join(t.TempDir(), "objects"))
	if err != nil {
		t.Fatal(err)
	}
	opts := LiveArchiveOptions{ChainID: testChainID, Prefix: "archive", ReadyHeight: 3, SegmentBlocks: 3}
	first, err := ArchiveReady(ctx, reader, store, opts)
	if err != nil {
		t.Fatal(err)
	}
	opts.ReadyHeight = 5
	if _, err := ArchiveReady(ctx, reader, store, opts); err == nil || !strings.Contains(err.Error(), "does not extend archived block 3") {
		t.Fatalf("archive err=%v, want unlinked continuation rejection", err)
	}
	manifest, err := LoadManifest(ctx, store, first.ManifestKey)
	if err != nil {
		t.Fatal(err)
	}
	if manifest.LastHeight != 3 {
		t.Fatalf("manifest advanced to %d after rejected continuation", manifest.LastHeight)
	}
	segments, err := store.List(ctx, "archive/chains/"+testChainID+"/segments/")
	if err != nil {
		t.Fatal(err)
	}
	if len(segments) != 1 {
		t.Fatalf("rejected continuation uploaded objects: %+v", segments)
	}
}

func TestArchiveReadyRefusesUnlinkedBlockWithinSegment(t *testing.T) {
	reader := mapBlockReader{1: makeTestBlock(t, 1), 2: forkedTestBlock(t, 2)}
	store, err := NewLocalObjectStore(filepath.Join(t.TempDir(), "objects"))
	if err != nil {
		t.Fatal(err)
	}
	_, err = ArchiveReady(context.Background(), reader, store, LiveArchiveOptions{ChainID: testChainID, ReadyHeight: 2})
	if err == nil || !strings.Contains(err.Error(), "does not extend archived block 1") {
		t.Fatalf("archive err=%v, want unlinked block rejection", err)
	}
}

func TestVerifyRejectsUnlinkedSegments(t *testing.T) {
	for _, sampleEvery := range []int64{0, 1} {
		ctx := context.Background()
		store, err := NewLocalObjectStore(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		first := putTestSegment(t, store, makeTestRecord(t, 1), makeTestRecord(t, 2))
		forked, err := BlockToRecord(forkedTestBlock(t, 3))
		if err != nil {
			t.Fatal(err)
		}
		second := putTestSegment(t, store, forked, makeTestRecord(t, 4))
		manifest, err := NewManifest(testChainID, []SegmentManifest{first, second})
		if err != nil {
			t.Fatal(err)
		}
		key := ManifestKey("archive", testChainID, DefaultManifest)
		if err := SaveManifest(ctx, store, key, &manifest); err != nil {
			t.Fatal(err)
		}
		_, err = Verify(ctx, store, VerifyOptions{ManifestKey: key, SampleEvery: sampleEvery})
		if err == nil || !strings.Contains(err.Error(), "does not extend archived block 2") {
			t.Fatalf("sample-every=%d verify err=%v, want cross-segment link failure", sampleEvery, err)
		}
	}
}

func putTestSegment(t *testing.T, store ObjectStore, records ...BlockRecord) SegmentManifest {
	t.Helper()
	data, segment, err := EncodeSegment(records, CompressionGzip)
	if err != nil {
		t.Fatal(err)
	}
	segment.Key = SegmentKey("archive", testChainID, segment)
	if err := store.Put(context.Background(), segment.Key, data); err != nil {
		t.Fatal(err)
	}
	return segment
}
