package blocksyncarchive

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/01builders/cometbft-archive/internal/archive"
)

func TestArchiveBlockSourceCachesManifest(t *testing.T) {
	ctx := context.Background()
	localStore, err := archive.NewLocalObjectStore(filepath.Join(t.TempDir(), "objects"))
	if err != nil {
		t.Fatal(err)
	}
	records := make([]archive.BlockRecord, 0, 2)
	for height := int64(1); height <= 2; height++ {
		record, recordErr := archive.BlockToRecord(makeIngestBlock(t, height))
		if recordErr != nil {
			t.Fatal(recordErr)
		}
		records = append(records, record)
	}
	data, segment, err := archive.EncodeSegment(records, archive.CompressionGzip)
	if err != nil {
		t.Fatal(err)
	}
	segment.Key = archive.SegmentKey("archive", ingestTestChainID, segment)
	if putErr := localStore.Put(ctx, segment.Key, data); putErr != nil {
		t.Fatal(putErr)
	}
	manifest, err := archive.NewManifest(ingestTestChainID, []archive.SegmentManifest{segment})
	if err != nil {
		t.Fatal(err)
	}
	manifestKey := archive.ManifestKey("archive", ingestTestChainID, archive.DefaultManifest)
	if saveErr := archive.SaveManifest(ctx, localStore, manifestKey, manifest); saveErr != nil {
		t.Fatal(saveErr)
	}

	countingStore := &countingObjectStore{ObjectStore: localStore, manifestKey: manifestKey}
	source, err := NewArchiveBlockSource(countingStore, manifestKey)
	if err != nil {
		t.Fatal(err)
	}
	source.manifestCacheTTL = time.Hour
	if _, err := source.AdvertisedRange(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := source.AdvertisedRange(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := source.LoadBlock(ctx, 2); err != nil {
		t.Fatal(err)
	}
	if countingStore.manifestGets != 1 {
		t.Fatalf("manifest gets=%d, want 1", countingStore.manifestGets)
	}
}

func TestNewArchiveBlockSourceRejectsUnsafeManifestKey(t *testing.T) {
	localStore, err := archive.NewLocalObjectStore(filepath.Join(t.TempDir(), "objects"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := NewArchiveBlockSource(localStore, "../escape"); err == nil {
		t.Fatal("expected unsafe manifest key error")
	}
}

func TestArchiveBlockSourceCanDisableManifestCache(t *testing.T) {
	ctx := context.Background()
	localStore, err := archive.NewLocalObjectStore(filepath.Join(t.TempDir(), "objects"))
	if err != nil {
		t.Fatal(err)
	}
	record, err := archive.BlockToRecord(makeIngestBlock(t, 1))
	if err != nil {
		t.Fatal(err)
	}
	data, segment, err := archive.EncodeSegment([]archive.BlockRecord{record}, archive.CompressionGzip)
	if err != nil {
		t.Fatal(err)
	}
	segment.Key = archive.SegmentKey("archive", ingestTestChainID, segment)
	if putErr := localStore.Put(ctx, segment.Key, data); putErr != nil {
		t.Fatal(putErr)
	}
	manifest, err := archive.NewManifest(ingestTestChainID, []archive.SegmentManifest{segment})
	if err != nil {
		t.Fatal(err)
	}
	manifestKey := archive.ManifestKey("archive", ingestTestChainID, archive.DefaultManifest)
	if saveErr := archive.SaveManifest(ctx, localStore, manifestKey, manifest); saveErr != nil {
		t.Fatal(saveErr)
	}

	countingStore := &countingObjectStore{ObjectStore: localStore, manifestKey: manifestKey}
	source, err := NewArchiveBlockSource(countingStore, manifestKey)
	if err != nil {
		t.Fatal(err)
	}
	if err := source.SetManifestCacheTTL(0); err != nil {
		t.Fatal(err)
	}
	if _, err := source.AdvertisedRange(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := source.AdvertisedRange(ctx); err != nil {
		t.Fatal(err)
	}
	if countingStore.manifestGets != 2 {
		t.Fatalf("manifest gets=%d, want 2", countingStore.manifestGets)
	}
	if err := source.SetManifestCacheTTL(-time.Second); err == nil {
		t.Fatal("expected negative manifest cache TTL error")
	}
}

func TestMergeAdvertisedRangesNeverBridgesGaps(t *testing.T) {
	tests := []struct {
		name string
		cold PeerRange
		hot  PeerRange
		want PeerRange
	}{
		{
			name: "touching cold and hot",
			cold: PeerRange{Base: 1, Height: 10},
			hot:  PeerRange{Base: 11, Height: 20},
			want: PeerRange{Base: 1, Height: 20},
		},
		{
			name: "overlapping cold and hot",
			cold: PeerRange{Base: 1, Height: 12},
			hot:  PeerRange{Base: 10, Height: 20},
			want: PeerRange{Base: 1, Height: 20},
		},
		{
			name: "gap keeps higher hot range",
			cold: PeerRange{Base: 1, Height: 10},
			hot:  PeerRange{Base: 20, Height: 30},
			want: PeerRange{Base: 20, Height: 30},
		},
		{
			name: "gap keeps higher cold range",
			cold: PeerRange{Base: 20, Height: 30},
			hot:  PeerRange{Base: 1, Height: 10},
			want: PeerRange{Base: 20, Height: 30},
		},
		{
			name: "empty cold uses hot",
			cold: PeerRange{},
			hot:  PeerRange{Base: 7, Height: 8},
			want: PeerRange{Base: 7, Height: 8},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := mergeAdvertisedRanges(tt.cold, tt.hot); got != tt.want {
				t.Fatalf("got %+v, want %+v", got, tt.want)
			}
		})
	}
}

func TestSegmentForHeightFindsSortedManifestSegment(t *testing.T) {
	segments := []archive.SegmentManifest{
		{FirstHeight: 1, LastHeight: 10},
		{FirstHeight: 11, LastHeight: 20},
		{FirstHeight: 21, LastHeight: 30},
	}
	segment, ok := segmentForHeight(segments, 17)
	if !ok {
		t.Fatal("expected segment for height 17")
	}
	if segment.FirstHeight != 11 || segment.LastHeight != 20 {
		t.Fatalf("got segment %d-%d, want 11-20", segment.FirstHeight, segment.LastHeight)
	}
	if _, ok := segmentForHeight(segments, 31); ok {
		t.Fatal("unexpected segment for height 31")
	}
}

type countingObjectStore struct {
	archive.ObjectStore
	manifestKey  string
	manifestGets int
}

func (s *countingObjectStore) Get(ctx context.Context, key string) ([]byte, error) {
	if key == s.manifestKey {
		s.manifestGets++
	}
	return s.ObjectStore.Get(ctx, key)
}
