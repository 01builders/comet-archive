package blocksyncarchive

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/01builders/cometbft-archive/internal/archive"
)

type gatedSegmentStore struct {
	archive.ObjectStore
	key     string
	gets    atomic.Int64
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (s *gatedSegmentStore) Get(ctx context.Context, key string) ([]byte, error) {
	if key == s.key {
		s.gets.Add(1)
		s.once.Do(func() { close(s.entered) })
		select {
		case <-s.release:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return s.ObjectStore.Get(ctx, key)
}

func coldCacheFixture(t *testing.T) (*archive.LocalObjectStore, archive.Manifest) {
	t.Helper()
	store, err := archive.NewLocalObjectStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	var segments []archive.SegmentManifest
	for h := int64(1); h <= 2; h++ {
		record, recordErr := archive.BlockToRecord(makeIngestBlock(t, h))
		if recordErr != nil {
			t.Fatal(recordErr)
		}
		data, segment, segmentErr := archive.EncodeSegment([]archive.BlockRecord{record}, archive.CompressionGzip)
		if segmentErr != nil {
			t.Fatal(segmentErr)
		}
		segment.Key = archive.SegmentKey("archive", ingestTestChainID, segment)
		if checkErr := store.Put(t.Context(), segment.Key, data); checkErr != nil {
			t.Fatal(checkErr)
		}
		segments = append(segments, segment)
	}
	manifest, err := archive.NewManifest(ingestTestChainID, segments)
	if err != nil {
		t.Fatal(err)
	}
	if checkErr := archive.SaveManifest(t.Context(), store, "manifest.json", &manifest); checkErr != nil {
		t.Fatal(checkErr)
	}
	return store, manifest
}

func TestColdSegmentFetchCoalescesAndFollowerCanCancel(t *testing.T) {
	store, manifest := coldCacheFixture(t)
	gate := &gatedSegmentStore{ObjectStore: store, key: manifest.Segments[0].Key, entered: make(chan struct{}), release: make(chan struct{})}
	source, err := NewArchiveBlockSource(gate, "manifest.json")
	if err != nil {
		t.Fatal(err)
	}
	results := make(chan error, 16)
	go func() { _, loadErr := source.LoadBlock(t.Context(), 1); results <- loadErr }()
	select {
	case <-gate.entered:
	case <-time.After(time.Second):
		t.Fatal("fetch did not start")
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, checkErr := source.LoadBlock(ctx, 1); !errors.Is(checkErr, context.Canceled) {
		t.Fatalf("canceled follower: %v", checkErr)
	}
	for range 15 {
		go func() { _, loadErr := source.LoadBlock(t.Context(), 1); results <- loadErr }()
	}
	close(gate.release)
	for range 16 {
		if checkErr := <-results; checkErr != nil {
			t.Fatal(checkErr)
		}
	}
	if gate.gets.Load() != 1 {
		t.Fatalf("segment fetches=%d", gate.gets.Load())
	}
	block, err := source.LoadBlock(t.Context(), 1)
	if err != nil {
		t.Fatal(err)
	}
	block.ChainID = "mutated caller block"
	next, err := source.LoadBlock(t.Context(), 1)
	if err != nil || next.ChainID != ingestTestChainID {
		t.Fatalf("cache exposed mutable block: %v", err)
	}
}

func TestColdSegmentCacheEvictsAndInvalidatesDescriptors(t *testing.T) {
	store, manifest := coldCacheFixture(t)
	source, err := NewArchiveBlockSource(store, "manifest.json")
	if err != nil {
		t.Fatal(err)
	}
	if _, checkErr := source.LoadBlock(t.Context(), 1); checkErr != nil {
		t.Fatal(checkErr)
	}
	source.segmentCacheLimit = source.segmentBytes
	if _, checkErr := source.LoadBlock(t.Context(), 2); checkErr != nil {
		t.Fatal(checkErr)
	}
	if source.segmentBytes > source.segmentCacheLimit || len(source.segments) > 1 {
		t.Fatal("cache exceeded byte limit")
	}
	if checkErr := source.SetManifestCacheTTL(0); checkErr != nil {
		t.Fatal(checkErr)
	}
	manifest.Segments[1].SHA256 = strings.Repeat("0", 64)
	if checkErr := archive.SaveManifest(t.Context(), store, "manifest.json", &manifest); checkErr != nil {
		t.Fatal(checkErr)
	}
	if _, checkErr := source.LoadBlock(t.Context(), 2); checkErr == nil {
		t.Fatal("served cached block under changed descriptor")
	}
}

func TestColdPeerBudgetAndInvalidMessageDisconnect(t *testing.T) {
	ingestor, err := NewHotIngestor(newTestBlockStore(t), IngestOptions{ChainID: ingestTestChainID})
	if err != nil {
		t.Fatal(err)
	}
	reactor, err := NewReactor(ingestor, nil, ReactorOptions{RequestLimit: 2})
	if err != nil {
		t.Fatal(err)
	}
	peer := newFakePeer("limited")
	reactor.AddPeer(peer)
	reactor.coldBudgets[PeerID(peer.ID())] = coldPeerBudget{updated: time.Now(), tokens: 0}
	if reactor.enqueueColdBlockRequest(peer, 1) {
		t.Fatal("rate limit did not reject")
	}
	if !reactor.enqueueColdBlockRequest(newFakePeer("other"), 1) {
		t.Fatal("one peer exhausted another's budget")
	}
	reactor.disconnectPeer(peer, errors.New("bad protocol message"))
	select {
	case <-peer.Quit():
	default:
		t.Fatal("invalid peer remains connected")
	}
	if reactor.PeerCount() != 0 {
		t.Fatal("disconnected peer remains in reactor")
	}
}
