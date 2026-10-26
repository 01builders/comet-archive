package blocksyncarchive

import (
	"container/list"
	"context"
	"encoding/json"
	"errors"
	"slices"
	"sync"
	"time"

	"github.com/01builders/cometbft-archive/internal/archive"
	"github.com/cometbft/cometbft/types"
)

var ErrColdBlockNotFound = errors.New("cold block not found")

const DefaultColdManifestCacheTTL = 5 * time.Second

type ColdBlockSource interface {
	AdvertisedRange(ctx context.Context) (PeerRange, error)
	LoadBlock(ctx context.Context, height int64) (*types.Block, error)
}

type ArchiveBlockSource struct {
	store            archive.ObjectStore
	manifestKey      string
	manifestCacheTTL time.Duration

	mu                sync.Mutex
	cachedManifest    archive.Manifest
	cachedManifestAt  time.Time
	manifestFlight    *manifestLoad
	segments          map[string]*list.Element
	segmentLRU        list.List
	segmentBytes      int
	segmentCacheLimit int
	flights           map[string]*segmentLoad
	fetchSlots        chan struct{}
}

func NewArchiveBlockSource(store archive.ObjectStore, manifestKey string) (*ArchiveBlockSource, error) {
	if store == nil {
		return nil, errors.New("object store is required")
	}
	if manifestKey == "" {
		return nil, errors.New("manifest key is required")
	}
	if err := archive.ValidateObjectKey(manifestKey); err != nil {
		return nil, err
	}
	return &ArchiveBlockSource{
		store:             store,
		manifestKey:       manifestKey,
		manifestCacheTTL:  DefaultColdManifestCacheTTL,
		segments:          make(map[string]*list.Element),
		segmentCacheLimit: coldSegmentCacheBytes,
		flights:           make(map[string]*segmentLoad),
		fetchSlots:        make(chan struct{}, 4),
	}, nil
}

func (s *ArchiveBlockSource) SetManifestCacheTTL(ttl time.Duration) error {
	if ttl < 0 {
		return errors.New("manifest cache TTL cannot be negative")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.manifestCacheTTL = ttl
	if ttl == 0 {
		s.cachedManifestAt = time.Time{}
	}
	return nil
}

func (s *ArchiveBlockSource) AdvertisedRange(ctx context.Context) (PeerRange, error) {
	manifest, err := s.loadManifest(ctx)
	if errors.Is(err, archive.ErrObjectNotFound) {
		return PeerRange{}, nil
	}
	if err != nil {
		return PeerRange{}, err
	}
	if manifest.LastHeight == 0 {
		return PeerRange{}, nil
	}
	return PeerRange{Base: manifest.FirstHeight, Height: manifest.LastHeight}, nil
}

func (s *ArchiveBlockSource) LoadBlock(ctx context.Context, height int64) (*types.Block, error) {
	if height <= 0 {
		return nil, ErrColdBlockNotFound
	}
	manifest, err := s.loadManifest(ctx)
	if errors.Is(err, archive.ErrObjectNotFound) {
		return nil, ErrColdBlockNotFound
	}
	if err != nil {
		return nil, err
	}
	segment, ok := segmentForHeight(manifest.Segments, height)
	if !ok {
		return nil, ErrColdBlockNotFound
	}
	records, err := s.loadSegment(ctx, segment)
	if err != nil {
		return nil, err
	}
	index := height - segment.FirstHeight
	if index < 0 || index >= int64(len(records)) {
		return nil, ErrColdBlockNotFound
	}
	return archive.RecordToBlock(records[index])
}

type manifestLoad struct {
	done     chan struct{}
	manifest archive.Manifest
	err      error
}

type segmentLoad struct {
	done    chan struct{}
	records []archive.BlockRecord
	err     error
}

type cachedSegment struct {
	key     string
	records []archive.BlockRecord
	bytes   int
}

const coldSegmentCacheBytes = 64 << 20

func (s *ArchiveBlockSource) loadSegment(ctx context.Context, segment archive.SegmentManifest) ([]archive.BlockRecord, error) {
	// Include every descriptor field: a refreshed manifest must never reuse a
	// cached segment under changed checksums, ranges, or block indexes.
	descriptor, err := json.Marshal(segment)
	if err != nil {
		return nil, err
	}
	key := string(descriptor)
acquire:
	for {
		s.mu.Lock()
		if entry := s.segments[key]; entry != nil {
			s.segmentLRU.MoveToFront(entry)
			cached, ok := entry.Value.(cachedSegment)
			if !ok {
				panic("invalid segment cache entry")
			}
			records := cached.records
			s.mu.Unlock()
			return records, nil
		}
		if flight := s.flights[key]; flight != nil {
			s.mu.Unlock()
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-flight.done:
				return flight.records, flight.err
			}
		}
		// Bound distinct in-flight segment loads as well as retained cache memory.
		select {
		case s.fetchSlots <- struct{}{}:
			break acquire
		default:
			s.mu.Unlock()
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case s.fetchSlots <- struct{}{}:
			}
			<-s.fetchSlots
			continue
		}
	}
	flight := &segmentLoad{done: make(chan struct{})}
	s.flights[key] = flight
	s.mu.Unlock()
	data, err := s.store.Get(ctx, segment.Key)
	var records []archive.BlockRecord
	if err == nil {
		records, err = archive.DecodeSegment(data, segment)
	}
	size := len(key)
	for _, record := range records {
		size += len(record.Bytes) + len(record.Hash) + 64
	}
	s.mu.Lock()
	if err == nil && size <= s.segmentCacheLimit {
		for s.segmentBytes+size > s.segmentCacheLimit {
			entry := s.segmentLRU.Back()
			cached, ok := entry.Value.(cachedSegment)
			if !ok {
				panic("invalid segment cache entry")
			}
			delete(s.segments, cached.key)
			s.segmentBytes -= cached.bytes
			s.segmentLRU.Remove(entry)
		}
		s.segments[key] = s.segmentLRU.PushFront(cachedSegment{key: key, records: records, bytes: size})
		s.segmentBytes += size
	}
	flight.records, flight.err = records, err
	delete(s.flights, key)
	close(flight.done)
	s.mu.Unlock()
	<-s.fetchSlots
	return records, err
}

func (s *ArchiveBlockSource) loadManifest(ctx context.Context) (archive.Manifest, error) {
	s.mu.Lock()
	if !s.cachedManifestAt.IsZero() && s.manifestCacheTTL > 0 && time.Since(s.cachedManifestAt) < s.manifestCacheTTL {
		manifest := s.cachedManifest
		s.mu.Unlock()
		return manifest, nil
	}
	if flight := s.manifestFlight; flight != nil {
		s.mu.Unlock()
		select {
		case <-ctx.Done():
			return archive.Manifest{}, ctx.Err()
		case <-flight.done:
			return flight.manifest, flight.err
		}
	}
	flight := &manifestLoad{done: make(chan struct{})}
	s.manifestFlight = flight
	s.mu.Unlock()
	manifest, err := archive.LoadManifest(ctx, s.store, s.manifestKey)
	s.mu.Lock()
	if err == nil {
		s.cachedManifest, s.cachedManifestAt = manifest, time.Now()
	}
	flight.manifest, flight.err = manifest, err
	s.manifestFlight = nil
	close(flight.done)
	s.mu.Unlock()
	return manifest, err
}

func segmentForHeight(segments []archive.SegmentManifest, height int64) (archive.SegmentManifest, bool) {
	index, ok := slices.BinarySearchFunc(segments, height, func(segment archive.SegmentManifest, target int64) int {
		if target < segment.FirstHeight {
			return 1
		}
		if target > segment.LastHeight {
			return -1
		}
		return 0
	})
	if ok {
		return segments[index], true
	}
	return archive.SegmentManifest{}, false
}

func mergeAdvertisedRanges(cold, hot PeerRange) PeerRange {
	if cold.Height == 0 {
		return hot
	}
	if hot.Height == 0 {
		return cold
	}
	if cold.Height+1 < hot.Base || hot.Height+1 < cold.Base {
		if hot.Height >= cold.Height {
			return hot
		}
		return cold
	}
	base := min(hot.Base, cold.Base)
	height := max(hot.Height, cold.Height)
	return PeerRange{Base: base, Height: height}
}
