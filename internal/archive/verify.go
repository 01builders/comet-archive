package archive

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
)

var ErrManifestNotFound = errors.New("manifest not found")

type VerifyOptions struct {
	ManifestKey string
	SampleEvery int64
	// Concurrency bounds the worker pool used to verify segments in parallel.
	// Non-positive values select a sensible default (min(GOMAXPROCS, 8)).
	Concurrency int
}

type VerifyResult struct {
	Manifest        Manifest `json:"-"`
	SegmentsChecked int
	BlocksChecked   int
}

func Verify(ctx context.Context, store ObjectStore, opts VerifyOptions) (VerifyResult, error) {
	return verifyArchive(ctx, store, opts, nil)
}

// IncrementalVerifier caches full verification of immutable segments for one
// store and manifest. It must be used serially. Entries are bounded by the
// current manifest, and all descriptor fields participate in cache identity.
type IncrementalVerifier struct {
	store       ObjectStore
	manifestKey string
	verified    map[string][sha256.Size]byte
}

func NewIncrementalVerifier(store ObjectStore, manifestKey string) *IncrementalVerifier {
	return &IncrementalVerifier{store: store, manifestKey: manifestKey, verified: make(map[string][sha256.Size]byte)}
}

func (v *IncrementalVerifier) Verify(ctx context.Context) (VerifyResult, error) {
	return verifyArchive(ctx, v.store, VerifyOptions{ManifestKey: v.manifestKey}, v.verified)
}

func verifyArchive(ctx context.Context, store ObjectStore, opts VerifyOptions, verified map[string][sha256.Size]byte) (VerifyResult, error) {
	if opts.ManifestKey == "" {
		return VerifyResult{}, errors.New("manifest key is required")
	}
	if opts.SampleEvery < 0 {
		return VerifyResult{}, errors.New("sample every cannot be negative")
	}
	if store == nil {
		return VerifyResult{}, errors.New("object store is required")
	}
	manifest, err := LoadManifest(ctx, store, opts.ManifestKey)
	if errors.Is(err, ErrObjectNotFound) {
		return VerifyResult{}, fmt.Errorf("%w: %w", ErrManifestNotFound, err)
	}
	if err != nil {
		return VerifyResult{}, err
	}
	if contextErr := ctx.Err(); contextErr != nil {
		return VerifyResult{}, contextErr
	}
	if len(manifest.Segments) == 0 {
		clear(verified)
		return VerifyResult{Manifest: manifest}, nil
	}

	pending, fingerprints, err := pendingSegments(manifest, verified)
	if err != nil {
		return VerifyResult{}, err
	}
	result, err := verifySegments(ctx, store, opts, manifest, pending)
	if err != nil {
		return VerifyResult{}, err
	}
	if verified != nil {
		clear(verified)
		for key, fingerprint := range fingerprints {
			verified[key] = fingerprint
		}
	}
	result.Manifest = manifest
	return result, nil
}

func pendingSegments(manifest Manifest, verified map[string][sha256.Size]byte) ([]int, map[string][sha256.Size]byte, error) {
	pending := make([]int, 0, len(manifest.Segments))
	fingerprints := make(map[string][sha256.Size]byte, len(manifest.Segments))
	for i, segment := range manifest.Segments {
		data, err := json.Marshal(segment)
		if err != nil {
			return nil, nil, err
		}
		fingerprint := sha256.Sum256(data)
		fingerprints[segment.Key] = fingerprint
		if previous, ok := verified[segment.Key]; !ok || previous != fingerprint {
			pending = append(pending, i)
		}
	}
	return pending, fingerprints, nil
}

func verifySegments(ctx context.Context, store ObjectStore, opts VerifyOptions, manifest Manifest, pending []int) (VerifyResult, error) {
	workers := min(segmentConcurrency(opts.Concurrency), len(pending))

	errs := make([]error, len(manifest.Segments))
	var blocksChecked int64
	var segmentsChecked int64

	workerCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	jobs := make(chan int)
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Go(func() {
			for i := range jobs {
				if err := workerCtx.Err(); err != nil {
					errs[i] = err
					continue
				}
				segment := manifest.Segments[i]
				blocks, err := verifySegment(workerCtx, store, segment, manifest.FirstHeight, opts.SampleEvery)
				if err != nil {
					errs[i] = err
					cancel()
					continue
				}
				atomic.AddInt64(&blocksChecked, int64(blocks))
				atomic.AddInt64(&segmentsChecked, 1)
			}
		})
	}

	for _, i := range pending {
		select {
		case <-workerCtx.Done():
			close(jobs)
			wg.Wait()
			if err := firstSegmentError(errs, ctx.Err()); err != nil {
				return VerifyResult{}, err
			}
			// no errors recorded — context was canceled externally
			return VerifyResult{}, ctx.Err()
		case jobs <- i:
		}
	}
	close(jobs)
	wg.Wait()
	if err := firstSegmentError(errs, ctx.Err()); err != nil {
		return VerifyResult{}, err
	}
	return VerifyResult{
		SegmentsChecked: int(segmentsChecked),
		BlocksChecked:   int(blocksChecked),
	}, nil
}

// verifySegment performs the per-segment verification work and returns the
// number of blocks counted toward BlocksChecked.
func verifySegment(ctx context.Context, store ObjectStore, segment SegmentManifest, manifestFirstHeight int64, sampleEvery int64) (int, error) {
	info, err := store.Stat(ctx, segment.Key)
	if err != nil {
		return 0, fmt.Errorf("stat %s: %w", segment.Key, err)
	}
	if info.Size != segment.SizeBytes {
		return 0, fmt.Errorf("object %s size %d, expected %d", segment.Key, info.Size, segment.SizeBytes)
	}
	data, err := store.Get(ctx, segment.Key)
	if err != nil {
		return 0, fmt.Errorf("get %s: %w", segment.Key, err)
	}
	validateAllBlocks := sampleEvery <= 0
	var validateRecord func(BlockRecord) error
	if validateAllBlocks {
		validateRecord = validateDecodedBlockRecord
	}
	records, err := decodeSegmentRecords(data, segment, validateRecord)
	if err != nil {
		return 0, fmt.Errorf("decode %s: %w", segment.Key, err)
	}
	if validateAllBlocks {
		return len(records), nil
	}
	blocks := 0
	for _, record := range records {
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		if (record.Height-manifestFirstHeight)%sampleEvery != 0 {
			continue
		}
		if _, err := RecordToBlock(record); err != nil {
			return 0, fmt.Errorf("block %d: %w", record.Height, err)
		}
		blocks++
	}
	return blocks, nil
}
