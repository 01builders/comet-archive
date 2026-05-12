package archive

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"time"
)

type LiveArchiveOptions struct {
	ChainID       string
	Prefix        string
	ManifestName  string
	ManifestKey   string
	StartHeight   int64
	ReadyHeight   int64
	SegmentBlocks int
	Compression   string
}

type LiveArchiveResult struct {
	Manifest       Manifest
	ManifestKey    string
	Segments       int
	Uploaded       int
	Reused         int
	BlocksArchived int
	FirstArchived  int64
	LastArchived   int64
}

func ArchiveReadyFromHead(ctx context.Context, reader BlockReader, store ObjectStore, opts LiveArchiveOptions, safetyWindow int64) (LiveArchiveResult, error) {
	if safetyWindow < 0 {
		return LiveArchiveResult{}, errors.New("safety window cannot be negative")
	}
	if opts.ChainID == "" {
		return LiveArchiveResult{}, errors.New("chain ID is required")
	}
	if opts.Compression == "" {
		opts.Compression = DefaultCompression
	}
	if err := ValidateCompression(opts.Compression); err != nil {
		return LiveArchiveResult{}, err
	}
	manifestKey, err := ResolveManifestKey(opts.Prefix, opts.ChainID, opts.ManifestName, opts.ManifestKey)
	if err != nil {
		return LiveArchiveResult{}, err
	}
	if reader == nil {
		return LiveArchiveResult{}, errors.New("block reader is required")
	}
	if store == nil {
		return LiveArchiveResult{}, errors.New("object store is required")
	}
	result := LiveArchiveResult{ManifestKey: manifestKey}
	manifest, err := loadOrCreateLiveManifest(ctx, store, manifestKey, opts.ChainID)
	if err != nil {
		return LiveArchiveResult{}, err
	}
	result.Manifest = manifest
	result.Segments = len(manifest.Segments)
	head := reader.Height()
	if head == 0 || head <= safetyWindow {
		return result, nil
	}
	opts.ReadyHeight = head - safetyWindow
	return ArchiveReady(ctx, reader, store, opts)
}

func ArchiveReady(ctx context.Context, reader BlockReader, store ObjectStore, opts LiveArchiveOptions) (LiveArchiveResult, error) {
	if opts.ChainID == "" {
		return LiveArchiveResult{}, errors.New("chain ID is required")
	}
	if opts.ReadyHeight <= 0 {
		return LiveArchiveResult{}, errors.New("ready height must be positive")
	}
	if opts.SegmentBlocks <= 0 {
		opts.SegmentBlocks = DefaultSegmentBlocks
	}
	if opts.SegmentBlocks > MaxSegmentBlocks {
		return LiveArchiveResult{}, fmt.Errorf("segment blocks %d exceeds maximum %d", opts.SegmentBlocks, MaxSegmentBlocks)
	}
	if opts.Compression == "" {
		opts.Compression = DefaultCompression
	}
	if err := ValidateCompression(opts.Compression); err != nil {
		return LiveArchiveResult{}, err
	}
	manifestKey, err := ResolveManifestKey(opts.Prefix, opts.ChainID, opts.ManifestName, opts.ManifestKey)
	if err != nil {
		return LiveArchiveResult{}, err
	}
	if reader == nil {
		return LiveArchiveResult{}, errors.New("block reader is required")
	}
	if store == nil {
		return LiveArchiveResult{}, errors.New("object store is required")
	}
	manifest, err := loadOrCreateLiveManifest(ctx, store, manifestKey, opts.ChainID)
	if err != nil {
		return LiveArchiveResult{}, err
	}
	if manifest.ChainID != opts.ChainID {
		return LiveArchiveResult{}, fmt.Errorf("manifest chain ID %q, expected %q", manifest.ChainID, opts.ChainID)
	}
	nextHeight := manifest.LastHeight + 1
	if manifest.LastHeight == 0 {
		nextHeight = opts.StartHeight
		if nextHeight == 0 {
			nextHeight = reader.Base()
		}
	}
	if nextHeight <= 0 {
		return LiveArchiveResult{}, errors.New("source blockstore is empty")
	}
	sourceHeight := reader.Height()
	if sourceHeight == 0 {
		return LiveArchiveResult{}, errors.New("source blockstore is empty")
	}
	end := opts.ReadyHeight
	if end > sourceHeight {
		end = sourceHeight
	}
	result := LiveArchiveResult{ManifestKey: manifestKey}
	if end < nextHeight {
		result.Manifest = manifest
		result.Segments = len(manifest.Segments)
		return result, nil
	}
	for height := nextHeight; height <= end; {
		if err := ctx.Err(); err != nil {
			return LiveArchiveResult{}, err
		}
		last := height + int64(opts.SegmentBlocks) - 1
		if last > end {
			last = end
		}
		segment, uploaded, err := archiveLiveSegment(ctx, reader, store, opts, height, last)
		if err != nil {
			return LiveArchiveResult{}, err
		}
		manifest.Segments = appendSegment(manifest.Segments, segment)
		rebuilt, err := NewManifest(opts.ChainID, manifest.Segments)
		if err != nil {
			return LiveArchiveResult{}, err
		}
		if !manifest.CreatedAt.IsZero() {
			rebuilt.CreatedAt = manifest.CreatedAt
		}
		rebuilt.UpdatedAt = time.Now().UTC()
		manifest = rebuilt
		if err := SaveManifest(ctx, store, manifestKey, manifest); err != nil {
			return LiveArchiveResult{}, err
		}
		if uploaded {
			result.Uploaded++
		} else {
			result.Reused++
		}
		result.BlocksArchived += int(last - height + 1)
		if result.FirstArchived == 0 {
			result.FirstArchived = height
		}
		result.LastArchived = last
		height = last + 1
	}
	result.Manifest = manifest
	result.Segments = len(manifest.Segments)
	return result, nil
}

func loadOrCreateLiveManifest(ctx context.Context, store ObjectStore, key, chainID string) (Manifest, error) {
	manifest, err := LoadManifest(ctx, store, key)
	if errors.Is(err, ErrObjectNotFound) {
		return NewManifest(chainID, nil)
	}
	return manifest, err
}

func archiveLiveSegment(
	ctx context.Context,
	reader BlockReader,
	store ObjectStore,
	opts LiveArchiveOptions,
	first int64,
	last int64,
) (SegmentManifest, bool, error) {
	records := make([]BlockRecord, 0, last-first+1)
	for height := first; height <= last; height++ {
		if err := ctx.Err(); err != nil {
			return SegmentManifest{}, false, err
		}
		block := reader.LoadBlock(height)
		if block == nil {
			return SegmentManifest{}, false, fmt.Errorf("source block at height %d is missing", height)
		}
		if block.ChainID != opts.ChainID {
			return SegmentManifest{}, false, fmt.Errorf("source block at height %d has chain ID %q, expected %q", height, block.ChainID, opts.ChainID)
		}
		record, err := BlockToRecord(block)
		if err != nil {
			return SegmentManifest{}, false, fmt.Errorf("encode block %d: %w", height, err)
		}
		records = append(records, record)
	}
	data, segment, err := EncodeSegment(records, opts.Compression)
	if err != nil {
		return SegmentManifest{}, false, err
	}
	segment.Key = SegmentKey(opts.Prefix, opts.ChainID, segment)
	uploaded, err := putImmutableSegment(ctx, store, segment, data)
	return segment, uploaded, err
}

func putImmutableSegment(ctx context.Context, store ObjectStore, segment SegmentManifest, data []byte) (bool, error) {
	exists, err := store.Exists(ctx, segment.Key)
	if err != nil {
		return false, err
	}
	if exists {
		if err := verifyStoredSegment(ctx, store, segment); err != nil {
			return false, err
		}
		return false, nil
	}
	if immutableStore, ok := store.(ImmutableObjectStore); ok {
		if err := immutableStore.PutIfAbsent(ctx, segment.Key, data); err != nil {
			if errors.Is(err, ErrObjectAlreadyExists) {
				if verifyErr := verifyStoredSegment(ctx, store, segment); verifyErr != nil {
					return false, verifyErr
				}
				return false, nil
			}
			return false, err
		}
		if err := verifyStoredSegment(ctx, store, segment); err != nil {
			return false, err
		}
		return true, nil
	}
	if err := store.Put(ctx, segment.Key, data); err != nil {
		return false, err
	}
	if err := verifyStoredSegment(ctx, store, segment); err != nil {
		return false, err
	}
	return true, nil
}

func verifyStoredSegment(ctx context.Context, store ObjectStore, segment SegmentManifest) error {
	current, err := store.Get(ctx, segment.Key)
	if err != nil {
		return err
	}
	sum := sha256.Sum256(current)
	if int64(len(current)) != segment.SizeBytes || hex.EncodeToString(sum[:]) != segment.SHA256 {
		return fmt.Errorf("object %s differs from expected segment", segment.Key)
	}
	return nil
}
