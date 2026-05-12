package archive

import (
	"context"
	"errors"
	"fmt"
)

type VerifyOptions struct {
	ManifestKey string
	SampleEvery int64
}

type VerifyResult struct {
	SegmentsChecked int
	BlocksChecked   int
}

func Verify(ctx context.Context, store ObjectStore, opts VerifyOptions) (VerifyResult, error) {
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
	if err != nil {
		return VerifyResult{}, err
	}
	result := VerifyResult{}
	for _, segment := range manifest.Segments {
		if err := ctx.Err(); err != nil {
			return VerifyResult{}, err
		}
		info, err := store.Stat(ctx, segment.Key)
		if err != nil {
			return VerifyResult{}, fmt.Errorf("stat %s: %w", segment.Key, err)
		}
		if info.Size != segment.SizeBytes {
			return VerifyResult{}, fmt.Errorf("object %s size %d, expected %d", segment.Key, info.Size, segment.SizeBytes)
		}
		data, err := store.Get(ctx, segment.Key)
		if err != nil {
			return VerifyResult{}, fmt.Errorf("get %s: %w", segment.Key, err)
		}
		validateAllBlocks := opts.SampleEvery <= 0
		var validateRecord func(BlockRecord) error
		if validateAllBlocks {
			validateRecord = validateDecodedBlockRecord
		}
		records, err := decodeSegmentRecords(data, segment, validateRecord)
		if err != nil {
			return VerifyResult{}, fmt.Errorf("decode %s: %w", segment.Key, err)
		}
		if validateAllBlocks {
			result.BlocksChecked += len(records)
			result.SegmentsChecked++
			continue
		}
		for _, record := range records {
			if err := ctx.Err(); err != nil {
				return VerifyResult{}, err
			}
			if (record.Height-manifest.FirstHeight)%opts.SampleEvery != 0 {
				continue
			}
			if _, err := RecordToBlock(record); err != nil {
				return VerifyResult{}, fmt.Errorf("block %d: %w", record.Height, err)
			}
			result.BlocksChecked++
		}
		result.SegmentsChecked++
	}
	return result, nil
}
