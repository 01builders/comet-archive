package archive

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	sm "github.com/cometbft/cometbft/state"
	"github.com/cometbft/cometbft/store"
	"github.com/cometbft/cometbft/types"
)

type PruneHotOptions struct {
	ManifestKey            string
	RetainBlocks           int64
	EvidenceMaxAgeBlocks   int64
	EvidenceMaxAgeDuration time.Duration
}

type PruneHotResult struct {
	BaseBefore      int64
	BaseAfter       int64
	Head            int64
	ArchivedThrough int64
	PruneToHeight   int64
	Pruned          uint64
	EvidenceRetains int64
}

func PruneVerifiedHotStore(ctx context.Context, blockStore *store.BlockStore, objectStore ObjectStore, opts PruneHotOptions) (PruneHotResult, error) {
	if blockStore == nil {
		return PruneHotResult{}, errors.New("block store is required")
	}
	if opts.ManifestKey == "" {
		return PruneHotResult{}, errors.New("manifest key is required")
	}
	if opts.RetainBlocks <= 0 {
		return PruneHotResult{}, errors.New("retain blocks must be positive")
	}
	if objectStore == nil {
		return PruneHotResult{}, errors.New("object store is required")
	}
	verifyResult, err := Verify(ctx, objectStore, VerifyOptions{ManifestKey: opts.ManifestKey})
	if err != nil {
		return PruneHotResult{}, fmt.Errorf("verify archive before prune: %w", err)
	}
	if verifyResult.BlocksChecked == 0 {
		return PruneHotResult{}, errors.New("archive verification checked no blocks")
	}
	return PruneHotStore(ctx, blockStore, verifyResult.Manifest, opts)
}

// PruneHotStore applies a fully verified manifest snapshot. The caller must
// serialize this operation with hot-store writes; verification needs no lock.
func PruneHotStore(ctx context.Context, blockStore *store.BlockStore, manifest Manifest, opts PruneHotOptions) (PruneHotResult, error) {
	if blockStore == nil {
		return PruneHotResult{}, errors.New("block store is required")
	}
	if opts.RetainBlocks <= 0 {
		return PruneHotResult{}, errors.New("retain blocks must be positive")
	}
	if err := ctx.Err(); err != nil {
		return PruneHotResult{}, err
	}
	base := blockStore.Base()
	head := blockStore.Height()
	result := PruneHotResult{
		BaseBefore:      base,
		BaseAfter:       base,
		Head:            head,
		ArchivedThrough: manifest.LastHeight,
	}
	if base == 0 || head == 0 {
		return result, nil
	}
	retainPruneTo := head - opts.RetainBlocks + 1
	archivePruneTo := manifest.LastHeight + 1
	pruneTo := min(retainPruneTo, archivePruneTo)
	if pruneTo <= base {
		return result, nil
	}
	if pruneTo > head {
		pruneTo = head
	}
	if manifest.FirstHeight > base {
		return PruneHotResult{}, fmt.Errorf("archive starts at %d after hot store base %d", manifest.FirstHeight, base)
	}
	state, err := pruneState(blockStore, opts)
	if err != nil {
		return PruneHotResult{}, err
	}
	if boundaryErr := validatePruneBoundary(blockStore, manifest, state.ChainID, pruneTo-1); boundaryErr != nil {
		return PruneHotResult{}, boundaryErr
	}
	pruned, evidencePoint, err := blockStore.PruneBlocks(pruneTo, state)
	if err != nil {
		return PruneHotResult{}, err
	}
	result.PruneToHeight = pruneTo
	result.Pruned = pruned
	result.EvidenceRetains = evidencePoint
	result.BaseAfter = blockStore.Base()
	return result, nil
}

func validatePruneBoundary(blockStore *store.BlockStore, manifest Manifest, chainID string, height int64) error {
	if manifest.ChainID != chainID {
		return fmt.Errorf("archive chain ID %q does not match hot store chain ID %q", manifest.ChainID, chainID)
	}
	if height < manifest.FirstHeight || height > manifest.LastHeight {
		return fmt.Errorf("prune boundary %d is outside archived range %d-%d", height, manifest.FirstHeight, manifest.LastHeight)
	}
	block := blockStore.LoadBlock(height)
	if block == nil {
		return fmt.Errorf("hot store prune boundary block %d is missing", height)
	}
	position := sort.Search(len(manifest.Segments), func(i int) bool {
		return manifest.Segments[i].LastHeight >= height
	})
	if position == len(manifest.Segments) {
		return fmt.Errorf("archive has no block index at prune boundary %d", height)
	}
	segment := manifest.Segments[position]
	index := height - segment.FirstHeight
	if index < 0 || index >= int64(len(segment.Blocks)) {
		return fmt.Errorf("archive has no block index at prune boundary %d", height)
	}
	if !strings.EqualFold(hex.EncodeToString(block.Hash()), segment.Blocks[index].Hash) {
		return fmt.Errorf("archive block hash at prune boundary %d does not match hot store", height)
	}
	return nil
}

func pruneState(blockStore *store.BlockStore, opts PruneHotOptions) (sm.State, error) {
	head := blockStore.Height()
	latest := blockStore.LoadBlock(head)
	if latest == nil {
		return sm.State{}, fmt.Errorf("latest block %d is missing", head)
	}
	params := *types.DefaultConsensusParams()
	if opts.EvidenceMaxAgeBlocks > 0 {
		params.Evidence.MaxAgeNumBlocks = opts.EvidenceMaxAgeBlocks
	}
	if opts.EvidenceMaxAgeDuration > 0 {
		params.Evidence.MaxAgeDuration = opts.EvidenceMaxAgeDuration
	}
	return sm.State{
		ChainID:         latest.ChainID,
		InitialHeight:   blockStore.Base(),
		LastBlockHeight: head,
		LastBlockTime:   latest.Time,
		ConsensusParams: params,
		LastBlockID:     latest.LastBlockID,
		AppHash:         latest.AppHash,
		LastResultsHash: latest.LastResultsHash,
	}, nil
}
