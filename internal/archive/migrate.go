package archive

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

type MigrationOptions struct {
	ChainID       string
	StartHeight   int64
	EndHeight     int64
	SegmentBlocks int
	Prefix        string
	ManifestName  string
	ManifestKey   string
	Compression   string
}

type MigrationResult struct {
	Manifest    Manifest
	ManifestKey string
	Segments    int
	Uploaded    int
	Reused      int
}

type MigrationState struct {
	ChainID       string            `json:"chain_id"`
	StartHeight   int64             `json:"start_height"`
	EndHeight     int64             `json:"end_height"`
	NextHeight    int64             `json:"next_height"`
	Segments      []SegmentManifest `json:"segments"`
	LastUpdatedAt time.Time         `json:"last_updated_at"`
}

func Migrate(ctx context.Context, reader BlockReader, store ObjectStore, opts MigrationOptions) (MigrationResult, error) {
	if opts.ChainID == "" {
		return MigrationResult{}, errors.New("chain ID is required")
	}
	if opts.SegmentBlocks <= 0 {
		opts.SegmentBlocks = DefaultSegmentBlocks
	}
	if opts.SegmentBlocks > MaxSegmentBlocks {
		return MigrationResult{}, fmt.Errorf("segment blocks %d exceeds maximum %d", opts.SegmentBlocks, MaxSegmentBlocks)
	}
	if opts.Compression == "" {
		opts.Compression = DefaultCompression
	}
	if err := ValidateCompression(opts.Compression); err != nil {
		return MigrationResult{}, err
	}
	manifestKey, err := ResolveManifestKey(opts.Prefix, opts.ChainID, opts.ManifestName, opts.ManifestKey)
	if err != nil {
		return MigrationResult{}, err
	}
	stateKey, err := MigrationStateKey(opts.Prefix, opts.ChainID, opts.ManifestName, opts.ManifestKey)
	if err != nil {
		return MigrationResult{}, err
	}
	if reader == nil {
		return MigrationResult{}, errors.New("block reader is required")
	}
	if store == nil {
		return MigrationResult{}, errors.New("object store is required")
	}
	start, end, err := ClampRange(reader, opts.StartHeight, opts.EndHeight)
	if err != nil {
		return MigrationResult{}, err
	}
	if validateErr := validateExistingMigrationManifest(ctx, store, manifestKey, opts.ChainID, start, end); validateErr != nil {
		return MigrationResult{}, validateErr
	}
	state, foundState, err := loadMigrationStateWithFound(ctx, store, stateKey)
	if err != nil {
		return MigrationResult{}, err
	}
	loadedLegacyState := false
	if !foundState && opts.ManifestKey == "" && (opts.ManifestName == "" || opts.ManifestName == DefaultManifest) {
		legacyStateKey := legacyMigrationStateKey(opts.Prefix, opts.ChainID)
		if legacyStateKey != stateKey {
			legacyState, foundLegacyState, legacyErr := loadMigrationStateWithFound(ctx, store, legacyStateKey)
			if legacyErr != nil {
				return MigrationResult{}, legacyErr
			}
			if foundLegacyState {
				state = legacyState
				loadedLegacyState = true
			}
		}
	}
	if state.ChainID == "" {
		state = MigrationState{
			ChainID:     opts.ChainID,
			StartHeight: start,
			EndHeight:   end,
			NextHeight:  start,
		}
	} else if state.ChainID != opts.ChainID || state.StartHeight != start || state.EndHeight != end {
		return MigrationResult{}, fmt.Errorf("existing migration state is for %s %d-%d, requested %s %d-%d",
			state.ChainID, state.StartHeight, state.EndHeight, opts.ChainID, start, end)
	}
	if loadedLegacyState {
		if saveErr := saveMigrationState(ctx, store, stateKey, state); saveErr != nil {
			return MigrationResult{}, saveErr
		}
	}

	result := MigrationResult{}
	for height := state.NextHeight; height <= end; {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return MigrationResult{}, ctxErr
		}
		last := height + int64(opts.SegmentBlocks) - 1
		if last > end {
			last = end
		}
		segment, uploaded, segErr := buildAndUploadSegment(ctx, reader, store, opts.ChainID, opts.Prefix, opts.Compression, height, last)
		if segErr != nil {
			return MigrationResult{}, segErr
		}
		if uploaded {
			result.Uploaded++
		} else {
			result.Reused++
		}
		state.Segments = appendSegment(state.Segments, segment)
		state.NextHeight = last + 1
		state.LastUpdatedAt = time.Now().UTC()
		if saveErr := saveMigrationState(ctx, store, stateKey, state); saveErr != nil {
			return MigrationResult{}, saveErr
		}
		height = last + 1
	}
	if verifyErr := verifyCompletedMigrationSegments(ctx, store, state.Segments); verifyErr != nil {
		return MigrationResult{}, verifyErr
	}
	manifest, err := NewManifest(opts.ChainID, state.Segments)
	if err != nil {
		return MigrationResult{}, err
	}
	if err := SaveManifest(ctx, store, manifestKey, manifest); err != nil {
		return MigrationResult{}, err
	}
	result.Manifest = manifest
	result.ManifestKey = manifestKey
	result.Segments = len(manifest.Segments)
	return result, nil
}

func validateExistingMigrationManifest(ctx context.Context, store ObjectStore, key, chainID string, start, end int64) error {
	manifest, err := LoadManifest(ctx, store, key)
	if errors.Is(err, ErrObjectNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	if manifest.ChainID != chainID {
		return fmt.Errorf("existing manifest %s is for chain %q, requested %q", key, manifest.ChainID, chainID)
	}
	if manifest.LastHeight == 0 {
		return nil
	}
	if manifest.FirstHeight != start || manifest.LastHeight != end {
		return fmt.Errorf("existing manifest %s is for range %d-%d, requested %d-%d", key, manifest.FirstHeight, manifest.LastHeight, start, end)
	}
	return nil
}

func verifyCompletedMigrationSegments(ctx context.Context, store ObjectStore, segments []SegmentManifest) error {
	for _, segment := range segments {
		info, err := store.Stat(ctx, segment.Key)
		if err != nil {
			return fmt.Errorf("stat completed segment %s: %w", segment.Key, err)
		}
		if info.Size != segment.SizeBytes {
			return fmt.Errorf("completed segment %s size %d, expected %d", segment.Key, info.Size, segment.SizeBytes)
		}
		data, err := store.Get(ctx, segment.Key)
		if err != nil {
			return fmt.Errorf("get completed segment %s: %w", segment.Key, err)
		}
		sum := sha256.Sum256(data)
		if hex.EncodeToString(sum[:]) != segment.SHA256 {
			return fmt.Errorf("completed segment %s checksum mismatch", segment.Key)
		}
		if _, err := DecodeSegment(data, segment); err != nil {
			return fmt.Errorf("decode completed segment %s: %w", segment.Key, err)
		}
	}
	return nil
}

func appendSegment(segments []SegmentManifest, segment SegmentManifest) []SegmentManifest {
	for i, existing := range segments {
		if existing.FirstHeight == segment.FirstHeight && existing.LastHeight == segment.LastHeight {
			segments[i] = segment
			return segments
		}
	}
	return append(segments, segment)
}

func legacyMigrationStateKey(prefix, chainID string) string {
	return fmt.Sprintf("%s/chains/%s/migration/state.json", cleanPrefix(prefix), chainID)
}

func loadMigrationState(ctx context.Context, store ObjectStore, key string) (MigrationState, error) {
	state, _, err := loadMigrationStateWithFound(ctx, store, key)
	return state, err
}

func loadMigrationStateWithFound(ctx context.Context, store ObjectStore, key string) (MigrationState, bool, error) {
	data, err := store.Get(ctx, key)
	if errors.Is(err, ErrObjectNotFound) {
		return MigrationState{}, false, nil
	}
	if err != nil {
		return MigrationState{}, false, err
	}
	var state MigrationState
	if err := json.Unmarshal(data, &state); err != nil {
		return MigrationState{}, false, err
	}
	if err := validateMigrationState(state); err != nil {
		return MigrationState{}, false, fmt.Errorf("invalid migration state %s: %w", key, err)
	}
	return state, true, nil
}

func validateMigrationState(state MigrationState) error {
	if state.ChainID == "" {
		return errors.New("chain ID is required")
	}
	if state.StartHeight <= 0 {
		return errors.New("start height must be positive")
	}
	if state.EndHeight < state.StartHeight {
		return fmt.Errorf("end height %d before start height %d", state.EndHeight, state.StartHeight)
	}
	if state.NextHeight < state.StartHeight || state.NextHeight > state.EndHeight+1 {
		return fmt.Errorf("next height %d outside migration range %d-%d", state.NextHeight, state.StartHeight, state.EndHeight)
	}
	if len(state.Segments) == 0 {
		if state.NextHeight != state.StartHeight {
			return fmt.Errorf("next height %d requires completed segments", state.NextHeight)
		}
		return nil
	}
	expectedFirst := state.StartHeight
	seenKeys := make(map[string]struct{}, len(state.Segments))
	for _, segment := range state.Segments {
		if err := segment.Validate(); err != nil {
			return err
		}
		if _, ok := seenKeys[segment.Key]; ok {
			return fmt.Errorf("duplicate segment key %q", segment.Key)
		}
		seenKeys[segment.Key] = struct{}{}
		if segment.FirstHeight != expectedFirst {
			return fmt.Errorf("segment range gap before height %d", expectedFirst)
		}
		expectedFirst = segment.LastHeight + 1
	}
	if state.NextHeight != expectedFirst {
		return fmt.Errorf("next height %d does not match completed segments ending at %d", state.NextHeight, expectedFirst-1)
	}
	return nil
}

func saveMigrationState(ctx context.Context, store ObjectStore, key string, state MigrationState) error {
	if err := ValidateObjectKey(key); err != nil {
		return fmt.Errorf("migration state key: %w", err)
	}
	if store == nil {
		return errors.New("object store is required")
	}
	if err := validateMigrationState(state); err != nil {
		return err
	}
	data, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		return err
	}
	data = append(bytes.TrimSpace(data), '\n')
	return store.Put(ctx, key, data)
}
