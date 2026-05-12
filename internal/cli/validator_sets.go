package cli

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/cometbft/cometbft/light/provider"
	providerhttp "github.com/cometbft/cometbft/light/provider/http"
	"github.com/cometbft/cometbft/types"
	"github.com/cosmos/gogoproto/proto"

	cmtproto "github.com/cometbft/cometbft/proto/tendermint/types"
)

func loadValidatorSetFiles(values []string) (map[int64]*types.ValidatorSet, error) {
	sets := make(map[int64]*types.ValidatorSet, len(values))
	for _, value := range values {
		ref, err := parseValidatorSetRef(value)
		if err != nil {
			return nil, err
		}
		data, err := os.ReadFile(ref.path)
		if err != nil {
			return nil, fmt.Errorf("read validator set %q: %w", ref.path, err)
		}
		var pb cmtproto.ValidatorSet
		if decodeErr := proto.Unmarshal(data, &pb); decodeErr != nil {
			return nil, fmt.Errorf("decode validator set %q: %w", ref.path, decodeErr)
		}
		vals, err := types.ValidatorSetFromProto(&pb)
		if err != nil {
			return nil, fmt.Errorf("validator set %q: %w", ref.path, err)
		}
		if _, ok := sets[ref.height]; ok {
			return nil, fmt.Errorf("duplicate validator set height %d", ref.height)
		}
		sets[ref.height] = vals
	}
	return sets, nil
}

type validatorSetRef struct {
	height int64
	path   string
}

func parseValidatorSetRef(value string) (validatorSetRef, error) {
	heightText, path, ok := strings.Cut(value, ":")
	if !ok {
		return validatorSetRef{}, fmt.Errorf("validator set %q must be in height:path form", value)
	}
	height, err := strconv.ParseInt(strings.TrimSpace(heightText), 10, 64)
	if err != nil {
		return validatorSetRef{}, fmt.Errorf("validator set %q has invalid height: %w", value, err)
	}
	if height <= 0 {
		return validatorSetRef{}, fmt.Errorf("validator set %q has invalid height %d", value, height)
	}
	path = strings.TrimSpace(path)
	if path == "" {
		return validatorSetRef{}, fmt.Errorf("validator set %q has empty path", value)
	}
	return validatorSetRef{height: height, path: path}, nil
}

type httpValidatorSetSource struct {
	provider provider.Provider
	timeout  time.Duration
}

func newHTTPValidatorSetSource(chainID, remote string, timeout time.Duration) (*httpValidatorSetSource, error) {
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	lightProvider, err := providerhttp.New(chainID, remote)
	if err != nil {
		return nil, err
	}
	return &httpValidatorSetSource{provider: lightProvider, timeout: timeout}, nil
}

func (s *httpValidatorSetSource) ValidatorSet(height int64) (*types.ValidatorSet, error) {
	ctx, cancel := context.WithTimeout(context.Background(), s.timeout)
	defer cancel()
	lightBlock, err := s.provider.LightBlock(ctx, height)
	if err != nil {
		return nil, err
	}
	return lightBlock.ValidatorSet, nil
}
