COMETBFT_REF := 3b0311fc6a8c6b7024e3b1e226a5f9808ba3ccf1
GOFMT_FILES := $(shell find . -name '*.go' -not -path './vendor/*')
SOAK_METRICS_URL ?=
SOAK_DURATION ?= 24h
SOAK_INTERVAL ?= 30s
SOAK_MIN_PEERS ?= 1
SOAK_MAX_HEAD_LAG ?= 100
SOAK_MIN_COLD_RESPONSES_DELTA ?= 1
SOAK_MAX_COLD_QUEUE ?= 0
SOAK_MAX_BUFFERED_RESPONSES ?= 64

.PHONY: fmt fmt-check test e2e-real-cometbft production-soak lint race verify-cometbft

fmt:
	gofmt -w $(GOFMT_FILES)

fmt-check:
	test -z "$$(gofmt -l $(GOFMT_FILES))"

test:
	go test ./...

e2e-real-cometbft:
	go test ./e2e -run 'TestBinaryServeSyncs(FromRealCometBFTBlocksyncPeer|ToHeadFromNormalCometBFTKVNode)' -count=1

production-soak:
	test -n "$(SOAK_METRICS_URL)"
	go run ./cmd/cometbft-archive soak \
		--metrics-url "$(SOAK_METRICS_URL)" \
		--duration "$(SOAK_DURATION)" \
		--interval "$(SOAK_INTERVAL)" \
		--min-peers "$(SOAK_MIN_PEERS)" \
		--max-head-lag "$(SOAK_MAX_HEAD_LAG)" \
		--min-cold-responses-delta "$(SOAK_MIN_COLD_RESPONSES_DELTA)" \
		--max-cold-queue "$(SOAK_MAX_COLD_QUEUE)" \
		--max-buffered-responses "$(SOAK_MAX_BUFFERED_RESPONSES)"

lint:
	golangci-lint run ./...

race:
	go test -race ./internal/archive ./internal/blocksyncarchive ./internal/cli -count=1

verify-cometbft:
	test "$$(git -C ../cometbft rev-parse HEAD)" = "$(COMETBFT_REF)"
