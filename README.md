# cometbft-archive

Experimental tooling for running an archive-capable CometBFT blocksync peer and
migrating immutable block ranges into S3-compatible object storage.

## Overview

The live peer is a blocksync-only process. It does not run consensus, state
execution, mempool, evidence, validator signing, or ABCI connections. It
embeds CometBFT's P2P transport, blocksync wire messages, block types, and
blockstore APIs as libraries, and owns the archive-specific request planner,
hot-store ingestion, serving policy, and S3 archival loop.

Responsibilities:

- Participate in P2P blocksync and follow the advertised head.
- Persist hot blocks locally with bounded retention.
- Archive immutable ranges to object storage after a safety delay.
- Serve hot blocks and manifest-indexed cold archive blocks to peers.
- Verify archived ranges before pruning the hot blockstore.

Archive segment objects are immutable. Existing objects are reused only when
size and checksum match, uploads use create-if-absent semantics, and fresh
uploads are read back and verified before the manifest advertises them.

See [docs/architecture.md](docs/architecture.md) for the storage layout and
protocol model, [docs/operations.md](docs/operations.md) for preflight,
metrics, and soak guidance.

## Validation modes

Live ingest defaults to `--validation=storage-only`. Stronger modes:

- `--validation=checkpoint --checkpoint <height>:<hexhash>` pins trusted block
  hashes at selected heights.
- `--validation=validator-set --validator-set <height>:<validators.pb>`
  verifies the next block's `LastCommit` against an operator-supplied
  `tendermint.types.ValidatorSet`.
- `--validator-set-rpc <rpc-url>` fetches missing validator sets from a
  trusted CometBFT RPC endpoint on demand. RPC-only mode is trust-on-RPC; use
  pinned files or checkpoints when independent rejection is required.

## Metrics and health

`--metrics-listen <host:port>` exposes `/healthz`, `/readyz`, and `/metrics`.
`/healthz` is process liveness; `/readyz` requires live peer connectivity and
a non-zero advertised provider height. Metrics cover archive/prune run and
error counts, last error strings, archived/pruned block totals, last
attempted/successful maintenance times, P2P peer count, peer best height,
blocksync inflight requests, buffered out-of-order responses, request
timeouts, local hot range, combined served range, hot/cold/no-block serving
counters, cold serving errors, active cold reads, cold queue depth and
queue-full events, next ingest height, and pending block height.

## Serve config

`serve --config <file.json>` loads settings from JSON; CLI flags override.
See [serve.config.example.json](serve.config.example.json) for a full
example. `segment_blocks` defaults to `100`, is capped at `1000`, and encoded
segment payloads are capped at 1 GiB.

## Other commands

- `prune-hot` enforces local retention only after a full archive
  verification succeeds, then delegates pruning to the blockstore API.
  `serve` uses the same verified path when `--prune-interval > 0`.
- `soak` polls a running `serve` diagnostics endpoint and fails on readiness
  drops, low peer count, stalled peer head, or rising archive/prune/cold
  serving errors. Use `--min-cold-responses-delta` to require evidence that
  peers received manifest-backed cold responses.

## Object store URLs

```text
file:///tmp/archive-objects
s3://bucket/root-prefix?region=us-east-1
s3://bucket/root-prefix?region=us-east-1&endpoint=http%3A%2F%2F127.0.0.1%3A9000&path_style=true
```

The S3 backend uses the standard AWS SDK credential chain. Do not point it at
a real bucket unless you intend to write archive objects there.

## Development

This module currently uses the sibling CometBFT checkout declared in
`go.mod`:

```text
replace github.com/cometbft/cometbft => ../cometbft
```

Use CometBFT commit `3b0311fc6a8c6b7024e3b1e226a5f9808ba3ccf1` for local and
CI builds until this project is cut over to a tagged CometBFT release.

```text
make fmt
make fmt-check
make verify-cometbft
make e2e-real-cometbft
make test
make lint
make race
```

For an operator-approved deployment soak against a running `serve` process:

```text
make production-soak SOAK_METRICS_URL=http://127.0.0.1:26660
```

`make lint` runs `golangci-lint` with the strict rule set in `.golangci.yml`.
