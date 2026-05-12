# cometbft-archive

Experimental tooling for running an archive-capable CometBFT blocksync peer and
migrating immutable block ranges into S3-compatible object storage.

The live peer is a custom blocksync-only process. It does not start a normal
CometBFT node, and it does not run consensus, state execution, mempool,
evidence, validator signing, or ABCI application connections. Instead it pulls
in CometBFT's P2P transport, blocksync wire messages, block types, and
blockstore APIs as libraries, then owns the archive-specific request planner,
hot-store ingestion, serving policy, and S3 archival loop. This is the intended
runtime model: participate in P2P blocksync, stay near the advertised head,
persist hot blocks, archive blocks to object storage after a safety delay, and
offer both locally retained hot blocks and manifest-indexed cold archive blocks
back to other blocksync peers.
Archive segment objects are treated as immutable: existing objects are reused
only when their size and checksum match, and S3-compatible uploads use
create-if-absent requests for segment data. Fresh segment uploads are read back
and checked before the manifest can advertise them to peers.

The archive peer deliberately does not embed the stock CometBFT node or copy
the normal node runtime wholesale. The stock blocksync lifecycle is tied to
state execution and a handoff into consensus after catch-up. The archive peer
keeps only the protocol-compatible blocksync pieces it needs and replaces that
lifecycle with archive ingestion, verified upload, bounded retention, and hot
range serving.

This implementation includes resumable migration, manifest-based inspection,
archive verification, hydration into a bounded local cache, incremental archiving of
safety-windowed hot blockstore ranges, verified pruning of archived hot
blockstore ranges, a local filesystem backend for tests, and an S3-compatible
backend. The `serve` command can run in dry-run mode to describe the custom
blocksync archive contract without opening the configured object store, or in
live mode to start a CometBFT P2P switch with only the archive blocksync
reactor mounted. The live peer participates in CometBFT P2P/blocksync, follows
the advertised head by continuing to poll peer status after catch-up, serves
locally retained hot blocks and cold object-store blocks to other blocksync
peers, archives ready block ranges to object storage, and periodically prunes
verified archived blocks from the hot blockstore.

Live ingest defaults to `--validation=storage-only`. Operators can pin trusted
block hashes at selected heights with
`--validation=checkpoint --checkpoint <height>:<hexhash>`, or verify commits
against operator-supplied validator sets with
`--validation=validator-set --validator-set <height>:<validators.pb>`. The
validator set file is a protobuf-encoded `tendermint.types.ValidatorSet`, and
the archive peer verifies the next block's `LastCommit` before persisting the
committed block at that height. `--validator-set-rpc <rpc-url>` can fetch
missing validator sets from a trusted CometBFT RPC endpoint on demand. RPC-only
validator-set mode is trust-on-RPC; use pinned validator-set files or
checkpoints when the archive peer must reject a faulty provider independently.

Live mode can expose `/healthz`, `/readyz`, and `/metrics` by setting
`--metrics-listen <host:port>`. `/healthz` is process liveness; `/readyz`
requires live blocksync peer connectivity and a non-zero advertised provider
height. Metrics report archive/prune run counts, error counts, last
archive/prune error strings, archived blocks, pruned blocks, last attempted and
successful maintenance times, P2P peer count, peer best height, blocksync inflight
requests, buffered out-of-order responses, blocksync request timeouts, local
hot range, combined served range, hot/cold/no-block serving counters, cold
serving errors, active cold reads, cold queue depth, cold queue-full events,
next ingest height, and pending block height.
`serve --config <file.json>` can load the live-node settings from JSON; explicit
CLI flags override config values.

Example `serve` config:

```json
{
  "store": "s3://bucket/root-prefix?region=us-east-1",
  "chain_id": "chain-mainnet",
  "db_dir": "/var/lib/cometbft-archive/data",
  "db_backend": "goleveldb",
  "p2p_listen": "tcp://0.0.0.0:26656",
  "moniker": "archive-peer-1",
  "node_key_file": "/var/lib/cometbft-archive/config/node_key.json",
  "persistent_peers": ["aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa@host:26656"],
  "request_limit": 32,
  "cold_workers": 8,
  "cold_manifest_cache_ttl": "5s",
  "request_timeout": "30s",
  "status_request_interval": "10s",
  "pex": true,
  "addr_book_file": "/var/lib/cometbft-archive/config/addrbook.json",
  "addr_book_strict": true,
  "seeds": ["bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb@seed-host:26656"],
  "private_peer_ids": [],
  "metrics_listen": "127.0.0.1:26660",
  "safety_window": 100,
  "archive_interval": "30s",
  "prune_interval": "5m",
  "retain_blocks": 1000,
  "segment_blocks": 100,
  "compression": "gzip",
  "validation": "storage-only",
  "validator_sets": [],
  "validator_set_rpc": "",
  "validator_set_timeout": "5s",
  "dry_run": false
}
```

`segment_blocks` defaults to `100` and is capped at `1000`; encoded segment
payloads are capped at 1 GiB to keep construction and verification bounded in
memory.

`prune-hot` enforces local retention only after a full archive verification
succeeds, then delegates pruning to CometBFT's blockstore API. `serve` uses the
same verified pruning path when `--prune-interval` is greater than zero.
`soak` polls a running `serve` diagnostics endpoint and fails if readiness
drops, peer count is too low, peer head height does not advance, or live
archive/prune/cold-serving errors increase during the check. Production soaks
that exercise pruned history can set `--min-cold-responses-delta` to require
proof that peers received manifest-backed cold block responses.

Object store URLs:

```text
file:///tmp/archive-objects
s3://bucket/root-prefix?region=us-east-1
s3://bucket/root-prefix?region=us-east-1&endpoint=http%3A%2F%2F127.0.0.1%3A9000&path_style=true
```

The S3 backend uses the standard AWS SDK credential chain. Do not point it at a
real bucket unless you intend to write archive objects there.

See [docs/architecture.md](docs/architecture.md) for the storage layout and
protocol model, [docs/operations.md](docs/operations.md) for live peer
preflight, metrics, and soak guidance, and
[docs/production-readiness-audit.md](docs/production-readiness-audit.md) for
the goal-to-evidence checklist.

## Development

This module currently uses the sibling CometBFT checkout declared in
`go.mod`:

```text
replace github.com/cometbft/cometbft => ../cometbft
```

Use CometBFT commit `3b0311fc6a8c6b7024e3b1e226a5f9808ba3ccf1` for local and
CI builds until this archive project is cut over to a tagged CometBFT module
release.

```text
make fmt
make fmt-check
make verify-cometbft
make e2e-real-cometbft
make test
make lint
make race
```

For an operator-approved deployment soak against a running `serve` process, use:

```text
make production-soak SOAK_METRICS_URL=http://127.0.0.1:26660
```

`make lint` runs `golangci-lint` with the repository's explicit strict rule set
in `.golangci.yml`.
