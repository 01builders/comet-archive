# CometBFT Archive Operations

This runbook is for running `cometbft-archive serve` as a custom
blocksync-only archive peer. The process uses CometBFT P2P and blocksync
libraries, but it does not start a normal CometBFT node and does not run
consensus, mempool, evidence, validator signing, state sync, or ABCI execution.

## Preflight

Run a dry-run before starting the peer:

```text
cometbft-archive serve --config /etc/cometbft-archive/serve.json --dry-run=true
```

Use [serve.config.example.json](serve.config.example.json) as the starting
point for `/etc/cometbft-archive/serve.json`, replacing the bucket, chain ID,
database path, node key path, persistent peers, and validation source for the
target chain.

Dry-run validates the serve config, including validation mode, PEX bootstrap
settings, P2P and metrics listen addresses, request limits, intervals,
retention, segment size, local validator-set files, and validator-set timeout.
It does not open the configured object store and does not start P2P.

Recommended production config choices:

- set a persistent `node_key_file` so the peer ID is stable across restarts
- set a clear `moniker`
- configure at least one `persistent_peers` entry
- tune `request_limit` as the outbound sync pipeline window. The archive peer
  can buffer out-of-order block responses within this window while preserving
  ordered hot-store ingestion. Values above `1024` are rejected to keep request
  planning, response buffering, and cold queue memory bounded
- tune `request_timeout` above normal network round-trip and disk read latency
- tune `cold_workers` to the object-store latency and throughput budget; `0`
  uses `request_limit`, while a smaller explicit value caps concurrent S3-backed
  historical block reads without reducing outbound sync planning
- tune `cold_manifest_cache_ttl` to balance archive-range freshness against
  repeated manifest reads; `0s` disables the cache for maximum freshness
- keep `segment_blocks` at the default `100` unless migration throughput testing
  justifies a higher value; the process rejects values above `1000` to keep
  segment construction bounded in memory, and encoded segment payloads are
  capped at 1 GiB
- if `pex` is true, configure at least one `seeds` entry or an `addr_book_file`
- use `validation=checkpoint` or `validation=validator-set` for stronger live
  ingest checks; `storage-only` is a mirror mode and cannot independently track
  validator-set changes
- treat `validation_trust_model=rpc-trusted-validator-set-source` as
  trust-on-RPC. For stronger fault isolation, provide pinned validator-set
  files with `validator_sets` or trusted checkpoint hashes
- set `metrics_listen` on a private interface or behind an authenticated
  metrics collector
- keep `retain_blocks` comfortably larger than the safety window and larger
  than the expected operational recovery window
- run only one writer for a given archive namespace. The namespace is the
  combination of object-store URL prefix, archive `prefix`, `chain_id`, and
  `manifest_name`. Extra archive peers may use distinct manifests or read-only
  verification, but two live writers must not publish the same manifest key
- if `manifest_key` or `--manifest-key` is used, use that same explicit key for
  migration, live serving, verification, cold serving, and pruning. Writer
  commands publish to the explicit key rather than the derived
  `prefix/chains/<chain-id>/manifests/<manifest-name>` path. `serve` still
  requires `chain_id` because it is the P2P network ID and live ingest chain
  check

## Start

Start the live peer with:

```text
cometbft-archive serve --config /etc/cometbft-archive/serve.json --dry-run=false
```

At startup the command prints JSON containing the node ID, listen address,
archive range, config knobs, and `consensus_node: false`. The process then runs
until interrupted.

## Metrics

`/healthz` reports process liveness. `/readyz` reports live blocksync
readiness and returns HTTP 503 until the peer has at least one P2P peer and a
non-zero advertised provider height. `/metrics` reports JSON counters and
gauges:

- `archive_runs`, `archive_errors`, `blocks_archived`,
  `last_archive_attempt_unix_nano`, `last_archive_success_unix_nano`
- `prune_runs`, `prune_errors`, `blocks_pruned`,
  `last_prune_attempt_unix_nano`, `last_prune_success_unix_nano`
- `last_archive_error`, `last_prune_error`
- `p2p_peers`
- `peer_best_height`
- `blocksync_inflight_requests`
- `blocksync_buffered_responses`
- `blocksync_request_timeouts`
- `blocksync_hot_responses`
- `blocksync_cold_responses`
- `blocksync_no_block_responses`
- `blocksync_cold_errors`
- `blocksync_cold_active`
- `blocksync_cold_queue`
- `blocksync_cold_queue_full`
- `hot_base`, `hot_height`
- `served_base`, `served_height`
- `next_height`
- `pending_height`

Useful checks:

- `p2p_peers` should become non-zero after peer dialing or PEX discovery
- `peer_best_height` should advance with the network
- `next_height` should approach `peer_best_height + 1`
- `hot_height` should advance as blocks are persisted
- `served_base` and `served_height` should cover the contiguous range the peer
  advertises through blocksync, including manifest-indexed cold storage when it
  touches or overlaps the hot range. This range should update after archive
  maintenance advances the manifest and the status interval elapses
- `last_archive_attempt_unix_nano` and `last_prune_attempt_unix_nano` should
  continue advancing while maintenance is retrying, even if the last success
  timestamp is old
- `last_archive_error` and `last_prune_error` should be empty after recovery
- if a provider stalls, unanswered requests should retry after
  `request_timeout`; `blocksync_request_timeouts` should increase and then
  stabilize after healthy providers answer
- `blocksync_inflight_requests` should not remain pinned at the request limit
  while `peer_best_height` keeps advancing
- `blocksync_buffered_responses` should not remain pinned near `request_limit`;
  sustained buffering means future-height responses are arriving while earlier
  heights are stalled
- `blocksync_cold_responses` should increase when peers request pruned
  historical heights that are still covered by the archive manifest
- `blocksync_cold_errors` should remain stable; increases indicate object-store
  read, manifest, or segment decode failures while serving cold blocks
- `blocksync_cold_active` shows object-store-backed cold reads currently being
  served by the bounded worker pool
- `blocksync_cold_queue` should not remain pinned at the request limit; a
  sustained full queue means peers are asking for cold history faster than the
  object store can serve it
- `blocksync_cold_queue_full` should remain stable; increases mean cold
  requests were left for peer-side timeout/retry instead of receiving an
  immediate false `NoBlockResponse`
- `archive_errors` and `prune_errors` should remain stable after startup
- `hot_base` should advance only after successful archive verification and
  pruning

The live maintenance loop treats archive and prune errors as retryable. It
records the error counters and retries on the next interval. Failed archive
upload skips pruning for that interval, and failed archive verification prevents
pruning, preserving local hot data.

## External Soak Checklist

Before declaring a deployment production-ready for a chain, run a soak against
normal CometBFT nodes:

1. Start with at least two normal full-node peers in `persistent_peers`.
2. Confirm `p2p_peers > 0` and `peer_best_height` advances.
3. Confirm `next_height` follows the peer best height after initial catch-up.
4. Confirm blocks are archived after `safety_window` and `archive_interval`.
5. Confirm `verify` succeeds against the live manifest.
6. Confirm `hot_base` advances only after verified prune runs.
7. Restart the archive peer and confirm it resumes from the existing hot store.
8. Drop one provider and confirm the peer keeps syncing from another provider.
9. Force one transient object-store failure and confirm errors are counted,
   syncing continues, and the next interval retries.
10. Leave the peer running for at least one retention window and confirm cold
    object-store ranges remain advertised and requestable after hot pruning.

The local test suite covers a bounded form of step 8 with real loopback P2P
switches. The external soak still needs to be run for each target chain because
network latency, peer behavior, PEX seed quality, and provider diversity are
deployment-specific.

Any failure in this checklist should be treated as a deployment blocker or as
input for chain-specific tuning of peers, PEX, request limits, archive interval,
prune interval, safety window, and retain blocks.

The `soak` command automates the live diagnostics portion of the checklist
against a running `serve --dry-run=false` process:

```text
cometbft-archive soak --metrics-url http://127.0.0.1:26660 --duration 24h --interval 30s --max-head-lag 100
```

It fails if `/readyz` is not continuously ready, `p2p_peers` stays below the
configured minimum, `peer_best_height` does not advance, archive/prune error
counters increase beyond the allowed deltas, or the last archive/prune error
strings remain set at the end of the run. It also fails by default if
`blocksync_cold_errors` or `blocksync_cold_queue_full` increase. Operators can
add `--max-cold-queue <n>` to fail the soak when object-store-backed cold
serving backs up beyond an acceptable queue depth,
`--max-buffered-responses <n>` to fail when out-of-order blocksync responses
accumulate beyond the configured sync window budget, and `--max-head-lag <n>`
to fail when `next_height` falls too far behind `peer_best_height`. For a soak
that actively exercises pruned history, add `--min-cold-responses-delta <n>` so
the run fails unless peers receive at least that many manifest-backed cold block
responses. It does not replace manual checks for object-store contents,
restart behavior, or provider diversity; use it as the repeatable gate inside
the broader soak.
