# Production Readiness Audit

This document maps `goal.md` to the current implementation evidence. The local
acceptance gate for this tranche is the real CometBFT e2e suite; a longer
external soak remains recommended before a specific chain deployment.

## Objective

Build `cometbft-archive` as a custom CometBFT blocksync-only archive peer that
can migrate local blockstore history into immutable S3-compatible range
segments, verify and inspect that archive, hydrate/cache blocks, stay near the
network head through P2P blocksync, archive future blocks after a safety delay,
serve both hot and cold blocks to other blocksync peers, and avoid consensus,
mempool, evidence, validator signing, state sync, and ABCI execution.

## Evidence Checklist

| Requirement | Evidence |
| --- | --- |
| Separate Go project named `cometbft-archive` | `go.mod` module `github.com/01builders/cometbft-archive`; CLI entrypoint `cmd/cometbft-archive/main.go`. |
| Depend on CometBFT instead of copying runtime | `go.mod` requires CometBFT and uses local replace to pinned sibling checkout; `Makefile` has `verify-cometbft` pin check. |
| Segment-based archive, not one object per block | `internal/archive/segment.go`, `manifest.go`, `migrate.go`; tests in `segment_test.go` and `migrate_integration_test.go`. |
| Manifest/index records chain, ranges, keys, compression, checksums, and block index | `internal/archive/manifest.go`; validation tests in `manifest_test.go`, `objectstore_test.go`, and `migrate_integration_test.go`. |
| Migration CLI reads existing local blockstore and uploads selected range | `internal/cli/root.go` `migrate`; `internal/archive/blockstore.go`; integration coverage in `migrate_integration_test.go` and CLI e2e tests. |
| Migration is resumable and idempotent | Migration state in `internal/archive/migrate.go`; tests cover resume, legacy default-state fallback, rerun, existing object reuse, missing segment detection, and invalid state. |
| Verification detects missing objects, checksum mismatch, corrupt segment data, and manifest inconsistency | `internal/archive/verify.go`; tests `TestVerifyDetectsMissingAndCorruptObjects`, `TestVerifyDetectsManifestInconsistency`, sampled/full verification tests. |
| CLI includes `migrate`, `verify`, `inspect`, `hydrate`, `archive-ready`, `prune-hot`, `soak`, and `serve` | `internal/cli/root.go`; help/e2e coverage in `e2e/cli_e2e_test.go` and `internal/cli/root_test.go`. |
| Custom blocksync-only peer without consensus/ABCI/mempool/evidence/state sync | `internal/blocksyncarchive/node.go`; tests verify only archive blocksync and optional PEX reactors are mounted. |
| P2P block sync can request, ingest, retry, fail over, and serve hot blocks | `planner.go`, `reactor.go`, `node.go`; tests in `planner_test.go`, `reactor_test.go`, and `node_test.go`; binary e2e includes syncing from a stock CometBFT blocksync reactor and from a normal CometBFT kvstore node. |
| Cold archive blocks are advertised and served, not just hot local blocks | `cold_source.go`, reactor cold path; tests cover merged hot+cold advertised range and P2P cold serving; binary e2e verifies `serve` can prune an old hot block then serve that height from cold object storage over blocksync. |
| Cold requests are bounded and slow S3 reads do not spawn unbounded workers | `--cold-workers`, bounded cold queue, metrics; tests cover queue cap, worker concurrency, active request metrics, and queue-full soak checks. |
| Archive future blocks after safety delay and prune only verified hot data | `internal/archive/live.go`, `prune.go`, live maintenance loops; tests cover safety window, incremental archive, publish-after-verify, verified prune, and corruption blocking prune; binary e2e verifies newly synced normal CometBFT kvstore blocks are archived by `serve` maintenance, pass archive verification, and can be served from cold storage after verified prune. |
| Object store supports local/mock and S3-compatible backends | `objectstore.go`, `s3_objectstore.go`; fake S3 endpoint tests cover operations, create-if-absent, namespace isolation, unsafe keys, strict S3 query parsing, prefix handling, and local symlink escape rejection. |
| Bounded memory for large archives | Segment block cap, 1 GiB encoded segment payload cap, decompression cap, S3 read cap, cache-size enforcement; tests cover caps and cache behavior. |
| Operator diagnostics and soak command | `/healthz`, `/readyz`, `/metrics`, `soak` command in `internal/cli`; tests cover readiness, metrics fields, head advance, error deltas, minimum cold-response proof, cold queue gates, and head-lag gate. |
| CometBFT core changes minimized and documented | No local CometBFT source changes in this repo; `docs/cometbft-interface-gaps.md` documents current dependency/interface posture. |
| No external bucket access without approval | Current tests use local filesystem and fake S3 endpoint only; deployment soaks against real object storage require explicit operator approval and configuration. |

## Verification Gates

Current local gates that have passed:

```sh
make fmt
make fmt-check
make e2e-real-cometbft
make test
make lint
make race
make verify-cometbft
go mod verify
go list ./...
```

## Deployment Soak

The real CometBFT e2e suite is the acceptance gate for this repository tranche.
Before using the archive peer for a specific production chain, operators should
still run an external soak against that chain's peers and intended object
storage backend. Use the checklist in `docs/operations.md`, including:

- real persistent peers on the target chain, if a real-chain soak is approved
- target object-store endpoint and credentials
- validation mode appropriate for the chain, preferably checkpoint or
  validator-set validation
- diagnostics enabled through `metrics_listen`
- `make production-soak SOAK_METRICS_URL=<metrics-url>` or
  `cometbft-archive soak` with head-lag, cold-error, cold-queue,
  archive-error, prune-error, and minimum cold-response gates
- at least one retention window so archived cold blocks are advertised and
  served after hot pruning
