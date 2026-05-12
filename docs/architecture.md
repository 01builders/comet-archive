# CometBFT Archive Architecture

## Goals

`cometbft-archive` runs an archive-capable CometBFT blocksync peer and migrates
CometBFT block data into immutable range segments backed by object storage. The
live process is a custom blocksync-only peer: it participates in CometBFT P2P
and the blocksync protocol so it can catch up, continue requesting newly
available heights from peers, and serve locally retained hot blocks plus
archived cold blocks to other peers through blocksync.

The live archive peer is not a normal CometBFT node. It does not participate in
consensus, does not run mempool or evidence reactors, does not sign validator
votes, and does not execute blocks through ABCI. The implementation reuses
CometBFT libraries for P2P transport, blocksync protobuf messages, block data
types, and local blockstore persistence, while owning the archive-specific
request planner, hot-store ingestion, serving policy, retention policy, and S3
archive worker. Archive behavior is layered on top of blocksync ingestion: the
worker tails the local hot blockstore or ingest stream and uploads sufficiently
old block ranges to object storage after a configured safety delay.

## Storage Layout

Object storage is treated as append-only for completed archive objects:

```text
chains/<chain-id>/segments/<first>-<last>-<sha256>.cba
chains/<chain-id>/manifests/manifest.json
chains/<chain-id>/migration/manifest.json.state.json
```

Segments contain bounded contiguous height ranges. Each segment is uploaded
only after its checksum is computed and recorded, and fresh uploads are read
back and checked before any manifest publishes them. Existing objects are
reused when the key and checksum match, making migration idempotent.
Segment object writes use create-if-absent semantics when the configured object
store supports them, so immutable segment keys are not silently overwritten by
concurrent or accidental uploads. Manifests and migration state remain mutable
because they are the archive index and resumability checkpoints.
Each archive namespace, defined by the object-store URL prefix, archive prefix,
chain ID, and manifest name, is therefore a single-writer target. Running two
live archive peers or migrations against the same manifest key can race on that
mutable high-water mark even though the segment objects themselves remain
immutable and checksum-addressed.

The implementation ships two object-store backends:

- `file://` for tests, dry-runs, and local development
- `s3://` for S3-compatible object storage using the AWS SDK credential chain

The object-store interface is narrow: `Get`, `Put`, `Exists`, `Stat`, and
`List`. The S3 backend maps those operations to `GetObject`, `PutObject`,
`HeadObject`, and `ListObjectsV2`. Endpoint override and path-style addressing
are available for MinIO and other S3-compatible services through store URL query
parameters:

```text
s3://bucket/root-prefix?region=us-east-1
s3://bucket/root-prefix?region=us-east-1&endpoint=http%3A%2F%2F127.0.0.1%3A9000&path_style=true
```

## Segment Format

Segments are binary files with a short uncompressed header followed by an
optional compressed payload:

```text
magic: "CBTASEG1"
payload: repeated length-prefixed block records
```

The default compression is `gzip` because it is in the Go standard library and
keeps the dependency set small. The manifest records the compression
algorithm per segment, so a future `zstd` implementation is format-compatible.

Each block record stores:

- height
- CometBFT block hash
- protobuf-encoded `types.Block`

Records are encoded with explicit lengths and checksummed at the segment level.
The verifier reconstructs records through CometBFT protobuf decoding and
validates height continuity, recorded block hashes, object size, and SHA-256.

## Manifest Format

The manifest is JSON and versioned. It records:

- archive format version
- chain ID
- inclusive first and last height
- creation/update timestamps
- segment list
- segment object keys
- compression algorithm
- first/last height per segment
- object size
- SHA-256 checksum
- per-block height and hash index

The manifest is the archive index. It is intentionally sufficient to verify the
archive without consulting CometBFT core, while block decoding still uses
CometBFT protobuf types.

## Migration Flow

`cometbft-archive migrate` is the offline backfill path. It opens an existing
CometBFT blockstore database, reads the selected height range, groups blocks
into bounded immutable segments, uploads each segment, and writes a manifest.

The command never prunes or mutates the source blockstore. A partial migration
leaves uploaded segment objects plus a local/object-store migration state file.
On resume, completed segments whose key and checksum match are skipped. If an
object exists but its checksum differs, migration fails rather than overwriting
data silently.

Migration options include source database directory/backend, chain ID, height
range, segment size in blocks, object-store URL, archive prefix, and manifest
name. `--manifest-key` can override the derived manifest object key; when it is
used, migration resume state is scoped beside that explicit key. For default
manifests, the reader also recognizes the original `migration/state.json` path
and rewrites progress to manifest-scoped state on resume. The default segment
size is 100 blocks, the implementation rejects values above 1000, and encoded
segment payloads are capped at 1 GiB to keep in-memory segment construction
bounded.

## Live Archive Flow

`cometbft-archive archive-ready` is the incremental archive path used by the
live blocksync peer and its maintenance loop. The custom blocksync ingestor
writes accepted blocks into the hot local blockstore/cache. Once a height is
older than the configured safety window, the live archive worker receives that
ready height and archives only the missing manifest continuation.

The manifest is the durable high-water mark for live archiving. On each run, the
worker starts at `manifest.LastHeight + 1` or the configured start height for a
new manifest, writes immutable segments up to the ready height, and saves the
manifest after each completed segment. If `--manifest-key` is set, the live
worker writes that explicit manifest key and uses the same key for cold serving
and verified pruning. Existing segment objects are reused when their size and
checksum match. This makes the live archive path restartable without relying on
a fixed final end height.

## Hot Retention And Pruning

`cometbft-archive prune-hot` enforces bounded local hot storage after archive
verification. It first runs a full archive verification for the selected
manifest. Only after the verifier confirms segment checksums, object sizes,
decoding, heights, and hashes does it compute a prune height from:

- the archive high-water mark, `manifest.LastHeight + 1`
- the configured local retention window, `head - retain_blocks + 1`

The command prunes to the lower of those two heights and delegates the actual
deletion to CometBFT's `BlockStore.PruneBlocks` API. It does not delete
blockstore keys directly. Evidence retention parameters may be overridden for
archive deployments, but the default path keeps CometBFT's default evidence
retention semantics.

`serve --dry-run=false` runs the same verified pruning path from the live
maintenance loop when `--prune-interval` is greater than zero. Maintenance
serializes archive upload and pruning for the local hot blockstore: it archives
ready blocks first, skips pruning for an empty or missing manifest, verifies the
archive before each prune attempt, and retains the latest `--retain-blocks`
local blocks subject to CometBFT evidence retention.
In the live peer, transient archive or prune errors do not stop P2P syncing or
hot block serving. The maintenance loop records the failure in metrics and
retries on the next interval. A failed archive upload prevents that interval's
prune attempt, and a failed archive verification prevents local pruning, so the
node keeps hot data rather than evicting blocks based on an unsafe archive.

Live mode can expose an HTTP diagnostics listener with `--metrics-listen`.
`/healthz` returns process liveness. `/readyz` returns success only when live
blocksync peer status shows at least one peer and a non-zero advertised best
height. `/metrics` returns JSON counters for archive runs, archive errors,
prune runs, prune errors, archived blocks, pruned blocks, last attempted and
successful maintenance timestamps, last archive/prune error messages, P2P peer
count, peer best height, blocksync inflight requests, buffered out-of-order
responses, request timeouts, hot/cold/no-block serving counters, cold serving
errors, active cold reads, cold queue depth, cold queue-full events, local hot
range, combined served range, next ingest height, and pending block height.
Readiness fails closed if the live P2P status metrics are absent or malformed.

`serve --config <file.json>` loads the same live-node settings from JSON for
repeatable deployments. Config files can provide object-store settings,
blockstore path/backend, P2P listen address and moniker, persistent peers,
request limits, request timeout, status polling interval, optional PEX
settings, diagnostics listen address, archive/prune intervals, retention
settings, segment settings, validation mode, checkpoints, validator-set files,
and an optional validator-set RPC source.
Explicit CLI flags override config values.

## Verification Flow

`cometbft-archive verify` loads the manifest and validates:

- manifest schema and range consistency
- contiguous non-overlapping segments
- required objects exist
- object sizes match
- segment SHA-256 checksums match
- segment records decode as CometBFT blocks
- record heights and hashes match the manifest
- full archive reads or deterministic sampling

The verifier detects missing objects, checksum mismatches, corrupt segment data,
and manifest inconsistencies. Full verification is the default.

## Inspect And Hydration

`inspect` prints manifest and segment summaries without reading every segment.
`hydrate` copies blocks for a selected range from object storage into a local
cache directory as immutable protobuf block files:

```text
<cache>/chains/<chain-id>/blocks/<height>.block
```

Hydration enforces a cache byte limit after each block write by removing oldest
hydrated files first.
The cache is a serving aid only; it is not a replacement for CometBFT's normal
blockstore and does not advertise itself as a full contiguous local chain.

## Live Blocksync Archive Model

`serve` starts or describes a blocksync-only archive peer with a custom
blocksync system. This is a deliberate library-level integration with CometBFT,
not a wrapped `cometbft node` process:

- it participates in CometBFT P2P networking and the blocksync channel
- it can optionally mount CometBFT's PEX reactor for peer discovery without
  mounting consensus or state execution reactors
- it requests blocks from peers using CometBFT-compatible blocksync messages
- it keeps requesting new heights as peer status advances instead of switching
  into consensus at the head
- it does not run the consensus reactor, mempool reactor, evidence reactor, or
  ABCI application connections
- it stores received block data in a bounded local hot blockstore/cache that can
  answer low-latency blocksync requests
- it uploads ranges older than a safety delay to object storage
- it advertises contiguous cold object-store coverage together with the hot
  blockstore range, so peers can request historical heights even after local
  hot pruning. The status loop periodically broadcasts the current served range
  as the archive manifest advances, so peers can learn newly available cold
  coverage without waiting for another ingest event
- it serves hot blocks first and falls back to the archive manifest/object
  store for cold heights; cold responses can be slower because they may require
  object-store reads
- it evicts local history only after archive upload and verification policies
  allow it

The archive layer must not weaken normal blocksync semantics. A status response
advertises one contiguous range. The archive peer therefore merges cold and hot
availability only when the manifest range and hot blockstore range overlap or
touch. If an operator creates a gap between cold and hot storage, the peer
advertises the higher useful contiguous range rather than claiming heights it
cannot serve.

The archive peer is meant to stay on the blocksync path permanently. It is not
a short-lived catch-up helper for a consensus node. After initial catch-up it
continues polling peer status, requests newly advertised heights, persists
accepted blocks to the hot store, archives heights older than the configured
safety window to object storage, and serves both retained hot blocks and
manifest-indexed cold blocks to other blocksync peers. That lets the process
sit near the network head while keeping local disk bounded and without ever
executing application state.

The stock CometBFT blocksync reactor is not a pure downloader today. In regular
node startup it verifies blocks against state, persists them, applies them
through the block executor, and then switches to consensus when caught up. A
blocksync-only archive peer therefore does not start a normal node or embed the
stock reactor unchanged. It uses the same P2P channel and message types so it
can interoperate with normal blocksync peers, but it replaces the lifecycle that
would otherwise hand off to consensus.

## Custom Blocksync System

The live archive node owns a custom blocksync system that speaks the normal
CometBFT blocksync channel and message types for hot-range syncing and serving.
It reuses stable CometBFT library pieces instead of copying the normal node
runtime or importing the stock node as a black box:

- P2P transport, switch, peer management, node keys, address book, and channel
  descriptors
- `tendermint/blocksync` protobuf message types
- block, part-set, commit, and blockstore encoding helpers
- CometBFT `BlockStore` APIs for local hot storage when compatible

The archive implementation owns the behavior that differs from a normal node:

- peer status tracking and source selection for archive ingestion
- request scheduling for catch-up and near-head polling
- block response handling, deduplication, and retry policy
- validation policy selection
- persistence into the bounded hot store/cache
- archive segment creation and S3 upload
- serving policy for local hot ranges and manifest-indexed cold ranges
- retention and eviction after upload verification

This custom system must not start consensus, mempool, evidence, state sync, or
ABCI services. It must not call `BlockExecutor.ApplyBlock`,
`ApplyVerifiedBlock`, or any path that mutates application state. Its only live
chain responsibility is to collect, store, archive, prune, and serve block data
according to the configured validation policy.

This choice keeps the integration boundary explicit. The code imports
CometBFT's transport, message, block, and storage APIs as libraries because
those define the network compatibility surface. It does not fork or duplicate
the whole normal node because that would reintroduce the consensus/state
execution lifecycle the archive peer is specifically avoiding, and would make
archive behavior harder to audit.

The current implementation includes the internal archive blocksync reactor and a
P2P node wrapper. The reactor exposes the blocksync channel descriptor, sends
local status, polls peers with `StatusRequest` messages, tracks peer status,
plans block requests, ingests block responses into the hot store, serves
locally available hot blocks, falls back to object storage for archived cold
blocks, and returns `NoBlockResponse` for heights absent from both stores. The
node wrapper builds a CometBFT P2P switch, mounts the custom
archive blocksync reactor, and can optionally mount PEX for peer discovery. It
deliberately omits consensus, mempool, evidence, state sync, and ABCI services.
Unanswered block requests expire after the configured request timeout. Timed
out peer/height pairs are treated as soft failures: the planner prefers another
advertising peer for that height, but retries the same peer if it is the only
source. Local real-switch coverage includes a peer that advertises a range but
never answers block requests; after timeout, the archive peer fetches the same
height from another provider. Timeout and no-block markers are pruned once the
archive peer advances past the affected height, so retry metadata remains
bounded by the active sync window rather than growing with total chain history.
When PEX is enabled, configuration must provide at least one seed or an address
book file; otherwise startup fails early because CometBFT's PEX reactor cannot
bootstrap from an empty address book.
When a peer advertises a height but answers `NoBlockResponse`, the planner
marks that peer/height pair unavailable and retries the height through another
advertising peer instead of repeatedly asking the same peer. The marker is
cleared if the peer later advertises a changed range, so periodic status polling
can recover from stale peer state.

`serve --dry-run=true` describes the live node contract without starting P2P
and without opening the configured object store. `serve --dry-run=false` opens
the object store and hot blockstore, then starts the custom
blocksync archive peer until interrupted. While serving, it also runs a periodic
archive loop that writes `head - safety_window` block ranges into immutable
archive segments and a periodic verified prune loop that bounds the local hot
blockstore after archive verification. In live mode, archive upload and verified
pruning run through the hot ingestor's store lock, serializing them with P2P
ingest and hot block serving. This avoids concurrent blockstore mutation during
prune, at the cost of making `archive_interval`, `prune_interval`, and
`segment_blocks` operational tuning knobs for large deployments.
Archive and prune failures are non-fatal to the live P2P process: they are
recorded in diagnostics and retried, while pruning remains gated by successful
archive verification.
The test suite includes a bounded local
soak of this custom path: one archive peer serves locally retained blocks,
another archive peer syncs through the custom blocksync reactor, periodic
maintenance archives safety-windowed heights into object storage, archive
verification runs, and verified pruning advances the consumer's hot-store base.
There is also a head-following test where a connected provider advances after
initial sync and the archive peer discovers the new height through periodic
blocksync status polling.
It also includes stock CometBFT blocksync compatibility in two forms: direct
reactor tests for both directions of the shared `BlockRequest`/`BlockResponse`
serving path, and a loopback P2P switch test where an archive peer dials a
stock CometBFT blocksync reactor and syncs blocks over the real blocksync
channel handshake. The binary e2e suite also starts a normal CometBFT node with
the built-in kvstore app, lets it produce blocks through the normal node
runtime, then starts `cometbft-archive serve` and verifies the archive peer
blocksyncs near that node's head, archives safety-windowed synced blocks through
the live maintenance loop, verifies the resulting object-store manifest, prunes
old hot storage after verification, and still serves a pruned height from cold
object storage over blocksync.
Restart behavior is covered by a loopback test that seeds
an existing hot blockstore, starts a fresh archive ingestor, and verifies that
it resumes P2P sync from the next persisted height instead of starting over.
Hot and cold block serving are covered by real-switch requester tests that dial
an archive node and receive `BlockResponse` messages for a locally retained
height and a manifest-indexed object-store height over the blocksync channel.
Peer failover is covered by a real P2P test where one peer advertises a height
but returns `NoBlockResponse`, after which the archive request planner fetches
that height from another advertising peer.

Remaining serving work is production hardening around the running node:
long-running network soak testing against normal CometBFT nodes, multi-provider
head-following under churn, and operational tuning for PEX/seed deployments.
Local P2P provider churn is covered by a real-switch test where the archive
peer is connected to two providers, one provider drops after initial sync, the
other provider advances, and the archive peer continues following the new head
through blocksync status polling.

### Head Following

Normal blocksync is designed as a catch-up mechanism that eventually hands off
to consensus. The archive blocksync system keeps running instead:

1. On peer connect or periodic status update, record each peer's advertised
   `[base, height]`.
2. Request missing heights from the lowest needed local height up to the best
   advertised peer height.
3. Persist accepted blocks contiguously in the hot store/cache.
4. When caught up to the current best advertised height, continue polling peer
   status and request newly advertised heights as they appear.
5. Archive heights older than `head - safety_window` into immutable segments.

The node advertises the contiguous range it can serve from hot storage plus the
archive manifest. Hot blocks are served synchronously from the local blockstore.
Cold blocks are served by loading the matching archive segment from object
storage and decoding the requested height. Because the blocksync status message
can only describe one `[base, height]` range, disjoint cold/hot coverage is not
merged into a misleading advertised range.

Validation policy is explicit:

- a storage-only mirror can persist peer-supplied blocks after basic decoding,
  height continuity, hash, and commit continuity checks. A block is persisted
  only after the next block's `LastCommit` has the expected height and references
  the pending block hash and part-set header
- checkpoint validation can pin trusted block hashes at selected heights through
  `serve --validation=checkpoint --checkpoint <height>:<hexhash>` and reject
  divergent blocks during ingestion
- validator-set validation can pin protobuf-encoded trusted validator sets at
  selected heights through
  `serve --validation=validator-set --validator-set <height>:<validators.pb>`.
  When block `H+1` arrives, the archive ingestor verifies that `H+1.LastCommit`
  commits block `H` with +2/3 voting power from the trusted validator set for
  height `H` before persisting block `H`
- `serve --validator-set-rpc <rpc-url>` can fetch missing validator sets on
  demand from a trusted CometBFT RPC endpoint. The HTTP provider validates
  response shape and chain ID, but RPC-only validator-set mode is still
  trust-on-RPC rather than independent light-client verification
- without state execution, checkpoints, or an external validator-set source, the
  archive node cannot independently track validator set changes across the
  chain
- the current validator-set mode is intentionally explicit: operators must
  provide trusted validator sets as files or configure a validator-set RPC
  source, and `serve` reports the resulting `validation_trust_model` in its
  startup contract output

## CometBFT Core Changes

No CometBFT core changes are required for the current archive implementation.
Offline migration uses exported blockstore APIs (`store.NewBlockStore`,
`LoadBlock`, `Base`, `Height`) and CometBFT protobuf conversion APIs.

The live archive peer reuses CometBFT P2P and blocksync protocol components as
libraries, but it does not embed consensus or ABCI execution.
Interface gaps for that tranche are documented separately in
[cometbft-interface-gaps.md](cometbft-interface-gaps.md).
