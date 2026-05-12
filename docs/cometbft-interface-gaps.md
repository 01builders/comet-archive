# CometBFT Interface Gaps

No CometBFT core changes are required by the current archive implementation.

The live archive node tranche participates in CometBFT P2P and blocksync
without running consensus, mempool, evidence, or ABCI execution. It uses a
custom blocksync system that speaks the normal CometBFT blocksync channel and
message types for hot-range syncing plus hot and cold archive serving.

The stock blocksync reactor currently assumes a normal node lifecycle: it
validates blocks against state, applies them through the block executor, and
switches to consensus when caught up. That reactor is useful reference material,
but it is not the archive node runtime. The archive peer should pull CometBFT
P2P, protobuf message, block, and blockstore APIs in as libraries, then provide
its own request planner, ingestion loop, hot/cold serving policy, and archive
maintenance loop.

Future archive-serving work would benefit from upstreamable interfaces:

- a stable read-only `BlockReader` interface covering `Base`, `Height`,
  `LoadBlock`, `LoadBlockMeta`, and close semantics
- a stable P2P switch construction helper for custom reactor sets
- exported blocksync protocol helpers for peer status tracking, request
  scheduling, response decoding, and request retry without coupling to state
  execution
- stable hooks/interfaces for serving blocksync requests from bounded local hot
  storage and slower archive object storage
- explicit validation policy hooks beyond the currently local storage-only,
  checkpoint-verified, operator-supplied validator-set, and RPC-sourced
  validator-set modes, especially stronger provider diversity and bisection
  support
- retention/pruning hooks that allow archive verification to gate eviction of
  old local blocks
- explicit P2P capability negotiation for deep archive range serving, separate
  from normal hot-cache blocksync
- bounded asynchronous archive range request plumbing for cold object-store
  ranges
- optional exported helpers for opening configured blockstore databases outside
  the main `cometbft` binary

These are integration seams for an archive blocksync peer, not consensus rule
changes.
