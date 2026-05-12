# CometBFT Archive Node And S3 Storage MVP

  ## Objective

  Design and build an initial `cometbft-archive` project that can run as an archive-capable CometBFT blocksync peer with a custom blocksync system, migrate local CometBFT blockstore data into immutable S3-compatible object storage, verify the archive, and lay the foundation for serving blocks from a bounded local cache backed by object storage.

  ## Context

  CometBFT currently stores blocks in a local KV block store such as RocksDB, GoLevelDB, or PebbleDB. The current `BlockStore` model assumes all blocks between `base` and `height` are locally contiguous, and block sync serves peers by loading blocks synchronously from local storage.

  The desired architecture is different: long-term block history should be offloaded to S3-compatible object storage, while an archive-capable blocksync peer uses a custom blocksync system to participate in CometBFT P2P and block sync, catch up, and stay near the head. It should keep only a bounded local hot cache, for example around 10 GB, and archive older block ranges after a configured safety delay. It should not run consensus, mempool, validator signing, or ABCI execution.

  ## Feature Requirements

  - Create or scaffold a separate experimental repository or project named `cometbft-archive`, unless repo constraints require starting as a subdirectory or design doc first.
  - Depend on CometBFT as a Go module where possible instead of copying large portions of CometBFT.
  - Implement an archive storage design based on immutable range segments, not one object per block.
  - Include a manifest/index format that records chain ID, height ranges, object keys, compression, checksums, and enough metadata to verify archive integrity.
  - Build a migration CLI that reads an existing local CometBFT blockstore and uploads block history to S3-compatible object storage without requiring a resync.
  - Make migration resumable and idempotent.
  - Include archive verification commands that can validate manifests, segment checksums, and sampled or full block reads.
  - Define the P2P archive-node design clearly, including the custom blocksync system, how it participates in CometBFT block sync without consensus or ABCI execution, and how it avoids advertising unavailable cold blocks as locally available.
  - Prefer an MVP implementation order of migration and verification first, then archive serving and P2P.

  ## Acceptance Criteria

  - There is a clear architecture document covering storage layout, manifest/index format, upload flow, verification flow, cache/hydration behavior, and P2P serving model.
  - A CLI shape exists for at least:
    - `cometbft-archive migrate`
    - `cometbft-archive verify`
    - `cometbft-archive inspect`
    - `cometbft-archive hydrate` or equivalent
    - `cometbft-archive archive-ready` or equivalent live archive worker entry point
    - `cometbft-archive serve` for the custom blocksync archive peer, with dry-run contract output and live P2P mode
  - The migration command can read a local CometBFT blockstore path and export a selected height range.
  - The archive format is segment-based and supports resume after partial failure.
  - The verifier can detect missing objects, checksum mismatches, corrupt segment data, and manifest inconsistencies.
  - The implementation avoids running or changing CometBFT consensus while allowing the archive-capable node to run P2P and block sync.
  - Any required changes to CometBFT core are minimized and documented as upstreamable interface gaps.

  ## Scope

  - Architecture and design for S3-backed archive storage.
  - Initial Go project structure for `cometbft-archive`.
  - Object-store abstraction for S3-compatible backends.
  - Local filesystem or mock object-store backend for tests.
  - Migration CLI from existing local CometBFT blockstore data.
  - Archive manifest and segment writer/reader.
  - Verification tooling.
  - Initial implementation for an archive-capable blocksync peer with a custom blocksync system that reuses CometBFT P2P, blocksync messages, and blockstore persistence without consensus or ABCI execution.

  ## Non-Goals

  - Do not replace CometBFT’s normal consensus node block store in this MVP.
  - Do not run or modify consensus rules, validator signing behavior, or ABCI execution.
  - Do not require a node to resync in order to populate S3.
  - Do not design around one S3 object per block unless a benchmark proves it is necessary.
  - Do not implement production-grade cloud deployment, monitoring, or autoscaling in the first tranche.
  - Do not use real S3 credentials or write to external buckets without explicit approval.

  ## Assumptions

  - The archive project should start outside CometBFT core unless implementation discovers unavoidable unexported internals.
  - S3-compatible storage is the target, but the implementation should test against a local/mock object store first.
  - Range segments are the preferred storage primitive.
  - Compression should default to zstd if dependency and licensing fit the repo; otherwise choose a pragmatic standard alternative and document the tradeoff.
  - The first useful milestone is migration plus verification, before full P2P serving.
  - The archive node is an archive-capable blocksync peer with custom blocksync behavior, not a consensus node.

  ## Constraints

  - Follow repository AGENTS.md instructions when present.
  - Preserve unrelated user changes.
  - Prefer small, reversible changes.
  - Do not claim checks passed unless they were actually run.
  - Keep CometBFT core changes minimal and explicitly justified.
  - Avoid destructive file operations without approval.
  - Do not use real cloud credentials or access non-local services without approval.
  - Keep persistence semantics explicit: never prune local data based only on a failed or unverified upload.
  - Design for large archives: bounded memory, resumable operations, and no whole-chain loading into memory.

  ## Execution Approach

  1. Inspect the current CometBFT blockstore, blocksync, node startup, and CLI patterns enough to identify reusable APIs and interface gaps.
  2. Decide whether to create a new sibling repo/project or start with a design/scaffold inside the current workspace, stopping for approval if filesystem permissions or product direction require it.
  3. Draft the architecture document first, including storage layout, manifest format, segment format, migration lifecycle, verification lifecycle, cache/hydration model, live blocksync participation, and P2P serving strategy.
  4. Scaffold the Go project and CLI commands.
  5. Implement segment writer/reader and manifest/index types with tests against local files or an in-memory object store.
  6. Implement `migrate` for a bounded height range from a local CometBFT blockstore.
  7. Implement resumability using a local migration state file or DB.
  8. Implement `verify` and `inspect`.
  9. Implement `serve` as a CometBFT blocksync peer with a custom blocksync system and archive upload worker, making clear what is MVP versus future work.
  10. Run formatting, unit tests, and targeted integration tests.
  11. Produce a concise final report with files changed, commands run, and remaining risks.

  ## Definition Of Done

  - Architecture document exists and is specific enough for another engineer to implement from it.
  - CLI commands are discoverable through help output.
  - Segment and manifest formats have tests.
  - Migration can export at least a small local blockstore fixture or generated test chain range into local/mock object storage.
  - Verification catches both valid and intentionally corrupted archive data.
  - Resume behavior is tested for at least one interrupted migration scenario.
  - No CometBFT consensus rules or validator signing behavior are changed or run by the archive peer.
  - The live archive peer can sync and serve hot blocks over the custom blocksync reactor without starting consensus, mempool, evidence, state sync, or ABCI services.
  - Any CometBFT core interface gaps are documented separately from archive implementation.
  - No real external bucket is touched unless explicitly approved.
  - Working tree contains no unrelated edits or accidental generated clutter.

  ## Verification

  - Discover project checks from `go.mod`, `Makefile`, `justfile`, CI config, and existing repo conventions.
  - Run `gofmt`/`go test` for all new packages.
  - Run targeted tests for segment read/write, manifest validation, migration resume, and verifier corruption detection.
  - If a CometBFT fixture is used, verify migrated block hashes match source block hashes.
  - If checks cannot run, state exactly why and list the command that should be run.

  ## Approval Gates

  - Stop before creating a new repository outside the writable workspace if approval is required.
  - Stop before destructive file operations.
  - Stop before force-push, history rewrite, or broad SCM cleanup.
  - Stop before modifying CometBFT consensus, blockstore persistence semantics, public P2P protocols, or public APIs beyond the stated MVP.
  - Stop before using real S3 credentials or accessing non-local services.
  - Stop before publishing packages, creating releases, or deploying anything.
  - Stop if implementation requires choosing between incompatible archive protocol designs not settled by the brief.
  - Stop if verification cannot be run and the remaining risk is material.

  ## Final Response

  - Summarize what was built.
  - List files changed or created.
  - List verification commands and results.
  - Call out any CometBFT core changes or interface gaps.
  - State remaining risks and recommended next tranche.
