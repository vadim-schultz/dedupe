# dedupe Architecture Overview

## Scope and Goals

- High-throughput, cross-platform file deduplication CLI focused on streaming discovery and parallel processing.
- Prioritizes constant memory usage, early elimination of non-duplicates, and actionable reporting (text, JSON, CSV).
- Designed so components can be swapped (walkers, pipelines, report formatters) without altering the public CLI contract.

## Build and Runtime Requirements

- Rust 1.70+ (Edition 2021) with `cargo` for compilation and testing.
- Tokio runtime (multi-threaded) required at execution time; assumes a POSIX-like filesystem but remains cross-platform.
- Relies on native threads (Tokio + Rayon); ensure available CPU cores and file system permissions for traversal.

### Key Crates

- CLI & config: `clap`, `config`, `serde`, `serde_yaml`.
- Async and concurrency: `tokio`, `tokio::sync` channels, `async-trait`, `crossbeam-channel`, `rayon`, `parking_lot`.
- Filesystem and hashing: `walkdir`, `camino`, `infer`, `blake3`, `memmap2`.
- Telemetry: `tracing`, `tracing-subscriber`, `indicatif`, `tokio-metrics`.
- Reporting & serialization: `serde_json`, `chrono`.
- Testing & benches: `rstest`, `proptest` (feature `testing`), `criterion` (feature `benchmarks`).

## Audit Status (2025-10-16)

- [x] Extract CLI logic into a dedicated module (`src/cli.rs`); `src/main.rs` now only parses arguments and delegates. _(Updated 2025-10-16)_
- [x] Replace progress bars with compact braille-style mini-bars and reuse them across all stages. _(Updated 2025-10-16)_
- [x] Standardise every stage/worker progress readout on the braille mini-bar style. _(Updated 2025-10-16)_
- [x] Ship only the highest-performance configuration; `HighPerformanceConfig::default()` now matches `ultra_performance()`, and legacy performance paths were removed. _(Updated 2025-10-16)_
- [x] Remove legacy walkers; `walker_hp.rs` is the sole walker exported by the crate. _(Updated 2025-10-16)_
- [x] Remove legacy pipeline implementations; only the streaming pipeline remains. _(Updated 2025-10-16)_
- [x] Move configuration/data fixtures under `tests/fixtures/**` with config/data/output separation. _(Updated 2025-10-16)_
- [x] Add fixtures/tests that exercise all four streaming stages end-to-end. _(Updated 2025-10-16)_
- [x] Ensure the crate builds and its test suite passes (`cargo test`, Rust 1.90.0). _(Updated 2025-10-16)_
- [ ] Expand automated test coverage beyond configuration/report formatters to meet “extensive” coverage goals. _(Pending)_
- [x] Remove dead code and duplicated implementations; `cargo test` runs clean with no warnings. _(Updated 2025-10-16)_

## High-Level Architecture

```
CLI (clap) -> Config bootstrap -> ProgressTracker
		  |-> HighPerformanceWalker (async discovery)
					 |-> tokio::mpsc<FileInfo>
								|-> StreamingPipeline (ordered stages)
										  Metadata -> QuickCheck -> Statistical -> Hash
														  |-> DuplicateGroup collection
																	 |-> DeduplicationReport -> Formatter (text/json/csv)
```

- **Presentation Layer**: `src/main.rs` (CLI) and `report` module (formatters).
- **Logic Layer**: Walkers (`walker_*`), streaming/parallel pipeline, duplicate grouping, configuration.
- **Infrastructure Layer**: Async runtime, progress tracking, error handling, utilities.

## Core Execution Flow

1. **CLI parsing** (`Cli` in `src/cli.rs`): gather directory, depth, minimum size, thread count, and output preferences. `src/main.rs` merely parses and relays to `cli::run`. _(Updated 2025-10-16)_
2. **Tracing setup**: `tracing_subscriber::FmtSubscriber` configured based on `--verbose`.
3. **Config assembly** (`Config` struct): merges CLI-derived overrides with defaults; options include depth, min size, thread count, quick check sample size, operation mode.
4. **Progress Tracking**: instantiate `ProgressTracker`, which renders compact braille mini-bars for discovery and every pipeline stage via `indicatif::MultiProgress`. _(Updated 2025-10-16)_
5. **Walker Launch** _(Updated 2025-10-16)_:
   - Build a single `HighPerformanceConfig` (default == ultra performance) and adjust thread counts from CLI overrides.
   - Instantiate `HighPerformanceWalker`, start `stream_files` on a background Tokio task feeding `FileInfo` into a `tokio::mpsc` channel.
6. **Streaming Pipeline**:
   - Create `StreamingPipeline` with `StreamingPipelineConfig` tuned to performance mode (channel capacity, batch size, flush interval).
   - Register stages in order: metadata, quick-check, statistical, hash.
   - Convert `FileInfo` stream into `StreamingMessage::Batch` events; each stage runs in its own Tokio task, consuming and producing messages across bounded channels.
7. **Aggregation**:
   - Pipeline returns `Vec<DuplicateGroup>` (exact duplicates) alongside walker statistics (files discovered, errors, etc.).
   - `ProgressTracker::finish_all` closes bars.
8. **Reporting**:
   - Build `ScanConfig` descriptor.
   - Convert `DuplicateGroup` -> `Vec<Vec<FileInfo>>` for reporting.
   - Instantiate `DeduplicationReport`, format using requested formatter, emit to stdout or file.

## Domain Types

- `types::FileInfo`: canonical per-file record (path, size, metadata, optional quick-check/statistical/hash results).
- `types::QuickCheckInfo`: rolling checksum details (sample checksum, sample size, samples taken).
- `types::StatisticalInfo`: entropy, simhash, fingerprint (blake3 digest subset).
- `types::DuplicateGroup`: collection of `FileInfo` plus similarity metadata.
- `Config`: operational parameters (depth, thresholds, threading, extensions, mode).
- `HighPerformanceConfig`, `StreamingPipelineConfig`: tune concurrency and buffering.
- `report::DeduplicationReport`: output payload with stats plus duplicate groups.

## Walkers

### `walker_hp.rs`

- Current default in CLI.
- Multi-worker directory scanning (Tokio tasks) with crossbeam directory queue (bounded for backpressure).
- Batching file emission via `tokio::mpsc`, tuned by `HighPerformanceConfig` (workers, buffer sizes, batch timeout).
- Collects `HighPerformanceStats` (atomic counters) for telemetry.
- Skips expensive work (type inference) until pipeline stage to reduce IO pressure.
  _Legacy walkers (`walker.rs`, `walker_simple.rs`, `walker_streaming.rs`, `walker_parallel.rs`) were removed in October 2025 to comply with the high-performance-only directive._ _(Updated 2025-10-16)_

## Processing Pipelines

### Streaming Pipeline (`pipeline::streaming`)

- Central orchestrator for CLI path.
- Converts `FileInfo` into `StreamingMessage` batches, fan-out across stages connected via bounded `tokio::mpsc` channels.
- Each stage runs inside `tokio::spawn`, processes messages, and forwards results; flush/shutdown semantics ensure final accumulation.
- Result collector aggregates `StreamingMessage::Groups` into final `Vec<DuplicateGroup>`.
- `ProgressTrackingStage` helper enables per-stage `indicatif` bars if tracker available.
  _Legacy batch and parallel pipeline implementations have been removed from `src/pipeline/` in favour of the streaming-only path._ _(Updated 2025-10-16)_

#### Stage Responsibilities

1. **`StreamingMetadataStage`**
   - Enrich with filesystem metadata (size update, timestamps, readonly/hidden flags).
   - Skips files below minimum size or that fail metadata fetch.
   - Populates MIME via `infer` if missing.
2. **`StreamingQuickCheckStage`**
   - Reads small samples (start/middle/end) using async IO; produces rolling checksum in `QuickCheckInfo`.
   - Skips tiny files; tolerant of IO failures (passes file onward without quick-check data).
3. **`StreamingStatisticalStage`**
   - Reads up to 64KiB sample, computes entropy, simhash, 16-byte fingerprint.
   - Provides richer similarity signal to prune mismatched files before hashing.
4. **`StreamingHashStage`**
   - Multi-level grouping: size -> quick-check checksum -> statistical simhash -> full BLAKE3 hash.
   - Maintains in-memory maps until flush, calculates group similarity heuristic, emits `DuplicateGroup`s.

## Reporting Pipeline

- `report::DeduplicationReport::new` builds stats (total files, duplicate sets, potential savings, throughput).
- Formatters in `report/formatters/{text,json,csv}.rs` convert to distribution-specific strings.
- CLI writes to stdout or path; JSON/CSV fully deterministic for downstream automation.

## Progress, Logging, and Metrics

- `tracing` macros across walkers/pipeline for structured diagnostics; CLI toggles INFO/DEBUG via `--verbose`.
- `ProgressTracker` centralizes `indicatif::MultiProgress` handling, renders braille mini-bars via `render_braille_bar`, and exposes helper constructors for scanning and stage bars while ensuring graceful shutdown (`finish_all`).
- `tokio-metrics` integrated within `ParallelPipeline` for future instrumentation.

## Configuration Loading

- `Config::load` merges defaults, optional YAML file (`config.example.yaml` template), and environment (`DEDUPE_*`).
- Validation enforces sensible bounds (positive threads, sample sizes, threshold <= 100).
- CLI path currently constructs `Config` directly; `Config::load` supports alternative entry points (tests, future CLI flags).

## Error Handling

- `types::PipelineError`: canonical error for streaming pipeline (IO, configuration, channel send/receive, runtime).
- `utils::error::Error`: general-purpose errors for utilities.
- Walkers, stages, and report generation prefer `anyhow::Result` for ergonomic propagation while mapping into domain errors where exposed.

## Testing and Quality

- Integration tests under `tests/` cover pipeline behavior, walkers, and reporting (see `pipeline_test.rs`, `walker_test.rs`, etc.).
- Internal module tests (e.g., `pipeline::parallel`, `pipeline::stats`) verify stage-specific logic.
- `rstest` plus `tempfile` enable filesystem fixtures.
- Feature flags: `testing` enables property-based tests with `proptest`; `benchmarks` enables Criterion benches (`benches/parallel_pipeline.rs`).
- Concurrency verification via optional `loom` dependency.
- [x] Fixtures and sample data now live under `tests/fixtures/{config,data,output}`. _(Updated 2025-10-16)_
- [x] Create additional fixtures/tests that cover metadata, quick-check, statistical, and hash stages end-to-end (`tests/pipeline_stage_test.rs`). _(Updated 2025-10-16)_

## Extending the System

1. **New Pipeline Stage**
   - Implement `StreamingStage` for streaming path or `PipelineStage` for batch path.
   - Register stage in `StreamingPipeline` (order matters: ensure cheap filters precede expensive ones).
   - Provide backpressure-aware handling of `StreamingMessage::{Batch,Flush,Shutdown}`.
2. **New Walker**
   - Expose struct that emits `FileInfo` (populated minimally) into pipeline-friendly channel.
   - Reuse `ProgressTracker` for consistent UX.
3. **Custom Reports**
   - Add formatter module under `report/formatters/` with function returning `Result<String>`.
   - Extend CLI `Args::format` parsing in `main.rs` to accept new type; update match arm in report emission.
4. **CLI Enhancements**
   - Extend `Args` struct; propagate to config or pipeline creation as needed.
   - Update `README.md` and `USAGE.md` for discoverability.

## Recreating the Project

1. Initialize crate: `cargo new dedupe --bin`.
2. Add dependencies (see `Cargo.toml` for versions and features).
3. Create module layout:
   - `src/main.rs`, `src/lib.rs` (re-export modules for integration tests/examples).
   - `src/config.rs`, `src/types.rs`, `src/utils/`, `src/report/`, `src/pipeline/`, `src/walker_*.rs`.
4. Implement domain types (`FileInfo`, `DuplicateGroup`, etc.) and configuration loader.
5. Build walkers progressively (start with `walker.rs`, iterate to streaming/high-performance variants).
6. Implement pipeline traits (`PipelineStage`, `StreamingStage`) and stage modules (metadata -> hash).
7. Wire CLI main flow: parse args -> config -> progress tracker -> walker + streaming pipeline -> report.
8. Port tests from `tests/` and add sample fixtures in `tests/` or `examples/` for manual validation.
9. Provide documentation (`README.md`, `USAGE.md`) and configuration example (`config.example.yaml`).

## Operational Considerations

- Batch sizes, flush intervals, and channel capacities should be tuned according to target hardware; defaults assume modern multi-core systems.
- `StreamingHashStage` keeps in-memory maps; ensure memory remains bounded by periodic flushes (`StreamingMessage::Flush`).
- Ensure file handles are released promptly (Tokio async blocks drop handles at scope end).
- Hidden/system files filtered early; adjust walker logic if full traversal needed.

## Future Work Hooks (from code and README)

- Additional output modes (interactive delete, symlink).
- Remote filesystem adapters.
- Incremental scanning indexing.
- GUI or TUI front-end leveraging pipeline APIs exposed via `lib.rs`.

- [x] `cargo fmt` and `cargo test` complete successfully on Rust 1.90.0 (2025-10-16). _(Updated 2025-10-16)_
- [ ] Broaden the automated test suite to achieve high coverage (current focus is config/report formatters). _(Pending)_
- [x] Codebase builds without compiler warnings; redundant walkers/pipelines removed to maintain clear interfaces. _(Updated 2025-10-16)_
