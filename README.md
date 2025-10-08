# Dedupe - Fast File Deduplication Tool

A high-performance file deduplication tool written in Rust that uses a multi-stage pipeline approach to efficiently identify and handle duplicate files.

## Design Overview

### Core Features

- Multi-stage pipeline for efficient duplicate detection
- Configurable directory traversal depth
- Concurrent processing using Rust channels
- Multiple operation modes (report/remove/interactive)
- Similarity detection for near-duplicate files
- Progress tracking and statistics

### Pipeline Stages

The application uses a pipeline of increasingly precise (and computationally expensive) stages to identify duplicates:

1. **Basic Metadata Stage**

   - File size comparison
   - File type detection
   - O(1) operation per file
   - Eliminates obvious non-duplicates quickly

2. **Quick Content Check Stage**

   - Partial file reading (first/last N bytes)
   - Basic structure analysis
   - O(n) where n is the sample size
   - Eliminates files that are definitely different

3. **Statistical Analysis Stage**

   - Entropy calculation
   - Byte frequency distribution
   - Fuzzy hash generation (ssdeep/tlsh)
   - O(n) where n is file size
   - Provides similarity metrics

4. **Full Hash Stage**
   - Complete file hash (Blake3 for speed)
   - O(n) where n is file size
   - Guarantees exact matches
   - Only performed on likely duplicates

### Architecture

```
[Directory Walker] ---> [Stage 1 Filter] ---> [Stage 2 Filter] ---> [Stage 3 Filter] ---> [Stage 4 Filter]
     |                      |                      |                      |                      |
     v                      v                      v                      v                      v
[Progress Bar]         [Statistics]           [Statistics]           [Statistics]           [Results]
```

### Concurrency Model

- Each stage runs in its own thread pool
- Rust channels connect stages
- Bounded channel capacity for backpressure
- Work stealing for load balancing
- Pipeline parallelism for maximum throughput

### Configuration Options

- Traversal depth (number or unlimited)
- Minimum file size threshold
- Similarity threshold percentage
- Number of threads per stage
- Operation mode:
  - Report only
  - Remove duplicates (keep newest)
  - Interactive mode
  - Symlink duplicates
- Output format (text, JSON, CSV)

### Performance Optimizations

1. **Early Termination**

   - Skip further stages if file is unique
   - Abort comparison when differences found

2. **Memory Management**

   - Streaming file processing
   - Memory-mapped files for large files
   - LRU cache for frequent comparisons

3. **I/O Optimization**

   - Batched file operations
   - Asynchronous I/O
   - Directory traversal optimization

4. **Algorithmic Improvements**
   - Sorted metadata comparisons
   - Optimized hash table usage
   - Efficient string comparisons

### Similarity Detection

For near-duplicate detection:

1. Calculate fuzzy hashes (ssdeep/tlsh)
2. Use locality-sensitive hashing (LSH)
3. Compare byte frequency distributions
4. Calculate edit distance for text files

### Output Format

```json
{
  "statistics": {
    "total_files": 1000,
    "duplicate_sets": 50,
    "space_savings": "1.2GB"
  },
  "duplicates": [
    {
      "hash": "...",
      "similarity": 100,
      "files": [
        {
          "path": "...",
          "size": 1024,
          "modified": "2025-10-08T..."
        }
      ]
    }
  ]
}
```

### Error Handling

- Graceful handling of permission issues
- Resume capability after interruption
- Detailed error reporting
- Dry run mode for safety

### Future Enhancements

1. Remote file system support
2. Plugin system for custom comparison rules
3. GUI interface
4. Incremental scanning
5. Deduplication across compressed archives

## Implementation Notes

- Use `tokio` for async I/O
- Use `rayon` for parallel processing
- Use `blake3` for fast hashing
- Use `walkdir` for efficient traversal
- Use `clap` for CLI arguments
- Use `serde` for serialization

## Implementation Strategy

### Phase 1: Foundation (1-2 days)

1. **Project Setup**

   - Basic CLI structure with `clap` v4.0 with derive features
   - Logging infrastructure using `tracing` ecosystem (`tracing` + `tracing-subscriber`)
   - Error handling with `thiserror` for custom errors
   - Configuration using `config` crate with YAML support
   - Unit test framework with `rstest` for parameterized testing

2. **Directory Walker**
   - File traversal using `walkdir` with custom filters
   - File type detection using `infer` crate
   - Metadata collection with `fs_extra` for extended attributes
   - Progress reporting with `indicatif` for rich terminal UI
   - Cross-platform path handling with `camino` for UTF-8 paths

### Phase 2: Single-Threaded Pipeline (2-3 days)

1. **Basic Pipeline Structure**

   - Define trait hierarchy using `async-trait`
   - Pipeline executor using `futures` combinators
   - Metrics using `metrics` crate with `metrics-exporter-prometheus`
   - Error propagation with `anyhow` for pipeline errors
   - Event system using `eventbus` for inter-stage communication

2. **First Stage Implementation**
   - File grouping using `indexmap` for ordered collections
   - Fast integer-based size comparison with `bytesize`
   - Memory-efficient metadata storage using `smallvec`

### Phase 3: Multi-Threading (2-3 days)

1. **Channel Implementation**

   - Async channels using `tokio::sync::mpsc` with backpressure
   - Thread pool management with `rayon`
   - Load balancing using `crossbeam-deque` for work stealing
   - Thread monitoring with `thread_local` and `parking_lot` for synchronization
   - Health metrics using `tokio-metrics`

2. **Pipeline Parallelization**
   - Parallel executor using `tokio` runtime
   - Performance tracking with `criterion` for benchmarks
   - Thread pool optimization with `num_cpus` for auto-scaling
   - Load testing using `iai` for cycle-accurate benchmarks
   - Thread sanitizer integration with `loom` for concurrency testing

### Phase 4: Additional Stages (3-4 days)

1. **Quick Content Check**

   - Memory-mapped I/O using `memmap2` for large files
   - Buffered reading with `buf_redux` for optimal performance
   - Magic number detection with `file-format` crate
   - I/O benchmarking with `criterion` and `hyperfine`
   - Async file operations using `tokio::fs`

2. **Statistical Analysis**
   - Entropy calculation using `statrs` for statistical functions
   - Byte frequency analysis with SIMD using `packed_simd`
   - Similarity scoring with `simhash-rs` for fast comparison
   - Accuracy validation using `proptest` for property testing
   - Parallel processing with `rayon` iterators

### Phase 5: Final Stages (2-3 days)

1. **Full Hash Implementation**

   - Parallel hashing with `blake3` with SIMD acceleration
   - Memory mapping with `memmap2` for huge files
   - Stream processing using `bytes` for efficient memory usage
   - Performance comparison using `hashbrown` for fast hash tables
   - Chunked processing with `bytes` for streaming large files

2. **Similarity Detection**
   - Fuzzy hashing with `ssdeep` or custom implementation
   - LSH implementation using `fasthash` for quick hashing
   - MinHash implementation with `minhash` crate
   - Edit distance calculation using `strsim` for text files
   - Parallel similarity computation with `rayon`

### Phase 6: User Interface and Actions (2-3 days)

1. **Report Generation**

   - JSON serialization with `serde_json` and custom formatters
   - CSV handling with `csv` crate for tabular output
   - Terminal UI using `crossterm` or `termion`
   - Progress bars with `indicatif` for visual feedback
   - Template-based reports using `tinytemplate`

2. **Action Implementation**
   - Safe file operations with `fs_extra` for removal
   - Symlink handling using `symlink` crate
   - Interactive prompts with `dialoguer` for user input
   - Atomic operations using `atomic-write` for safety
   - Undo logging with `log` for recovery capabilities

### Phase 7: Polish (2-3 days)

1. **Optimization**

   - Profiling with `flamegraph` and `perf` integration
   - Memory profiling using `dhat` and `heaptrack`
   - I/O analysis with `iotop` integration
   - Thread analysis using `tokio-console`
   - CPU optimization with `hwloc` for NUMA awareness

2. **Documentation and Testing**
   - API docs using `rustdoc` with examples
   - Configuration examples with `figment`
   - Integration tests using `test-context`
   - Performance tracking with `bencher` cloud integration
   - Coverage reporting with `grcov` and `tarpaulin`

### Development Guidelines

1. **Incremental Value**

   - Each phase produces a working tool
   - Features build upon previous work
   - Regular performance benchmarking
   - Continuous integration setup

2. **Testing Strategy**

   - Unit tests with each component
   - Integration tests for stages
   - Performance benchmarks
   - Error case coverage

3. **Code Organization**

   - Separate modules per stage
   - Clear trait boundaries
   - Minimal dependencies between components
   - Clean error propagation

4. **Quality Assurance**
   - Continuous benchmark running
   - Memory leak detection
   - Error handling validation
   - Edge case testing

### Milestone Deliverables

1. **v0.1.0** (Phase 1-2)

   - Basic file traversal
   - Single-threaded pipeline
   - Size-based deduplication
   - Simple reporting

2. **v0.2.0** (Phase 3)

   - Multi-threaded operation
   - Performance metrics
   - Improved reporting
   - Basic CLI interface

3. **v0.3.0** (Phase 4)

   - Quick content checking
   - Statistical analysis
   - Progress visualization
   - Configuration options

4. **v0.4.0** (Phase 5)

   - Full hash comparison
   - Similarity detection
   - Memory optimizations
   - Performance improvements

5. **v1.0.0** (Phase 6-7)
   - Complete feature set
   - Multiple output formats
   - Comprehensive documentation
   - Production-ready performance

### Getting Started with Development

1. **Environment Setup**

   ```bash
   cargo new dedupe
   cd dedupe
   # Add initial dependencies
   cargo add clap tokio rayon blake3 walkdir serde
   ```

2. **Initial Structure**

   ```rust
   src/
     main.rs           # Entry point
     config.rs         # Configuration
     walker.rs         # Directory traversal
     pipeline/
       mod.rs         # Pipeline traits
       metadata.rs    # First stage
       quick_check.rs # Second stage
       stats.rs      # Third stage
       hash.rs       # Fourth stage
     utils/
       mod.rs        # Utility functions
       progress.rs   # Progress reporting
       error.rs      # Error types
   ```

3. **First Implementation Step**
   ```rust
   // Start with basic CLI and file traversal
   cargo run -- --depth 2 /path/to/directory
   ```
