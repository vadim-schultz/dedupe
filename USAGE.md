# Dedupe - High-Performance Streaming File Deduplication Tool

A lightning-fast, always-streaming file deduplication tool written in Rust that uses advanced algorithms to find and report duplicate files. Features high-performance concurrent directory scanning and real-time streaming processing pipeline that processes files as they're discovered.

## Installation

Build from source:

```bash
git clone https://github.com/vadim-schultz/dedupe
cd dedupe
cargo build --release
```

## Quick Start

Basic usage to find duplicates in the current directory:

```bash
dedupe
```

Scan a specific directory:

```bash
dedupe /path/to/directory
```

Generate JSON report:

```bash
dedupe --format json --output duplicates.json /path/to/directory
```

## Command Line Options

```
USAGE:
    dedupe [OPTIONS] [DIR]

ARGS:
    <DIR>    Directory to scan for duplicates [default: .]

OPTIONS:
    -p, --performance <PERFORMANCE>  Performance mode: standard, high, ultra [default: high]
    -d, --depth <DEPTH>             Maximum depth to traverse [default: unlimited]
    -m, --min-size <MIN_SIZE>       Minimum file size to consider (in bytes) [default: 1024]
    -v, --verbose                   Enable verbose logging with debug information
    -f, --format <FORMAT>           Report output format: text, json, csv [default: text]
    -o, --output <OUTPUT>           Output file path (if not specified, output to console)
    -h, --help                      Print help
    -V, --version                   Print version
```

## Report Formats

### Text Format (Default)

Human-readable report with emojis and clear formatting:

```bash
dedupe /path/to/directory
```

### JSON Format

Structured data for programmatic processing:

```bash
dedupe --format json --output report.json /path/to/directory
```

### CSV Format

Tabular data suitable for spreadsheets:

```bash
dedupe --format csv --output report.csv /path/to/directory
```

## Examples

### Basic Scanning

1. **Scan current directory with defaults:**

   ```bash
   dedupe
   ```

2. **Scan with limited depth:**

   ```bash
   dedupe --depth 5 /home/user/documents
   ```

3. **Skip small files (< 10MB):**
   ```bash
   dedupe --min-size 10485760 /media/photos
   ```

### Report Generation

4. **Generate JSON report for automation:**

   ```bash
   dedupe --format json --output duplicates.json ~/Downloads
   ```

5. **CSV export for analysis:**

   ```bash
   dedupe --format csv --output analysis.csv --verbose /data
   ```

6. **Compact scanning (no debug info):**
   ```bash
   dedupe --min-size 1048576 /large/dataset > report.txt
   ```

### High-Performance Scanning

7. **Standard performance mode (balanced):**

   ```bash
   dedupe --performance standard /home/user/documents
   ```

8. **High-performance mode (recommended, default):**

   ```bash
   dedupe --performance high /large/dataset
   ```

9. **Ultra performance mode (maximum speed):**

   ```bash
   dedupe --performance ultra /massive/directory
   ```

10. **Verbose mode for debugging:**
    ```bash
    dedupe --verbose --depth 2 /problematic/path
    ```

## High-Performance Streaming Architecture

Dedupe uses a revolutionary always-streaming, always-parallel architecture:

### Concurrent File Discovery

- **High-Performance Walker**: Multi-worker concurrent directory scanning
- **Real-Time Processing**: Files processed immediately as they're discovered
- **Performance Modes**: Standard (2 workers), High (4 workers), Ultra (8 workers)

### Streaming Processing Pipeline

1. **Metadata Stage**: Instant file system metadata collection and grouping
2. **QuickCheck Stage**: Fast preliminary content sampling (8KB samples)
3. **Statistical Analysis**: Advanced similarity analysis using:
   - Entropy calculation for content randomness
   - SimHash fingerprinting for fuzzy matching
   - Statistical fingerprints for content patterns
4. **Hash Stage**: Full content hashing (BLAKE3) for exact matches

### Key Performance Features

- **Zero Wait Time**: Processing begins immediately, no scan completion delay
- **Constant Memory**: Memory usage independent of directory size
- **Scalable Concurrency**: Automatic scaling based on performance mode
- **Progress Tracking**: Real-time progress and throughput metrics

## Performance Features

- **Always Streaming**: All files processed in real-time as discovered (no waiting for scan completion)
- **Always Parallel**: Multi-worker concurrent directory scanning and processing by default
- **Performance Modes**: Standard/High/Ultra modes for different scenarios
- **Intelligent Scaling**: Automatic worker and thread scaling based on performance mode
- **Fast Algorithms**: BLAKE3 hashing, statistical fingerprinting, entropy analysis
- **Memory Efficient**: Constant memory usage regardless of directory size
- **Real-time Progress**: Live progress tracking and throughput metrics

## Output Information

### Scan Statistics

- Total files scanned and processing time
- File size distribution and throughput metrics
- Thread utilization and performance data

### Duplicate Analysis

- Number of duplicate groups and files
- Potential storage savings
- Space efficiency percentage

### File Details

- File paths, sizes, and modification dates
- Content similarity percentages
- Primary file recommendations (newest/largest)

## Performance Tips

1. **Choose the right performance mode** for your use case:

   ```bash
   dedupe --performance standard /small/directory    # Balanced performance
   dedupe --performance high /large/directory        # Default: high performance
   dedupe --performance ultra /massive/dataset       # Maximum speed
   ```

2. **Adjust minimum file size** to skip irrelevant small files:

   ```bash
   dedupe --min-size 10485760 --performance ultra /media/library  # Skip files < 10MB
   ```

3. **Use appropriate depth limits** for very deep directory trees:

   ```bash
   dedupe --depth 10 --performance high /very/deep/structure
   ```

4. **Combine settings for optimal performance**:

   ```bash
   dedupe --performance ultra --min-size 1048576 --verbose /large/dataset
   ```

5. **Use release builds** for maximum performance:

   ```bash
   cargo run --release -- --performance ultra /path/to/directory
   ```

6. **Monitor performance with verbose mode**:
   ```bash
   dedupe --performance high --verbose /path/to/scan
   ```

## Safety Features

- **Read-only operation**: Never modifies or deletes files
- **Non-intrusive scanning**: No temporary files or system changes
- **Error resilience**: Continues processing despite individual file errors
- **Progress reporting**: Clear feedback on scan progress and results
- **Comprehensive logging**: Detailed operation logs with verbose mode

## Exit Codes

- **0**: Success - scan completed successfully
- **1**: Error - invalid arguments, file system errors, or processing failures

## Technical Specifications

### Performance Modes

- **Standard Mode**: 2 concurrent directory workers, balanced resource usage
- **High Mode**: 4 concurrent directory workers, optimized performance (default)
- **Ultra Mode**: 8 concurrent directory workers, maximum speed

### Algorithms & Features

- **Minimum File Size**: 1KB (configurable)
- **Hash Algorithm**: BLAKE3 (cryptographically secure, fastest available)
- **Similarity Threshold**: 95% (configurable in code)
- **Streaming Architecture**: Always-on streaming processing, zero wait time
- **Memory Usage**: Constant memory usage with streaming pipeline
- **Concurrency**: Always parallel by default, performance mode controlled scaling
- **Supported Platforms**: Windows, Linux, macOS (via Rust cross-compilation)
