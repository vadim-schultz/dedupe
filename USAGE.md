# Dedupe - High-Performance File Deduplication Tool

A fast, parallel file deduplication tool written in Rust that uses advanced algorithms to find and report duplicate files across your system.

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
    -d, --depth <DEPTH>        Maximum depth to traverse [default: unlimited]
    -m, --min-size <MIN_SIZE>  Minimum file size to consider (in bytes) [default: 1024]
    -p, --parallel             Use parallel pipeline processing (enabled by default)
    -t, --threads <THREADS>    Number of worker threads (0 = auto-detect) [default: 0]
    -v, --verbose              Enable verbose logging with debug information
    -f, --format <FORMAT>      Report output format: text, json, csv [default: text]
    -o, --output <OUTPUT>      Output file path (if not specified, output to console)
    -h, --help                 Print help
    -V, --version              Print version
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

### Performance Optimization

7. **Use specific thread count:**

   ```bash
   dedupe --threads 8 /massive/directory
   ```

8. **Verbose mode for debugging:**
   ```bash
   dedupe --verbose --depth 2 /problematic/path
   ```

## Algorithm Details

Dedupe uses a multi-stage pipeline for efficient duplicate detection:

1. **Metadata Stage**: Quick file system metadata collection
2. **QuickCheck Stage**: Fast preliminary content sampling (8KB samples)
3. **Statistical Analysis**: Advanced similarity analysis using:
   - Entropy calculation for content randomness
   - SimHash fingerprinting for fuzzy matching
   - Statistical fingerprints for content patterns
4. **Hash Stage**: Full content hashing (BLAKE3) for exact matches

## Performance Features

- **Parallel Processing**: Multi-threaded pipeline with configurable worker threads
- **Intelligent Defaults**: 1KB minimum file size, auto-detected CPU cores
- **Fast Algorithms**: BLAKE3 hashing, statistical fingerprinting
- **Memory Efficient**: Streaming file processing with batched operations
- **Progress Tracking**: Real-time progress bars and throughput metrics

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

1. **Adjust minimum file size** to skip irrelevant small files:

   ```bash
   dedupe --min-size 10485760  # Skip files < 10MB
   ```

2. **Use appropriate depth limits** for large directory trees:

   ```bash
   dedupe --depth 10 /very/deep/structure
   ```

3. **Optimize thread count** for your system:

   ```bash
   dedupe --threads $(nproc) /path/to/scan
   ```

4. **Use release builds** for maximum performance:
   ```bash
   cargo run --release -- /path/to/directory
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

- **Minimum File Size**: 1KB (configurable)
- **Hash Algorithm**: BLAKE3 (cryptographically secure)
- **Similarity Threshold**: 95% (configurable in code)
- **Default Threading**: Auto-detect CPU cores
- **Memory Usage**: Optimized for large file sets with streaming processing
- **Supported Platforms**: Windows, Linux, macOS (via Rust cross-compilation)
