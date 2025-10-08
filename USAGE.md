# Dedupe - File Deduplication Tool

A fast and flexible file deduplication tool written in Rust that helps you find and manage duplicate files across your system.

## Installation

```bash
cargo install dedupe
```

Or build from source:

```bash
git clone https://github.com/vadim-schultz/dedupe
cd dedupe
cargo build --release
```

## Quick Start

Basic usage to find duplicates in a directory:

```bash
dedupe /path/to/directory
```

Scan with a maximum depth of 3 directories:

```bash
dedupe --depth 3 /path/to/directory
```

Use a configuration file:

```bash
dedupe --config my-config.yaml /path/to/directory
```

## Configuration

Dedupe can be configured using a YAML configuration file. Create a copy of `config.example.yaml` and modify it according to your needs:

```bash
cp config.example.yaml my-config.yaml
```

### Configuration Options

#### File Selection

- `min_file_size`: Minimum file size to consider (in bytes)
- `max_depth`: Maximum directory depth to scan
- `include_types`: List of file types to include (mime types or extensions)
- `exclude_types`: List of file types to exclude
- `exclude_dirs`: Directories to skip during scanning
- `follow_links`: Whether to follow symbolic links

#### Deduplication Strategy

- `strategy`: Choose between "hash" (content-based) or "metadata" (quick comparison)
- `hash_algorithm`: Select the hashing algorithm (sha256, sha1, or blake3)
- `duplicate_action`: Action to take when duplicates are found:
  - `report`: List duplicates without modifying files
  - `hardlink`: Replace duplicates with hardlinks
  - `symlink`: Replace duplicates with symbolic links
  - `delete`: Remove duplicate files

#### Performance

- `parallel_threshold`: Minimum file size for parallel processing
- `max_threads`: Maximum number of threads for parallel operations

## Command Line Options

```
USAGE:
    dedupe [OPTIONS] <PATH>

ARGS:
    <PATH>    Directory to scan for duplicates

OPTIONS:
    -c, --config <FILE>     Path to configuration file
    -d, --depth <DEPTH>     Maximum directory depth to scan
    -s, --size <SIZE>       Minimum file size to consider (in bytes)
    -v, --verbose           Enable verbose output
    -q, --quiet            Suppress progress bars
    -h, --help             Print help information
    -V, --version          Print version information
```

## Examples

1. Find duplicates in your home directory, limiting to image files:

```yaml
# config.yaml
include_types:
  - "image/jpeg"
  - "image/png"
  - ".jpg"
  - ".png"
min_file_size: 10240 # 10KB
```

```bash
dedupe --config config.yaml ~/
```

2. Clean up duplicate downloads, replacing them with hardlinks:

```yaml
# config.yaml
exclude_dirs:
  - ".git"
  - "node_modules"
duplicate_action: "hardlink"
```

```bash
dedupe --config config.yaml ~/Downloads
```

3. Quick scan using metadata comparison:

```yaml
# config.yaml
strategy: "metadata"
min_file_size: 1048576 # 1MB
```

```bash
dedupe --config config.yaml /data
```

## Exit Codes

- 0: Success
- 1: General error
- 2: Invalid configuration
- 3: Permission error
- 4: No duplicates found

## Notes

1. When using `delete` or `hardlink` actions, always run with `report` first to review what will be modified.
2. The tool never modifies the original files when finding duplicates, only the duplicates.
3. Symbolic links are not followed by default for safety.
4. File modification times are preserved when creating hardlinks.

## Performance Tips

1. Use appropriate `min_file_size` to skip small files
2. Enable parallel processing with `parallel_threshold`
3. Use `max_depth` to limit directory traversal
4. Choose `blake3` for fastest hashing
5. Use `metadata` strategy for quick initial scans

## Safety Features

- Read-only by default (report mode)
- Preserves original files
- Checks file permissions before modifications
- Validates hardlink/symlink operations
- Maintains file timestamps
- Verifies file integrity after operations
