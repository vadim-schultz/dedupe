//! Quick Content Check Stage - Phase 4 Implementation
//! 
//! This stage provides fast content-based filtering using:
//! - Memory-mapped I/O for large files
//! - Magic number detection for file type verification
//! - Buffered sampling of file headers/footers
//! - Async file operations for optimal performance

use std::sync::Arc;
use std::time::Instant;
use anyhow::{Context, Result};
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncSeekExt, SeekFrom};
use memmap2::Mmap;
use tracing::{debug, instrument, warn};

use crate::types::FileInfo;
use crate::config::Config;
use super::{PipelineStage, ProcessingResult};

/// Configuration for quick content checking
#[derive(Debug, Clone)]
pub struct QuickCheckConfig {
    /// Number of bytes to read from file start
    pub header_bytes: usize,
    /// Number of bytes to read from file end
    pub footer_bytes: usize,
    /// Minimum file size for memory mapping
    pub mmap_threshold: u64,
    /// Maximum file size to process (safety limit)
    pub max_file_size: u64,
    /// Enable magic number detection
    pub detect_file_type: bool,
}

impl Default for QuickCheckConfig {
    fn default() -> Self {
        Self {
            header_bytes: 4096,      // 4KB header sample
            footer_bytes: 1024,      // 1KB footer sample
            mmap_threshold: 1024 * 1024, // 1MB threshold for mmap
            max_file_size: 10 * 1024 * 1024 * 1024, // 10GB max
            detect_file_type: true,
        }
    }
}

/// File content sample for quick comparison
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ContentSample {
    /// File header bytes
    pub header: Vec<u8>,
    /// File footer bytes (if file is large enough)
    pub footer: Option<Vec<u8>>,
    /// Detected MIME type
    pub mime_type: Option<String>,
    /// File structure indicators
    pub structure_hints: ContentStructure,
}

/// Basic file structure analysis
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ContentStructure {
    /// Has binary header (non-printable bytes in first 512 bytes)
    pub is_binary: bool,
    /// Has text-like content (mostly printable ASCII)
    pub is_text: bool,
    /// Has compression signatures
    pub is_compressed: bool,
    /// Has executable signatures
    pub is_executable: bool,
}

/// Quick Content Check stage implementation
#[derive(Debug)]
pub struct QuickCheckStage {
    config: Arc<Config>,
    stage_config: QuickCheckConfig,
}

impl QuickCheckStage {
    /// Create a new quick check stage
    pub fn new(config: Arc<Config>) -> Self {
        Self {
            config,
            stage_config: QuickCheckConfig::default(),
        }
    }

    /// Create with custom configuration
    pub fn with_config(config: Arc<Config>, stage_config: QuickCheckConfig) -> Self {
        Self {
            config,
            stage_config,
        }
    }

    /// Sample file content using memory mapping for large files
    #[instrument(skip(self), fields(file_size = %file_info.size))]
    pub async fn sample_content(&self, file_info: &FileInfo) -> Result<ContentSample> {
        let file_size = file_info.size;
        
        if file_size > self.stage_config.max_file_size {
            warn!("File too large for processing: {} bytes", file_size);
            return Ok(ContentSample::empty());
        }

        if file_size >= self.stage_config.mmap_threshold {
            self.sample_with_mmap(file_info).await
        } else {
            self.sample_with_buffered_read(file_info).await
        }
    }

    /// Sample using memory-mapped I/O for large files
    async fn sample_with_mmap(&self, file_info: &FileInfo) -> Result<ContentSample> {
        let file = std::fs::File::open(&file_info.path)
            .with_context(|| format!("Failed to open file: {}", file_info.path))?;
        
        let mmap = unsafe { 
            Mmap::map(&file)
                .with_context(|| format!("Failed to memory map file: {}", file_info.path))?
        };

        let file_size = mmap.len() as u64;
        let header_size = std::cmp::min(self.stage_config.header_bytes, mmap.len());
        let header = mmap[..header_size].to_vec();

        let footer = if file_size > self.stage_config.footer_bytes as u64 {
            let footer_start = mmap.len() - std::cmp::min(self.stage_config.footer_bytes, mmap.len());
            Some(mmap[footer_start..].to_vec())
        } else {
            None
        };

        let mime_type = if self.stage_config.detect_file_type {
            detect_mime_type(&header)
        } else {
            None
        };

        let structure_hints = analyze_structure(&header, footer.as_ref());

        Ok(ContentSample {
            header,
            footer,
            mime_type,
            structure_hints,
        })
    }

    /// Sample using buffered async reading for smaller files
    async fn sample_with_buffered_read(&self, file_info: &FileInfo) -> Result<ContentSample> {
        let mut file = File::open(&file_info.path).await
            .with_context(|| format!("Failed to open file: {}", file_info.path))?;
        
        let file_size = file_info.size;
        
        // Read header
        let mut header = vec![0u8; std::cmp::min(self.stage_config.header_bytes, file_size as usize)];
        file.read_exact(&mut header).await
            .with_context(|| format!("Failed to read header from: {}", file_info.path))?;

        // Read footer if file is large enough
        let footer = if file_size > self.stage_config.footer_bytes as u64 {
            let footer_size = std::cmp::min(self.stage_config.footer_bytes, file_size as usize);
            let footer_start = file_size - footer_size as u64;
            
            file.seek(SeekFrom::Start(footer_start)).await
                .with_context(|| format!("Failed to seek in file: {}", file_info.path))?;
            
            let mut footer_bytes = vec![0u8; footer_size];
            file.read_exact(&mut footer_bytes).await
                .with_context(|| format!("Failed to read footer from: {}", file_info.path))?;
            
            Some(footer_bytes)
        } else {
            None
        };

        let mime_type = if self.stage_config.detect_file_type {
            detect_mime_type(&header)
        } else {
            None
        };

        let structure_hints = analyze_structure(&header, footer.as_ref());

        Ok(ContentSample {
            header,
            footer,
            mime_type,
            structure_hints,
        })
    }
}

#[async_trait::async_trait]
impl PipelineStage for QuickCheckStage {
    fn name(&self) -> &'static str {
        "QuickCheck"
    }

    #[instrument(skip(self, files))]
    async fn process_impl(&self, files: Vec<FileInfo>) -> Result<ProcessingResult> {
        let start_time = Instant::now();
        debug!("Processing {} files in QuickCheck stage", files.len());

        let mut groups = std::collections::HashMap::new();
        let mut processed_count = 0;
        let mut error_count = 0;

        for file_info in files {
            match self.sample_content(&file_info).await {
                Ok(sample) => {
                    groups.entry(sample).or_insert_with(Vec::new).push(file_info);
                    processed_count += 1;
                }
                Err(e) => {
                    warn!("Failed to sample file {}: {}", file_info.path, e);
                    error_count += 1;
                    // Add to a special "unprocessable" group
                    groups.entry(ContentSample::error()).or_insert_with(Vec::new).push(file_info);
                }
            }
        }

        let duration = start_time.elapsed();
        debug!("QuickCheck stage completed: {} processed, {} errors in {:?}", 
              processed_count, error_count, duration);

        // Convert groups to the expected format
        let mut duplicate_groups = Vec::new();
        let mut unique_files = Vec::new();
        
        for group in groups.into_values() {
            if group.len() > 1 {
                duplicate_groups.push(group);
            } else {
                unique_files.extend(group);
            }
        }

        if duplicate_groups.is_empty() {
            Ok(ProcessingResult::Skip(unique_files))
        } else if unique_files.is_empty() {
            Ok(ProcessingResult::Duplicates(duplicate_groups))
        } else {
            // Mix of duplicates and uniques, continue with potential duplicates
            Ok(ProcessingResult::Continue(
                duplicate_groups.into_iter().flatten().collect()
            ))
        }
    }
}

impl ContentSample {
    /// Create an empty sample for unprocessable files
    pub fn empty() -> Self {
        Self {
            header: Vec::new(),
            footer: None,
            mime_type: None,
            structure_hints: ContentStructure::unknown(),
        }
    }

    /// Create an error sample for files that couldn't be processed
    pub fn error() -> Self {
        Self {
            header: vec![0xFF], // Special marker for error cases
            footer: None,
            mime_type: None,
            structure_hints: ContentStructure::unknown(),
        }
    }
}

impl ContentStructure {
    /// Create unknown structure
    pub fn unknown() -> Self {
        Self {
            is_binary: false,
            is_text: false,
            is_compressed: false,
            is_executable: false,
        }
    }
}

/// Detect MIME type from file header
fn detect_mime_type(header: &[u8]) -> Option<String> {
    if header.len() < 4 {
        return None;
    }

    // Common magic numbers
    match &header[..std::cmp::min(8, header.len())] {
        // Images
        [0xFF, 0xD8, 0xFF, ..] => Some("image/jpeg".to_string()),
        [0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A] => Some("image/png".to_string()),
        [0x47, 0x49, 0x46, 0x38, ..] => Some("image/gif".to_string()),
        
        // Archives
        [0x50, 0x4B, 0x03, 0x04, ..] | [0x50, 0x4B, 0x05, 0x06, ..] => Some("application/zip".to_string()),
        [0x52, 0x61, 0x72, 0x21, ..] => Some("application/x-rar-compressed".to_string()),
        [0x1F, 0x8B, ..] => Some("application/gzip".to_string()),
        
        // Documents  
        [0x25, 0x50, 0x44, 0x46, ..] => Some("application/pdf".to_string()),
        [0xD0, 0xCF, 0x11, 0xE0, ..] => Some("application/msword".to_string()),
        
        // Executables
        [0x4D, 0x5A, ..] => Some("application/x-executable".to_string()), // PE/COFF
        [0x7F, 0x45, 0x4C, 0x46, ..] => Some("application/x-executable".to_string()), // ELF
        
        // Media
        [0x00, 0x00, 0x00, 0x18, 0x66, 0x74, 0x79, 0x70] |
        [0x00, 0x00, 0x00, 0x1C, 0x66, 0x74, 0x79, 0x70] => Some("video/mp4".to_string()),
        
        _ => {
            // Check if it looks like text
            if header.iter().take(512).all(|&b| b.is_ascii() && (b.is_ascii_graphic() || b.is_ascii_whitespace())) {
                Some("text/plain".to_string())
            } else {
                None
            }
        }
    }
}

/// Analyze basic file structure
fn analyze_structure(header: &[u8], _footer: Option<&Vec<u8>>) -> ContentStructure {
    let header_sample = &header[..std::cmp::min(512, header.len())];
    
    // Check if binary (has non-printable bytes)
    let non_printable_count = header_sample.iter()
        .filter(|&&b| !b.is_ascii_graphic() && !b.is_ascii_whitespace())
        .count();
    let is_binary = non_printable_count > header_sample.len() / 4;
    
    // Check if text (mostly printable ASCII)
    let printable_count = header_sample.iter()
        .filter(|&&b| b.is_ascii_graphic() || b.is_ascii_whitespace())
        .count();
    let is_text = printable_count > header_sample.len() * 3 / 4;
    
    // Check for compression signatures
    let is_compressed = header.len() >= 4 && matches!(&header[..4], 
        [0x1F, 0x8B, _, _] |      // gzip
        [0x50, 0x4B, 0x03, 0x04] | // zip
        [0x50, 0x4B, 0x05, 0x06] | // zip empty
        [0x52, 0x61, 0x72, 0x21]   // rar
    );
    
    // Check for executable signatures  
    let is_executable = header.len() >= 4 && matches!(&header[..4],
        [0x4D, 0x5A, _, _] |      // PE/COFF (Windows)
        [0x7F, 0x45, 0x4C, 0x46] | // ELF (Linux/Unix)
        [0xFE, 0xED, 0xFA, 0xCE] | // Mach-O (macOS)
        [0xCE, 0xFA, 0xED, 0xFE]   // Mach-O (macOS, different endian)
    );

    ContentStructure {
        is_binary,
        is_text,
        is_compressed,
        is_executable,
    }
}