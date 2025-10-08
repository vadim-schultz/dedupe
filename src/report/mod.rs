//! Report generation module for dedupe results
//! 
//! Provides functionality to generate reports in multiple formats:
//! - Text (human-readable console output)
//! - JSON (structured data for APIs)
//! - CSV (tabular data for spreadsheets)

use std::time::{Duration, SystemTime};
use serde::{Deserialize, Serialize};
use anyhow::Result;

use crate::types::FileInfo;

pub mod formatters;

/// Statistics about the deduplication process
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeduplicationStats {
    /// Total number of files scanned
    pub total_files: usize,
    /// Total size of all files scanned
    pub total_size: u64,
    /// Number of duplicate groups found
    pub duplicate_groups: usize,
    /// Total number of duplicate files
    pub duplicate_files: usize,
    /// Total size that could be saved by removing duplicates
    pub potential_savings: u64,
    /// Time taken for the scan
    pub scan_duration: Duration,
    /// Files processed per second
    pub throughput: f64,
}

/// Information about a group of duplicate files
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DuplicateGroup {
    /// Unique identifier for this group
    pub id: usize,
    /// Hash or similarity score that identifies this group
    pub identifier: String,
    /// Similarity percentage (100 for exact duplicates)
    pub similarity: f64,
    /// Total size of files in this group
    pub total_size: u64,
    /// Size that can be saved (total_size - size of one file)
    pub potential_savings: u64,
    /// Files in this duplicate group
    pub files: Vec<FileEntry>,
}

/// Information about a single file in a duplicate group
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileEntry {
    /// Full path to the file
    pub path: String,
    /// File size in bytes
    pub size: u64,
    /// Last modified time
    pub modified: SystemTime,
    /// File type/MIME type if detected
    pub file_type: Option<String>,
    /// Whether this is the "primary" file (newest, or first found)
    pub is_primary: bool,
}

/// Complete deduplication report
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeduplicationReport {
    /// Overall statistics
    pub stats: DeduplicationStats,
    /// Groups of duplicate files
    pub duplicate_groups: Vec<DuplicateGroup>,
    /// Report generation timestamp
    pub generated_at: SystemTime,
    /// Configuration used for the scan
    pub scan_config: ScanConfig,
}

/// Configuration information included in reports
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScanConfig {
    /// Directory that was scanned
    pub directory: String,
    /// Maximum depth traversed
    pub max_depth: Option<usize>,
    /// Minimum file size considered
    pub min_file_size: u64,
    /// Similarity threshold used
    pub similarity_threshold: u8,
    /// Whether parallel processing was used
    pub parallel_processing: bool,
    /// Number of threads used
    pub thread_count: usize,
}

impl DeduplicationReport {
    /// Create a new report from scan results
    pub fn new(
        duplicate_groups: Vec<Vec<FileInfo>>,
        total_files: usize,
        scan_duration: Duration,
        throughput: f64,
        scan_config: ScanConfig,
    ) -> Self {
        let mut report_groups = Vec::new();
        let mut total_size = 0u64;
        let mut duplicate_files = 0;
        let mut potential_savings = 0u64;

        for (id, group) in duplicate_groups.iter().enumerate() {
            if group.len() <= 1 {
                continue; // Skip non-duplicate groups
            }

            let group_size = group[0].size;
            let group_potential_savings = group_size * (group.len() - 1) as u64;
            potential_savings += group_potential_savings;
            duplicate_files += group.len();

            // Convert to FileEntry and mark the newest as primary
            let mut files: Vec<FileEntry> = group
                .iter()
                .map(|f| FileEntry {
                    path: f.path.to_string(),
                    size: f.size,
                    modified: f.modified,
                    file_type: f.file_type.clone(),
                    is_primary: false,
                })
                .collect();

            // Sort by modification time (newest first) and mark the first as primary
            files.sort_by(|a, b| b.modified.cmp(&a.modified));
            if !files.is_empty() {
                files[0].is_primary = true;
            }

            report_groups.push(DuplicateGroup {
                id: id + 1,
                identifier: format!("group_{}", id + 1),
                similarity: 100.0, // Assuming exact duplicates for now
                total_size: group_size * group.len() as u64,
                potential_savings: group_potential_savings,
                files,
            });
        }

        // Calculate total size (approximation)
        for group in &duplicate_groups {
            total_size += group[0].size * group.len() as u64;
        }

        let stats = DeduplicationStats {
            total_files,
            total_size,
            duplicate_groups: report_groups.len(),
            duplicate_files,
            potential_savings,
            scan_duration,
            throughput,
        };

        Self {
            stats,
            duplicate_groups: report_groups,
            generated_at: SystemTime::now(),
            scan_config,
        }
    }

    /// Generate a text report
    pub fn to_text(&self) -> Result<String> {
        formatters::text::format_report(self)
    }

    /// Generate a JSON report
    pub fn to_json(&self) -> Result<String> {
        formatters::json::format_report(self)
    }

    /// Generate a JSON report with pretty printing
    pub fn to_json_pretty(&self) -> Result<String> {
        formatters::json::format_report_pretty(self)
    }

    /// Generate a CSV report
    pub fn to_csv(&self) -> Result<String> {
        formatters::csv::format_report(self)
    }
}

impl Default for DeduplicationStats {
    fn default() -> Self {
        Self {
            total_files: 0,
            total_size: 0,
            duplicate_groups: 0,
            duplicate_files: 0,
            potential_savings: 0,
            scan_duration: Duration::from_secs(0),
            throughput: 0.0,
        }
    }
}