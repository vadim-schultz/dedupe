//! CSV formatter for tabular data

use anyhow::Result;
use std::fmt::Write;

use crate::report::DeduplicationReport;

/// Format a deduplication report as CSV
/// Creates a CSV with one row per duplicate file
pub fn format_report(report: &DeduplicationReport) -> Result<String> {
    let mut output = String::new();

    // CSV Header
    writeln!(output, "group_id,similarity,file_path,file_size,modified_date,file_type,is_primary,potential_savings")?;

    // Data rows
    for group in &report.duplicate_groups {
        for file in &group.files {
            let modified = file
                .modified
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| {
                    let dt =
                        chrono::DateTime::from_timestamp(d.as_secs() as i64, 0).unwrap_or_default();
                    dt.format("%Y-%m-%d %H:%M:%S").to_string()
                })
                .unwrap_or_else(|_| "unknown".to_string());

            let file_type = file.file_type.as_deref().unwrap_or("unknown");
            let savings = if file.is_primary { 0 } else { file.size };

            writeln!(
                output,
                "{},{},{},{},\"{}\",\"{}\",{},{}",
                group.id,
                group.similarity,
                escape_csv_field(&file.path),
                file.size,
                modified,
                file_type,
                file.is_primary,
                savings
            )?;
        }
    }

    Ok(output)
}

/// Escape a field for CSV output
fn escape_csv_field(field: &str) -> String {
    if field.contains(',') || field.contains('"') || field.contains('\n') {
        format!("\"{}\"", field.replace('"', "\"\""))
    } else {
        field.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::report::{DeduplicationStats, DuplicateGroup, FileEntry, ScanConfig};
    use std::time::SystemTime;

    #[test]
    fn test_format_empty_csv() {
        let report = DeduplicationReport {
            stats: DeduplicationStats::default(),
            duplicate_groups: vec![],
            generated_at: SystemTime::now(),
            scan_config: ScanConfig {
                directory: "/test".to_string(),
                max_depth: Some(5),
                min_file_size: 1024,
                similarity_threshold: 95,
                parallel_processing: true,
                thread_count: 4,
            },
        };

        let csv = format_report(&report).unwrap();
        let lines: Vec<&str> = csv.lines().collect();
        assert_eq!(lines.len(), 1); // Only header
        assert!(lines[0].contains("group_id"));
        assert!(lines[0].contains("file_path"));
    }

    #[test]
    fn test_format_csv_with_data() {
        let group = DuplicateGroup {
            id: 1,
            identifier: "test".to_string(),
            similarity: 100.0,
            total_size: 2048,
            potential_savings: 1024,
            files: vec![
                FileEntry {
                    path: "/path/to/file1.txt".to_string(),
                    size: 1024,
                    modified: SystemTime::now(),
                    file_type: Some("text/plain".to_string()),
                    is_primary: true,
                },
                FileEntry {
                    path: "/path/to/file2.txt".to_string(),
                    size: 1024,
                    modified: SystemTime::now(),
                    file_type: Some("text/plain".to_string()),
                    is_primary: false,
                },
            ],
        };

        let report = DeduplicationReport {
            stats: DeduplicationStats::default(),
            duplicate_groups: vec![group],
            generated_at: SystemTime::now(),
            scan_config: ScanConfig {
                directory: "/test".to_string(),
                max_depth: None,
                min_file_size: 0,
                similarity_threshold: 100,
                parallel_processing: true,
                thread_count: 4,
            },
        };

        let csv = format_report(&report).unwrap();
        let lines: Vec<&str> = csv.lines().collect();
        assert_eq!(lines.len(), 3); // Header + 2 data rows
        assert!(lines[1].contains("file1.txt"));
        assert!(lines[2].contains("file2.txt"));
        assert!(lines[1].contains("true")); // is_primary
        assert!(lines[2].contains("false")); // is_primary
    }

    #[test]
    fn test_escape_csv_field() {
        assert_eq!(escape_csv_field("simple"), "simple");
        assert_eq!(escape_csv_field("with,comma"), "\"with,comma\"");
        assert_eq!(escape_csv_field("with\"quote"), "\"with\"\"quote\"");
        assert_eq!(escape_csv_field("with\nnewline"), "\"with\nnewline\"");
    }
}
