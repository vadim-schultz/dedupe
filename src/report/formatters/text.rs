//! Text formatter for human-readable console output

use anyhow::Result;
use bytesize::ByteSize;
use std::fmt::Write;

use crate::report::DeduplicationReport;

/// Format a deduplication report as human-readable text
pub fn format_report(report: &DeduplicationReport) -> Result<String> {
    let mut output = String::new();

    // Header
    writeln!(output, "═══════════════════════════════════════")?;
    writeln!(output, "           DEDUPLICATION REPORT")?;
    writeln!(output, "═══════════════════════════════════════")?;
    writeln!(output)?;

    // Statistics summary
    let stats = &report.stats;
    writeln!(output, "📊 SCAN STATISTICS")?;
    writeln!(output, "─────────────────────────────────────")?;
    writeln!(output, "Files scanned:      {:>8}", stats.total_files)?;
    writeln!(
        output,
        "Total size:         {:>8}",
        ByteSize::b(stats.total_size)
    )?;
    writeln!(
        output,
        "Scan duration:      {:>8.2}s",
        stats.scan_duration.as_secs_f64()
    )?;
    writeln!(
        output,
        "Throughput:         {:>8.1} files/sec",
        stats.throughput
    )?;
    writeln!(output)?;

    // Duplicate statistics
    writeln!(output, "🔍 DUPLICATE ANALYSIS")?;
    writeln!(output, "─────────────────────────────────────")?;
    writeln!(output, "Duplicate groups:   {:>8}", stats.duplicate_groups)?;
    writeln!(output, "Duplicate files:    {:>8}", stats.duplicate_files)?;
    writeln!(
        output,
        "Potential savings:  {:>8}",
        ByteSize::b(stats.potential_savings)
    )?;

    if stats.total_size > 0 {
        let savings_percentage = (stats.potential_savings as f64 / stats.total_size as f64) * 100.0;
        writeln!(output, "Space efficiency:   {:>8.1}%", savings_percentage)?;
    }
    writeln!(output)?;

    // Configuration used
    let config = &report.scan_config;
    writeln!(output, "⚙️  SCAN CONFIGURATION")?;
    writeln!(output, "─────────────────────────────────────")?;
    writeln!(output, "Directory:          {}", config.directory)?;
    writeln!(
        output,
        "Min file size:      {}",
        ByteSize::b(config.min_file_size)
    )?;
    writeln!(
        output,
        "Similarity threshold: {}%",
        config.similarity_threshold
    )?;
    writeln!(
        output,
        "Parallel processing: {}",
        if config.parallel_processing {
            "Yes"
        } else {
            "No"
        }
    )?;
    writeln!(output, "Thread count:       {}", config.thread_count)?;
    if let Some(depth) = config.max_depth {
        writeln!(output, "Max depth:          {}", depth)?;
    } else {
        writeln!(output, "Max depth:          Unlimited")?;
    }
    writeln!(output)?;

    // Duplicate groups details
    if !report.duplicate_groups.is_empty() {
        writeln!(output, "📁 DUPLICATE GROUPS")?;
        writeln!(output, "─────────────────────────────────────")?;

        for group in &report.duplicate_groups {
            writeln!(
                output,
                "Group {}: {} files, {} total size, {} savings",
                group.id,
                group.files.len(),
                ByteSize::b(group.total_size),
                ByteSize::b(group.potential_savings)
            )?;

            for file in &group.files {
                let marker = if file.is_primary { "🟢 " } else { "🔴 " };
                let modified = file
                    .modified
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| {
                        let dt = chrono::DateTime::from_timestamp(d.as_secs() as i64, 0)
                            .unwrap_or_default();
                        dt.format("%Y-%m-%d %H:%M:%S").to_string()
                    })
                    .unwrap_or_else(|_| "unknown".to_string());

                writeln!(output, "  {}{} ({})", marker, file.path, modified)?;
            }
            writeln!(output)?;
        }
    } else {
        writeln!(output, "✅ No duplicates found!")?;
        writeln!(output)?;
    }

    // Footer with legend
    if !report.duplicate_groups.is_empty() {
        writeln!(
            output,
            "Legend: 🟢 = Keep (newest/primary)  🔴 = Duplicate (can be removed)"
        )?;
    }

    Ok(output)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::report::{DeduplicationStats, ScanConfig};
    use std::time::{Duration, SystemTime};

    #[test]
    fn test_format_empty_report() {
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

        let text = format_report(&report).unwrap();
        assert!(text.contains("DEDUPLICATION REPORT"));
        assert!(text.contains("No duplicates found"));
        assert!(text.contains("/test"));
    }

    #[test]
    fn test_format_report_with_stats() {
        let stats = DeduplicationStats {
            total_files: 100,
            total_size: 1024 * 1024,
            duplicate_groups: 5,
            duplicate_files: 15,
            potential_savings: 512 * 1024,
            scan_duration: Duration::from_secs(10),
            throughput: 10.0,
        };

        let report = DeduplicationReport {
            stats,
            duplicate_groups: vec![],
            generated_at: SystemTime::now(),
            scan_config: ScanConfig {
                directory: "/test".to_string(),
                max_depth: None,
                min_file_size: 0,
                similarity_threshold: 90,
                parallel_processing: false,
                thread_count: 1,
            },
        };

        let text = format_report(&report).unwrap();
        assert!(text.contains("Files scanned:           100"));
        assert!(text.contains("Duplicate groups:          5"));
        assert!(text.contains("10.0 files/sec"));
        assert!(text.contains("Unlimited"));
    }
}
