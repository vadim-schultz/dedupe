//! JSON formatter for structured output

use anyhow::Result;
use serde_json;

use crate::report::DeduplicationReport;

/// Format a deduplication report as JSON
pub fn format_report(report: &DeduplicationReport) -> Result<String> {
    Ok(serde_json::to_string(report)?)
}

/// Format a deduplication report as pretty-printed JSON
pub fn format_report_pretty(report: &DeduplicationReport) -> Result<String> {
    Ok(serde_json::to_string_pretty(report)?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::report::{DeduplicationStats, ScanConfig};
    use std::time::SystemTime;

    #[test]
    fn test_format_json() {
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

        let json = format_report(&report).unwrap();
        assert!(json.contains("stats"));
        assert!(json.contains("duplicate_groups"));
        assert!(json.contains("/test"));

        // Verify it's valid JSON
        let _: serde_json::Value = serde_json::from_str(&json).unwrap();
    }

    #[test]
    fn test_format_json_pretty() {
        let report = DeduplicationReport {
            stats: DeduplicationStats::default(),
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

        let json = format_report_pretty(&report).unwrap();
        assert!(json.contains("{\n")); // Check for pretty formatting
        assert!(json.contains("  ")); // Check for indentation

        // Verify it's valid JSON
        let _: serde_json::Value = serde_json::from_str(&json).unwrap();
    }
}
