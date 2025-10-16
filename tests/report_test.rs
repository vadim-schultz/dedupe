//! Tests for the report generation functionality

use anyhow::Result;
use std::time::{Duration, SystemTime};

use dedupe::report::{DeduplicationReport, DeduplicationStats, ScanConfig};
use dedupe::types::FileInfo;

fn create_test_file_info(path: &str, size: u64) -> FileInfo {
    FileInfo {
        path: path.try_into().unwrap(),
        size,
        file_type: Some("text/plain".to_string()),
        modified: SystemTime::now(),
        created: Some(SystemTime::now()),
        readonly: false,
        hidden: false,
        checksum: None,
        metadata: None,
        quick_check: None,
        statistical_info: None,
    }
}

fn create_test_scan_config() -> ScanConfig {
    ScanConfig {
        directory: "/test".to_string(),
        max_depth: Some(5),
        min_file_size: 1024,
        similarity_threshold: 95,
        parallel_processing: true,
        thread_count: 4,
    }
}

#[test]
fn test_deduplication_report_creation() {
    let duplicate_groups = vec![
        vec![
            create_test_file_info("/path/file1.txt", 1024),
            create_test_file_info("/path/file1_copy.txt", 1024),
        ],
        vec![
            create_test_file_info("/path/file2.txt", 2048),
            create_test_file_info("/path/file2_copy.txt", 2048),
            create_test_file_info("/path/file2_copy2.txt", 2048),
        ],
    ];

    let report = DeduplicationReport::new(
        duplicate_groups,
        100,
        Duration::from_secs(10),
        10.0,
        create_test_scan_config(),
    );

    assert_eq!(report.stats.total_files, 100);
    assert_eq!(report.stats.duplicate_groups, 2);
    assert_eq!(report.stats.duplicate_files, 5); // 2 + 3 files
    assert_eq!(report.stats.potential_savings, 1024 + 2048 * 2); // 1 copy of each group
    assert_eq!(report.stats.scan_duration, Duration::from_secs(10));
    assert_eq!(report.stats.throughput, 10.0);
}

#[test]
fn test_empty_report() {
    let report = DeduplicationReport::new(
        vec![],
        50,
        Duration::from_secs(5),
        10.0,
        create_test_scan_config(),
    );

    assert_eq!(report.stats.total_files, 50);
    assert_eq!(report.stats.duplicate_groups, 0);
    assert_eq!(report.stats.duplicate_files, 0);
    assert_eq!(report.stats.potential_savings, 0);
    assert!(report.duplicate_groups.is_empty());
}

#[test]
fn test_single_file_groups_ignored() {
    let duplicate_groups = vec![
        vec![create_test_file_info("/path/unique.txt", 1024)], // Single file - should be ignored
        vec![
            create_test_file_info("/path/dup1.txt", 2048),
            create_test_file_info("/path/dup2.txt", 2048),
        ], // Actual duplicate group
    ];

    let report = DeduplicationReport::new(
        duplicate_groups,
        50,
        Duration::from_secs(1),
        50.0,
        create_test_scan_config(),
    );

    assert_eq!(report.stats.duplicate_groups, 1); // Only one group with >1 files
    assert_eq!(report.duplicate_groups.len(), 1);
    assert_eq!(report.duplicate_groups[0].files.len(), 2);
}

#[test]
fn test_primary_file_selection() -> Result<()> {
    use std::time::{Duration as StdDuration, UNIX_EPOCH};

    let old_time = UNIX_EPOCH + StdDuration::from_secs(1000);
    let new_time = UNIX_EPOCH + StdDuration::from_secs(2000);

    let mut old_file = create_test_file_info("/path/old.txt", 1024);
    old_file.modified = old_time;

    let mut new_file = create_test_file_info("/path/new.txt", 1024);
    new_file.modified = new_time;

    let duplicate_groups = vec![vec![old_file, new_file]];

    let report = DeduplicationReport::new(
        duplicate_groups,
        10,
        Duration::from_secs(1),
        10.0,
        create_test_scan_config(),
    );

    assert_eq!(report.duplicate_groups.len(), 1);
    let group = &report.duplicate_groups[0];
    assert_eq!(group.files.len(), 2);

    // The newer file should be marked as primary
    let primary_file = group.files.iter().find(|f| f.is_primary).unwrap();
    assert!(primary_file.path.contains("new.txt"));

    let duplicate_file = group.files.iter().find(|f| !f.is_primary).unwrap();
    assert!(duplicate_file.path.contains("old.txt"));

    Ok(())
}

#[test]
fn test_text_report_generation() -> Result<()> {
    let report = DeduplicationReport::new(
        vec![],
        10,
        Duration::from_secs(1),
        10.0,
        create_test_scan_config(),
    );

    let text = report.to_text()?;
    assert!(text.contains("DEDUPLICATION REPORT"));
    assert!(text.contains("Files scanned:"));
    assert!(text.contains("No duplicates found"));
    assert!(text.contains("/test")); // Directory from config
    Ok(())
}

#[test]
fn test_json_report_generation() -> Result<()> {
    let report = DeduplicationReport::new(
        vec![],
        10,
        Duration::from_secs(1),
        10.0,
        create_test_scan_config(),
    );

    let json = report.to_json()?;
    assert!(json.contains("\"stats\""));
    assert!(json.contains("\"duplicate_groups\""));
    assert!(json.contains("\"total_files\":10"));

    // Verify it's valid JSON
    let _: serde_json::Value = serde_json::from_str(&json)?;
    Ok(())
}

#[test]
fn test_json_pretty_report_generation() -> Result<()> {
    let report = DeduplicationReport::new(
        vec![],
        10,
        Duration::from_secs(1),
        10.0,
        create_test_scan_config(),
    );

    let json = report.to_json_pretty()?;
    assert!(json.contains("{\n")); // Check for pretty formatting
    assert!(json.contains("  ")); // Check for indentation

    // Verify it's valid JSON
    let _: serde_json::Value = serde_json::from_str(&json)?;
    Ok(())
}

#[test]
fn test_csv_report_generation() -> Result<()> {
    let duplicate_groups = vec![vec![
        create_test_file_info("/path/file1.txt", 1024),
        create_test_file_info("/path/file2.txt", 1024),
    ]];

    let report = DeduplicationReport::new(
        duplicate_groups,
        10,
        Duration::from_secs(1),
        10.0,
        create_test_scan_config(),
    );

    let csv = report.to_csv()?;
    let lines: Vec<&str> = csv.lines().collect();

    assert!(!lines.is_empty());
    assert!(lines[0].contains("group_id")); // Header
    assert!(lines[0].contains("file_path"));
    assert!(lines[0].contains("is_primary"));

    // Should have header + 2 data rows
    assert_eq!(lines.len(), 3);
    assert!(lines[1].contains("file1.txt") || lines[1].contains("file2.txt"));
    assert!(lines[2].contains("file1.txt") || lines[2].contains("file2.txt"));

    Ok(())
}

#[test]
fn test_scan_config_serialization() -> Result<()> {
    let config = create_test_scan_config();
    let json = serde_json::to_string(&config)?;
    assert!(json.contains("parallel_processing"));
    assert!(json.contains("thread_count"));
    assert!(json.contains("/test"));
    Ok(())
}

#[test]
fn test_deduplication_stats_default() {
    let stats = DeduplicationStats::default();
    assert_eq!(stats.total_files, 0);
    assert_eq!(stats.total_size, 0);
    assert_eq!(stats.duplicate_groups, 0);
    assert_eq!(stats.duplicate_files, 0);
    assert_eq!(stats.potential_savings, 0);
    assert_eq!(stats.scan_duration, Duration::from_secs(0));
    assert_eq!(stats.throughput, 0.0);
}
