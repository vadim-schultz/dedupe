use std::sync::Arc;
use tempfile::TempDir;
use tokio::fs;
use crate::{
    config::Config,
    walker_hp::{HighPerformanceWalker, HighPerformanceConfig},
    pipeline::streaming::{StreamingPipeline, StreamingPipelineConfig},
    pipeline::{
        StreamingMetadataStage,
        StreamingQuickCheckStage, 
        StreamingStatisticalStage,
        StreamingHashStage,
    },
    utils::progress::ProgressTracker,
};

#[tokio::test]
async fn test_streaming_pipeline() -> Result<(), Box<dyn std::error::Error>> {
    // Create a temporary directory with test files
    let temp_dir = TempDir::new()?;
    let temp_path = temp_dir.path().to_path_buf();

    // Create some test files
    let file1 = temp_path.join("file1.txt");
    let file2 = temp_path.join("file2.txt");
    let file3 = temp_path.join("duplicate.txt");
    let file4 = temp_path.join("duplicate_copy.txt");

    fs::write(&file1, "unique content 1").await?;
    fs::write(&file2, "unique content 2").await?;
    fs::write(&file3, "duplicate content").await?;
    fs::write(&file4, "duplicate content").await?; // Duplicate of file3

    // Create config
    let config = Arc::new(Config {
        min_file_size: 1,
        max_depth: Some(10),
        threads_per_stage: 2,
        ..Config::default()
    });

    // Create progress tracker
    let progress_tracker = Arc::new(ProgressTracker::new());

    // Create high-performance walker
    let hp_config = HighPerformanceConfig {
        scanner_workers: 2,
        discovery_buffer_size: 1000,
        max_directory_queue_size: 1000,
        output_batch_size: 10,
        batch_timeout: std::time::Duration::from_millis(100),
    };

    let hp_walker = HighPerformanceWalker::new(
        config.clone(),
        progress_tracker.clone(),
        Some(hp_config),
    );

    // Create streaming pipeline
    let streaming_config = StreamingPipelineConfig {
        channel_capacity: 1000,
        batch_size: 10,
        flush_interval: std::time::Duration::from_millis(50),
        max_concurrent_files: 10,
        thread_count: 2,
    };

    let mut streaming_pipeline = StreamingPipeline::new(streaming_config);
    streaming_pipeline.set_progress_tracker(progress_tracker.clone());

    // Add stages
    streaming_pipeline.add_stage(StreamingMetadataStage::new(config.clone()));
    streaming_pipeline.add_stage(StreamingQuickCheckStage::new(config.clone()));
    streaming_pipeline.add_stage(StreamingStatisticalStage::new(config.clone()));
    streaming_pipeline.add_stage(StreamingHashStage::new(config.clone()));

    // Create file stream
    let (file_tx, file_rx) = tokio::sync::mpsc::channel(1000);

    // Start file discovery and pipeline processing
    let discovery_task = tokio::spawn({
        let hp_walker = hp_walker;
        let file_tx = file_tx.clone();
        let temp_path_clone = temp_path.clone();
        async move {
            hp_walker.stream_files(&[temp_path_clone], file_tx).await
        }
    });

    drop(file_tx); // Close sender

    let pipeline_task = tokio::spawn({
        async move {
            streaming_pipeline.start_streaming(file_rx).await
        }
    });

    // Wait for both tasks
    let (discovery_result, pipeline_result) = tokio::join!(discovery_task, pipeline_task);

    let _scan_stats = discovery_result??;
    let duplicate_groups = pipeline_result??;

    // Verify results
    println!("Found {} duplicate groups", duplicate_groups.len());
    
    // We should have at least one duplicate group (file3 and file4)
    let has_duplicates = duplicate_groups.iter().any(|group| group.files.len() >= 2);
    assert!(has_duplicates, "Should find at least one duplicate group");

    // Print results for verification
    for (i, group) in duplicate_groups.iter().enumerate() {
        if group.files.len() > 1 {
            println!("Duplicate group {}: {} files", i + 1, group.files.len());
            for file in &group.files {
                println!("  - {}", file.path);
            }
        }
    }

    Ok(())
}

#[tokio::test]
async fn test_high_performance_walker() -> Result<(), Box<dyn std::error::Error>> {
    // Create a temporary directory with test files
    let temp_dir = TempDir::new()?;
    let temp_path = temp_dir.path().to_path_buf();

    // Create some test files
    for i in 0..10 {
        let file_path = temp_path.join(format!("test_file_{}.txt", i));
        fs::write(&file_path, format!("content for file {}", i)).await?;
    }

    // Create config
    let config = Arc::new(Config {
        min_file_size: 1,
        max_depth: Some(10),
        threads_per_stage: 2,
        ..Config::default()
    });

    // Create progress tracker
    let progress_tracker = Arc::new(ProgressTracker::new());

    // Create high-performance walker with simple config
    let hp_walker = HighPerformanceWalker::new(
        config.clone(),
        progress_tracker.clone(),
        None, // Use default config
    );

    // Create file channel
    let (file_tx, mut file_rx) = tokio::sync::mpsc::channel(1000);

    // Start file discovery
    let discovery_task = tokio::spawn({
        let hp_walker = hp_walker;
        let temp_path_clone = temp_path.clone();
        async move {
            hp_walker.stream_files(&[temp_path_clone], file_tx).await
        }
    });

    // Collect files
    let mut discovered_files = Vec::new();
    while let Some(file) = file_rx.recv().await {
        discovered_files.push(file);
    }

    // Wait for discovery to complete
    let scan_stats = discovery_task.await??;

    // Verify results
    println!("Discovered {} files", discovered_files.len());
    println!("Scan stats: {:?}", scan_stats);

    assert!(discovered_files.len() >= 10, "Should discover at least 10 files");
    assert_eq!(scan_stats.files_discovered, discovered_files.len());

    Ok(())
}