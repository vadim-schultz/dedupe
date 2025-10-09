use anyhow::Result;
use std::sync::Arc;
use tempfile::TempDir;
use std::fs;

use dedupe::{Config, StreamingWalker, StreamingConfig, types::FileInfo};
use dedupe::utils::progress::ProgressTracker;
use dedupe::pipeline::ProcessingResult;

/// Test the streaming walker with a simple directory structure
#[tokio::test]
async fn test_streaming_walker_basic() -> Result<()> {
    // Create temporary directory structure
    let temp_dir = TempDir::new()?;
    let root_path = temp_dir.path();
    
    // Create some test files
    let file1 = root_path.join("file1.txt");
    let file2 = root_path.join("file2.txt");
    let subdir = root_path.join("subdir");
    fs::create_dir(&subdir)?;
    let file3 = subdir.join("file3.txt");
    
    fs::write(&file1, "Hello, world! This is a test file.")?;
    fs::write(&file2, "Another test file with different content.")?;
    fs::write(&file3, "File in subdirectory.")?;
    
    // Create config
    let config = Arc::new(Config {
        max_depth: Some(10),
        min_file_size: 1,
        threads_per_stage: 2,
        ..Config::default()
    });
    
    // Create progress tracker
    let progress_tracker = Arc::new(ProgressTracker::new());
    
    // Create streaming walker with simple config
    let streaming_config = StreamingConfig::simple_streaming_config(Some(2));
    let streaming_walker = StreamingWalker::new(
        config.clone(),
        progress_tracker.clone(),
        Some(streaming_config)
    );
    
    // Track processed files
    let processed_files = Arc::new(std::sync::Mutex::new(Vec::new()));
    let files_clone = Arc::clone(&processed_files);
    
    // Test streaming to pipeline
    let result = streaming_walker.stream_to_pipeline(
        &[root_path],
        move |batch: Vec<FileInfo>| {
            let files = Arc::clone(&files_clone);
            async move {
                // Store the batch for verification
                {
                    let mut files_guard = files.lock().unwrap();
                    files_guard.extend(batch);
                }
                
                // Return Continue to indicate processing succeeded
                Ok(ProcessingResult::Continue(Vec::new()))
            }
        }
    ).await?;
    
    // Verify results
    let final_files = processed_files.lock().unwrap();
    assert!(final_files.len() >= 3, "Should discover at least 3 files");
    
    // Check that files were found
    let file_names: Vec<String> = final_files
        .iter()
        .map(|f| f.path.file_name().unwrap_or("").to_string())
        .collect();
    
    assert!(file_names.contains(&"file1.txt".to_string()));
    assert!(file_names.contains(&"file2.txt".to_string()));
    assert!(file_names.contains(&"file3.txt".to_string()));
    
    // Verify statistics
    let stats = result.stats;
    assert!(stats.files_discovered >= 3);
    assert!(stats.directories_processed >= 2); // root + subdir
    
    progress_tracker.finish_all();
    
    println!("✅ Streaming walker test completed successfully");
    println!("   Files discovered: {}", stats.files_discovered);
    println!("   Directories processed: {}", stats.directories_processed);
    println!("   Duration: {:.2}s", result.duration.as_secs_f64());
    
    Ok(())
}

/// Test streaming walker performance characteristics
#[tokio::test]
async fn test_streaming_walker_performance() -> Result<()> {
    // Create temporary directory with more files
    let temp_dir = TempDir::new()?;
    let root_path = temp_dir.path();
    
    // Create multiple subdirectories with files
    for i in 0..5 {
        let subdir = root_path.join(format!("dir_{}", i));
        fs::create_dir(&subdir)?;
        
        for j in 0..10 {
            let file = subdir.join(format!("file_{}_{}.txt", i, j));
            fs::write(&file, format!("Content for file {} in directory {}", j, i))?;
        }
    }
    
    // Create config
    let config = Arc::new(Config {
        max_depth: Some(10),
        min_file_size: 1,
        threads_per_stage: 4,
        ..Config::default()
    });
    
    // Create progress tracker
    let progress_tracker = Arc::new(ProgressTracker::new());
    
    // Test high throughput config
    let streaming_config = StreamingConfig::high_throughput_config();
    let streaming_walker = StreamingWalker::new(
        config.clone(),
        progress_tracker.clone(),
        Some(streaming_config)
    );
    
    let batch_count = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let files_count = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    
    let batches_clone = Arc::clone(&batch_count);
    let files_clone = Arc::clone(&files_count);
    
    let start_time = std::time::Instant::now();
    
    let result = streaming_walker.stream_to_pipeline(
        &[root_path],
        move |batch: Vec<FileInfo>| {
            let batches = Arc::clone(&batches_clone);
            let files = Arc::clone(&files_clone);
            
            async move {
                batches.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                files.fetch_add(batch.len(), std::sync::atomic::Ordering::Relaxed);
                
                // Simulate some processing time
                tokio::time::sleep(std::time::Duration::from_millis(1)).await;
                
                Ok(ProcessingResult::Continue(Vec::new()))
            }
        }
    ).await?;
    
    let total_time = start_time.elapsed();
    
    let final_batches = batch_count.load(std::sync::atomic::Ordering::Relaxed);
    let final_files = files_count.load(std::sync::atomic::Ordering::Relaxed);
    
    // Verify performance characteristics
    assert!(final_files >= 50, "Should process at least 50 files");
    assert!(final_batches > 0, "Should create at least one batch");
    
    let discovery_rate = result.stats.files_discovered as f64 / result.duration.as_secs_f64();
    let processing_rate = final_files as f64 / total_time.as_secs_f64();
    
    progress_tracker.finish_all();
    
    println!("✅ Performance test completed");
    println!("   Files discovered: {}", result.stats.files_discovered);
    println!("   Files processed: {}", final_files);
    println!("   Batches created: {}", final_batches);
    println!("   Discovery rate: {:.0} files/sec", discovery_rate);
    println!("   Processing rate: {:.0} files/sec", processing_rate);
    println!("   Total time: {:.3}s", total_time.as_secs_f64());
    
    // Basic performance assertions
    assert!(discovery_rate > 10.0, "Discovery rate should be reasonable");
    assert!(processing_rate > 10.0, "Processing rate should be reasonable");
    
    Ok(())
}