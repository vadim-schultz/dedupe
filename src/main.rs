use std::sync::Arc;
use clap::Parser;
use tracing::Level;
use tracing_subscriber::FmtSubscriber;

mod config;
mod types;
mod walker_hp; // High-performance walker
mod pipeline;
mod utils;
mod report;

use crate::{
    config::Config,
    types::FileInfo,
    report::{DeduplicationReport, ScanConfig},
    walker_hp::{HighPerformanceWalker, HighPerformanceConfig},
    utils::{
        progress::ProgressTracker,
    },
};

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Directory to scan for duplicates
    #[arg(default_value = ".")]
    dir: String,

    /// Maximum depth to traverse
    #[arg(short, long, default_value_t = std::u32::MAX)]
    depth: u32,

    /// Minimum file size to consider (in bytes)
    #[arg(short, long, default_value_t = 1024)]
    min_size: u64,

    /// Performance mode: standard, high, ultra
    #[arg(short = 'p', long, default_value = "high")]
    performance: String,

    /// Number of worker threads (0 = auto-detect optimal count)
    #[arg(short, long, default_value_t = 0)]
    threads: usize,

    /// Enable verbose logging and detailed statistics
    #[arg(short, long, default_value_t = false)]
    verbose: bool,

    /// Report output format
    #[arg(short = 'f', long, default_value = "text")]
    format: String,

    /// Output file path (if not specified, output to console)
    #[arg(short, long)]
    output: Option<String>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Parse command line arguments first to check verbosity
    let args = Args::parse();
    
    // Initialize logging based on verbosity
    let log_level = if args.verbose { Level::DEBUG } else { Level::INFO };
    FmtSubscriber::builder()
        .with_max_level(log_level)
        .compact() // Use compact format instead of pretty for cleaner output
        .without_time() // Remove timestamps for cleaner output
        .with_target(false) // Remove target info like "at src\main.rs:125"
        .init();

    println!("🚀 Starting high-performance file deduplication scan");
    println!("📁 Directory: {}", args.dir);
    
    // Determine optimal thread count based on performance mode
    let thread_count = if args.threads > 0 { 
        args.threads 
    } else {
        match args.performance.as_str() {
            "ultra" => (num_cpus::get() * 4).max(8),
            "high" => (num_cpus::get() * 2).max(4),
            _ => num_cpus::get(),
        }
    };
    
    // Configure high-performance settings
    let hp_config = match args.performance.as_str() {
        "ultra" => HighPerformanceConfig::ultra_performance(),
        "high" => HighPerformanceConfig::default(),
        _ => HighPerformanceConfig {
            scanner_workers: thread_count,
            ..HighPerformanceConfig::default()
        },
    };
    
    if args.verbose {
        println!("📏 Maximum depth: {}", args.depth);
        println!("📐 Minimum file size: {} bytes", args.min_size);
        println!("⚡ Performance mode: {} (always streaming + parallel)", args.performance);
        println!("🧵 Worker threads: {}", thread_count);
        println!("🔍 Scanner workers: {}", hp_config.scanner_workers);
        println!("📦 Batch size: {}", hp_config.output_batch_size);
    }

    // Create high-performance configuration
    let config = Arc::new(Config {
        max_depth: Some(args.depth as usize),
        min_file_size: args.min_size,
        threads_per_stage: thread_count,
        quick_check_sample_size: match args.performance.as_str() {
            "ultra" => 8192,
            "high" => 4096,
            _ => 2048,
        },
        similarity_threshold: 95,
        mode: config::OperationMode::Report,
        parallel_scan: true, // Always use parallel scanning for performance
        extensions: Vec::new(), // Include all file types by default
        ..Config::default()
    });

    // Create progress tracker for visual feedback
    let progress_tracker = Arc::new(ProgressTracker::new());
    
    // Always use high-performance streaming pipeline
    println!("🌊 Starting high-performance streaming scan and processing...");
    
    let start_time = std::time::Instant::now();
    
    // Create high-performance streaming walker
    let hp_walker = HighPerformanceWalker::new(
        config.clone(),
        progress_tracker.clone(),
        Some(hp_config),
    );
    
    // Create streaming pipeline configuration
    use crate::pipeline::streaming::{StreamingPipeline, StreamingPipelineConfig};
    
    let streaming_config = StreamingPipelineConfig {
        channel_capacity: match args.performance.as_str() {
            "ultra" => 100_000,
            "high" => 50_000,
            _ => 20_000,
        },
        batch_size: match args.performance.as_str() {
            "ultra" => 1000,
            "high" => 500,
            _ => 200,
        },
        flush_interval: match args.performance.as_str() {
            "ultra" => std::time::Duration::from_millis(10),
            "high" => std::time::Duration::from_millis(25),
            _ => std::time::Duration::from_millis(50),
        },
        max_concurrent_files: thread_count * 4,
        thread_count,
    };
    
    // Create and configure streaming pipeline with stages
    // Create file stream channel using the config
    let channel_capacity = streaming_config.channel_capacity;
    let scan_dir = args.dir.clone(); // Clone for use in async closure
    
    let mut streaming_pipeline = StreamingPipeline::new(streaming_config);
    streaming_pipeline.set_progress_tracker(progress_tracker.clone());
    
    // Add streaming pipeline stages
    use crate::pipeline::{
        streaming_metadata::StreamingMetadataStage,
        streaming_quickcheck::StreamingQuickCheckStage,
        streaming_statistical::StreamingStatisticalStage,
        streaming_hash::StreamingHashStage,
    };
    
    streaming_pipeline.add_stage(StreamingMetadataStage::new(config.clone()));
    streaming_pipeline.add_stage(StreamingQuickCheckStage::new(config.clone()));
    streaming_pipeline.add_stage(StreamingStatisticalStage::new(config.clone()));
    streaming_pipeline.add_stage(StreamingHashStage::new(config.clone()));
    
    // Create file stream channel
    let (file_tx, file_rx) = tokio::sync::mpsc::channel(channel_capacity);
    
    // Start file discovery in parallel
    let discovery_handle = tokio::spawn({
        let hp_walker = hp_walker;
        let file_tx = file_tx.clone();
        async move {
            hp_walker.stream_files(&[std::path::Path::new(&scan_dir)], file_tx).await
        }
    });
    
    // Close the sender and start streaming pipeline
    drop(file_tx);
    
    let (pipeline_result, discovery_result) = tokio::join!(
        streaming_pipeline.start_streaming(file_rx),
        discovery_handle
    );
    
    let scan_stats = match discovery_result {
        Ok(Ok(stats)) => stats,
        Ok(Err(e)) => {
            eprintln!("Discovery error: {}", e);
            return Err(e.into());
        }
        Err(e) => {
            eprintln!("Discovery task error: {}", e);
            return Err(e.into());
        }
    };
    
    let all_duplicate_groups = match pipeline_result {
        Ok(groups) => groups,
        Err(e) => {
            eprintln!("Pipeline error: {}", e);
            return Err(e.into());
        }
    };
    
    let execution_time = start_time.elapsed();
    let total_files_found = scan_stats.files_discovered;
    
    // Print performance statistics if verbose
    if args.verbose {
        println!("\n⚡ High-Performance Scan Statistics:");
        println!("   ├─ Files discovered: {}", scan_stats.files_discovered);
        println!("   ├─ Directories processed: {}", scan_stats.directories_processed);
        println!("   ├─ Files sent to pipeline: {}", scan_stats.files_sent_to_pipeline);
        println!("   ├─ Total size: {} MB", scan_stats.total_size_bytes / 1024 / 1024);
        println!("   ├─ Scan duration: {:.3}s", execution_time.as_secs_f64());
        
        if scan_stats.errors_encountered > 0 {
            println!("   └─ ⚠️  {} errors encountered", scan_stats.errors_encountered);
        } else {
            println!("   └─ ✅ No errors");
        }
        
        if execution_time.as_secs_f64() > 0.0 {
            let discovery_rate = scan_stats.files_discovered as f64 / execution_time.as_secs_f64();
            println!("🚀 Performance: {:.0} files/sec discovery + processing rate", discovery_rate);
        }
    }
    
    // Ensure all progress bars are finished
    progress_tracker.finish_all();
    
    if total_files_found == 0 {
        return Ok(());
    }
    
    // Generate and display report
    let scan_config = ScanConfig {
        directory: args.dir.clone(),
        max_depth: if args.depth == std::u32::MAX { None } else { Some(args.depth as usize) },
        min_file_size: args.min_size,
        similarity_threshold: config.similarity_threshold,
        parallel_processing: true,
        thread_count,
    };

    // Convert DuplicateGroup to Vec<Vec<FileInfo>> format expected by report
    let duplicate_groups_for_report: Vec<Vec<FileInfo>> = all_duplicate_groups
        .into_iter()
        .map(|group| group.files)
        .collect();
    
    let report = DeduplicationReport::new(
        duplicate_groups_for_report,
        total_files_found,
        execution_time,
        if execution_time.as_secs_f64() > 0.0 { 
            total_files_found as f64 / execution_time.as_secs_f64() 
        } else { 
            0.0 
        },
        scan_config,
    );

    // Generate report in requested format
    let report_content = match args.format.to_lowercase().as_str() {
        "text" => report.to_text()?,
        "json" => report.to_json_pretty()?,
        "csv" => report.to_csv()?,
        _ => {
            eprintln!("Error: Unsupported format '{}'. Supported formats: text, json, csv", args.format);
            std::process::exit(1);
        }
    };

    // Output to file or console
    match args.output {
        Some(output_path) => {
            std::fs::write(&output_path, &report_content)?;
            println!("💾 Report saved to: {}", output_path);
        }
        None => {
            println!("{}", report_content);
        }
    }
    
    Ok(())
}
