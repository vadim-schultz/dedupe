use std::sync::Arc;
use clap::Parser;
use tracing::Level;
use tracing_subscriber::FmtSubscriber;

mod config;
mod types;
mod walker_simple;
mod pipeline;
mod utils;
mod report;

use crate::{
    config::Config,
    report::{DeduplicationReport, ScanConfig},

    walker_simple::SimpleWalker,
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

    /// Use parallel pipeline processing (enabled by default)
    #[arg(short, long, default_value_t = true)]
    parallel: bool,

    /// Use parallel scanning (faster for large directories)
    #[arg(long, default_value_t = false)]
    parallel_scan: bool,

    /// Number of worker threads for parallel processing (0 = auto-detect)
    #[arg(short, long, default_value_t = 0)]
    threads: usize,

    /// Enable verbose logging
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

    println!("🚀 Starting file deduplication scan");
    println!("📁 Directory: {}", args.dir);
    
    let thread_count = if args.threads > 0 { args.threads } else { num_cpus::get() };
    
    if args.verbose {
        println!("📏 Maximum depth: {}", args.depth);
        println!("📐 Minimum file size: {} bytes", args.min_size);
        println!("⚙️  Parallel processing: {}", args.parallel);
        println!("🔍 Parallel scanning: {}", args.parallel_scan);
        println!("🧵 Worker threads: {}", thread_count);
    }

    // Create configuration
    let config = Arc::new(Config {
        max_depth: Some(args.depth as usize),
        min_file_size: args.min_size,
        threads_per_stage: thread_count,
        quick_check_sample_size: 4096,
        similarity_threshold: 95,
        mode: config::OperationMode::Report,
        parallel_scan: args.parallel_scan,
        extensions: Vec::new(), // Include all file types by default
        ..Config::default()
    });

    // Create progress tracker for visual feedback
    let progress_tracker = Arc::new(ProgressTracker::new());
    
    println!("🔍 Scanning files...");
    let walker = SimpleWalker::new(config.clone(), progress_tracker.clone());
    let scan_result = walker.walk(&[std::path::Path::new(&args.dir)]).await?;
    
    if args.verbose {
        println!("📊 Scan Statistics:");
        println!("   ├─ Directories processed: {}", scan_result.stats.directories_processed);
        println!("   ├─ Files scanned: {}", scan_result.stats.files_scanned);
        println!("   ├─ Errors encountered: {}", scan_result.stats.errors);
        println!("   └─ Scan duration: {:.2}s", scan_result.stats.total_duration.as_secs_f64());
    }
    
    let files = scan_result.files;
    println!("✅ Found {} files", files.len());
    
    let total_files_found = files.len();
    
    if total_files_found == 0 {
        println!("✅ No files found to analyze.");
        progress_tracker.finish_all();
    } else {
        // Use the sophisticated pipeline with multiple stages and progress bars
        let start_time = std::time::Instant::now();
        println!("🔄 Analyzing files for duplicates...");
        
        // Import pipeline components
        use crate::pipeline::{
            ParallelPipeline, ParallelConfig,
            metadata::MetadataStage,
            quick_check::QuickCheckStage,
            stats::StatisticalStage,
            hash::HashStage,
        };
        
        // Create parallel pipeline with progress tracking
        let pipeline_config = ParallelConfig {
            threads_per_stage: thread_count,
            channel_capacity: 1000,
            batch_size: 50,
            max_concurrent_tasks: thread_count * 2,
            metrics_interval: std::time::Duration::from_secs(1),
        };
        
        let mut pipeline = ParallelPipeline::new(pipeline_config)?;
        pipeline.set_progress_tracker(progress_tracker.clone());
        
        // Add pipeline stages with progress bars
        pipeline.add_stage(MetadataStage::new(config.clone()));
        pipeline.add_stage(QuickCheckStage::new(config.clone()));
        pipeline.add_stage(StatisticalStage::new(config.clone()));
        pipeline.add_stage(HashStage::new(None));
        
        // Execute pipeline with full progress visualization
        let duplicate_groups = pipeline.execute(files).await?;
        let execution_time = start_time.elapsed();
        
        // Ensure all progress bars are finished and cleaned up
        progress_tracker.finish_all();
        
        // Generate and display report
        let scan_config = ScanConfig {
            directory: args.dir.clone(),
            max_depth: if args.depth == std::u32::MAX { None } else { Some(args.depth as usize) },
            min_file_size: args.min_size,
            similarity_threshold: config.similarity_threshold,
            parallel_processing: args.parallel,
            thread_count,
        };

        let report = DeduplicationReport::new(
            duplicate_groups,
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
    }
    
    Ok(())
}
