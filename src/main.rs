use std::sync::Arc;
use clap::Parser;
use tracing::{info, Level};
use tracing_subscriber::FmtSubscriber;

mod config;
mod walker;
mod pipeline;
mod utils;
mod report;

use crate::{
    config::Config,
    walker::Walker,
    pipeline::{MetadataStage, QuickCheckStage, StatisticalStage, HashStage, ParallelPipeline, ParallelConfig},
    utils::progress::ProgressTracker,
    report::{DeduplicationReport, ScanConfig},
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

    info!("Starting file deduplication scan");
    info!("Directory: {}", args.dir);
    
    let thread_count = if args.threads > 0 { args.threads } else { num_cpus::get() };
    
    if args.verbose {
        info!("Maximum depth: {}", args.depth);
        info!("Minimum file size: {} bytes", args.min_size);
        info!("Parallel processing: {}", args.parallel);
        info!("Worker threads: {}", thread_count);
    }

    // Create configuration
    let config = Arc::new(Config {
        max_depth: Some(args.depth as usize),
        min_file_size: args.min_size,
        threads_per_stage: thread_count,
        quick_check_sample_size: 4096,
        similarity_threshold: 95,
        mode: config::OperationMode::Report,
        ..Config::default()
    });

    // Initialize components
    let walker = Walker::new(config.clone());
    let _progress = ProgressTracker::new();

    info!("Scanning files...");
    let files = walker.walk(&args.dir)?;
    info!("Found {} files", files.len());
    
    let total_files_found = files.len();
    
    // Process files through pipeline
    let start_time = std::time::Instant::now();
    let duplicate_groups = if args.parallel {
        use tracing::debug;
        debug!("Initializing parallel pipeline with {} threads", thread_count);
        
        let parallel_config = ParallelConfig {
            threads_per_stage: thread_count,
            channel_capacity: 1000,
            batch_size: 50,
            max_concurrent_tasks: thread_count * 2,
            ..ParallelConfig::default()
        };
        
        let mut parallel_pipeline = ParallelPipeline::new(parallel_config)?;
        parallel_pipeline.add_stage(MetadataStage::new(config.clone()));
        parallel_pipeline.add_stage(QuickCheckStage::new(config.clone()));
        parallel_pipeline.add_stage(StatisticalStage::new(config.clone()));
        parallel_pipeline.add_stage(HashStage::new(None));
        
        info!("Processing files...");
        let result = parallel_pipeline.execute(files).await?;
        
        let metrics = parallel_pipeline.metrics();
        info!("Completed processing ({} files/sec)", metrics.throughput);
        
        result
    } else {
        use tracing::debug;
        debug!("Using sequential pipeline processing");
        
        let mut pipeline = pipeline::Pipeline::new();
        pipeline.add_stage(MetadataStage::new(config.clone()));
        pipeline.add_stage(QuickCheckStage::new(config.clone()));
        pipeline.add_stage(StatisticalStage::new(config.clone()));
        pipeline.add_stage(HashStage::new(None));
        
        info!("Processing files...");
        pipeline.execute(files).await?
    };
    
    let execution_time = start_time.elapsed();
    
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
            info!("Report saved to: {}", output_path);
        }
        None => {
            println!("{}", report_content);
        }
    }
    
    Ok(())
}
