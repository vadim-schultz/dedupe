use std::sync::Arc;
use clap::Parser;
use tracing::{info, Level};
use tracing_subscriber::FmtSubscriber;

mod config;
mod walker;
mod pipeline;
mod utils;

use crate::{
    config::Config,
    walker::Walker,
    pipeline::{MetadataStage, QuickCheckStage, StatisticalStage, HashStage},
    utils::progress::ProgressTracker,
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
    #[arg(short, long, default_value_t = 1)]
    min_size: u64,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize logging
    FmtSubscriber::builder()
        .with_max_level(Level::INFO)
        .pretty()
        .init();



    // Parse command line arguments
    let args = Args::parse();
    
    info!("Starting dedupe scan on directory: {}", args.dir);
    info!("Maximum depth: {}", args.depth);
    info!("Minimum file size: {} bytes", args.min_size);

    // Create configuration
    let config = Arc::new(Config {
        max_depth: Some(args.depth as usize),
        min_file_size: args.min_size,
        threads_per_stage: num_cpus::get(),
        quick_check_sample_size: 4096,
        similarity_threshold: 95,
        mode: config::OperationMode::Report,
        ..Config::default()
    });

    // Initialize components
    let walker = Walker::new(config.clone());
    let _progress = ProgressTracker::new();

    // Initialize pipeline
    let mut pipeline = pipeline::Pipeline::new();
    pipeline.add_stage(MetadataStage::new(config.clone()));
    pipeline.add_stage(QuickCheckStage::new(Some(config.quick_check_sample_size)));
    pipeline.add_stage(StatisticalStage::new(config.similarity_threshold));
    pipeline.add_stage(HashStage::new(None));

    info!("Scanning directory for files...");
    let files = walker.walk(&args.dir)?;
    info!("Found {} files to process", files.len());
    
    // Process files through pipeline
    let duplicate_groups = pipeline.execute(files).await?;
    
    // Report results
    for (i, group) in duplicate_groups.iter().enumerate() {
        if group.len() > 1 {
            info!("Duplicate group {}:", i + 1);
            for file in group {
                info!("  - {} ({} bytes)", file.path, file.size);
            }
        }
    }
    
    Ok(())
}
