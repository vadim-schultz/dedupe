use clap::Parser;
use tracing::{info, Level};
use tracing_subscriber::FmtSubscriber;

mod config;
mod walker;
mod pipeline;
mod utils;

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

    // TODO: Initialize pipeline and start processing
    
    Ok(())
}
