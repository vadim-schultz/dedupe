use anyhow::Result;
use std::sync::Arc;
use clap::Parser;

use dedupe::{Config, StreamingWalker, StreamingConfig};
use dedupe::utils::progress::ProgressTracker;
use dedupe::pipeline::ProcessingResult;

/// Demo program showing streaming directory walker
#[derive(Parser, Debug)]
#[command(about = "Demo of streaming directory walker")]
struct Args {
    /// Directory to scan
    #[arg(default_value = ".")]
    dir: String,

    /// Use high throughput config
    #[arg(long)]
    high_throughput: bool,

    /// Number of scanner workers
    #[arg(long, default_value_t = 4)]
    workers: usize,

    /// Batch size for processing
    #[arg(long, default_value_t = 50)]
    batch_size: usize,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    println!("🌊 Streaming Walker Demo");
    println!("📁 Scanning directory: {}", args.dir);
    println!("🧵 Using {} workers", args.workers);

    // Create config
    let config = Arc::new(Config {
        max_depth: Some(10),
        min_file_size: 1,
        threads_per_stage: args.workers,
        ..Config::default()
    });

    // Create progress tracker
    let progress_tracker = Arc::new(ProgressTracker::new());

    // Create streaming config
    let streaming_config = if args.high_throughput {
        println!("⚡ Using high-throughput configuration");
        StreamingConfig::high_throughput_config()
    } else {
        println!("📊 Using simple configuration");
        let mut config = StreamingConfig::simple_streaming_config(Some(args.workers));
        config.batch_size = args.batch_size;
        config
    };

    println!("🔧 Config: {} workers, batch size {}", 
        streaming_config.scanner_workers, streaming_config.batch_size);

    // Create streaming walker
    let streaming_walker = StreamingWalker::new(
        config,
        progress_tracker.clone(),
        Some(streaming_config)
    );

    // Track statistics
    let files_processed = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let batches_processed = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let total_size = Arc::new(std::sync::atomic::AtomicU64::new(0));

    let files_clone = Arc::clone(&files_processed);
    let batches_clone = Arc::clone(&batches_processed);
    let size_clone = Arc::clone(&total_size);

    println!("\n🚀 Starting streaming scan...\n");

    let start_time = std::time::Instant::now();

    // Stream files to a simple processor
    let result = streaming_walker.stream_to_pipeline(
        &[std::path::Path::new(&args.dir)],
        move |batch| {
            let files = Arc::clone(&files_clone);
            let batches = Arc::clone(&batches_clone);
            let size = Arc::clone(&size_clone);

            async move {
                let batch_size = batch.len();
                let batch_total_size: u64 = batch.iter().map(|f| f.size).sum();
                
                files.fetch_add(batch_size, std::sync::atomic::Ordering::Relaxed);
                batches.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                size.fetch_add(batch_total_size, std::sync::atomic::Ordering::Relaxed);

                // Simulate some processing
                let current_batches = batches.load(std::sync::atomic::Ordering::Relaxed);
                let current_files = files.load(std::sync::atomic::Ordering::Relaxed);
                
                if current_batches % 10 == 1 {
                    println!("  📦 Processed batch {}: {} files ({} total)", 
                        current_batches, batch_size, current_files);
                }

                // Return success
                Ok(ProcessingResult::Continue(Vec::new()))
            }
        }
    ).await?;

    let total_duration = start_time.elapsed();

    // Finish progress tracking
    progress_tracker.finish_all();

    // Print final results
    println!("\n✅ Streaming scan completed!");
    
    let final_files = files_processed.load(std::sync::atomic::Ordering::Relaxed);
    let final_batches = batches_processed.load(std::sync::atomic::Ordering::Relaxed);
    let final_size = total_size.load(std::sync::atomic::Ordering::Relaxed);

    println!("\n📊 Final Statistics:");
    println!("   ├─ Files discovered: {}", result.stats.files_discovered);
    println!("   ├─ Files processed: {}", final_files);
    println!("   ├─ Directories processed: {}", result.stats.directories_processed);
    println!("   ├─ Batches created: {}", final_batches);
    println!("   ├─ Pipeline batches: {}", result.stats.pipeline_batches_sent);
    println!("   ├─ Total size: {} MB", final_size / 1024 / 1024);
    println!("   ├─ Discovery duration: {:.3}s", result.duration.as_secs_f64());
    println!("   └─ Total duration: {:.3}s", total_duration.as_secs_f64());

    if result.duration.as_secs_f64() > 0.0 {
        let discovery_rate = result.stats.files_discovered as f64 / result.duration.as_secs_f64();
        let processing_rate = final_files as f64 / total_duration.as_secs_f64();
        
        println!("\n🚀 Performance:");
        println!("   ├─ Discovery rate: {:.0} files/sec", discovery_rate);
        println!("   └─ Processing rate: {:.0} files/sec", processing_rate);
    }

    if result.stats.errors_encountered > 0 {
        println!("\n⚠️  {} errors encountered during scan", result.stats.errors_encountered);
    } else {
        println!("\n✅ No errors encountered");
    }

    println!("\n🎯 Streaming walker demonstration completed successfully!");

    Ok(())
}