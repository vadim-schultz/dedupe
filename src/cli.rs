use std::{path::Path, sync::Arc, time::Instant};

use anyhow::{Context, Result};
use clap::Parser;
use tracing::Level;
use tracing_subscriber::FmtSubscriber;

use crate::{
    config::{self, Config},
    pipeline::{
        StreamingHashStage, StreamingMetadataStage, StreamingPipeline, StreamingPipelineConfig,
        StreamingQuickCheckStage, StreamingStatisticalStage,
    },
    report::{DeduplicationReport, ScanConfig},
    types::{DuplicateGroup, FileInfo},
    utils::progress::ProgressTracker,
    walker_hp::{HighPerformanceConfig, HighPerformanceWalker},
};

/// Command line interface for the dedupe binary.
#[derive(Parser, Debug, Clone)]
#[command(author, version, about, long_about = None)]
pub struct Cli {
    /// Directory to scan for duplicates
    #[arg(default_value = ".")]
    pub dir: String,

    /// Maximum depth to traverse
    #[arg(short, long, default_value_t = std::u32::MAX)]
    pub depth: u32,

    /// Minimum file size to consider (in bytes)
    #[arg(short, long, default_value_t = 1024)]
    pub min_size: u64,

    /// Number of worker threads (0 = auto-detect optimal count)
    #[arg(short, long, default_value_t = 0)]
    pub threads: usize,

    /// Enable verbose logging and detailed statistics
    #[arg(short, long, default_value_t = false)]
    pub verbose: bool,

    /// Report output format
    #[arg(short = 'f', long, default_value = "text")]
    pub format: String,

    /// Output file path (if not specified, output to console)
    #[arg(short, long)]
    pub output: Option<String>,
}

/// Execute the deduplication workflow using high performance defaults.
pub async fn run(args: Cli) -> Result<()> {
    initialise_tracing(args.verbose)?;

    println!("🚀 Starting high-performance file deduplication scan");
    println!("📁 Directory: {}", args.dir);

    let thread_count = if args.threads > 0 {
        args.threads
    } else {
        (num_cpus::get() * 4).max(8)
    };

    if args.verbose {
        println!("📏 Maximum depth: {}", args.depth);
        println!("📐 Minimum file size: {} bytes", args.min_size);
        println!("🧵 Worker threads: {}", thread_count);
    }

    let config = build_config(&args, thread_count);
    let progress_tracker = Arc::new(ProgressTracker::new());
    let hp_config = build_high_performance_config(thread_count);

    println!("🌊 Starting high-performance streaming scan and processing...");

    let start_time = Instant::now();
    let scan_dir = args.dir.clone();
    let hp_walker = HighPerformanceWalker::new(
        Arc::clone(&config),
        Arc::clone(&progress_tracker),
        Some(hp_config),
    );

    let streaming_config = build_streaming_config(thread_count);
    let channel_capacity = streaming_config.channel_capacity;
    let mut streaming_pipeline = StreamingPipeline::new(streaming_config);
    streaming_pipeline.set_progress_tracker(progress_tracker.clone());
    streaming_pipeline.add_stage(
        StreamingMetadataStage::new(config.clone()).with_progress_tracker(progress_tracker.clone()),
    );
    streaming_pipeline.add_stage(
        StreamingQuickCheckStage::new(config.clone())
            .with_progress_tracker(progress_tracker.clone()),
    );
    streaming_pipeline.add_stage(
        StreamingStatisticalStage::new(config.clone())
            .with_progress_tracker(progress_tracker.clone()),
    );
    streaming_pipeline.add_stage(
        StreamingHashStage::new(config.clone()).with_progress_tracker(progress_tracker.clone()),
    );

    let (file_tx, file_rx) = tokio::sync::mpsc::channel(channel_capacity);

    let discovery_handle = tokio::spawn(async move {
        hp_walker
            .stream_files(&[Path::new(&scan_dir)], file_tx)
            .await
    });

    let pipeline_handle =
        tokio::spawn(async move { streaming_pipeline.start_streaming(file_rx).await });

    let (pipeline_result, discovery_result) = tokio::join!(pipeline_handle, discovery_handle);

    let discovery_stats = discovery_result.context("high-performance walker join failed")??;
    let duplicate_groups = pipeline_result.context("streaming pipeline join failed")??;

    let execution_time = start_time.elapsed();
    let total_files_found = discovery_stats.files_discovered;

    if args.verbose {
        println!("\n⚡ High-Performance Scan Statistics:");
        println!(
            "   ├─ Files discovered: {}",
            discovery_stats.files_discovered
        );
        println!(
            "   ├─ Directories processed: {}",
            discovery_stats.directories_processed
        );
        println!(
            "   ├─ Files sent to pipeline: {}",
            discovery_stats.files_sent_to_pipeline
        );
        println!(
            "   ├─ Total size: {} MB",
            discovery_stats.total_size_bytes / 1024 / 1024
        );
        println!("   ├─ Scan duration: {:.3}s", execution_time.as_secs_f64());
        if discovery_stats.errors_encountered > 0 {
            println!(
                "   └─ ⚠️  {} errors encountered",
                discovery_stats.errors_encountered
            );
        } else {
            println!("   └─ ✅ No errors");
        }
    }

    progress_tracker.finish_all();

    if total_files_found == 0 {
        return Ok(());
    }

    let report = build_report(
        args.clone(),
        config,
        duplicate_groups,
        total_files_found,
        execution_time,
    );
    emit_report(&args, report?).context("failed to emit report")?;

    Ok(())
}

fn initialise_tracing(verbose: bool) -> Result<()> {
    let log_level = if verbose { Level::DEBUG } else { Level::INFO };
    FmtSubscriber::builder()
        .with_max_level(log_level)
        .compact()
        .without_time()
        .with_target(false)
        .try_init()
        .map_err(|err| anyhow::anyhow!(err))
}

fn build_config(args: &Cli, thread_count: usize) -> Arc<Config> {
    Arc::new(Config {
        max_depth: Some(args.depth as usize),
        min_file_size: args.min_size,
        threads_per_stage: thread_count,
        quick_check_sample_size: 8192,
        similarity_threshold: 95,
        mode: config::OperationMode::Report,
        parallel_scan: true,
        extensions: Vec::new(),
    })
}

fn build_high_performance_config(thread_count: usize) -> HighPerformanceConfig {
    let mut config = HighPerformanceConfig::ultra_performance();
    config.scanner_workers = thread_count.max(config.scanner_workers);
    config
}

fn build_streaming_config(thread_count: usize) -> StreamingPipelineConfig {
    StreamingPipelineConfig {
        channel_capacity: 100_000,
        batch_size: 1000,
        flush_interval: std::time::Duration::from_millis(10),
        max_concurrent_files: thread_count * 4,
        thread_count,
    }
}

fn build_report(
    args: Cli,
    config: Arc<Config>,
    duplicate_groups: Vec<DuplicateGroup>,
    total_files_found: usize,
    execution_time: std::time::Duration,
) -> Result<String> {
    let scan_config = ScanConfig {
        directory: args.dir.clone(),
        max_depth: if args.depth == std::u32::MAX {
            None
        } else {
            Some(args.depth as usize)
        },
        min_file_size: args.min_size,
        similarity_threshold: config.similarity_threshold,
        parallel_processing: true,
        thread_count: config.threads_per_stage,
    };

    let duplicate_groups_for_report: Vec<Vec<FileInfo>> = duplicate_groups
        .into_iter()
        .map(|group| group.files)
        .collect();

    let throughput = if execution_time.as_secs_f64() > 0.0 {
        total_files_found as f64 / execution_time.as_secs_f64()
    } else {
        0.0
    };

    let report = DeduplicationReport::new(
        duplicate_groups_for_report,
        total_files_found,
        execution_time,
        throughput,
        scan_config,
    );

    let content = match args.format.to_lowercase().as_str() {
        "text" => report.to_text(),
        "json" => report.to_json_pretty(),
        "csv" => report.to_csv(),
        other => anyhow::bail!("unsupported format '{other}'. Supported formats: text, json, csv"),
    }?;

    Ok(content)
}

fn emit_report(args: &Cli, report_content: String) -> Result<()> {
    if let Some(path) = &args.output {
        std::fs::write(path, &report_content)
            .with_context(|| format!("failed to write report to {path}"))?;
        println!("💾 Report saved to: {}", path);
    } else {
        println!("{report_content}");
    }
    Ok(())
}
