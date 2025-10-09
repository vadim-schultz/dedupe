use std::sync::{Arc, atomic::{AtomicUsize, AtomicU64, AtomicBool, Ordering}};
use std::time::{Duration, Instant};
use tokio::fs;
use tokio::task::JoinHandle;
use tokio::sync::mpsc;
use tokio::time::timeout;
use anyhow::{Context, Result};
use camino::Utf8PathBuf;
use crossbeam_channel::{bounded, Receiver, Sender};
use futures::Stream;
use async_stream::stream;

use crate::{
    config::Config,
    types::FileInfo,
    utils::progress::ProgressTracker,
    pipeline::ProcessingResult,
};

/// Configuration for streaming walker behavior
#[derive(Debug, Clone)]
pub struct StreamingConfig {
    /// Number of concurrent directory scanners
    pub scanner_workers: usize,
    /// Buffer size for file discovery channel
    pub discovery_buffer_size: usize,
    /// Buffer size for pipeline input channel
    pub pipeline_buffer_size: usize,
    /// Batch size for sending files to pipeline stages
    pub batch_size: usize,
    /// Maximum time to wait when batching files
    pub batch_timeout: Duration,
    /// Whether to use depth-first or breadth-first directory traversal
    pub use_depth_first: bool,
    /// Maximum number of directories to queue before backpressure
    pub max_directory_queue_size: usize,
}

impl Default for StreamingConfig {
    fn default() -> Self {
        Self {
            scanner_workers: num_cpus::get().max(2),
            discovery_buffer_size: 10_000,
            pipeline_buffer_size: 1_000,
            batch_size: 100,
            batch_timeout: Duration::from_millis(100),
            use_depth_first: false,
            max_directory_queue_size: 50_000,
        }
    }
}

/// Statistics for streaming scan progress
#[derive(Debug)]
pub struct StreamingStats {
    pub files_discovered: AtomicUsize,
    pub directories_processed: AtomicUsize,
    pub files_sent_to_pipeline: AtomicUsize,
    pub pipeline_batches_sent: AtomicUsize,
    pub total_size_bytes: AtomicU64,
    pub errors_encountered: AtomicUsize,
    pub scan_complete: AtomicBool,
}

impl StreamingStats {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            files_discovered: AtomicUsize::new(0),
            directories_processed: AtomicUsize::new(0),
            files_sent_to_pipeline: AtomicUsize::new(0),
            pipeline_batches_sent: AtomicUsize::new(0),
            total_size_bytes: AtomicU64::new(0),
            errors_encountered: AtomicUsize::new(0),
            scan_complete: AtomicBool::new(false),
        })
    }

    /// Get a snapshot of current statistics
    pub fn snapshot(&self) -> StreamingStatsSnapshot {
        StreamingStatsSnapshot {
            files_discovered: self.files_discovered.load(Ordering::Relaxed),
            directories_processed: self.directories_processed.load(Ordering::Relaxed),
            files_sent_to_pipeline: self.files_sent_to_pipeline.load(Ordering::Relaxed),
            pipeline_batches_sent: self.pipeline_batches_sent.load(Ordering::Relaxed),
            total_size_bytes: self.total_size_bytes.load(Ordering::Relaxed),
            errors_encountered: self.errors_encountered.load(Ordering::Relaxed),
            scan_complete: self.scan_complete.load(Ordering::Relaxed),
        }
    }
}

/// Immutable snapshot of streaming statistics
#[derive(Debug, Clone)]
pub struct StreamingStatsSnapshot {
    pub files_discovered: usize,
    pub directories_processed: usize,
    pub files_sent_to_pipeline: usize,
    pub pipeline_batches_sent: usize,
    pub total_size_bytes: u64,
    pub errors_encountered: usize,
    pub scan_complete: bool,
}

/// Directory entry for processing queue
#[derive(Debug, Clone)]
struct DirectoryEntry {
    path: std::path::PathBuf,
    depth: usize,
}

/// Result from streaming scan operation
#[derive(Debug)]
pub struct StreamingResult {
    pub stats: StreamingStatsSnapshot,
    pub duration: Duration,
}

/// Streaming directory walker that feeds files to pipeline as they are discovered
pub struct StreamingWalker {
    config: Arc<Config>,
    streaming_config: StreamingConfig,
    progress_tracker: Arc<ProgressTracker>,
    stats: Arc<StreamingStats>,
}

impl StreamingWalker {
    pub fn new(
        config: Arc<Config>,
        progress_tracker: Arc<ProgressTracker>,
        streaming_config: Option<StreamingConfig>,
    ) -> Self {
        let streaming_config = streaming_config.unwrap_or_default();
        
        Self {
            config,
            streaming_config,
            progress_tracker,
            stats: StreamingStats::new(),
        }
    }

    /// Stream files as they are discovered and feed them to a pipeline processing function
    pub async fn stream_to_pipeline<F, Fut>(
        &self,
        root_paths: &[impl AsRef<std::path::Path>],
        mut process_batch: F,
    ) -> Result<StreamingResult>
    where
        F: FnMut(Vec<FileInfo>) -> Fut + Send + 'static,
        Fut: std::future::Future<Output = Result<ProcessingResult>> + Send,
    {
        let start_time = Instant::now();
        
        // Create discovery channel for files
        let (file_tx, mut file_rx) = mpsc::channel(self.streaming_config.discovery_buffer_size);
        
        // Create directory queue
        let (dir_tx, dir_rx) = bounded(self.streaming_config.max_directory_queue_size);
        
        // Initialize root directories
        for root_path in root_paths {
            dir_tx.send(DirectoryEntry {
                path: root_path.as_ref().to_path_buf(),
                depth: 0,
            })?;
        }
        
        // Create progress bars
        let discovery_progress = self.progress_tracker.create_scanning_bar(0);
        discovery_progress.set_message("Discovering files...");
        
        let pipeline_progress = self.progress_tracker.create_progress_bar(0, "Pipeline");
        pipeline_progress.set_message("Processing batches...");
        
        // Spawn directory scanner workers
        let scanner_handles = self.spawn_scanner_workers(
            dir_tx,
            dir_rx,
            file_tx.clone(),
            discovery_progress.clone(),
        ).await?;
        
        // Close the file sender from the main thread
        drop(file_tx);
        
        // Spawn file batch processor
        let stats_clone = Arc::clone(&self.stats);
        let batch_size = self.streaming_config.batch_size;
        let batch_timeout = self.streaming_config.batch_timeout;
        
        let processor_handle = tokio::spawn(async move {
            let mut current_batch = Vec::with_capacity(batch_size);
            let mut batch_deadline = None;
            
            loop {
                // Determine timeout for next receive
                let timeout_duration = match batch_deadline {
                    Some(deadline) => {
                        let now = Instant::now();
                        if now >= deadline {
                            // Time to send current batch
                            if !current_batch.is_empty() {
                                let batch = std::mem::take(&mut current_batch);
                                let batch_count = batch.len();
                                
                                match process_batch(batch).await {
                                    Ok(_) => {
                                        stats_clone.files_sent_to_pipeline.fetch_add(batch_count, Ordering::Relaxed);
                                        stats_clone.pipeline_batches_sent.fetch_add(1, Ordering::Relaxed);
                                    }
                                    Err(e) => {
                                        eprintln!("Error processing batch: {}", e);
                                        stats_clone.errors_encountered.fetch_add(1, Ordering::Relaxed);
                                    }
                                }
                                
                                batch_deadline = None;
                                current_batch = Vec::with_capacity(batch_size);
                            }
                            continue;
                        }
                        deadline.duration_since(now)
                    }
                    None => Duration::from_secs(60), // Long timeout when no batch is pending
                };
                
                // Try to receive next file with timeout
                match timeout(timeout_duration, file_rx.recv()).await {
                    Ok(Some(file_info)) => {
                        current_batch.push(file_info);
                        
                        // Set batch deadline if this is the first file in batch
                        if batch_deadline.is_none() {
                            batch_deadline = Some(Instant::now() + batch_timeout);
                        }
                        
                        // Send batch if full
                        if current_batch.len() >= batch_size {
                            let batch = std::mem::take(&mut current_batch);
                            let batch_count = batch.len();
                            
                            match process_batch(batch).await {
                                Ok(_) => {
                                    stats_clone.files_sent_to_pipeline.fetch_add(batch_count, Ordering::Relaxed);
                                    stats_clone.pipeline_batches_sent.fetch_add(1, Ordering::Relaxed);
                                }
                                Err(e) => {
                                    eprintln!("Error processing batch: {}", e);
                                    stats_clone.errors_encountered.fetch_add(1, Ordering::Relaxed);
                                }
                            }
                            
                            batch_deadline = None;
                            current_batch = Vec::with_capacity(batch_size);
                        }
                    }
                    Ok(None) => {
                        // Channel closed, send final batch if any
                        if !current_batch.is_empty() {
                            let batch = std::mem::take(&mut current_batch);
                            let batch_count = batch.len();
                            
                            match process_batch(batch).await {
                                Ok(_) => {
                                    stats_clone.files_sent_to_pipeline.fetch_add(batch_count, Ordering::Relaxed);
                                    stats_clone.pipeline_batches_sent.fetch_add(1, Ordering::Relaxed);
                                }
                                Err(e) => {
                                    eprintln!("Error processing final batch: {}", e);
                                    stats_clone.errors_encountered.fetch_add(1, Ordering::Relaxed);
                                }
                            }
                        }
                        break;
                    }
                    Err(_) => {
                        // Timeout reached, send current batch
                        if !current_batch.is_empty() {
                            let batch = std::mem::take(&mut current_batch);
                            let batch_count = batch.len();
                            
                            match process_batch(batch).await {
                                Ok(_) => {
                                    stats_clone.files_sent_to_pipeline.fetch_add(batch_count, Ordering::Relaxed);
                                    stats_clone.pipeline_batches_sent.fetch_add(1, Ordering::Relaxed);
                                }
                                Err(e) => {
                                    eprintln!("Error processing timed batch: {}", e);
                                    stats_clone.errors_encountered.fetch_add(1, Ordering::Relaxed);
                                }
                            }
                            
                            batch_deadline = None;
                            current_batch = Vec::with_capacity(batch_size);
                        }
                    }
                }
            }
            
            Ok::<(), anyhow::Error>(())
        });
        
        // Wait for all workers to complete
        for handle in scanner_handles {
            if let Err(e) = handle.await {
                eprintln!("Scanner worker error: {}", e);
                self.stats.errors_encountered.fetch_add(1, Ordering::Relaxed);
            }
        }
        
        // Wait for processor to complete
        if let Err(e) = processor_handle.await {
            eprintln!("Batch processor error: {}", e);
            self.stats.errors_encountered.fetch_add(1, Ordering::Relaxed);
        }
        
        // Mark scan as complete
        self.stats.scan_complete.store(true, Ordering::Relaxed);
        
        // Finish progress bars
        discovery_progress.finish_with_message("File discovery complete");
        pipeline_progress.finish_with_message("Pipeline processing complete");
        
        let duration = start_time.elapsed();
        
        Ok(StreamingResult {
            stats: self.stats.snapshot(),
            duration,
        })
    }

    /// Create a stream of files as they are discovered
    pub fn create_file_stream(
        &self,
        root_paths: Vec<impl AsRef<std::path::Path> + Send + 'static>,
    ) -> impl Stream<Item = Result<FileInfo>> + Send {
        let config = Arc::clone(&self.config);
        let stats = Arc::clone(&self.stats);
        let streaming_config = self.streaming_config.clone();
        
        stream! {
            // Create channels
            let (file_tx, mut file_rx) = mpsc::channel(streaming_config.discovery_buffer_size);
            let (dir_tx, dir_rx) = bounded(streaming_config.max_directory_queue_size);
            
            // Initialize root directories
            for root_path in root_paths {
                let _ = dir_tx.send(DirectoryEntry {
                    path: root_path.as_ref().to_path_buf(),
                    depth: 0,
                });
            }
            
            // Spawn scanner workers
            let mut handles = Vec::new();
            for worker_id in 0..streaming_config.scanner_workers {
                let worker_config = Arc::clone(&config);
                let worker_stats = Arc::clone(&stats);
                let worker_dir_rx = dir_rx.clone();
                let worker_dir_tx = dir_tx.clone();
                let worker_file_tx = file_tx.clone();
                let max_depth = config.max_depth.unwrap_or(usize::MAX);
                
                let handle = tokio::spawn(async move {
                    Self::scanner_worker(
                        worker_id,
                        worker_config,
                        worker_stats,
                        worker_dir_rx,
                        worker_dir_tx,
                        worker_file_tx,
                        max_depth,
                    ).await
                });
                
                handles.push(handle);
            }
            
            // Close our references to channels
            drop(dir_tx);
            drop(file_tx);
            
            // Yield files as they come in
            while let Some(file_info) = file_rx.recv().await {
                yield Ok(file_info);
            }
            
            // Wait for workers to complete
            for handle in handles {
                let _ = handle.await;
            }
            
            stats.scan_complete.store(true, Ordering::Relaxed);
        }
    }

    /// Spawn directory scanner worker tasks
    async fn spawn_scanner_workers(
        &self,
        dir_tx: Sender<DirectoryEntry>,
        dir_rx: Receiver<DirectoryEntry>,
        file_tx: mpsc::Sender<FileInfo>,
        progress: indicatif::ProgressBar,
    ) -> Result<Vec<JoinHandle<Result<()>>>> {
        let mut handles = Vec::new();
        let max_depth = self.config.max_depth.unwrap_or(usize::MAX);
        
        for worker_id in 0..self.streaming_config.scanner_workers {
            let worker_config = Arc::clone(&self.config);
            let worker_stats = Arc::clone(&self.stats);
            let worker_dir_rx = dir_rx.clone();
            let worker_dir_tx = dir_tx.clone();
            let worker_file_tx = file_tx.clone();
            let worker_progress = progress.clone();
            
            let handle = tokio::spawn(async move {
                Self::scanner_worker_task(
                    worker_id,
                    worker_config,
                    worker_stats,
                    worker_dir_rx,
                    worker_dir_tx,
                    worker_file_tx,
                    worker_progress,
                    max_depth,
                ).await
            });
            
            handles.push(handle);
        }
        
        // Close our reference to the directory sender
        drop(dir_tx);
        
        Ok(handles)
    }

    /// Scanner worker task that processes directories and discovers files
    async fn scanner_worker_task(
        worker_id: usize,
        config: Arc<Config>,
        stats: Arc<StreamingStats>,
        dir_rx: Receiver<DirectoryEntry>,
        dir_tx: Sender<DirectoryEntry>,
        file_tx: mpsc::Sender<FileInfo>,
        progress: indicatif::ProgressBar,
        max_depth: usize,
    ) -> Result<()> {
        let mut _processed_dirs = 0;
        let mut discovered_files = 0;
        
        loop {
            // Try to get next directory to process
            let entry = match dir_rx.recv_timeout(Duration::from_millis(500)) {
                Ok(entry) => entry,
                Err(crossbeam_channel::RecvTimeoutError::Timeout) => {
                    // Check if there are more directories to process
                    if dir_rx.is_empty() {
                        break;
                    }
                    continue;
                }
                Err(crossbeam_channel::RecvTimeoutError::Disconnected) => {
                    break;
                }
            };
            
            // Process this directory
            match Self::process_directory_streaming(
                &entry,
                &config,
                &dir_tx,
                &file_tx,
                max_depth,
                &mut discovered_files,
            ).await {
                Ok(_) => {
                    _processed_dirs += 1;
                    stats.directories_processed.fetch_add(1, Ordering::Relaxed);
                    
                    // Update progress occasionally
                    if _processed_dirs % 10 == 0 {
                        progress.inc(10);
                        progress.set_message(format!(
                            "Worker {}: {} dirs, {} files",
                            worker_id, _processed_dirs, discovered_files
                        ));
                    }
                }
                Err(e) => {
                    eprintln!("Worker {} error processing {:?}: {}", worker_id, entry.path, e);
                    stats.errors_encountered.fetch_add(1, Ordering::Relaxed);
                }
            }
        }
        
        stats.files_discovered.fetch_add(discovered_files, Ordering::Relaxed);
        
        Ok(())
    }

    /// Stream-oriented scanner worker (used by file stream)
    async fn scanner_worker(
        _worker_id: usize,
        config: Arc<Config>,
        stats: Arc<StreamingStats>,
        dir_rx: Receiver<DirectoryEntry>,
        dir_tx: Sender<DirectoryEntry>,
        file_tx: mpsc::Sender<FileInfo>,
        max_depth: usize,
    ) -> Result<()> {
        let mut _processed_dirs = 0;
        let mut discovered_files = 0;
        
        loop {
            let entry = match dir_rx.recv_timeout(Duration::from_millis(500)) {
                Ok(entry) => entry,
                Err(crossbeam_channel::RecvTimeoutError::Timeout) => {
                    if dir_rx.is_empty() {
                        break;
                    }
                    continue;
                }
                Err(crossbeam_channel::RecvTimeoutError::Disconnected) => {
                    break;
                }
            };
            
            match Self::process_directory_streaming(
                &entry,
                &config,
                &dir_tx,
                &file_tx,
                max_depth,
                &mut discovered_files,
            ).await {
                Ok(_) => {
                    _processed_dirs += 1;
                    stats.directories_processed.fetch_add(1, Ordering::Relaxed);
                }
                Err(e) => {
                    eprintln!("Worker {} error processing {:?}: {}", _worker_id, entry.path, e);
                    stats.errors_encountered.fetch_add(1, Ordering::Relaxed);
                }
            }
        }
        
        stats.files_discovered.fetch_add(discovered_files, Ordering::Relaxed);
        
        Ok(())
    }

    /// Process a single directory and send discovered files/subdirectories
    async fn process_directory_streaming(
        entry: &DirectoryEntry,
        config: &Config,
        dir_tx: &Sender<DirectoryEntry>,
        file_tx: &mpsc::Sender<FileInfo>,
        max_depth: usize,
        discovered_files: &mut usize,
    ) -> Result<()> {
        let mut entries = fs::read_dir(&entry.path).await
            .with_context(|| format!("Failed to read directory: {:?}", entry.path))?;

        while let Some(dir_entry) = entries.next_entry().await? {
            let path = dir_entry.path();
            let metadata = match dir_entry.metadata().await {
                Ok(meta) => meta,
                Err(_) => continue, // Skip files we can't read metadata for
            };

            // Skip hidden files/directories
            if Self::is_hidden(&path) {
                continue;
            }

            if metadata.is_dir() {
                // Queue subdirectory for processing if within depth limit
                if entry.depth + 1 < max_depth {
                    let _ = dir_tx.try_send(DirectoryEntry {
                        path: path.clone(),
                        depth: entry.depth + 1,
                    });
                }
            } else if metadata.is_file() {
                // Process file if it meets criteria
                let size = metadata.len();
                if size >= config.min_file_size {
                    if let Some(file_info) = Self::create_file_info_streaming(path, metadata, config).await? {
                        // Send file to processing pipeline
                        if file_tx.send(file_info).await.is_ok() {
                            *discovered_files += 1;
                        }
                    }
                }
            }
        }

        Ok(())
    }

    /// Create FileInfo from path and metadata (optimized for streaming)
    async fn create_file_info_streaming(
        path: std::path::PathBuf,
        metadata: std::fs::Metadata,
        config: &Config,
    ) -> Result<Option<FileInfo>> {
        let size = metadata.len();

        // Skip files smaller than minimum size
        if size < config.min_file_size {
            return Ok(None);
        }

        let utf8_path = Utf8PathBuf::try_from(path)?;

        // Check extension filter if configured
        let should_include = if config.extensions.is_empty() {
            true
        } else {
            utf8_path.extension()
                .map(|ext| {
                    let ext_lower = ext.to_lowercase();
                    config.extensions.iter().any(|e| e.to_lowercase() == ext_lower)
                })
                .unwrap_or(false)
        };

        if !should_include {
            return Ok(None);
        }

        // Get file type (optional, can be expensive)
        let file_type = infer::get_from_path(&utf8_path)
            .ok()
            .flatten()
            .map(|kind| kind.mime_type().to_string());

        let (readonly, hidden) = Self::get_file_attributes(&metadata, &utf8_path);

        Ok(Some(FileInfo {
            path: utf8_path,
            size,
            file_type,
            modified: metadata.modified()?,
            created: metadata.created().ok(),
            readonly,
            hidden,
            checksum: None, // Will be computed in later stages
            quick_check: None,
            statistical_info: None,
            metadata: None,
        }))
    }

    /// Get platform-specific file attributes
    fn get_file_attributes(metadata: &std::fs::Metadata, _path: &Utf8PathBuf) -> (bool, bool) {
        #[cfg(windows)]
        {
            use std::os::windows::fs::MetadataExt;
            let attrs = metadata.file_attributes();
            (
                (attrs & 0x1) != 0,  // FILE_ATTRIBUTE_READONLY
                (attrs & 0x2) != 0,  // FILE_ATTRIBUTE_HIDDEN
            )
        }

        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            let mode = metadata.mode();
            (
                (mode & 0o200) == 0,  // Write permission check
                _path.file_name()
                    .map(|name| name.starts_with('.'))
                    .unwrap_or(false),
            )
        }

        #[cfg(not(any(windows, unix)))]
        (false, false)
    }

    /// Check if path represents a hidden file/directory
    fn is_hidden(path: &std::path::Path) -> bool {
        path.file_name()
            .and_then(|name| name.to_str())
            .map(|name| name.starts_with('.'))
            .unwrap_or(false)
    }

    /// Get current statistics snapshot
    pub fn get_stats(&self) -> StreamingStatsSnapshot {
        self.stats.snapshot()
    }

    /// Check if scan is complete
    pub fn is_scan_complete(&self) -> bool {
        self.stats.scan_complete.load(Ordering::Relaxed)
    }
}

/// Utility functions for streaming configuration
impl StreamingConfig {
    /// Create a simple configuration for streaming
    pub fn simple_streaming_config(workers: Option<usize>) -> Self {
        Self {
            scanner_workers: workers.unwrap_or_else(|| num_cpus::get().max(2)),
            discovery_buffer_size: 5_000,
            pipeline_buffer_size: 500,
            batch_size: 50,
            batch_timeout: Duration::from_millis(50),
            use_depth_first: false,
            max_directory_queue_size: 25_000,
        }
    }

    /// Create a high-throughput configuration
    pub fn high_throughput_config() -> Self {
        Self {
            scanner_workers: (num_cpus::get() * 2).max(4),
            discovery_buffer_size: 50_000,
            pipeline_buffer_size: 5_000,
            batch_size: 200,
            batch_timeout: Duration::from_millis(200),
            use_depth_first: false,
            max_directory_queue_size: 100_000,
        }
    }
}

/// Utility functions for streaming walker
impl StreamingWalker {

    /// Print statistics summary
    pub fn print_stats(&self, duration: Duration) {
        let stats = self.get_stats();
        
        println!("📈 Streaming Scan Statistics:");
        println!("   ├─ Files discovered: {}", stats.files_discovered);
        println!("   ├─ Directories processed: {}", stats.directories_processed);
        println!("   ├─ Files sent to pipeline: {}", stats.files_sent_to_pipeline);
        println!("   ├─ Pipeline batches: {}", stats.pipeline_batches_sent);
        println!("   ├─ Total size: {} MB", stats.total_size_bytes / 1024 / 1024);
        println!("   ├─ Scan duration: {:.2}s", duration.as_secs_f64());
        
        if stats.errors_encountered > 0 {
            println!("   └─ ⚠️  {} errors encountered", stats.errors_encountered);
        } else {
            println!("   └─ ✅ No errors");
        }
        
        if duration.as_secs_f64() > 0.0 {
            let discovery_rate = stats.files_discovered as f64 / duration.as_secs_f64();
            let processing_rate = stats.files_sent_to_pipeline as f64 / duration.as_secs_f64();
            println!("📊 Performance:");
            println!("   ├─ Discovery rate: {:.0} files/sec", discovery_rate);
            println!("   └─ Pipeline rate: {:.0} files/sec", processing_rate);
        }
    }
}