use std::sync::{Arc, atomic::{AtomicUsize, AtomicU64, AtomicBool, Ordering}};
use std::time::{Duration, Instant};
use tokio::fs;
use tokio::task::JoinHandle;
use tokio::sync::mpsc;
use tokio::time::timeout;
use anyhow::{Context, Result};
use camino::Utf8PathBuf;
use crossbeam_channel::{bounded, Receiver, Sender};
use tracing::{debug, info, warn};

use crate::{
    config::Config,
    types::FileInfo,
    utils::progress::ProgressTracker,
};

/// High-performance streaming walker configuration
#[derive(Debug, Clone)]
pub struct HighPerformanceConfig {
    /// Number of concurrent directory scanners
    pub scanner_workers: usize,
    /// Buffer size for file discovery channel
    pub discovery_buffer_size: usize,
    /// Maximum number of directories to queue before backpressure
    pub max_directory_queue_size: usize,
    /// Batch size for processing
    pub output_batch_size: usize,
    /// Maximum time to wait when batching files
    pub batch_timeout: Duration,
}

impl Default for HighPerformanceConfig {
    fn default() -> Self {
        Self {
            scanner_workers: (num_cpus::get() * 2).max(4),
            discovery_buffer_size: 20_000,
            max_directory_queue_size: 100_000,
            output_batch_size: 200,
            batch_timeout: Duration::from_millis(25),
        }
    }
}

impl HighPerformanceConfig {
    pub fn ultra_performance() -> Self {
        Self {
            scanner_workers: (num_cpus::get() * 4).max(8),
            discovery_buffer_size: 100_000,
            max_directory_queue_size: 500_000,
            output_batch_size: 1000,
            batch_timeout: Duration::from_millis(10),
        }
    }
}

/// Statistics for high-performance scan
#[derive(Debug)]
pub struct HighPerformanceStats {
    pub files_discovered: AtomicUsize,
    pub directories_processed: AtomicUsize,
    pub files_sent_to_pipeline: AtomicUsize,
    pub total_size_bytes: AtomicU64,
    pub errors_encountered: AtomicUsize,
    pub scan_complete: AtomicBool,
}

impl HighPerformanceStats {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            files_discovered: AtomicUsize::new(0),
            directories_processed: AtomicUsize::new(0),
            files_sent_to_pipeline: AtomicUsize::new(0),
            total_size_bytes: AtomicU64::new(0),
            errors_encountered: AtomicUsize::new(0),
            scan_complete: AtomicBool::new(false),
        })
    }
    
    pub fn snapshot(&self) -> HighPerformanceSnapshot {
        HighPerformanceSnapshot {
            files_discovered: self.files_discovered.load(Ordering::Relaxed),
            directories_processed: self.directories_processed.load(Ordering::Relaxed),
            files_sent_to_pipeline: self.files_sent_to_pipeline.load(Ordering::Relaxed),
            total_size_bytes: self.total_size_bytes.load(Ordering::Relaxed),
            errors_encountered: self.errors_encountered.load(Ordering::Relaxed),
            scan_complete: self.scan_complete.load(Ordering::Relaxed),
        }
    }
}

#[derive(Debug, Clone)]
pub struct HighPerformanceSnapshot {
    pub files_discovered: usize,
    pub directories_processed: usize,
    pub files_sent_to_pipeline: usize,
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

/// High-performance streaming walker optimized for maximum throughput
pub struct HighPerformanceWalker {
    config: Arc<Config>,
    hp_config: HighPerformanceConfig,
    progress_tracker: Arc<ProgressTracker>,
    stats: Arc<HighPerformanceStats>,
}

impl HighPerformanceWalker {
    pub fn new(
        config: Arc<Config>,
        progress_tracker: Arc<ProgressTracker>,
        hp_config: Option<HighPerformanceConfig>,
    ) -> Self {
        let hp_config = hp_config.unwrap_or_default();
        
        Self {
            config,
            hp_config,
            progress_tracker,
            stats: HighPerformanceStats::new(),
        }
    }
    
    /// Stream files directly to a channel as they are discovered
    pub async fn stream_files(
        &self,
        root_paths: &[impl AsRef<std::path::Path>],
        file_sender: mpsc::Sender<FileInfo>,
    ) -> Result<HighPerformanceSnapshot> {
        let start_time = Instant::now();
        
        info!("Starting high-performance streaming scan with {} workers", 
              self.hp_config.scanner_workers);
        
        // Create directory queue
        let (dir_tx, dir_rx) = bounded(self.hp_config.max_directory_queue_size);
        
        // Initialize root directories
        for root_path in root_paths {
            dir_tx.send(DirectoryEntry {
                path: root_path.as_ref().to_path_buf(),
                depth: 0,
            })?;
        }
        
        // Create internal file channel for batching
        let (internal_file_tx, mut internal_file_rx) = mpsc::channel(self.hp_config.discovery_buffer_size);
        
        // Create progress bar
        let discovery_progress = self.progress_tracker.create_scanning_bar(0);
        discovery_progress.set_message("Discovering files...");
        
        // Spawn directory scanner workers
        let scanner_handles = self.spawn_scanner_workers(
            dir_tx,
            dir_rx,
            internal_file_tx,
            discovery_progress.clone(),
        ).await?;
        
        // Spawn file batcher that sends to external channel
        let stats_clone = Arc::clone(&self.stats);
        let batch_size = self.hp_config.output_batch_size;
        let batch_timeout = self.hp_config.batch_timeout;
        
        let batcher_handle = tokio::spawn(async move {
            let mut current_batch = Vec::with_capacity(batch_size);
            let mut last_batch_time = Instant::now();
            
            loop {
                // Try to receive files with timeout
                match timeout(batch_timeout, internal_file_rx.recv()).await {
                    Ok(Some(file)) => {
                        current_batch.push(file);
                        
                        // Send batch if full or timeout reached
                        if current_batch.len() >= batch_size || 
                           last_batch_time.elapsed() >= batch_timeout {
                            
                            if !current_batch.is_empty() {
                                let batch_len = current_batch.len();
                                
                                // Send files individually to maintain streaming
                                for file in current_batch.drain(..) {
                                    if file_sender.send(file).await.is_err() {
                                        return Ok(());
                                    }
                                }
                                
                                stats_clone.files_sent_to_pipeline.fetch_add(batch_len, Ordering::Relaxed);
                                last_batch_time = Instant::now();
                            }
                        }
                    }
                    Ok(None) => {
                        // Channel closed, send final batch
                        for file in current_batch.drain(..) {
                            if file_sender.send(file).await.is_err() {
                                break;
                            }
                        }
                        break;
                    }
                    Err(_) => {
                        // Timeout - send current batch if any
                        if !current_batch.is_empty() && last_batch_time.elapsed() >= batch_timeout {
                            let batch_len = current_batch.len();
                            
                            for file in current_batch.drain(..) {
                                if file_sender.send(file).await.is_err() {
                                    return Ok(());
                                }
                            }
                            
                            stats_clone.files_sent_to_pipeline.fetch_add(batch_len, Ordering::Relaxed);
                            last_batch_time = Instant::now();
                        }
                    }
                }
            }
            
            Ok::<(), anyhow::Error>(())
        });
        
        // Wait for all scanners to complete
        for handle in scanner_handles {
            if let Err(e) = handle.await {
                warn!("Scanner worker error: {}", e);
                self.stats.errors_encountered.fetch_add(1, Ordering::Relaxed);
            }
        }
        
        // Wait for batcher to complete
        if let Err(e) = batcher_handle.await {
            warn!("Batcher error: {}", e);
            self.stats.errors_encountered.fetch_add(1, Ordering::Relaxed);
        }
        
        // Mark scan as complete
        self.stats.scan_complete.store(true, Ordering::Relaxed);
        
        // Finish progress
        discovery_progress.finish_with_message("✅ File discovery complete");
        
        let duration = start_time.elapsed();
        let snapshot = self.stats.snapshot();
        
        info!("High-performance scan completed in {:.2}s", duration.as_secs_f64());
        info!("Files discovered: {}, Directories: {}, Errors: {}", 
              snapshot.files_discovered, snapshot.directories_processed, snapshot.errors_encountered);
        
        Ok(snapshot)
    }
    
    /// Spawn directory scanner worker tasks
    async fn spawn_scanner_workers(
        &self,
        dir_tx: Sender<DirectoryEntry>,
        dir_rx: Receiver<DirectoryEntry>,
        file_tx: mpsc::Sender<FileInfo>,
        progress: indicatif::ProgressBar,
    ) -> Result<Vec<JoinHandle<()>>> {
        let mut handles = Vec::new();
        let max_depth = self.config.max_depth.unwrap_or(usize::MAX);
        
        for worker_id in 0..self.hp_config.scanner_workers {
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
                ).await;
            });
            
            handles.push(handle);
        }
        
        // Close our reference to the directory sender
        drop(dir_tx);
        
        Ok(handles)
    }
    
    /// High-performance scanner worker task
    async fn scanner_worker_task(
        worker_id: usize,
        config: Arc<Config>,
        stats: Arc<HighPerformanceStats>,
        dir_rx: Receiver<DirectoryEntry>,
        dir_tx: Sender<DirectoryEntry>,
        file_tx: mpsc::Sender<FileInfo>,
        progress: indicatif::ProgressBar,
        max_depth: usize,
    ) {
        let mut processed_dirs = 0;
        let mut discovered_files = 0;
        
        debug!("Scanner worker {} started", worker_id);
        
        loop {
            // Try to get next directory to process
            let entry = match dir_rx.recv_timeout(Duration::from_millis(100)) {
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
            
            // Process this directory
            match Self::process_directory_hp(
                &entry,
                &config,
                &dir_tx,
                &file_tx,
                max_depth,
                &mut discovered_files,
            ).await {
                Ok(_) => {
                    processed_dirs += 1;
                    stats.directories_processed.fetch_add(1, Ordering::Relaxed);
                    
                    // Update progress occasionally
                    if processed_dirs % 5 == 0 {
                        progress.inc(5);
                        progress.set_message(format!(
                            "Worker {}: {} dirs, {} files",
                            worker_id, processed_dirs, discovered_files
                        ));
                    }
                }
                Err(e) => {
                    warn!("Worker {} error processing {:?}: {}", worker_id, entry.path, e);
                    stats.errors_encountered.fetch_add(1, Ordering::Relaxed);
                }
            }
        }
        
        stats.files_discovered.fetch_add(discovered_files, Ordering::Relaxed);
        debug!("Scanner worker {} completed: {} dirs, {} files", worker_id, processed_dirs, discovered_files);
    }
    
    /// Process a single directory with high performance optimizations
    async fn process_directory_hp(
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
            
            // Skip hidden files/directories early
            if Self::is_hidden(&path) {
                continue;
            }
            
            let metadata = match dir_entry.metadata().await {
                Ok(meta) => meta,
                Err(_) => continue, // Skip files we can't read metadata for
            };

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
                    if let Some(file_info) = Self::create_file_info_hp(path, metadata, config)? {
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
    
    /// Create FileInfo with high-performance optimizations
    fn create_file_info_hp(
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

        // Check extension filter if configured (optimized)
        if !config.extensions.is_empty() {
            let should_include = utf8_path.extension()
                .map(|ext| {
                    let ext_lower = ext.to_lowercase();
                    config.extensions.iter().any(|e| e.eq_ignore_ascii_case(&ext_lower))
                })
                .unwrap_or(false);

            if !should_include {
                return Ok(None);
            }
        }

        // Defer expensive operations like file type detection
        // These will be done in the metadata stage if needed
        
        let (readonly, hidden) = Self::get_file_attributes_fast(&metadata, &utf8_path);

        Ok(Some(FileInfo {
            path: utf8_path,
            size,
            file_type: None, // Detected later in pipeline
            modified: metadata.modified().unwrap_or_else(|_| std::time::SystemTime::UNIX_EPOCH),
            created: metadata.created().ok(),
            readonly,
            hidden,
            checksum: None,
            quick_check: None,
            statistical_info: None,
            metadata: Some(metadata),
        }))
    }
    
    /// Fast file attribute detection
    fn get_file_attributes_fast(metadata: &std::fs::Metadata, _path: &Utf8PathBuf) -> (bool, bool) {
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
                path.file_name()
                    .map(|name| name.starts_with('.'))
                    .unwrap_or(false),
            )
        }

        #[cfg(not(any(windows, unix)))]
        (false, false)
    }
    
    /// Fast hidden file check
    fn is_hidden(path: &std::path::Path) -> bool {
        path.file_name()
            .and_then(|name| name.to_str())
            .map(|name| name.starts_with('.'))
            .unwrap_or(false)
    }
    
    /// Get current statistics
    pub fn get_stats(&self) -> HighPerformanceSnapshot {
        self.stats.snapshot()
    }
    
    /// Print performance statistics
    pub fn print_stats(&self, duration: Duration) {
        let stats = self.get_stats();
        
        println!("⚡ High-Performance Scan Statistics:");
        println!("   ├─ Files discovered: {}", stats.files_discovered);
        println!("   ├─ Directories processed: {}", stats.directories_processed);
        println!("   ├─ Files sent to pipeline: {}", stats.files_sent_to_pipeline);
        println!("   ├─ Total size: {} MB", stats.total_size_bytes / 1024 / 1024);
        println!("   ├─ Scan duration: {:.3}s", duration.as_secs_f64());
        
        if stats.errors_encountered > 0 {
            println!("   └─ ⚠️  {} errors encountered", stats.errors_encountered);
        } else {
            println!("   └─ ✅ No errors");
        }
        
        if duration.as_secs_f64() > 0.0 {
            let discovery_rate = stats.files_discovered as f64 / duration.as_secs_f64();
            println!("🚀 Performance: {:.0} files/sec discovery rate", discovery_rate);
        }
    }
}