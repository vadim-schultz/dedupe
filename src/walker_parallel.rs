use anyhow::{Result, Context};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::path::PathBuf;
use tokio::fs;
use tokio::task::JoinHandle;
use futures::future::join_all;
use rayon::prelude::*;
use crossbeam_channel::{unbounded, Receiver, Sender};
use indicatif::ProgressBar;
use camino::Utf8PathBuf;

use crate::config::Config;
use crate::walker::FileInfo;
use crate::utils::ProgressTracker;

/// Parallel file scanner that uses async I/O and work-stealing
pub struct ParallelWalker {
    config: Arc<Config>,
    progress_tracker: Arc<ProgressTracker>,
}

/// Directory entry for parallel processing
#[derive(Debug, Clone)]
struct ScanEntry {
    path: PathBuf,
    depth: usize,
}

/// Statistics for scan progress
#[derive(Debug)]
struct ScanStats {
    files_processed: AtomicUsize,
    directories_processed: AtomicUsize,
    total_size: AtomicU64,
    errors_encountered: AtomicUsize,
}

#[derive(Debug)]
pub struct ScanResult {
    pub files: Vec<FileInfo>,
    pub stats: ScanStatistics,
}

#[derive(Debug)]
pub struct ScanStatistics {
    pub files_scanned: usize,
    pub directories_processed: usize,
    pub total_size_bytes: u64,
    pub errors: usize,
    pub total_duration: std::time::Duration,
}

impl ParallelWalker {
    pub fn new(config: Arc<Config>, progress_tracker: Arc<ProgressTracker>) -> Self {
        Self {
            config,
            progress_tracker,
        }
    }
    
    /// Walk the directory tree and return only files (for compatibility)
    pub async fn walk_files<P: AsRef<std::path::Path>>(&self, root_path: P) -> Result<Vec<FileInfo>> {
        let result = self.walk(root_path).await?;
        Ok(result.files)
    }

    /// Parallel directory walk using async I/O and work-stealing
    pub async fn walk<P: AsRef<std::path::Path>>(&self, root_path: P) -> Result<ScanResult> {
        let start_time = std::time::Instant::now();
        let root_path = root_path.as_ref().to_path_buf();
        let max_depth = self.config.max_depth.unwrap_or(usize::MAX);
        let thread_count = self.config.threads_per_stage.max(1);

        // Create channels for work distribution
        let (dir_tx, dir_rx) = unbounded::<ScanEntry>();
        let (file_tx, file_rx) = unbounded::<FileInfo>();
        
        // Statistics tracking
        let stats = Arc::new(ScanStats {
            files_processed: AtomicUsize::new(0),
            directories_processed: AtomicUsize::new(0),
            total_size: AtomicU64::new(0),
            errors_encountered: AtomicUsize::new(0),
        });

        // Create progress bars with tracker
        let scanning_progress = self.progress_tracker.create_scanning_bar(1000); // Estimated, will be updated

        // Start with root directory
        dir_tx.send(ScanEntry {
            path: root_path.clone(),
            depth: 0,
        })?;

        // Spawn directory walker tasks
        let mut walker_tasks: Vec<JoinHandle<Result<()>>> = Vec::new();
        
        for worker_id in 0..thread_count {
            let dir_rx = dir_rx.clone();
            let dir_tx = dir_tx.clone();
            let file_tx = file_tx.clone();
            let config = self.config.clone();
            let stats = stats.clone();
            let progress = Some(scanning_progress.clone());
            
            let task = tokio::spawn(async move {
                Self::directory_walker_task(
                    worker_id,
                    dir_rx,
                    dir_tx,
                    file_tx,
                    config,
                    max_depth,
                    stats,
                    progress,
                ).await
            });
            
            walker_tasks.push(task);
        }

        // Spawn file collector task
        let collector_stats = stats.clone();
        let collector_task = tokio::spawn(async move {
            Self::file_collector_task(file_rx, collector_stats).await
        });

        // Keep the original senders for work distribution
        // Workers will detect completion when no more work is available

        // Wait for all walker tasks to complete
        let walker_results = join_all(walker_tasks).await;
        for result in walker_results {
            result??; // Unwrap JoinHandle result and task result
        }

        // Wait for file collector and get results
        let files = collector_task.await??;

        // Finish progress bar
        let final_count = stats.files_processed.load(Ordering::Relaxed);
        scanning_progress.set_length(final_count as u64);
        scanning_progress.finish_with_message("Scan complete");

        // Print scan statistics
        let files_count = stats.files_processed.load(Ordering::Relaxed);
        let dirs_count = stats.directories_processed.load(Ordering::Relaxed);
        let errors_count = stats.errors_encountered.load(Ordering::Relaxed);
        let total_size = stats.total_size.load(Ordering::Relaxed);

        println!("📈 Scan completed:");
        println!("   Files: {}, Directories: {}, Total size: {} MB", 
            files_count, dirs_count, total_size / 1024 / 1024);
        
        if errors_count > 0 {
            println!("   ⚠️  {} errors encountered during scan", errors_count);
        }

        let duration = start_time.elapsed();
        
        Ok(ScanResult {
            files,
            stats: ScanStatistics {
                files_scanned: files_count,
                directories_processed: dirs_count,
                total_size_bytes: total_size,
                errors: errors_count,
                total_duration: duration,
            },
        })
    }

    /// Directory walker task - processes directories and discovers files
    async fn directory_walker_task(
        worker_id: usize,
        dir_rx: Receiver<ScanEntry>,
        dir_tx: Sender<ScanEntry>,
        file_tx: Sender<FileInfo>,
        config: Arc<Config>,
        max_depth: usize,
        stats: Arc<ScanStats>,
        progress: Option<ProgressBar>,
    ) -> Result<()> {
        loop {
            // Use try_recv with timeout to detect completion
            let entry = match dir_rx.recv_timeout(std::time::Duration::from_millis(100)) {
                Ok(entry) => entry,
                Err(crossbeam_channel::RecvTimeoutError::Timeout) => {
                    // Check if there's any more work to do
                    if dir_rx.is_empty() && dir_tx.is_empty() {
                        break; // No more work, exit gracefully
                    }
                    continue;
                }
                Err(crossbeam_channel::RecvTimeoutError::Disconnected) => {
                    break; // Channel closed, exit
                }
            };
            if let Err(e) = Self::process_directory(
                worker_id,
                &entry,
                &dir_tx,
                &file_tx,
                &config,
                max_depth,
                &stats,
                &progress,
            ).await {
                eprintln!("Worker {}: Error processing directory {:?}: {}", 
                    worker_id, entry.path, e);
                stats.errors_encountered.fetch_add(1, Ordering::Relaxed);
            }
            
            stats.directories_processed.fetch_add(1, Ordering::Relaxed);
        }
        
        Ok(())
    }

    /// Process a single directory entry
    async fn process_directory(
        _worker_id: usize,
        entry: &ScanEntry,
        dir_tx: &Sender<ScanEntry>,
        file_tx: &Sender<FileInfo>,
        config: &Config,
        max_depth: usize,
        stats: &ScanStats,
        progress: &Option<ProgressBar>,
    ) -> Result<()> {
        let mut entries = fs::read_dir(&entry.path).await
            .with_context(|| format!("Failed to read directory: {:?}", entry.path))?;

        while let Some(dir_entry) = entries.next_entry().await? {
            let path = dir_entry.path();
            let metadata = match dir_entry.metadata().await {
                Ok(meta) => meta,
                Err(e) => {
                    eprintln!("Failed to get metadata for {:?}: {}", path, e);
                    continue;
                }
            };

            // Skip hidden files/directories
            if Self::is_hidden(&path) {
                continue;
            }

            if metadata.is_dir() {
                // Add subdirectory to work queue if within depth limit
                if entry.depth < max_depth {
                    let _ = dir_tx.send(ScanEntry {
                        path: path.clone(),
                        depth: entry.depth + 1,
                    });
                }
            } else if metadata.is_file() {
                // Process file
                if let Some(file_info) = Self::create_file_info(path, metadata, config).await? {
                    stats.files_processed.fetch_add(1, Ordering::Relaxed);
                    stats.total_size.fetch_add(file_info.size, Ordering::Relaxed);
                    
                    // Update progress
                    if let Some(pb) = progress {
                        pb.inc(1);
                        pb.set_message(format!("Found {} files", 
                            stats.files_processed.load(Ordering::Relaxed)));
                    }
                    
                    let _ = file_tx.send(file_info);
                }
            }
        }

        Ok(())
    }

    /// Create FileInfo from path and metadata
    async fn create_file_info(
        path: PathBuf,
        metadata: std::fs::Metadata,
        config: &Config,
    ) -> Result<Option<FileInfo>> {
        let size = metadata.len();

        // Skip files smaller than minimum size
        if size < config.min_file_size {
            return Ok(None);
        }

        let utf8_path = Utf8PathBuf::try_from(path)?;
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
                path.file_name()
                    .map(|name| name.starts_with('.'))
                    .unwrap_or(false),
            )
        }

        #[cfg(not(any(windows, unix)))]
        (false, false)
    }

    /// Check if path represents a hidden file/directory
    fn is_hidden(path: &PathBuf) -> bool {
        path.file_name()
            .and_then(|name| name.to_str())
            .map(|name| name.starts_with('.'))
            .unwrap_or(false)
    }

    /// File collector task - gathers all discovered files
    async fn file_collector_task(
        file_rx: Receiver<FileInfo>,
        _stats: Arc<ScanStats>,
    ) -> Result<Vec<FileInfo>> {
        let mut files = Vec::new();
        
        while let Ok(file_info) = file_rx.recv() {
            files.push(file_info);
        }
        
        Ok(files)
    }
}

/// Performance comparison utilities
pub mod benchmark {
    use super::*;
    use std::time::Instant;

    /// Compare parallel vs sequential scanning performance
    pub async fn compare_scanners(
        path: impl AsRef<std::path::Path>,
        config: Arc<Config>,
    ) -> Result<()> {
        let path = path.as_ref();
        
        println!("🏁 Benchmarking file scanners on: {:?}", path);
        
        // Test sequential walker
        println!("\n📊 Testing Sequential Walker...");
        let start = Instant::now();
        let sequential_walker = crate::walker::Walker::new(config.clone());
        let seq_files = sequential_walker.walk(path)?;
        let seq_duration = start.elapsed();
        
        // Test parallel walker
        println!("\n⚡ Testing Parallel Walker...");
        let start = Instant::now();
        let parallel_walker = ParallelWalker::new(config.clone());
        let par_files = parallel_walker.walk(path).await?;
        let par_duration = start.elapsed();
        
        // Results comparison
        println!("\n🏆 Performance Comparison:");
        println!("Sequential: {} files in {:?} ({:.1} files/sec)", 
            seq_files.len(), seq_duration, 
            seq_files.len() as f64 / seq_duration.as_secs_f64());
        println!("Parallel:   {} files in {:?} ({:.1} files/sec)", 
            par_files.files.len(), par_duration,
            par_files.files.len() as f64 / par_duration.as_secs_f64());
        
        let speedup = seq_duration.as_secs_f64() / par_duration.as_secs_f64();
        println!("Speedup: {:.2}x", speedup);
        
        if speedup > 1.0 {
            println!("✅ Parallel scanning is {:.1}% faster!", (speedup - 1.0) * 100.0);
        } else {
            println!("⚠️  Sequential scanning was {:.1}% faster", (1.0 - speedup) * 100.0);
        }
        
        Ok(())
    }
}