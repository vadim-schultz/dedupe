use std::sync::{Arc, atomic::{AtomicUsize, AtomicU64, Ordering}};
use tokio::fs;
use anyhow::{Context, Result};
use camino::Utf8PathBuf;
use rayon::prelude::*;
use crossbeam_channel::unbounded;

use crate::{
    config::Config,
    types::FileInfo,
    utils::progress::ProgressTracker,
};

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

struct ScanStats {
    files_processed: AtomicUsize,
    directories_processed: AtomicUsize,
    total_size: AtomicU64,
    errors_encountered: AtomicUsize,
}

pub struct SimpleWalker {
    config: Arc<Config>,
    progress_tracker: Arc<ProgressTracker>,
}

impl SimpleWalker {
    pub fn new(config: Arc<Config>, progress_tracker: Arc<ProgressTracker>) -> Self {
        Self {
            config,
            progress_tracker,
        }
    }

    /// Walk the directory tree and collect files
    pub async fn walk<P: AsRef<std::path::Path>>(&self, root_paths: &[P]) -> Result<ScanResult> {
        let start_time = std::time::Instant::now();
        
        let files = if self.config.parallel_scan {
            self.walk_parallel(root_paths).await?
        } else {
            self.walk_sequential(root_paths).await?
        };

        let duration = start_time.elapsed();
        
        Ok(ScanResult {
            stats: ScanStatistics {
                files_scanned: files.len(),
                directories_processed: 0, // Will be updated by actual implementation
                total_size_bytes: files.iter().map(|f| f.size).sum(),
                errors: 0,
                total_duration: duration,
            },
            files,
        })
    }

    /// Parallel multi-worker scanning with individual progress bars
    async fn walk_parallel<P: AsRef<std::path::Path>>(&self, root_paths: &[P]) -> Result<Vec<FileInfo>> {
        let num_workers = rayon::current_num_threads();
        let bars = self.progress_tracker.create_stage_bars("Scanning", num_workers, 0);
        
        // First, collect all directories to distribute among workers
        let mut all_dirs = Vec::new();
        for root_path in root_paths {
            self.collect_all_directories(root_path.as_ref(), &mut all_dirs).await?;
        }
        
        if all_dirs.is_empty() {
            for bar in bars {
                bar.finish_with_message("No directories found");
            }
            return Ok(Vec::new());
        }
        
        // Distribute directories among workers
        let dirs_per_worker = (all_dirs.len() + num_workers - 1) / num_workers;
        let mut worker_assignments: Vec<Vec<std::path::PathBuf>> = vec![Vec::new(); num_workers];
        
        for (i, dir) in all_dirs.into_iter().enumerate() {
            let worker_id = i / dirs_per_worker;
            if worker_id < num_workers {
                worker_assignments[worker_id].push(dir);
            }
        }
        
        // Create communication channel
        let (sender, receiver) = unbounded();
        
        // Spawn worker threads
        let handles: Vec<_> = worker_assignments.into_par_iter().enumerate().map(|(worker_id, dirs)| {
            let config = Arc::clone(&self.config);
            let progress_tracker = Arc::clone(&self.progress_tracker);
            let sender = sender.clone();
            
            std::thread::spawn(move || {
                let mut worker_files = Vec::new();
                let mut files_processed = 0;
                
                for dir in dirs {
                    if let Ok(entries) = std::fs::read_dir(&dir) {
                        for entry in entries.flatten() {
                            if let Ok(metadata) = entry.metadata() {
                                if metadata.is_file() {
                                    let size = metadata.len();
                                    
                                    if size >= config.min_file_size {
                                        if let Ok(utf8_path) = Utf8PathBuf::from_path_buf(entry.path()) {
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
                                            
                                            if should_include {
                                                let file_info = FileInfo {
                                                    path: utf8_path,
                                                    size,
                                                    file_type: None,
                                                    modified: metadata.modified().unwrap_or(std::time::SystemTime::UNIX_EPOCH),
                                                    created: metadata.created().ok(),
                                                    readonly: metadata.permissions().readonly(),
                                                    hidden: false,
                                                    checksum: None,
                                                };
                                                worker_files.push(file_info);
                                            }
                                        }
                                    }
                                }
                            }
                            
                            files_processed += 1;
                            if files_processed % 100 == 0 {
                                progress_tracker.update_stage_progress("Scanning", worker_id, files_processed, 0);
                            }
                        }
                    }
                }
                
                // Final progress update
                progress_tracker.update_stage_progress("Scanning", worker_id, files_processed, 0);
                let _ = sender.send((worker_id, worker_files));
            })
        }).collect();
        
        drop(sender);
        
        // Collect results from all workers
        let mut all_files = Vec::new();
        while let Ok((_worker_id, worker_files)) = receiver.recv() {
            all_files.extend(worker_files);
        }
        
        // Wait for all threads to complete
        for handle in handles {
            let _ = handle.join();
        }
        
        // Finish all progress bars
        for bar in bars.into_iter() {
            bar.finish_with_message("✅ Complete");
        }
        
        Ok(all_files)
    }

    /// Sequential scanning with single progress bar
    async fn walk_sequential<P: AsRef<std::path::Path>>(&self, root_paths: &[P]) -> Result<Vec<FileInfo>> {
        let scanning_bar = self.progress_tracker.create_scanning_bar(0);
        scanning_bar.set_message("Scanning files...");
        
        let mut all_files = Vec::new();
        let mut files_processed = 0;
        
        for root_path in root_paths {
            let mut files = Vec::new();
            self.scan_directory_sequential(root_path.as_ref(), &mut files, &mut files_processed, &scanning_bar).await?;
            all_files.extend(files);
        }
        
        scanning_bar.finish_with_message("Sequential scan complete");
        Ok(all_files)
    }

    /// Collect all directories recursively for distribution among workers
    async fn collect_all_directories(&self, root_path: &std::path::Path, all_dirs: &mut Vec<std::path::PathBuf>) -> Result<()> {
        let mut stack = vec![root_path.to_path_buf()];
        
        while let Some(current_dir) = stack.pop() {
            if let Ok(entries) = fs::read_dir(&current_dir).await {
                all_dirs.push(current_dir.clone());
                
                let mut dir_entries = entries;
                while let Some(entry) = dir_entries.next_entry().await? {
                    if let Ok(metadata) = entry.metadata().await {
                        if metadata.is_dir() {
                            stack.push(entry.path());
                        }
                    }
                }
            }
        }
        
        Ok(())
    }

    /// Sequential directory scanning helper
    async fn scan_directory_sequential(
        &self, 
        dir_path: &std::path::Path, 
        files: &mut Vec<FileInfo>,
        files_processed: &mut usize,
        _progress_bar: &indicatif::ProgressBar
    ) -> Result<()> {
        let mut stack = vec![dir_path.to_path_buf()];
        
        while let Some(current_path) = stack.pop() {
            if let Ok(entries) = fs::read_dir(&current_path).await {
                let mut dir_entries = entries;
                while let Some(entry) = dir_entries.next_entry().await? {
                    if let Ok(metadata) = entry.metadata().await {
                        if metadata.is_dir() {
                            stack.push(entry.path());
                        } else if metadata.is_file() {
                            let size = metadata.len();
                            
                            if size >= self.config.min_file_size {
                                if let Ok(utf8_path) = Utf8PathBuf::from_path_buf(entry.path()) {
                                    let should_include = if self.config.extensions.is_empty() {
                                        true
                                    } else {
                                        utf8_path.extension()
                                            .map(|ext| {
                                                let ext_lower = ext.to_lowercase();
                                                self.config.extensions.iter().any(|e| e.to_lowercase() == ext_lower)
                                            })
                                            .unwrap_or(false)
                                    };
                                    
                                    if should_include {
                                        let file_info = FileInfo {
                                            path: utf8_path,
                                            size,
                                            file_type: None,
                                            modified: metadata.modified().unwrap_or(std::time::SystemTime::UNIX_EPOCH),
                                            created: metadata.created().ok(),
                                            readonly: metadata.permissions().readonly(),
                                            hidden: false,
                                            checksum: None,
                                        };
                                        files.push(file_info);
                                    }
                                }
                            }
                        }
                    }
                    
                    *files_processed += 1;
                    if *files_processed % 100 == 0 {
                        self.progress_tracker.update_scanning_progress(0, *files_processed, 0);
                    }
                }
            }
        }
        
        Ok(())
    }

    /// Recursive directory walking
    async fn walk_directory_recursive(
        &self,
        dir_path: &std::path::Path,
        current_depth: usize,
        max_depth: usize,
        files: &mut Vec<FileInfo>,
        stats: &Arc<ScanStats>,
        progress: &indicatif::ProgressBar,
    ) -> Result<()> {
        if current_depth >= max_depth {
            return Ok(());
        }

        let mut entries = fs::read_dir(dir_path).await
            .with_context(|| format!("Failed to read directory: {:?}", dir_path))?;

        stats.directories_processed.fetch_add(1, Ordering::Relaxed);

        while let Some(dir_entry) = entries.next_entry().await? {
            let path = dir_entry.path();
            let metadata = match dir_entry.metadata().await {
                Ok(meta) => meta,
                Err(_) => {
                    stats.errors_encountered.fetch_add(1, Ordering::Relaxed);
                    continue;
                }
            };

            // Skip hidden files/directories on Windows
            if self.is_hidden(&path) {
                continue;
            }

            if metadata.is_dir() {
                // Recursively process subdirectory
                Box::pin(self.walk_directory_recursive(
                    &path,
                    current_depth + 1,
                    max_depth,
                    files,
                    stats,
                    progress,
                )).await?;
            } else if metadata.is_file() {
                // Process file
                if metadata.len() >= self.config.min_file_size {
                    let utf8_path = match Utf8PathBuf::from_path_buf(path) {
                        Ok(p) => p,
                        Err(_) => {
                            stats.errors_encountered.fetch_add(1, Ordering::Relaxed);
                            continue;
                        }
                    };

                    let file_info = FileInfo {
                        path: utf8_path,
                        size: metadata.len(),
                        file_type: None, // Will be detected later if needed
                        modified: metadata.modified().unwrap_or_else(|_| std::time::SystemTime::UNIX_EPOCH),
                        created: metadata.created().ok(),
                        readonly: metadata.permissions().readonly(),
                        hidden: false,
                        checksum: None,
                    };

                    files.push(file_info);
                    stats.files_processed.fetch_add(1, Ordering::Relaxed);
                    stats.total_size.fetch_add(metadata.len(), Ordering::Relaxed);
                    
                    // Update progress
                    progress.inc(1);
                }
            }
        }

        Ok(())
    }

    /// Check if a path is hidden (Windows-specific)
    fn is_hidden(&self, path: &std::path::Path) -> bool {
        #[cfg(windows)]
        {
            use std::os::windows::fs::MetadataExt;
            if let Ok(metadata) = path.metadata() {
                const FILE_ATTRIBUTE_HIDDEN: u32 = 0x2;
                (metadata.file_attributes() & FILE_ATTRIBUTE_HIDDEN) != 0
            } else {
                false
            }
        }
        #[cfg(not(windows))]
        {
            path.file_name()
                .and_then(|name| name.to_str())
                .map(|name| name.starts_with('.'))
                .unwrap_or(false)
        }
    }
}