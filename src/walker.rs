use anyhow::{Result, Context};
use walkdir::{DirEntry, WalkDir};
use indicatif::{ProgressBar, ProgressStyle, MultiProgress};
use camino::Utf8PathBuf;
use std::sync::Arc;
use crate::config::Config;

#[derive(Debug, Clone)]
pub struct FileInfo {
    pub path: Utf8PathBuf,
    pub size: u64,
    pub file_type: Option<String>,
    pub modified: std::time::SystemTime,
    pub created: Option<std::time::SystemTime>,
    pub readonly: bool,
    pub hidden: bool,
    pub checksum: Option<String>,
}

pub struct Walker {
    config: Arc<Config>,
    multi_progress: Arc<MultiProgress>,
    files_progress: ProgressBar,
    size_progress: ProgressBar,
}

impl Walker {
    pub fn new(config: Arc<Config>) -> Self {
        let multi_progress = Arc::new(MultiProgress::new());
        
        let files_progress = multi_progress.add(ProgressBar::new(0));
        files_progress.set_style(
            ProgressStyle::default_bar()
                .template("[{elapsed_precise}] {bar:40.cyan/blue} {pos:>7}/{len:7} files {msg}")
                .unwrap()
                .progress_chars("=> ")
        );

        let size_progress = multi_progress.add(ProgressBar::new(0));
        size_progress.set_style(
            ProgressStyle::default_bar()
                .template("[{elapsed_precise}] {bar:40.green/blue} {bytes:>7}/{total_bytes} {msg}")
                .unwrap()
                .progress_chars("=> ")
        );
        
        Self { 
            config, 
            multi_progress,
            files_progress,
            size_progress,
        }
    }

    pub fn walk<P: AsRef<std::path::Path>>(&self, path: P) -> Result<Vec<FileInfo>> {
        let walker = WalkDir::new(path)
            .min_depth(1)
            .max_depth(self.config.max_depth.unwrap_or(std::usize::MAX))
            .into_iter();

        let mut files = Vec::new();
        let mut total_size: u64 = 0;
        
        // First pass to count files and total size
        let walker_count = walker.filter_entry(|e| Self::is_valid_entry(e)).collect::<Vec<_>>();
        self.files_progress.set_length(walker_count.len() as u64);
        
        for entry in walker_count {
            let entry = entry?;
            if let Some(file_info) = self.process_entry(&entry)
                .with_context(|| format!("Failed to process entry: {:?}", entry.path()))? 
            {
                total_size += file_info.size;
                self.files_progress.inc(1);
                self.size_progress.inc(file_info.size);
                files.push(file_info);
            }
        }
        
        self.size_progress.set_length(total_size);
        self.files_progress.finish_with_message("Scan complete");
        self.size_progress.finish_with_message(format!("Total size: {:.2} GB", total_size as f64 / 1024.0 / 1024.0 / 1024.0));
        
        Ok(files)
    }

    fn is_valid_entry(entry: &DirEntry) -> bool {
        // Skip hidden files and directories
        if entry
            .file_name()
            .to_str()
            .map(|s| s.starts_with('.'))
            .unwrap_or(false)
        {
            return false;
        }

        // Allow all directories to be traversed
        if entry.file_type().is_dir() {
            return true;
        }

        // Only allow regular files
        entry.file_type().is_file()
    }

    fn process_entry(&self, entry: &DirEntry) -> Result<Option<FileInfo>> {
        if !entry.file_type().is_file() {
            return Ok(None);
        }

        let metadata = entry.metadata()?;
        let size = metadata.len();

        // Skip files smaller than minimum size
        if size < self.config.min_file_size {
            return Ok(None);
        }

        let path = Utf8PathBuf::try_from(entry.path().to_path_buf())?;
        let file_type = infer::get_from_path(&path)
            .ok()
            .flatten()
            .map(|kind| kind.mime_type().to_string());

        #[cfg(windows)]
        let (readonly, hidden) = {
            use std::os::windows::fs::MetadataExt;
            let attrs = metadata.file_attributes();
            (
                (attrs & 0x1) != 0,  // FILE_ATTRIBUTE_READONLY
                (attrs & 0x2) != 0,  // FILE_ATTRIBUTE_HIDDEN
            )
        };

        #[cfg(unix)]
        let (readonly, hidden) = {
            use std::os::unix::fs::MetadataExt;
            let mode = metadata.mode();
            (
                (mode & 0o200) == 0,  // Write permission check
                path.file_name()
                    .map(|name| name.starts_with('.'))
                    .unwrap_or(false),
            )
        };

        #[cfg(not(any(windows, unix)))]
        let (readonly, hidden) = (false, false);

        Ok(Some(FileInfo {
            path,
            size,
            file_type,
            modified: metadata.modified()?,
            created: metadata.created().ok(),
            readonly,
            hidden,
            checksum: None, // Will be computed in later stages
        }))
    }
}
