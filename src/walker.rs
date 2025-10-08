use anyhow::Result;
use walkdir::{DirEntry, WalkDir};
use indicatif::{ProgressBar, ProgressStyle};
use camino::Utf8PathBuf;
use std::sync::Arc;
use crate::config::Config;

pub struct FileInfo {
    pub path: Utf8PathBuf,
    pub size: u64,
    pub file_type: Option<String>,
    pub modified: std::time::SystemTime,
}

pub struct Walker {
    config: Arc<Config>,
    progress: ProgressBar,
}

impl Walker {
    pub fn new(config: Arc<Config>) -> Self {
        let progress = ProgressBar::new(0);
        progress.set_style(
            ProgressStyle::default_bar()
                .template("[{elapsed_precise}] {bar:40.cyan/blue} {pos:>7}/{len:7} {msg}")
                .unwrap()
        );
        
        Self { config, progress }
    }

    pub fn walk<P: AsRef<std::path::Path>>(&self, path: P) -> Result<Vec<FileInfo>> {
        let walker = WalkDir::new(path)
            .min_depth(1)
            .max_depth(self.config.max_depth.unwrap_or(std::usize::MAX))
            .into_iter();

        let mut files = Vec::new();
        
        for entry in walker.filter_entry(|e| Self::is_valid_entry(e)) {
            let entry = entry?;
            if let Some(file_info) = self.process_entry(entry)? {
                files.push(file_info);
                self.progress.inc(1);
            }
        }
        
        self.progress.finish_with_message("Scan complete");
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

    fn process_entry(&self, entry: DirEntry) -> Result<Option<FileInfo>> {
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

        Ok(Some(FileInfo {
            path,
            size,
            file_type,
            modified: metadata.modified()?,
        }))
    }
}