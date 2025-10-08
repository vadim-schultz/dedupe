use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use std::sync::Arc;
use std::collections::HashMap;
use parking_lot::Mutex;

/// Enhanced progress tracker supporting multiple stages with dynamic progress bars
pub struct ProgressTracker {
    multi_progress: Arc<MultiProgress>,
    stage_bars: Arc<Mutex<HashMap<String, Vec<ProgressBar>>>>,
}

impl ProgressTracker {
    pub fn new() -> Self {
        Self {
            multi_progress: Arc::new(MultiProgress::new()),
            stage_bars: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Get the underlying MultiProgress instance for external rendering
    pub fn multi_progress(&self) -> Arc<MultiProgress> {
        self.multi_progress.clone()
    }

    /// Create a progress bar for file scanning
    pub fn create_scanning_bar(&self, total_files: u64) -> ProgressBar {
        let pb = self.multi_progress.add(ProgressBar::new(total_files));
        pb.set_style(
            ProgressStyle::default_bar()
                .template("🔍 Scanning:     [{elapsed_precise}] {bar:40.cyan/blue} {pos:>7}/{len:7} files {msg}")
                .unwrap()
                .progress_chars("█▉▊▋▌▍▎▏ "),
        );
        pb
    }

    /// Create progress bars for a pipeline stage
    pub fn create_stage_bars(&self, stage_name: &str, thread_count: usize, total_files: u64) -> Vec<ProgressBar> {
        let mut bars = Vec::new();
        let files_per_thread = if thread_count > 1 {
            (total_files + thread_count as u64 - 1) / thread_count as u64
        } else {
            total_files
        };

        for i in 0..thread_count {
            let pb = self.multi_progress.add(ProgressBar::new(files_per_thread));
            let stage_emoji = match stage_name.to_lowercase().as_str() {
                "metadata" => "📊",
                "quickcheck" => "⚡",
                "statistical" => "🧮",
                "hash" => "🔐",
                _ => "🔄",
            };
            
            let template = if thread_count > 1 {
                format!("{} {:<11} [{}/{}]: [{{elapsed_precise}}] {{bar:30.green/yellow}} {{pos:>7}}/{{len:7}} {{msg}}", 
                    stage_emoji, 
                    format!("{}:", stage_name),
                    i + 1, 
                    thread_count
                )
            } else {
                format!("{} {:<11}     [{{elapsed_precise}}] {{bar:40.green/yellow}} {{pos:>7}}/{{len:7}} {{msg}}", 
                    stage_emoji,
                    format!("{}:", stage_name)
                )
            };

            pb.set_style(
                ProgressStyle::default_bar()
                    .template(&template)
                    .unwrap()
                    .progress_chars("█▉▊▋▌▍▎▏ "),
            );
            
            bars.push(pb);
        }

        // Store bars for this stage
        let mut stage_bars = self.stage_bars.lock();
        stage_bars.insert(stage_name.to_string(), bars.clone());

        bars
    }

    /// Get progress bars for a specific stage
    pub fn get_stage_bars(&self, stage_name: &str) -> Option<Vec<ProgressBar>> {
        let stage_bars = self.stage_bars.lock();
        stage_bars.get(stage_name).cloned()
    }

    /// Finish all progress bars for a stage
    pub fn finish_stage(&self, stage_name: &str) {
        if let Some(bars) = self.get_stage_bars(stage_name) {
            for bar in bars {
                bar.finish_with_message("✅ Complete");
            }
        }
    }

    /// Create a legacy progress bar (for backward compatibility)
    pub fn create_progress_bar(&self, len: u64, name: &str) -> ProgressBar {
        let pb = self.multi_progress.add(ProgressBar::new(len));
        pb.set_style(
            ProgressStyle::default_bar()
                .template(&format!("{{spinner}} {}: [{{elapsed_precise}}] {{bar:40.cyan/blue}} {{pos}}/{{len}} {{msg}}", name))
                .unwrap()
                .progress_chars("=>-"),
        );
        pb
    }

    /// Update progress for a specific stage and worker
    pub fn update_stage_progress(&self, stage_name: &str, worker_id: usize, current: usize, _total: u64) {
        if let Some(bars) = self.get_stage_bars(stage_name) {
            if let Some(bar) = bars.get(worker_id) {
                bar.set_position(current as u64);
            }
        }
    }

    /// Update scanning progress (legacy method)
    pub fn update_scanning_progress(&self, _worker_id: usize, current: usize, _total: u64) {
        // This is a legacy method - scanning progress should use update_stage_progress instead
        // but we keep it for compatibility
        if let Some(bars) = self.get_stage_bars("Scanning") {
            if let Some(bar) = bars.first() {
                bar.set_position(current as u64);
            }
        }
    }

    /// Finish all progress bars and clean up the MultiProgress
    pub fn finish_all(&self) {
        // Finish all stored progress bars
        let stage_bars = self.stage_bars.lock();
        for (_, bars) in stage_bars.iter() {
            for bar in bars {
                if !bar.is_finished() {
                    bar.finish_with_message("✅ Complete");
                }
            }
        }
        drop(stage_bars);
        
        // Clear the MultiProgress to allow proper shutdown
        self.multi_progress.clear().ok();
    }
}

impl Default for ProgressTracker {
    fn default() -> Self {
        Self::new()
    }
}