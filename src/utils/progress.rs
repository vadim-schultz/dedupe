use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use std::sync::Arc;

pub struct ProgressTracker {
    multi_progress: Arc<MultiProgress>,
}

impl ProgressTracker {
    pub fn new() -> Self {
        Self {
            multi_progress: Arc::new(MultiProgress::new()),
        }
    }

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
}

impl Default for ProgressTracker {
    fn default() -> Self {
        Self::new()
    }
}