use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use parking_lot::Mutex;
use std::collections::HashMap;
use std::sync::Arc;

/// Braille-based progress tracker suitable for concise TTY output.
pub struct ProgressTracker {
    multi_progress: Arc<MultiProgress>,
    stage_bars: Arc<Mutex<HashMap<String, ProgressBar>>>,
    scanner_bar: Arc<Mutex<Option<ProgressBar>>>,
}

impl ProgressTracker {
    const BRAILLE_STEPS: [char; 9] = ['⠀', '⠂', '⠆', '⠒', '⠲', '⠷', '⠿', '⡿', '⣿'];
    const MINI_BAR_WIDTH: usize = 12;

    pub fn new() -> Self {
        Self {
            multi_progress: Arc::new(MultiProgress::new()),
            stage_bars: Arc::new(Mutex::new(HashMap::new())),
            scanner_bar: Arc::new(Mutex::new(None)),
        }
    }

    /// Get the underlying MultiProgress instance for external rendering
    pub fn multi_progress(&self) -> Arc<MultiProgress> {
        self.multi_progress.clone()
    }

    /// Create (or replace) the discovery progress bar using braille mini-bar styling.
    pub fn create_scanning_bar(&self) -> ProgressBar {
        let pb = self.multi_progress.add(Self::new_spinner());
        pb.set_message(Self::discovery_status(0, 0));
        *self.scanner_bar.lock() = Some(pb.clone());
        pb
    }

    /// Create (or fetch) the progress bar for a named stage.
    pub fn create_stage_bar(&self, stage_name: &str) -> ProgressBar {
        let mut stage_bars = self.stage_bars.lock();
        if let Some(existing) = stage_bars.get(stage_name) {
            return existing.clone();
        }

        let pb = self.multi_progress.add(Self::new_spinner());
        pb.set_message(Self::stage_progress(stage_name, 0));
        stage_bars.insert(stage_name.to_string(), pb.clone());
        pb
    }

    /// Render a braille mini-bar for a given count.
    pub fn render_braille_bar(count: usize) -> String {
        let steps_per_cell = Self::BRAILLE_STEPS.len() - 1;
        let mut bar = String::with_capacity(Self::MINI_BAR_WIDTH);

        for cell in 0..Self::MINI_BAR_WIDTH {
            let lower_bound = cell * steps_per_cell;
            let cell_value = count.saturating_sub(lower_bound);
            let index = if cell_value >= steps_per_cell {
                steps_per_cell
            } else {
                cell_value
            };
            bar.push(Self::BRAILLE_STEPS[index]);
        }

        bar
    }

    /// Format an activity line for a stage during processing.
    pub fn stage_progress(stage_name: &str, count: usize) -> String {
        format!(
            "{:<11} {} {:>7}",
            stage_name,
            Self::render_braille_bar(count),
            count
        )
    }

    /// Format the final line for a stage once completed.
    pub fn stage_complete(stage_name: &str, count: usize) -> String {
        format!(
            "{:<11} {} {:>7} ✅",
            stage_name,
            Self::render_braille_bar(Self::MINI_BAR_WIDTH * (Self::BRAILLE_STEPS.len() - 1)),
            count
        )
    }

    /// Format discovery progress (directories/files) for the walker.
    pub fn discovery_status(directories: usize, files: usize) -> String {
        format!(
            "{:<11} {} {:>7} files {:>5} dirs",
            "discover",
            Self::render_braille_bar(files),
            files,
            directories
        )
    }

    /// Finish all tracked progress bars and clear the multi progress container.
    pub fn finish_all(&self) {
        if let Some(scanner) = self.scanner_bar.lock().take() {
            if !scanner.is_finished() {
                scanner.finish()
            }
        }

        for bar in self.stage_bars.lock().values() {
            if !bar.is_finished() {
                bar.finish();
            }
        }

        self.multi_progress.clear().ok();
    }

    fn new_spinner() -> ProgressBar {
        let pb = ProgressBar::new_spinner();
        pb.set_style(
            ProgressStyle::with_template("{msg}")
                .expect("braille template")
                .tick_strings(&[""]),
        );
        pb
    }
}

impl Default for ProgressTracker {
    fn default() -> Self {
        Self::new()
    }
}
