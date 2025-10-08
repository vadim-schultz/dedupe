use std::fmt::Debug;
use anyhow::Result;
use async_trait::async_trait;
use crate::walker::FileInfo;

mod metadata;
mod quick_check;
mod stats;
mod hash;

pub use {
    metadata::MetadataStage,
    quick_check::QuickCheckStage,
    stats::StatisticalStage,
    hash::HashStage,
};

/// Represents the processing result for a file or group of files
#[derive(Debug)]
pub enum ProcessingResult {
    /// File(s) should proceed to next stage
    Continue(Vec<FileInfo>),
    /// File(s) can be skipped (unique or invalid)
    Skip(Vec<FileInfo>),
    /// Potential duplicates found
    Duplicates(Vec<Vec<FileInfo>>),
}

/// Core trait for pipeline stages
#[async_trait]
pub trait PipelineStage: Send + Sync + Debug {
    /// Returns the name of the stage for metrics and logging
    fn name(&self) -> &'static str;

    /// Process a batch of files and determine their fate
    async fn process(&self, files: Vec<FileInfo>) -> Result<ProcessingResult> {
        self.process_impl(files).await
    }

    /// Implementation specific to each stage
    async fn process_impl(&self, files: Vec<FileInfo>) -> Result<ProcessingResult>;
}

/// Pipeline executor that runs files through stages sequentially
#[derive(Debug)]
pub struct Pipeline {
    stages: Vec<Box<dyn PipelineStage>>,
}

impl Pipeline {
    pub fn new() -> Self {
        Self { stages: Vec::new() }
    }

    pub fn add_stage<S: PipelineStage + 'static>(&mut self, stage: S) {
        self.stages.push(Box::new(stage));
    }

    pub async fn execute(&self, files: Vec<FileInfo>) -> Result<Vec<Vec<FileInfo>>> {
        let mut current_files = files;
        let mut duplicate_groups = Vec::new();

        // Process through each stage
        for stage in &self.stages {
            match stage.process(current_files).await? {
                ProcessingResult::Continue(files) => {
                    current_files = files;
                }
                ProcessingResult::Skip(files) => {
                    // Add each file as its own group (they're unique)
                    duplicate_groups.extend(files.into_iter().map(|f| vec![f]));
                    break;
                }
                ProcessingResult::Duplicates(groups) => {
                    duplicate_groups.extend(groups);
                    break;
                }
            }
        }

        Ok(duplicate_groups)
    }
}
