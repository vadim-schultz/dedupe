use std::fmt::Debug;
use anyhow::Result;
use async_trait::async_trait;
use crate::walker::FileInfo;

mod metadata;
mod quick_check;
mod stats;
mod hash;
mod parallel;
mod thread_pool;

pub use {
    metadata::MetadataStage,
    quick_check::QuickCheckStage,
    stats::StatisticalStage,
    hash::HashStage,
    parallel::{ParallelPipeline, ParallelConfig, PipelineMetrics, MetricsSnapshot},
    thread_pool::{ThreadPoolManager, ThreadPoolConfig, WorkUnit, WorkPriority, WorkerStats},
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
        if files.is_empty() {
            return Ok(Vec::new());
        }

        let mut remaining = Some(files);
        let mut all_processed = Vec::new();
        let mut final_groups = Vec::new();

        // Process through each stage
        for stage in &self.stages {
            let to_process = match remaining.take() {
                Some(files) => files,
                None => break,
            };

            match stage.process(to_process).await? {
                ProcessingResult::Continue(files) => {
                    // Keep track of which files make it through
                    remaining = Some(files);
                }
                ProcessingResult::Skip(files) => {
                    // Save these for final grouping
                    all_processed.extend(files);
                }
                ProcessingResult::Duplicates(groups) => {
                    final_groups.extend(groups);
                    break;
                }
            }
        }

        // Handle remaining files from Continue results - group by size
        if let Some(files) = remaining {
            let mut size_groups = std::collections::HashMap::new();
            for file in files {
                size_groups.entry(file.size)
                    .or_insert_with(Vec::new)
                    .push(file);
            }
            
            for (_size, group) in size_groups {
                final_groups.push(group);
            }
        }

        // Add processed files as individual groups (from Skip results)
        for file in all_processed {
            final_groups.push(vec![file]);
        }

        Ok(final_groups)
    }
}
