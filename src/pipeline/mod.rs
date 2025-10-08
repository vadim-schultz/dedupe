use std::error::Error;
use async_trait::async_trait;
use crate::walker::FileInfo;

#[async_trait]
pub trait PipelineStage: Send + Sync {
    async fn process(&self, files: Vec<FileInfo>) -> Result<Vec<FileInfo>, Box<dyn Error>>;
}

mod metadata;
mod quick_check;
mod stats;
mod hash;

// These will be used when implementing the pipeline execution
#[allow(unused_imports)]
pub use {
    metadata::MetadataStage,
    quick_check::QuickCheckStage,
    stats::StatisticalStage,
    hash::HashStage,
};
