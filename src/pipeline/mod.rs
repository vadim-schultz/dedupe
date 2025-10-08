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

pub use metadata::MetadataStage;
pub use quick_check::QuickCheckStage;
pub use stats::StatisticalStage;
pub use hash::HashStage;