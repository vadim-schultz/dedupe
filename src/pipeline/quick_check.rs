use super::PipelineStage;
use crate::walker::FileInfo;
use async_trait::async_trait;
use std::error::Error;

pub struct QuickCheckStage {
    sample_size: usize,
}

impl QuickCheckStage {
    pub fn new(sample_size: usize) -> Self {
        Self { sample_size }
    }
}

#[async_trait]
impl PipelineStage for QuickCheckStage {
    async fn process(&self, files: Vec<FileInfo>) -> Result<Vec<FileInfo>, Box<dyn Error>> {
        // TODO: Implement quick content check
        // For now, just pass through all files
        Ok(files)
    }
}