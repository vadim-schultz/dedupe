use super::PipelineStage;
use crate::walker::FileInfo;
use async_trait::async_trait;
use std::error::Error;

pub struct StatisticalStage {
    similarity_threshold: u8,
}

impl StatisticalStage {
    pub fn new(similarity_threshold: u8) -> Self {
        Self { similarity_threshold }
    }
}

#[async_trait]
impl PipelineStage for StatisticalStage {
    async fn process(&self, files: Vec<FileInfo>) -> Result<Vec<FileInfo>, Box<dyn Error>> {
        // TODO: Implement statistical analysis
        // For now, just pass through all files
        Ok(files)
    }
}