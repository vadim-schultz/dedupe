use super::PipelineStage;
use crate::walker::FileInfo;
use async_trait::async_trait;
use std::error::Error;

pub struct HashStage;

impl HashStage {
    pub fn new() -> Self {
        Self
    }
}

#[async_trait]
impl PipelineStage for HashStage {
    async fn process(&self, files: Vec<FileInfo>) -> Result<Vec<FileInfo>, Box<dyn Error>> {
        // TODO: Implement full file hashing
        // For now, just pass through all files
        Ok(files)
    }
}