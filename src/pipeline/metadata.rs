use super::PipelineStage;
use crate::walker::FileInfo;
use async_trait::async_trait;
use std::collections::HashMap;
use std::error::Error;

pub struct MetadataStage {
    min_file_size: u64,
}

impl MetadataStage {
    pub fn new(min_file_size: u64) -> Self {
        Self { min_file_size }
    }
}

#[async_trait]
impl PipelineStage for MetadataStage {
    async fn process(&self, files: Vec<FileInfo>) -> Result<Vec<FileInfo>, Box<dyn Error>> {
        // Group files by size
        let mut size_groups: HashMap<u64, Vec<FileInfo>> = HashMap::new();
        
        for file in files {
            if file.size >= self.min_file_size {
                size_groups.entry(file.size).or_default().push(file);
            }
        }
        
        // Only keep groups with potential duplicates (more than one file)
        let potential_duplicates = size_groups
            .into_values()
            .filter(|group| group.len() > 1)
            .flatten()
            .collect();
            
        Ok(potential_duplicates)
    }
}