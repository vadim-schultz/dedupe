use anyhow::Result;
use async_trait::async_trait;
use std::sync::Arc;
use crate::config::Config;
use super::{PipelineStage, ProcessingResult};
use crate::types::FileInfo;

/// First pipeline stage that groups files by size and performs basic metadata checks
#[derive(Debug)]
pub struct MetadataStage {
    config: Arc<Config>,
}

impl MetadataStage {
    pub fn new(config: Arc<Config>) -> Self {
        Self { config }
    }
}

#[async_trait]
impl PipelineStage for MetadataStage {
    fn name(&self) -> &'static str {
        "metadata"
    }

    async fn process_impl(&self, files: Vec<FileInfo>) -> Result<ProcessingResult> {
        if files.is_empty() {
            return Ok(ProcessingResult::Skip(Vec::new()));
        }

        // First filter by min size
        let mut valid_files = Vec::new();
        for file in files {
            if file.size >= self.config.min_file_size {
                valid_files.push(file);
            }
        }

        if valid_files.is_empty() {
            return Ok(ProcessingResult::Skip(Vec::new()));
        }

        // Group by size
        let mut size_groups = std::collections::HashMap::new();
        for file in valid_files {
            size_groups.entry(file.size)
                .or_insert_with(Vec::new)
                .push(file);
        }

        // Collect duplicate-size files and unique files
        let mut to_continue = Vec::new();
        let mut unique_files = Vec::new();

        for (_size, group) in size_groups {
            if group.len() > 1 {
                // Multiple files of same size go to next stage
                to_continue.extend(group);
            } else {
                // Single files can be skipped
                unique_files.extend(group);
            }
        }

        if !to_continue.is_empty() {
            // We have potential duplicates - create groups for duplicates and individuals for unique files
            let mut result_groups = Vec::new();
            
            // Group the potential duplicates by size
            let mut dup_groups = std::collections::HashMap::new();
            for file in to_continue {
                dup_groups.entry(file.size)
                    .or_insert_with(Vec::new)
                    .push(file);
            }
            
            // Add duplicate groups
            for (_size, group) in dup_groups {
                result_groups.push(group);
            }
            
            // Add unique files as individual groups
            for file in unique_files {
                result_groups.push(vec![file]);
            }
            
            Ok(ProcessingResult::Duplicates(result_groups))
        } else {
            Ok(ProcessingResult::Skip(unique_files))
        }
    }
}