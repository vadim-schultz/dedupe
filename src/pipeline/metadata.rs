use std::collections::HashMap;
use anyhow::Result;
use async_trait::async_trait;
use indexmap::IndexMap;
use std::sync::Arc;
use crate::config::Config;
use super::{PipelineStage, ProcessingResult};
use crate::walker::FileInfo;

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
        // Group files by size first - this is O(n) operation
        let mut size_groups: IndexMap<u64, Vec<FileInfo>> = IndexMap::new();
        let total_files = files.len();
        
        for file in files {
            if file.size >= self.config.min_file_size {
                size_groups.entry(file.size)
                    .or_default()
                    .push(file);
            }
        }
        
        // Filter out groups with only one file - these can't be duplicates
        let mut unique_files = Vec::new();
        let mut potential_duplicates = Vec::new();
        
        for (_size, group) in size_groups {
            if group.len() == 1 {
                unique_files.extend(group);
            } else {
                // Further group by file type if available
                let mut type_groups: HashMap<Option<String>, Vec<FileInfo>> = HashMap::new();
                for file in group {
                    type_groups.entry(file.file_type.clone())
                        .or_default()
                        .push(file);
                }
                
                // Only keep groups with multiple files
                for (_type, type_group) in type_groups {
                    if type_group.len() > 1 {
                        potential_duplicates.push(type_group);
                    } else {
                        unique_files.extend(type_group);
                    }
                }
            }
        }
        
        if potential_duplicates.is_empty() {
            Ok(ProcessingResult::Skip(unique_files))
        } else if potential_duplicates.len() == 1 && potential_duplicates[0].len() == total_files {
            // If all files are in a single group, they might all be duplicates
            Ok(ProcessingResult::Duplicates(potential_duplicates))
        } else {
            Ok(ProcessingResult::Continue(
                potential_duplicates.into_iter().flatten().collect()
            ))
        }
    }
}