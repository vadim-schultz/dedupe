use std::collections::HashMap;
use std::fs::File;
use std::io::{self, Read, Seek, SeekFrom};
use anyhow::{Result, Context};
use async_trait::async_trait;
use tokio::task;
use super::{PipelineStage, ProcessingResult};
use crate::walker::FileInfo;

const SAMPLE_SIZE: usize = 4096; // 4KB samples from start and end

/// Second stage that performs quick content checks by reading small portions of files
#[derive(Debug)]
pub struct QuickCheckStage {
    sample_size: usize,
}

impl QuickCheckStage {
    pub fn new(sample_size: Option<usize>) -> Self {
        Self {
            sample_size: sample_size.unwrap_or(SAMPLE_SIZE),
        }
    }
    
    async fn read_samples(path: String, sample_size: usize) -> Result<(Vec<u8>, Vec<u8>)> {
        task::spawn_blocking(move || {
            let mut file = File::open(&path)
                .with_context(|| format!("Failed to open file: {}", path))?;
                
            let file_size = file.metadata()?.len() as usize;
            let read_size = sample_size.min(file_size / 2);
            
            // Read start sample
            let mut start_sample = vec![0; read_size];
            file.read_exact(&mut start_sample)?;
            
            // Read end sample
            file.seek(SeekFrom::End(-(read_size as i64)))?;
            let mut end_sample = vec![0; read_size];
            file.read_exact(&mut end_sample)?;
            
            Ok((start_sample, end_sample))
        }).await?
    }
}

#[async_trait]
impl PipelineStage for QuickCheckStage {
    fn name(&self) -> &'static str {
        "quick_check"
    }

    async fn process_impl(&self, files: Vec<FileInfo>) -> Result<ProcessingResult> {
        // Group files by size first
        let mut size_groups: HashMap<u64, Vec<FileInfo>> = HashMap::new();
        for file in files {
            size_groups.entry(file.size).or_default().push(file);
        }
        
        let mut unique_files = Vec::new();
        let mut potential_duplicates = Vec::new();
        
        // Process each size group
        for (_size, group) in size_groups {
            if group.len() == 1 {
                unique_files.extend(group);
                continue;
            }
            
            // Group files by their samples
            let mut sample_groups: HashMap<(Vec<u8>, Vec<u8>), Vec<FileInfo>> = HashMap::new();
            
            for file in group {
                match Self::read_samples(file.path.to_string(), self.sample_size).await {
                    Ok((start, end)) => {
                        sample_groups.entry((start, end))
                            .or_default()
                            .push(file);
                    }
                    Err(e) if e.downcast_ref::<io::Error>().map_or(false, |e| e.kind() == io::ErrorKind::PermissionDenied) => {
                        // Skip files we can't read
                        continue;
                    }
                    Err(e) => return Err(e),
                }
            }
            
            // Process the sample groups
            for (_samples, group) in sample_groups {
                if group.len() > 1 {
                    potential_duplicates.push(group);
                } else {
                    unique_files.extend(group);
                }
            }
        }
        
        if potential_duplicates.is_empty() {
            Ok(ProcessingResult::Skip(unique_files))
        } else {
            Ok(ProcessingResult::Continue(
                potential_duplicates.into_iter().flatten().collect()
            ))
        }
    }
}