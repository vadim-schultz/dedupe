use std::collections::HashMap;
use std::fs::File;
use std::io::Read;
use anyhow::{Result, Context};
use async_trait::async_trait;
use blake3::Hasher;
use tokio::task;
use super::{PipelineStage, ProcessingResult};
use crate::types::FileInfo;

/// Final stage that performs full file hashing for exact duplicate detection
#[derive(Debug)]
pub struct HashStage {
    chunk_size: usize,
}

impl HashStage {
    pub fn new(chunk_size: Option<usize>) -> Self {
        Self {
            chunk_size: chunk_size.unwrap_or(1024 * 1024), // 1MB default
        }
    }
    
    async fn calculate_hash(path: String, chunk_size: usize) -> Result<String> {
        task::spawn_blocking(move || {
            let mut file = File::open(&path)
                .with_context(|| format!("Failed to open file: {}", path))?;
            let mut hasher = Hasher::new();
            let mut buffer = vec![0; chunk_size];
            
            loop {
                match file.read(&mut buffer)? {
                    0 => break, // EOF
                    n => {
                        hasher.update(&buffer[..n]);
                        continue;
                    }
                }
            }
            
            Ok(hasher.finalize().to_hex().to_string())
        }).await?
    }
}

#[async_trait]
impl PipelineStage for HashStage {
    fn name(&self) -> &'static str {
        "hash"
    }

    async fn process_impl(&self, files: Vec<FileInfo>) -> Result<ProcessingResult> {
        // Group files by size first
        let mut size_groups: HashMap<u64, Vec<FileInfo>> = HashMap::new();
        for file in files {
            size_groups.entry(file.size).or_default().push(file);
        }
        
        let mut unique_files = Vec::new();
        let mut duplicate_groups = Vec::new();
        
        // Process each size group
        for (_size, group) in size_groups {
            if group.len() == 1 {
                unique_files.extend(group);
                continue;
            }
            
            // Calculate hashes for the group
            let mut hash_groups: HashMap<String, Vec<FileInfo>> = HashMap::new();
            
            for file in group {
                match Self::calculate_hash(file.path.to_string(), self.chunk_size).await {
                    Ok(hash) => {
                        hash_groups.entry(hash)
                            .or_default()
                            .push(file);
                    }
                    Err(_) => unique_files.push(file), // Skip files we can't hash
                }
            }
            
            // Collect duplicate groups
            for (_hash, group) in hash_groups {
                if group.len() > 1 {
                    duplicate_groups.push(group);
                } else {
                    unique_files.extend(group);
                }
            }
        }
        
        if duplicate_groups.is_empty() {
            Ok(ProcessingResult::Skip(unique_files))
        } else {
            Ok(ProcessingResult::Duplicates(duplicate_groups))
        }
    }
}