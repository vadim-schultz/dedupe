use std::collections::HashMap;
use std::fs::File;
use std::io::Read;
use anyhow::{Result, Context};
use async_trait::async_trait;
use tokio::task;
use super::{PipelineStage, ProcessingResult};
use crate::walker::FileInfo;

const BLOCK_SIZE: usize = 4096;
const MIN_BLOCKS: usize = 10;

/// Third stage that performs statistical analysis of file contents
#[derive(Debug)]
pub struct StatisticalStage {
    similarity_threshold: u8,
}

impl StatisticalStage {
    pub fn new(similarity_threshold: u8) -> Self {
        Self { similarity_threshold }
    }
    
    async fn calculate_stats(path: String) -> Result<FileStats> {
        task::spawn_blocking(move || {
            let mut file = File::open(&path)
                .with_context(|| format!("Failed to open file: {}", path))?;
                
            let mut buffer = [0u8; BLOCK_SIZE];
            let mut byte_counts = [0u64; 256];
            let mut total_bytes = 0;
            let mut block_count = 0;
            
            loop {
                match file.read(&mut buffer)? {
                    0 => break, // EOF
                    n => {
                        for &byte in &buffer[..n] {
                            byte_counts[byte as usize] += 1;
                        }
                        total_bytes += n;
                        block_count += 1;
                    }
                }
                
                // Read at least MIN_BLOCKS blocks for better stats
                if block_count >= MIN_BLOCKS {
                    break;
                }
            }
            
            Ok(FileStats {
                byte_freq: byte_counts.map(|count| count as f64 / total_bytes as f64),
                entropy: Self::calculate_entropy(&byte_counts, total_bytes),
            })
        }).await?
    }
    
    fn calculate_entropy(counts: &[u64; 256], total: usize) -> f64 {
        let total = total as f64;
        -counts.iter()
            .filter(|&&count| count > 0)
            .map(|&count| {
                let p = count as f64 / total;
                p * p.log2()
            })
            .sum::<f64>()
    }
    
    fn similarity_score(a: &FileStats, b: &FileStats) -> u8 {
        let freq_diff: f64 = a.byte_freq.iter()
            .zip(b.byte_freq.iter())
            .map(|(a, b)| (a - b).abs())
            .sum::<f64>();
            
        let entropy_diff = (a.entropy - b.entropy).abs();
        
        // Convert differences to similarity score (0-100)
        let freq_sim = ((1.0 - freq_diff) * 100.0).min(100.0).max(0.0);
        let entropy_sim = ((1.0 - entropy_diff) * 100.0).min(100.0).max(0.0);
        
        // Weighted average (give more weight to frequency distribution)
        ((freq_sim * 0.7 + entropy_sim * 0.3) as u8).min(100)
    }
}

#[derive(Debug)]
struct FileStats {
    byte_freq: [f64; 256],
    entropy: f64,
}

#[async_trait]
impl PipelineStage for StatisticalStage {
    fn name(&self) -> &'static str {
        "stats"
    }

    async fn process_impl(&self, files: Vec<FileInfo>) -> Result<ProcessingResult> {
        // Group files by size
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
            
            // Calculate stats for each file
            let mut files_with_stats: Vec<(FileInfo, FileStats)> = Vec::new();
            for file in group {
                match Self::calculate_stats(file.path.to_string()).await {
                    Ok(stats) => files_with_stats.push((file, stats)),
                    Err(_) => unique_files.push(file), // Skip files we can't analyze
                }
            }
            
            if files_with_stats.len() <= 1 {
                unique_files.extend(files_with_stats.into_iter().map(|(f, _)| f));
                continue;
            }
            
            // Group similar files
            let mut processed = vec![false; files_with_stats.len()];
            for i in 0..files_with_stats.len() {
                if processed[i] {
                    continue;
                }
                
                let mut similar_group = vec![files_with_stats[i].0.clone()];
                processed[i] = true;
                
                for j in (i + 1)..files_with_stats.len() {
                    if !processed[j] {
                        let similarity = Self::similarity_score(
                            &files_with_stats[i].1,
                            &files_with_stats[j].1,
                        );
                        
                        if similarity >= self.similarity_threshold {
                            similar_group.push(files_with_stats[j].0.clone());
                            processed[j] = true;
                        }
                    }
                }
                
                if similar_group.len() > 1 {
                    duplicate_groups.push(similar_group);
                } else {
                    unique_files.extend(similar_group);
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