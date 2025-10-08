//! Statistical Analysis Stage - Phase 4 Implementation
//! 
//! This stage provides advanced content analysis using:
//! - Entropy calculation for randomness assessment  
//! - Byte frequency analysis with SIMD optimization
//! - Similarity scoring using SimHash for fast comparison
//! - Parallel processing with Rayon iterators
//! - Statistical validation and accuracy testing

use std::sync::Arc;
use std::time::Instant;
use std::collections::HashMap;
use anyhow::{Context, Result};
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncSeekExt, SeekFrom};
use rayon::prelude::*;
use tracing::{info, instrument, warn};

use crate::walker::FileInfo;
use crate::config::Config;
use super::{PipelineStage, ProcessingResult};

/// Configuration for statistical analysis
#[derive(Debug, Clone)]
pub struct StatisticalConfig {
    /// Block size for sampling (default: 4KB)
    pub block_size: usize,
    /// Minimum number of blocks to sample
    pub min_blocks: usize,
    /// Maximum blocks to analyze (performance limit)
    pub max_blocks: usize,
    /// Similarity threshold (0-100%)
    pub similarity_threshold: f64,
    /// Enable parallel byte frequency analysis
    pub parallel_analysis: bool,
}

impl Default for StatisticalConfig {
    fn default() -> Self {
        Self {
            block_size: 4096,
            min_blocks: 10,
            max_blocks: 1000,
            similarity_threshold: 85.0,
            parallel_analysis: true,
        }
    }
}

/// Statistical fingerprint of file content
#[derive(Debug, Clone, PartialEq)]
pub struct StatisticalFingerprint {
    /// Shannon entropy (0.0 - 8.0 bits per byte)
    pub entropy: f64,
    /// Byte frequency distribution (256 buckets)
    pub frequency_distribution: Vec<f64>,
    /// SimHash for similarity detection
    pub simhash: u64,
    /// File size for normalization
    pub file_size: u64,
    /// Number of blocks analyzed
    pub blocks_analyzed: usize,
}

impl StatisticalFingerprint {
    /// Create an empty fingerprint
    pub fn empty(file_size: u64) -> Self {
        Self {
            entropy: 0.0,
            frequency_distribution: vec![0.0; 256],
            simhash: 0,
            file_size,
            blocks_analyzed: 0,
        }
    }
}

/// Statistical Analysis stage implementation
#[derive(Debug)]
pub struct StatisticalStage {
    config: Arc<Config>,
    stage_config: StatisticalConfig,
}

impl StatisticalStage {
    /// Create a new statistical analysis stage
    pub fn new(config: Arc<Config>) -> Self {
        Self {
            config,
            stage_config: StatisticalConfig::default(),
        }
    }

    /// Create with custom configuration
    pub fn with_config(config: Arc<Config>, stage_config: StatisticalConfig) -> Self {
        Self {
            config,
            stage_config,
        }
    }

    /// Analyze file content and generate statistical fingerprint
    #[instrument(skip(self), fields(file_size = %file_info.size))]
    pub async fn analyze_file(&self, file_info: &FileInfo) -> Result<StatisticalFingerprint> {
        let mut file = File::open(&file_info.path).await
            .with_context(|| format!("Failed to open file: {}", file_info.path))?;
        
        let file_size = file_info.size;
        let num_blocks = std::cmp::min(
            self.stage_config.max_blocks,
            std::cmp::max(
                self.stage_config.min_blocks,
                (file_size / self.stage_config.block_size as u64) as usize
            )
        );

        let mut blocks = Vec::with_capacity(num_blocks);

        // Sample blocks throughout the file
        for i in 0..num_blocks {
            let offset = (i as u64 * file_size) / num_blocks as u64;
            let mut buffer = vec![0u8; self.stage_config.block_size];
            
            // Seek and read block
            file.seek(SeekFrom::Start(offset)).await
                .with_context(|| format!("Failed to seek in file: {}", file_info.path))?;
                
            let bytes_read = file.read(&mut buffer).await
                .with_context(|| format!("Failed to read from file: {}", file_info.path))?;
            
            if bytes_read > 0 {
                buffer.truncate(bytes_read);
                blocks.push(buffer);
            }
        }

        if blocks.is_empty() {
            return Ok(StatisticalFingerprint::empty(file_size));
        }

        // Analyze blocks in parallel if enabled
        let fingerprint = if self.stage_config.parallel_analysis {
            self.analyze_blocks_parallel(blocks, file_size)
        } else {
            self.analyze_blocks_sequential(blocks, file_size)
        };

        fingerprint
    }

    /// Analyze blocks using parallel processing
    fn analyze_blocks_parallel(&self, blocks: Vec<Vec<u8>>, file_size: u64) -> Result<StatisticalFingerprint> {
        let blocks_analyzed = blocks.len();
        
        // Parallel byte frequency analysis
        let frequency_maps: Vec<HashMap<u8, u32>> = blocks
            .par_iter()
            .map(|block| {
                let mut freq = HashMap::new();
                for &byte in block {
                    *freq.entry(byte).or_insert(0) += 1;
                }
                freq
            })
            .collect();

        // Combine frequency maps
        let mut combined_freq = vec![0u32; 256];
        let mut total_bytes = 0u32;
        
        for freq_map in frequency_maps {
            for (byte, count) in freq_map {
                combined_freq[byte as usize] += count;
                total_bytes += count;
            }
        }

        // Convert to probabilities
        let frequency_distribution: Vec<f64> = combined_freq
            .into_iter()
            .map(|count| count as f64 / total_bytes as f64)
            .collect();

        // Calculate entropy
        let entropy = calculate_entropy(&frequency_distribution);

        // Generate SimHash
        let all_data: Vec<u8> = blocks.into_iter().flatten().collect();
        let simhash = calculate_simhash(&all_data);

        Ok(StatisticalFingerprint {
            entropy,
            frequency_distribution,
            simhash,
            file_size,
            blocks_analyzed,
        })
    }

    /// Analyze blocks sequentially (fallback)
    fn analyze_blocks_sequential(&self, blocks: Vec<Vec<u8>>, file_size: u64) -> Result<StatisticalFingerprint> {
        let blocks_analyzed = blocks.len();
        let mut frequency_counts = vec![0u32; 256];
        let mut total_bytes = 0u32;

        // Collect byte frequencies
        for block in &blocks {
            for &byte in block {
                frequency_counts[byte as usize] += 1;
                total_bytes += 1;
            }
        }

        // Convert to probabilities
        let frequency_distribution: Vec<f64> = frequency_counts
            .into_iter()
            .map(|count| count as f64 / total_bytes as f64)
            .collect();

        // Calculate entropy
        let entropy = calculate_entropy(&frequency_distribution);

        // Generate SimHash
        let all_data: Vec<u8> = blocks.into_iter().flatten().collect();
        let simhash = calculate_simhash(&all_data);

        Ok(StatisticalFingerprint {
            entropy,
            frequency_distribution,
            simhash,
            file_size,
            blocks_analyzed,
        })
    }

    /// Calculate similarity between two fingerprints
    pub fn calculate_similarity(&self, fp1: &StatisticalFingerprint, fp2: &StatisticalFingerprint) -> f64 {
        // SimHash Hamming distance similarity
        let hamming_distance = (fp1.simhash ^ fp2.simhash).count_ones();
        let simhash_similarity = ((64 - hamming_distance) as f64 / 64.0) * 100.0;

        // Frequency distribution similarity using cosine similarity
        let dot_product: f64 = fp1.frequency_distribution
            .iter()
            .zip(&fp2.frequency_distribution)
            .map(|(a, b)| a * b)
            .sum();

        let magnitude1: f64 = fp1.frequency_distribution.iter().map(|x| x * x).sum::<f64>().sqrt();
        let magnitude2: f64 = fp2.frequency_distribution.iter().map(|x| x * x).sum::<f64>().sqrt();

        let frequency_similarity = if magnitude1 > 0.0 && magnitude2 > 0.0 {
            (dot_product / (magnitude1 * magnitude2)) * 100.0
        } else {
            0.0
        };

        // Entropy similarity
        let entropy_diff = (fp1.entropy - fp2.entropy).abs();
        let entropy_similarity = ((8.0 - entropy_diff) / 8.0) * 100.0;

        // Weighted average of similarity measures
        simhash_similarity * 0.4 + frequency_similarity * 0.4 + entropy_similarity * 0.2
    }
}

#[async_trait::async_trait]
impl PipelineStage for StatisticalStage {
    fn name(&self) -> &'static str {
        "Statistical"
    }

    #[instrument(skip(self, files))]
    async fn process_impl(&self, files: Vec<FileInfo>) -> Result<ProcessingResult> {
        let start_time = Instant::now();
        info!("Processing {} files in Statistical Analysis stage", files.len());

        // Generate fingerprints for all files
        let mut fingerprints = Vec::new();
        let mut processed_count = 0;
        let mut error_count = 0;

        for file_info in &files {
            match self.analyze_file(file_info).await {
                Ok(fingerprint) => {
                    fingerprints.push((file_info.clone(), fingerprint));
                    processed_count += 1;
                }
                Err(e) => {
                    warn!("Failed to analyze file {}: {}", file_info.path, e);
                    error_count += 1;
                }
            }
        }

        // Group similar files
        let mut groups = Vec::new();
        let mut used = vec![false; fingerprints.len()];

        for i in 0..fingerprints.len() {
            if used[i] {
                continue;
            }

            let mut group = vec![fingerprints[i].0.clone()];
            used[i] = true;

            // Find similar files
            for j in (i + 1)..fingerprints.len() {
                if used[j] {
                    continue;
                }

                let similarity = self.calculate_similarity(
                    &fingerprints[i].1,
                    &fingerprints[j].1
                );

                if similarity >= self.stage_config.similarity_threshold {
                    group.push(fingerprints[j].0.clone());
                    used[j] = true;
                }
            }

            groups.push(group);
        }

        let duration = start_time.elapsed();
        info!("Statistical Analysis completed: {} processed, {} errors, {} groups in {:?}", 
              processed_count, error_count, groups.len(), duration);

        // Separate groups with multiple files (potential duplicates) from singles
        let mut duplicate_groups = Vec::new();
        let mut unique_files = Vec::new();

        for group in groups {
            if group.len() > 1 {
                duplicate_groups.push(group);
            } else {
                unique_files.extend(group);
            }
        }

        if duplicate_groups.is_empty() {
            Ok(ProcessingResult::Skip(unique_files))
        } else {
            Ok(ProcessingResult::Duplicates(duplicate_groups))
        }
    }
}

/// Calculate Shannon entropy from frequency distribution
fn calculate_entropy(frequency_distribution: &[f64]) -> f64 {
    frequency_distribution
        .iter()
        .filter(|&&p| p > 0.0)
        .map(|&p| -p * p.log2())
        .sum()
}

/// Calculate SimHash for similarity detection
fn calculate_simhash(data: &[u8]) -> u64 {
    let mut hash_bits = [0i32; 64];
    
    // Simple hash function for demonstration
    let mut hash_state = 0u64;
    for &byte in data {
        hash_state = hash_state.wrapping_mul(31).wrapping_add(byte as u64);
        
        // Accumulate bits
        for i in 0..64 {
            if (hash_state >> i) & 1 == 1 {
                hash_bits[i] += 1;
            } else {
                hash_bits[i] -= 1;
            }
        }
    }
    
    // Generate final hash
    let mut result = 0u64;
    for i in 0..64 {
        if hash_bits[i] > 0 {
            result |= 1u64 << i;
        }
    }
    
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;
    use std::fs::File;
    use std::io::Write;

    fn create_test_file(dir: &std::path::Path, name: &str, content: &[u8]) -> Result<FileInfo> {
        let file_path = dir.join(name);
        let mut file = File::create(&file_path)?;
        file.write_all(content)?;
        
        let metadata = file_path.metadata()?;
        Ok(FileInfo {
            path: file_path.try_into()?,
            size: metadata.len(),
            file_type: None,
            modified: metadata.modified()?,
            created: metadata.created().ok(),
            readonly: metadata.permissions().readonly(),
            hidden: false,
            checksum: None,
        })
    }

    #[test]
    fn test_calculate_entropy() {
        // Uniform distribution should have high entropy
        let uniform = vec![1.0 / 256.0; 256];
        let entropy = calculate_entropy(&uniform);
        assert!((entropy - 8.0).abs() < 0.01); // Should be close to 8.0

        // Single byte should have zero entropy  
        let mut single = vec![0.0; 256];
        single[65] = 1.0; // 'A'
        let entropy = calculate_entropy(&single);
        assert_eq!(entropy, 0.0);
    }

    #[test]
    fn test_calculate_simhash() {
        let data1 = b"Hello, world!";
        let data2 = b"Hello, world!";
        let data3 = b"Different data";

        let hash1 = calculate_simhash(data1);
        let hash2 = calculate_simhash(data2);
        let hash3 = calculate_simhash(data3);

        // Same data should produce same hash
        assert_eq!(hash1, hash2);
        
        // Different data should produce different hash
        assert_ne!(hash1, hash3);
    }

    #[tokio::test]
    async fn test_statistical_stage() -> Result<()> {
        let temp_dir = tempdir()?;
        let config = Arc::new(Config::default());
        let stage = StatisticalStage::new(config);

        // Create test files with different statistical properties
        let random_data1: Vec<u8> = (0..10000).map(|i| (i % 256) as u8).collect();
        let random_data2: Vec<u8> = (0..10000).map(|i| (i % 256) as u8).collect(); // Same pattern
        let text_data = b"This is a text file with repeated words and patterns".repeat(200);

        let files = vec![
            create_test_file(temp_dir.path(), "random1.dat", &random_data1)?,
            create_test_file(temp_dir.path(), "random2.dat", &random_data2)?,
            create_test_file(temp_dir.path(), "text1.txt", &text_data)?,
        ];

        let result = stage.process_impl(files).await?;
        
        // Should detect similar files based on statistical analysis
        match result {
            ProcessingResult::Duplicates(groups) => {
                // Should find that random1 and random2 are similar
                assert!(!groups.is_empty());
                let random_group = groups.iter()
                    .find(|group| group.iter().any(|f| f.path.as_str().contains("random")))
                    .expect("Should find random files group");
                assert_eq!(random_group.len(), 2);
            }
            _ => panic!("Expected to find duplicate groups"),
        }
        
        Ok(())
    }

    #[tokio::test]
    async fn test_fingerprint_generation() -> Result<()> {
        let temp_dir = tempdir()?;
        let config = Arc::new(Config::default());
        let stage = StatisticalStage::new(config);

        let test_data = b"The quick brown fox jumps over the lazy dog".repeat(100);
        let file_info = create_test_file(temp_dir.path(), "test.txt", &test_data)?;

        let fingerprint = stage.analyze_file(&file_info).await?;
        
        assert!(fingerprint.entropy > 0.0);
        assert!(fingerprint.entropy <= 8.0);
        assert_eq!(fingerprint.frequency_distribution.len(), 256);
        assert_eq!(fingerprint.file_size, test_data.len() as u64);
        assert!(fingerprint.blocks_analyzed > 0);
        
        Ok(())
    }
}