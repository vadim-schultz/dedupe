use serde::{Deserialize, Serialize};
use thiserror::Error;
use std::path::PathBuf;

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("Failed to read config file: {0}")]
    ReadError(#[from] std::io::Error),
    #[error("Failed to parse config: {0}")]
    ParseError(#[from] serde_yaml::Error),
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Config {
    /// Maximum directory traversal depth
    pub max_depth: Option<usize>,
    
    /// Minimum file size to consider (in bytes)
    pub min_file_size: u64,
    
    /// Number of threads per pipeline stage
    pub threads_per_stage: usize,
    
    /// Size of the sample for quick content check (in bytes)
    pub quick_check_sample_size: usize,
    
    /// Similarity threshold for fuzzy matching (0-100)
    pub similarity_threshold: u8,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            max_depth: None, // No limit
            min_file_size: 1, // 1 byte
            threads_per_stage: num_cpus::get(),
            quick_check_sample_size: 4096, // 4KB
            similarity_threshold: 95, // 95% similarity required
        }
    }
}

impl Config {
    pub fn load(path: PathBuf) -> Result<Self, ConfigError> {
        let contents = std::fs::read_to_string(path)?;
        Ok(serde_yaml::from_str(&contents)?)
    }
}