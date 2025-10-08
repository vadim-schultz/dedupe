use std::path::PathBuf;
use config::{Config as ConfigBuilder, ConfigError, Environment, File};
use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("Configuration error: {0}")]
    Config(#[from] ConfigError),
    #[error("Invalid configuration: {0}")]
    Validation(String),
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

    /// Operation mode (report, remove, interactive)
    pub mode: OperationMode,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
#[serde(rename_all = "lowercase")]
pub enum OperationMode {
    Report,
    Remove,
    Interactive,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            max_depth: None,
            min_file_size: 1,
            threads_per_stage: num_cpus::get(),
            quick_check_sample_size: 4096,
            similarity_threshold: 95,
            mode: OperationMode::Report,
        }
    }
}

impl Config {
    pub fn load(config_path: Option<PathBuf>) -> Result<Self, Error> {
        let mut builder = ConfigBuilder::builder();

        // Add defaults
        builder = builder.add_source(config::Config::try_from(&Config::default())?);

        // Add config file if specified
        if let Some(path) = config_path {
            builder = builder.add_source(File::from(path));
        }

        // Add environment variables with prefix DEDUPE_
        builder = builder.add_source(Environment::with_prefix("DEDUPE"));

        let config: Config = builder.build()?.try_deserialize()?;
        config.validate()?;

        Ok(config)
    }

    pub fn validate(&self) -> Result<(), Error> {
        if self.similarity_threshold > 100 {
            return Err(Error::Validation(
                "similarity_threshold must be between 0 and 100".to_string(),
            ));
        }

        if self.threads_per_stage == 0 {
            return Err(Error::Validation(
                "threads_per_stage must be greater than 0".to_string(),
            ));
        }

        if self.quick_check_sample_size == 0 {
            return Err(Error::Validation(
                "quick_check_sample_size must be greater than 0".to_string(),
            ));
        }

        Ok(())
    }
}