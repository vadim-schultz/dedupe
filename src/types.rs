use std::time::SystemTime;
use camino::Utf8PathBuf;
use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileInfo {
    pub path: Utf8PathBuf,
    pub size: u64,
    pub file_type: Option<String>,
    pub modified: SystemTime,
    pub created: Option<SystemTime>,
    pub readonly: bool,
    pub hidden: bool,
    pub checksum: Option<String>,
    pub quick_check: Option<QuickCheckInfo>,
    pub statistical_info: Option<StatisticalInfo>,
    #[serde(skip)]
    pub metadata: Option<std::fs::Metadata>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QuickCheckInfo {
    pub sample_checksum: u64,
    pub sample_size: usize,
    pub samples_taken: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StatisticalInfo {
    pub entropy: f64,
    pub simhash: u64,
    pub fingerprint: Vec<u8>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DuplicateGroup {
    pub files: Vec<FileInfo>,
    pub similarity: f64,
    pub total_size: u64,
}

#[derive(Error, Debug)]
pub enum PipelineError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("Configuration error: {0}")]
    Configuration(String),
    #[error("Runtime error: {0}")]
    Runtime(String),
    #[error("Channel send error")]
    ChannelSend,
    #[error("Channel receive error")]
    ChannelReceive,
}