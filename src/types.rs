use std::time::SystemTime;
use camino::Utf8PathBuf;
use serde::{Deserialize, Serialize};

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
}