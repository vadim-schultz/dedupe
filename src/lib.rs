mod config;
pub mod types;
pub mod walker;
pub mod walker_simple;
pub mod walker_streaming;
pub mod walker_hp;
pub mod pipeline;
pub mod report;
pub mod utils;

#[cfg(test)]
pub mod test_utils;

#[cfg(test)]
mod tests;

pub use config::{Config, OperationMode};
pub use types::FileInfo;
pub use walker::Walker;
pub use walker_simple::SimpleWalker;
pub use walker_streaming::{StreamingWalker, StreamingConfig};
pub use walker_hp::{HighPerformanceWalker, HighPerformanceConfig};