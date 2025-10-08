mod config;
pub mod types;
pub mod walker;
pub mod walker_simple;
pub mod pipeline;
pub mod report;
pub mod utils;

#[cfg(test)]
pub mod test_utils;

pub use config::{Config, OperationMode};
pub use types::FileInfo;
pub use walker::Walker;
pub use walker_simple::SimpleWalker;