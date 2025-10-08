mod config;
pub mod walker;
pub mod pipeline;
pub mod report;
pub mod utils;

#[cfg(test)]
pub mod test_utils;

pub use config::{Config, OperationMode};
pub use walker::{Walker, FileInfo};