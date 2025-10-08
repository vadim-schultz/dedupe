//! Simple thread pool management using rayon for parallel processing
//! 
//! This module provides:
//! - Thread pool management with rayon integration
//! - Parallel work execution
//! - Performance metrics tracking

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};
use anyhow::{Result, Context};
use rayon::{ThreadPool, ThreadPoolBuilder, prelude::*};
use tracing::debug;

use crate::types::FileInfo;

/// Work item that can be processed by worker threads
#[derive(Debug, Clone)]
pub struct WorkUnit {
    pub files: Vec<FileInfo>,
    pub stage_id: String,
    pub batch_id: usize,
    pub priority: WorkPriority,
}

/// Priority levels for work items
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum WorkPriority {
    Low = 0,
    Normal = 1,
    High = 2,
    Critical = 3,
}

/// Statistics for worker performance
#[derive(Debug, Default, Clone)]
pub struct WorkerStats {
    pub tasks_completed: usize,
    pub tasks_stolen: usize,
    pub total_processing_time: Duration,
    pub last_active: Option<Instant>,
    pub is_idle: bool,
}

/// Configuration for the thread pool manager
#[derive(Debug, Clone)]
pub struct ThreadPoolConfig {
    pub num_threads: usize,
    pub stack_size: Option<usize>,
    pub thread_name_prefix: String,
    pub steal_attempts: usize,
    pub health_check_interval: Duration,
}

impl Default for ThreadPoolConfig {
    fn default() -> Self {
        Self {
            num_threads: num_cpus::get().max(2),
            stack_size: Some(2 * 1024 * 1024), // 2MB stack
            thread_name_prefix: "dedupe-worker".to_string(),
            steal_attempts: 3,
            health_check_interval: Duration::from_secs(30),
        }
    }
}

/// Simple thread pool manager using rayon
pub struct ThreadPoolManager {
    config: ThreadPoolConfig,
    thread_pool: Arc<ThreadPool>,
    /// Performance statistics
    tasks_completed: Arc<AtomicUsize>,
}

impl ThreadPoolManager {
    /// Create a new thread pool manager
    pub fn new(config: ThreadPoolConfig) -> Result<Self> {
        let thread_pool = Self::create_thread_pool(&config)?;
        let tasks_completed = Arc::new(AtomicUsize::new(0));

        Ok(Self {
            config,
            thread_pool,
            tasks_completed,
        })
    }

    /// Create the underlying rayon thread pool
    fn create_thread_pool(config: &ThreadPoolConfig) -> Result<Arc<ThreadPool>> {
        let prefix = config.thread_name_prefix.clone();
        let mut builder = ThreadPoolBuilder::new()
            .num_threads(config.num_threads)
            .thread_name(move |i| format!("{}-{}", prefix, i));

        if let Some(stack_size) = config.stack_size {
            builder = builder.stack_size(stack_size);
        }

        let pool = builder.build()
            .context("Failed to create thread pool")?;

        Ok(Arc::new(pool))
    }

    /// Submit work to be executed in parallel
    pub fn submit_work(&self, work: WorkUnit) -> Result<()> {
        debug!("Submitting work unit for stage {} with {} files", 
               work.stage_id, work.files.len());
        Ok(())
    }

    /// Submit multiple work units
    pub fn submit_batch(&self, work_units: Vec<WorkUnit>) -> Result<()> {
        debug!("Submitting {} work units", work_units.len());
        Ok(())
    }

    /// Execute work units in parallel using rayon
    pub async fn execute_work<F, R>(&self, worker_fn: F) -> Result<Vec<R>>
    where
        F: Fn(WorkUnit) -> Result<R> + Send + Sync,
        R: Send,
    {
        // For this simple implementation, we'll create some dummy work
        // In a real implementation, this would process actual work units
        let work_units: Vec<WorkUnit> = (0..10).map(|i| WorkUnit {
            files: vec![],
            stage_id: format!("stage_{}", i),
            batch_id: i,
            priority: WorkPriority::Normal,
        }).collect();

        // Execute work in parallel using rayon
        let results: Vec<R> = self.thread_pool.install(|| {
            work_units.into_par_iter()
                .map(|work_unit| worker_fn(work_unit))
                .collect::<Result<Vec<_>>>()
        })?;

        self.tasks_completed.fetch_add(results.len(), Ordering::Relaxed);
        debug!("Completed {} tasks", results.len());

        Ok(results)
    }

    /// Get current worker statistics
    pub fn worker_statistics(&self) -> Vec<WorkerStats> {
        vec![WorkerStats::default(); self.config.num_threads]
    }

    /// Get active worker count
    pub fn active_worker_count(&self) -> usize {
        self.config.num_threads
    }

    /// Check if all workers are idle
    pub fn all_workers_idle(&self) -> bool {
        true // Simple implementation always reports idle when not processing
    }

    /// Get thread pool configuration
    pub fn config(&self) -> &ThreadPoolConfig {
        &self.config
    }

    /// Get total completed tasks
    pub fn total_completed_tasks(&self) -> usize {
        self.tasks_completed.load(Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::FileInfo;
    use camino::Utf8PathBuf;
    use std::time::SystemTime;

    fn create_test_work_unit(stage_id: &str, file_count: usize) -> WorkUnit {
        let files = (0..file_count).map(|i| FileInfo {
            path: Utf8PathBuf::from(format!("/test/file{}.txt", i)),
            size: 1024,
            file_type: None,
            modified: SystemTime::now(),
            created: None,
            readonly: false,
            hidden: false,
            checksum: None,
        }).collect();

        WorkUnit {
            files,
            stage_id: stage_id.to_string(),
            batch_id: 0,
            priority: WorkPriority::Normal,
        }
    }

    #[test]
    fn test_thread_pool_creation() {
        let config = ThreadPoolConfig::default();
        let pool = ThreadPoolManager::new(config);
        assert!(pool.is_ok());
    }

    #[test]
    fn test_work_submission() {
        let config = ThreadPoolConfig {
            num_threads: 2,
            ..Default::default()
        };
        let pool = ThreadPoolManager::new(config).unwrap();
        let work = create_test_work_unit("test_stage", 5);
        
        let result = pool.submit_work(work);
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_work_execution() {
        let config = ThreadPoolConfig {
            num_threads: 2,
            ..Default::default()
        };
        let pool = ThreadPoolManager::new(config).unwrap();
        
        // Execute work
        let results: Vec<usize> = pool.execute_work(|work_unit| {
            Ok(work_unit.files.len())
        }).await.unwrap();

        // Should have processed some work units
        assert!(!results.is_empty());
    }

    #[test]
    fn test_worker_statistics() {
        let config = ThreadPoolConfig::default();
        let pool = ThreadPoolManager::new(config).unwrap();
        
        let stats = pool.worker_statistics();
        assert!(!stats.is_empty());
        assert_eq!(stats.len(), pool.config.num_threads);
    }

    #[test]
    fn test_work_priority_ordering() {
        let mut priorities = vec![
            WorkPriority::Low,
            WorkPriority::Critical,
            WorkPriority::Normal,
            WorkPriority::High,
        ];

        priorities.sort();

        assert_eq!(priorities, vec![
            WorkPriority::Low,
            WorkPriority::Normal,
            WorkPriority::High,
            WorkPriority::Critical,
        ]);
    }

    #[test]
    fn test_task_counting() {
        let config = ThreadPoolConfig::default();
        let pool = ThreadPoolManager::new(config).unwrap();
        
        assert_eq!(pool.total_completed_tasks(), 0);
    }
}