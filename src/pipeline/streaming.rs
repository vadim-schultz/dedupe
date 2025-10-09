use std::sync::Arc;
use tokio::sync::mpsc;
use async_trait::async_trait;
use tracing::{debug, info, warn};
use crate::{
    types::{FileInfo, DuplicateGroup, PipelineError},
    utils::progress::ProgressTracker,
};

/// Messages that flow through the streaming pipeline
#[derive(Debug)]
pub enum StreamingMessage {
    File(FileInfo),
    Batch(Vec<FileInfo>),
    Groups(Vec<DuplicateGroup>),
    Flush,
    Shutdown,
}

/// Trait for streaming pipeline stages
#[async_trait]
pub trait StreamingStage: Send + Sync {
    /// Process streaming messages
    async fn process_stream(
        &mut self,
        input: mpsc::Receiver<StreamingMessage>,
        output: mpsc::Sender<StreamingMessage>,
    ) -> Result<(), PipelineError>;
    
    /// Get stage name for logging and progress tracking
    fn stage_name(&self) -> &'static str;
    
    /// Whether this stage groups files (final stage)
    fn produces_groups(&self) -> bool {
        false
    }
}

/// Configuration for the streaming pipeline
#[derive(Clone, Debug)]
pub struct StreamingPipelineConfig {
    pub channel_capacity: usize,
    pub batch_size: usize,
    pub flush_interval: std::time::Duration,
    pub max_concurrent_files: usize,
    pub thread_count: usize,
}

impl Default for StreamingPipelineConfig {
    fn default() -> Self {
        Self {
            channel_capacity: 2000,
            batch_size: 100,
            flush_interval: std::time::Duration::from_millis(50),
            max_concurrent_files: 50000,
            thread_count: num_cpus::get(),
        }
    }
}

impl StreamingPipelineConfig {
    pub fn high_performance() -> Self {
        Self {
            channel_capacity: 10000,
            batch_size: 500,
            flush_interval: std::time::Duration::from_millis(25),
            max_concurrent_files: 100000,
            thread_count: num_cpus::get() * 2,
        }
    }
}

/// Main streaming pipeline that processes files as they're discovered
pub struct StreamingPipeline {
    stages: Vec<Box<dyn StreamingStage>>,
    config: StreamingPipelineConfig,
    progress_tracker: Option<Arc<ProgressTracker>>,
}

impl StreamingPipeline {
    pub fn new(config: StreamingPipelineConfig) -> Self {
        Self {
            stages: Vec::new(),
            config,
            progress_tracker: None,
        }
    }
    
    pub fn set_progress_tracker(&mut self, tracker: Arc<ProgressTracker>) {
        self.progress_tracker = Some(tracker);
    }
    
    pub fn add_stage<S: StreamingStage + 'static>(&mut self, stage: S) {
        self.stages.push(Box::new(stage));
    }
    
    /// Start the streaming pipeline with a file receiver
    pub async fn start_streaming(
        &mut self,
        file_stream: mpsc::Receiver<FileInfo>,
    ) -> Result<Vec<DuplicateGroup>, PipelineError> {
        if self.stages.is_empty() {
            return Err(PipelineError::Configuration("No stages added to pipeline".to_string()));
        }
        
        info!("Starting streaming pipeline with {} stages", self.stages.len());
        
        // Create channels between stages
        let mut stage_channels = Vec::new();
        
        // First stage gets files from input
        let (first_tx, first_rx) = mpsc::channel(self.config.channel_capacity);
        stage_channels.push((first_tx.clone(), Some(first_rx)));
        
        // Create intermediate channels
        for _ in 1..self.stages.len() {
            let (tx, rx) = mpsc::channel(self.config.channel_capacity);
            stage_channels.push((tx, Some(rx)));
        }
        
        // Final stage sends to result collector
        let (result_tx, mut result_rx) = mpsc::channel(self.config.channel_capacity);
        
        // Start all stages
        let mut stage_handles = Vec::new();
        let stages = std::mem::take(&mut self.stages);
        
        for (i, mut stage) in stages.into_iter().enumerate() {
            let input_rx = stage_channels[i].1.take().unwrap();
            let output_tx = if i == stage_channels.len() - 1 {
                result_tx.clone()
            } else {
                stage_channels[i + 1].0.clone()
            };
            
            let stage_name = stage.stage_name();
            let handle = tokio::spawn(async move {
                debug!("Starting stage: {}", stage_name);
                let result = stage.process_stream(input_rx, output_tx).await;
                if let Err(e) = &result {
                    warn!("Stage {} failed: {:?}", stage_name, e);
                }
                result
            });
            
            stage_handles.push(handle);
        }
        
        // File input handler - converts file stream to batched messages
        let first_tx_clone = first_tx.clone();
        let batch_size = self.config.batch_size;
        let flush_interval = self.config.flush_interval;
        
        let input_handle = tokio::spawn(async move {
            let mut file_stream = file_stream;
            let mut batch = Vec::new();
            let mut last_flush = std::time::Instant::now();
            let mut total_files = 0;
            
            while let Some(file) = file_stream.recv().await {
                batch.push(file);
                total_files += 1;
                
                if batch.len() >= batch_size || last_flush.elapsed() >= flush_interval {
                    if !batch.is_empty() {
                        if first_tx_clone.send(StreamingMessage::Batch(std::mem::take(&mut batch))).await.is_err() {
                            break;
                        }
                        last_flush = std::time::Instant::now();
                    }
                }
            }
            
            // Send final batch and shutdown
            if !batch.is_empty() {
                let _ = first_tx_clone.send(StreamingMessage::Batch(batch)).await;
            }
            let _ = first_tx_clone.send(StreamingMessage::Flush).await;
            let _ = first_tx_clone.send(StreamingMessage::Shutdown).await;
            
            debug!("Input handler processed {} files", total_files);
        });
        
        // Result collector
        let result_handle = tokio::spawn(async move {
            let mut all_groups = Vec::new();
            let mut processed_files = 0;
            
            while let Some(message) = result_rx.recv().await {
                match message {
                    StreamingMessage::Groups(mut groups) => {
                        processed_files += groups.iter().map(|g| g.files.len()).sum::<usize>();
                        all_groups.append(&mut groups);
                    }
                    StreamingMessage::Batch(files) => {
                        // Handle case where final stage doesn't group files
                        processed_files += files.len();
                    }
                    StreamingMessage::Shutdown => {
                        debug!("Result collector shutting down, processed {} files", processed_files);
                        break;
                    }
                    _ => {}
                }
            }
            
            all_groups
        });
        
        // Wait for input processing to complete
        input_handle.await.map_err(|e| PipelineError::Runtime(e.to_string()))?;
        
        // Wait for all stages to complete
        for handle in stage_handles {
            handle.await.map_err(|e| PipelineError::Runtime(e.to_string()))??;
        }
        
        // Get final results
        let results = result_handle.await.map_err(|e| PipelineError::Runtime(e.to_string()))?;
        
        info!("Streaming pipeline completed, found {} duplicate groups", results.len());
        Ok(results)
    }
}

/// Helper trait to create progress bars for stages
pub trait ProgressTrackingStage {
    fn create_progress_bar(&self, tracker: &ProgressTracker, stage_name: &str) -> indicatif::ProgressBar {
        tracker.create_progress_bar(0, stage_name)
    }
}

/// Utility for batching operations in streaming stages
pub struct StreamBatcher {
    batch: Vec<FileInfo>,
    batch_size: usize,
    last_flush: std::time::Instant,
    flush_interval: std::time::Duration,
}

impl StreamBatcher {
    pub fn new(batch_size: usize, flush_interval: std::time::Duration) -> Self {
        Self {
            batch: Vec::with_capacity(batch_size),
            batch_size,
            last_flush: std::time::Instant::now(),
            flush_interval,
        }
    }
    
    pub fn add_file(&mut self, file: FileInfo) -> Option<Vec<FileInfo>> {
        self.batch.push(file);
        
        if self.batch.len() >= self.batch_size || self.last_flush.elapsed() >= self.flush_interval {
            self.flush()
        } else {
            None
        }
    }
    
    pub fn flush(&mut self) -> Option<Vec<FileInfo>> {
        if self.batch.is_empty() {
            None
        } else {
            let batch = std::mem::take(&mut self.batch);
            self.last_flush = std::time::Instant::now();
            Some(batch)
        }
    }
}