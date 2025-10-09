use std::sync::Arc;
use tokio::sync::mpsc;
use async_trait::async_trait;
use tracing::{debug, warn};
use crate::{
    config::Config,
    types::{FileInfo, PipelineError, QuickCheckInfo},
    pipeline::streaming::{StreamingStage, StreamingMessage, ProgressTrackingStage},
    utils::progress::ProgressTracker,
};

/// Streaming quick check stage for fast content sampling
pub struct StreamingQuickCheckStage {
    config: Arc<Config>,
    progress_tracker: Option<Arc<ProgressTracker>>,
    processed_count: usize,
}

impl StreamingQuickCheckStage {
    pub fn new(config: Arc<Config>) -> Self {
        Self {
            config,
            progress_tracker: None,
            processed_count: 0,
        }
    }
    
    pub fn with_progress_tracker(mut self, tracker: Arc<ProgressTracker>) -> Self {
        self.progress_tracker = Some(tracker);
        self
    }
}

#[async_trait]
impl StreamingStage for StreamingQuickCheckStage {
    async fn process_stream(
        &mut self,
        mut input: mpsc::Receiver<StreamingMessage>,
        output: mpsc::Sender<StreamingMessage>,
    ) -> Result<(), PipelineError> {
        debug!("StreamingQuickCheckStage started");
        
        let progress_bar = self.progress_tracker.as_ref()
            .map(|tracker| self.create_progress_bar(tracker, "QuickCheck"));
        
        while let Some(message) = input.recv().await {
            match message {
                StreamingMessage::File(file) => {
                    if let Some(processed_file) = self.process_file(file).await? {
                        output.send(StreamingMessage::File(processed_file)).await
                            .map_err(|_| PipelineError::ChannelSend)?;
                    }
                    self.processed_count += 1;
                }
                StreamingMessage::Batch(files) => {
                    let mut processed_files = Vec::new();
                    
                    for file in files {
                        if let Some(processed_file) = self.process_file(file).await? {
                            processed_files.push(processed_file);
                        }
                        self.processed_count += 1;
                    }
                    
                    if !processed_files.is_empty() {
                        output.send(StreamingMessage::Batch(processed_files)).await
                            .map_err(|_| PipelineError::ChannelSend)?;
                    }
                }
                StreamingMessage::Flush => {
                    output.send(StreamingMessage::Flush).await
                        .map_err(|_| PipelineError::ChannelSend)?;
                }
                StreamingMessage::Shutdown => {
                    output.send(StreamingMessage::Shutdown).await
                        .map_err(|_| PipelineError::ChannelSend)?;
                    break;
                }
                _ => {
                    // Pass through other message types
                    output.send(message).await
                        .map_err(|_| PipelineError::ChannelSend)?;
                }
            }
            
            if let Some(pb) = &progress_bar {
                pb.set_position(self.processed_count as u64);
                if self.processed_count % 50 == 0 {
                    pb.set_message(format!("Sampled {} files", self.processed_count));
                }
            }
        }
        
        if let Some(pb) = progress_bar {
            pb.finish_with_message(format!("✅ Quick-checked {} files", self.processed_count));
        }
        
        debug!("StreamingQuickCheckStage completed, processed {} files", self.processed_count);
        Ok(())
    }
    
    fn stage_name(&self) -> &'static str {
        "QuickCheck"
    }
}

impl ProgressTrackingStage for StreamingQuickCheckStage {}

impl StreamingQuickCheckStage {
    async fn process_file(&self, mut file: FileInfo) -> Result<Option<FileInfo>, PipelineError> {
        // Skip files that already have quick check info
        if file.quick_check.is_some() {
            return Ok(Some(file));
        }
        
        // Skip very small files
        if file.size < 64 {
            return Ok(Some(file));
        }
        
        // Perform quick check sampling
        match tokio::fs::File::open(&file.path).await {
            Ok(mut fs_file) => {
                use tokio::io::{AsyncReadExt, AsyncSeekExt};
                
                let file_size = file.size;
                let sample_size = self.config.quick_check_sample_size.min(file_size as usize);
                let mut samples = Vec::new();
                
                // Take samples from beginning, middle, and end
                let positions = if file_size <= sample_size as u64 * 3 {
                    vec![0]
                } else {
                    vec![
                        0,
                        file_size / 2 - (sample_size as u64 / 2),
                        file_size.saturating_sub(sample_size as u64),
                    ]
                };
                
                let mut samples_taken = 0;
                
                for pos in positions {
                    if fs_file.seek(std::io::SeekFrom::Start(pos)).await.is_ok() {
                        let mut buffer = vec![0u8; sample_size];
                        match fs_file.read(&mut buffer).await {
                            Ok(bytes_read) => {
                                if bytes_read > 0 {
                                    buffer.truncate(bytes_read);
                                    samples.extend_from_slice(&buffer);
                                    samples_taken += 1;
                                }
                            }
                            Err(e) => {
                                warn!("Failed to read sample from {}: {}", file.path, e);
                                break;
                            }
                        }
                    }
                }
                
                if !samples.is_empty() {
                    // Calculate simple rolling hash for quick comparison
                    let mut checksum = 0u64;
                    let mut hash = 0u64;
                    const PRIME: u64 = 31;
                    
                    for &byte in &samples {
                        hash = hash.wrapping_mul(PRIME).wrapping_add(byte as u64);
                        checksum = checksum.wrapping_add(hash);
                    }
                    
                    file.quick_check = Some(QuickCheckInfo {
                        sample_checksum: checksum,
                        sample_size: samples.len(),
                        samples_taken,
                    });
                }
                
                Ok(Some(file))
            }
            Err(e) => {
                warn!("Failed to open file for quick check {}: {}", file.path, e);
                // Still pass the file through, just without quick check data
                Ok(Some(file))
            }
        }
    }
}