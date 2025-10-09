use std::sync::Arc;
use tokio::sync::mpsc;
use async_trait::async_trait;
use tracing::{debug, warn};
use crate::{
    config::Config,
    types::{FileInfo, PipelineError, StatisticalInfo},
    pipeline::streaming::{StreamingStage, StreamingMessage, ProgressTrackingStage},
    utils::progress::ProgressTracker,
};

/// Streaming statistical analysis stage
pub struct StreamingStatisticalStage {
    config: Arc<Config>,
    progress_tracker: Option<Arc<ProgressTracker>>,
    processed_count: usize,
}

impl StreamingStatisticalStage {
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
impl StreamingStage for StreamingStatisticalStage {
    async fn process_stream(
        &mut self,
        mut input: mpsc::Receiver<StreamingMessage>,
        output: mpsc::Sender<StreamingMessage>,
    ) -> Result<(), PipelineError> {
        debug!("StreamingStatisticalStage started");
        
        let progress_bar = self.progress_tracker.as_ref()
            .map(|tracker| self.create_progress_bar(tracker, "Statistical"));
        
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
                if self.processed_count % 25 == 0 {
                    pb.set_message(format!("Analyzed {} files", self.processed_count));
                }
            }
        }
        
        if let Some(pb) = progress_bar {
            pb.finish_with_message(format!("✅ Analyzed {} files", self.processed_count));
        }
        
        debug!("StreamingStatisticalStage completed, processed {} files", self.processed_count);
        Ok(())
    }
    
    fn stage_name(&self) -> &'static str {
        "Statistical"
    }
}

impl ProgressTrackingStage for StreamingStatisticalStage {}

impl StreamingStatisticalStage {
    async fn process_file(&self, mut file: FileInfo) -> Result<Option<FileInfo>, PipelineError> {
        // Skip files that already have statistical info
        if file.statistical_info.is_some() {
            return Ok(Some(file));
        }
        
        // Skip very small files
        if file.size < 256 {
            return Ok(Some(file));
        }
        
        // Read a larger sample for statistical analysis
        match tokio::fs::File::open(&file.path).await {
            Ok(mut fs_file) => {
                use tokio::io::AsyncReadExt;
                
                let sample_size = (file.size.min(64 * 1024)) as usize; // Up to 64KB sample
                let mut buffer = vec![0u8; sample_size];
                
                match fs_file.read(&mut buffer).await {
                    Ok(bytes_read) => {
                        if bytes_read > 0 {
                            buffer.truncate(bytes_read);
                            
                            let entropy = self.calculate_entropy(&buffer);
                            let simhash = self.calculate_simhash(&buffer);
                            let fingerprint = self.calculate_fingerprint(&buffer);
                            
                            file.statistical_info = Some(StatisticalInfo {
                                entropy,
                                simhash,
                                fingerprint,
                            });
                        }
                        
                        Ok(Some(file))
                    }
                    Err(e) => {
                        warn!("Failed to read file for statistical analysis {}: {}", file.path, e);
                        Ok(Some(file))
                    }
                }
            }
            Err(e) => {
                warn!("Failed to open file for statistical analysis {}: {}", file.path, e);
                Ok(Some(file))
            }
        }
    }
    
    fn calculate_entropy(&self, data: &[u8]) -> f64 {
        if data.is_empty() {
            return 0.0;
        }
        
        let mut counts = [0u32; 256];
        for &byte in data {
            counts[byte as usize] += 1;
        }
        
        let len = data.len() as f64;
        let mut entropy = 0.0;
        
        for &count in &counts {
            if count > 0 {
                let probability = count as f64 / len;
                entropy -= probability * probability.log2();
            }
        }
        
        entropy
    }
    
    fn calculate_simhash(&self, data: &[u8]) -> u64 {
        const FEATURES_SIZE: usize = 64;
        let mut features = [0i32; FEATURES_SIZE];
        
        // Create shingles (overlapping n-grams)
        for window in data.windows(4) {
            let mut hash = 0u64;
            for &byte in window {
                hash = hash.wrapping_mul(31).wrapping_add(byte as u64);
            }
            
            // Update feature vector
            for i in 0..FEATURES_SIZE {
                if (hash >> i) & 1 == 1 {
                    features[i] += 1;
                } else {
                    features[i] -= 1;
                }
            }
        }
        
        // Convert to final hash
        let mut simhash = 0u64;
        for i in 0..FEATURES_SIZE {
            if features[i] > 0 {
                simhash |= 1u64 << i;
            }
        }
        
        simhash
    }
    
    fn calculate_fingerprint(&self, data: &[u8]) -> Vec<u8> {
        use blake3::Hasher;
        
        // Create a reduced fingerprint using BLAKE3
        let mut hasher = Hasher::new();
        hasher.update(data);
        
        // Use first 16 bytes of hash as fingerprint
        hasher.finalize().as_bytes()[..16].to_vec()
    }
}