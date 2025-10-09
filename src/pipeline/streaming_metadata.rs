use std::sync::Arc;
use tokio::sync::mpsc;
use async_trait::async_trait;
use tracing::{debug, warn};
use crate::{
    config::Config,
    types::{FileInfo, PipelineError},
    pipeline::streaming::{StreamingStage, StreamingMessage, ProgressTrackingStage},
    utils::progress::ProgressTracker,
};

/// Streaming metadata enrichment stage
pub struct StreamingMetadataStage {
    config: Arc<Config>,
    progress_tracker: Option<Arc<ProgressTracker>>,
    processed_count: usize,
}

impl StreamingMetadataStage {
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
impl StreamingStage for StreamingMetadataStage {
    async fn process_stream(
        &mut self,
        mut input: mpsc::Receiver<StreamingMessage>,
        output: mpsc::Sender<StreamingMessage>,
    ) -> Result<(), PipelineError> {
        debug!("StreamingMetadataStage started");
        
        let progress_bar = self.progress_tracker.as_ref()
            .map(|tracker| self.create_progress_bar(tracker, "Metadata"));
        
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
                if self.processed_count % 100 == 0 {
                    pb.set_message(format!("Processed {} files", self.processed_count));
                }
            }
        }
        
        if let Some(pb) = progress_bar {
            pb.finish_with_message(format!("✅ Processed {} files", self.processed_count));
        }
        
        debug!("StreamingMetadataStage completed, processed {} files", self.processed_count);
        Ok(())
    }
    
    fn stage_name(&self) -> &'static str {
        "Metadata"
    }
}

impl ProgressTrackingStage for StreamingMetadataStage {}

impl StreamingMetadataStage {
    async fn process_file(&self, mut file: FileInfo) -> Result<Option<FileInfo>, PipelineError> {
        // Skip files that are too small
        if file.size < self.config.min_file_size {
            return Ok(None);
        }
        
        // Get file metadata if not already present
        if file.metadata.is_none() {
            match tokio::fs::metadata(&file.path).await {
                Ok(metadata) => {
                    // Update file info with actual metadata
                    file.size = metadata.len();
                    file.modified = metadata.modified().unwrap_or(file.modified);
                    file.created = metadata.created().ok().or(file.created);
                    
                    #[cfg(windows)]
                    {
                        use std::os::windows::fs::MetadataExt;
                        let attrs = metadata.file_attributes();
                        file.readonly = (attrs & 0x1) != 0;
                        file.hidden = (attrs & 0x2) != 0;
                    }
                    
                    #[cfg(unix)]
                    {
                        use std::os::unix::fs::MetadataExt;
                        let mode = metadata.mode();
                        file.readonly = (mode & 0o200) == 0;
                        file.hidden = file.path.file_name()
                            .map(|name| name.starts_with('.'))
                            .unwrap_or(false);
                    }
                    
                    file.metadata = Some(metadata);
                }
                Err(e) => {
                    warn!("Failed to get metadata for {}: {}", file.path, e);
                    return Ok(None);
                }
            }
        }
        
        // Detect file type if not present
        if file.file_type.is_none() {
            if let Some(kind) = infer::get_from_path(&file.path).ok().flatten() {
                file.file_type = Some(kind.mime_type().to_string());
            }
        }
        
        Ok(Some(file))
    }
}