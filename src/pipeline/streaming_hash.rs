use std::sync::Arc;
use std::collections::HashMap;
use tokio::sync::mpsc;
use async_trait::async_trait;
use tracing::{debug, warn, info};
use crate::{
    config::Config,
    types::{FileInfo, PipelineError, DuplicateGroup},
    pipeline::streaming::{StreamingStage, StreamingMessage, ProgressTrackingStage},
    utils::progress::ProgressTracker,
};

/// Streaming hash stage that performs final duplicate grouping
pub struct StreamingHashStage {
    config: Arc<Config>,
    progress_tracker: Option<Arc<ProgressTracker>>,
    processed_count: usize,
    // Group files by size first, then by various hashes
    size_groups: HashMap<u64, Vec<FileInfo>>,
    quick_check_groups: HashMap<u64, Vec<FileInfo>>,
    statistical_groups: HashMap<u64, Vec<FileInfo>>, 
    final_groups: HashMap<String, Vec<FileInfo>>,
}

impl StreamingHashStage {
    pub fn new(config: Arc<Config>) -> Self {
        Self {
            config,
            progress_tracker: None,
            processed_count: 0,
            size_groups: HashMap::new(),
            quick_check_groups: HashMap::new(),
            statistical_groups: HashMap::new(),
            final_groups: HashMap::new(),
        }
    }
    
    pub fn with_progress_tracker(mut self, tracker: Arc<ProgressTracker>) -> Self {
        self.progress_tracker = Some(tracker);
        self
    }
}

#[async_trait]
impl StreamingStage for StreamingHashStage {
    async fn process_stream(
        &mut self,
        mut input: mpsc::Receiver<StreamingMessage>,
        output: mpsc::Sender<StreamingMessage>,
    ) -> Result<(), PipelineError> {
        debug!("StreamingHashStage started");
        
        let progress_bar = self.progress_tracker.as_ref()
            .map(|tracker| self.create_progress_bar(tracker, "Hashing"));
        
        while let Some(message) = input.recv().await {
            match message {
                StreamingMessage::File(file) => {
                    self.process_file(file).await?;
                    self.processed_count += 1;
                }
                StreamingMessage::Batch(files) => {
                    for file in files {
                        self.process_file(file).await?;
                        self.processed_count += 1;
                    }
                }
                StreamingMessage::Flush => {
                    // Process accumulated files and send groups
                    let groups = self.finalize_groups().await?;
                    if !groups.is_empty() {
                        output.send(StreamingMessage::Groups(groups)).await
                            .map_err(|_| PipelineError::ChannelSend)?;
                    }
                    output.send(StreamingMessage::Flush).await
                        .map_err(|_| PipelineError::ChannelSend)?;
                }
                StreamingMessage::Shutdown => {
                    // Final processing and send results
                    let groups = self.finalize_groups().await?;
                    if !groups.is_empty() {
                        output.send(StreamingMessage::Groups(groups)).await
                            .map_err(|_| PipelineError::ChannelSend)?;
                    }
                    output.send(StreamingMessage::Shutdown).await
                        .map_err(|_| PipelineError::ChannelSend)?;
                    break;
                }
                _ => {
                    // Should not receive other message types at this stage
                    warn!("StreamingHashStage received unexpected message type");
                }
            }
            
            if let Some(pb) = &progress_bar {
                pb.set_position(self.processed_count as u64);
                if self.processed_count % 10 == 0 {
                    pb.set_message(format!("Hashed {} files", self.processed_count));
                }
            }
        }
        
        if let Some(pb) = progress_bar {
            pb.finish_with_message(format!("✅ Hashed {} files", self.processed_count));
        }
        
        debug!("StreamingHashStage completed, processed {} files", self.processed_count);
        Ok(())
    }
    
    fn stage_name(&self) -> &'static str {
        "Hash"
    }
    
    fn produces_groups(&self) -> bool {
        true
    }
}

impl ProgressTrackingStage for StreamingHashStage {}

impl StreamingHashStage {
    async fn process_file(&mut self, file: FileInfo) -> Result<(), PipelineError> {
        // First group by size - only files of same size can be duplicates
        self.size_groups.entry(file.size)
            .or_insert_with(Vec::new)
            .push(file);
        
        Ok(())
    }
    
    async fn finalize_groups(&mut self) -> Result<Vec<DuplicateGroup>, PipelineError> {
        let mut duplicate_groups = Vec::new();
        
        info!("Finalizing groups from {} size groups", self.size_groups.len());
        
        let size_groups = std::mem::take(&mut self.size_groups);
        for (_size, files) in size_groups {
            if files.len() < 2 {
                continue; // Single files can't be duplicates
            }
            
            // Group by quick check first (if available)
            let mut quick_groups = HashMap::new();
            for file in files {
                let key = file.quick_check
                    .as_ref()
                    .map(|qc| qc.sample_checksum)
                    .unwrap_or(0);
                
                quick_groups.entry(key)
                    .or_insert_with(Vec::new)
                    .push(file);
            }
            
            // Process each quick check group
            for (_, qc_files) in quick_groups {
                if qc_files.len() < 2 {
                    continue;
                }
                
                // Group by statistical similarity
                let mut stat_groups = HashMap::new();
                for file in qc_files {
                    let key = file.statistical_info
                        .as_ref()
                        .map(|si| si.simhash)
                        .unwrap_or(0);
                    
                    stat_groups.entry(key)
                        .or_insert_with(Vec::new)
                        .push(file);
                }
                
                // Process each statistical group
                for (_, stat_files) in stat_groups {
                    if stat_files.len() < 2 {
                        continue;
                    }
                    
                    // For files that pass all preliminary checks, compute full hash
                    let hash_groups = self.group_by_full_hash(stat_files).await?;
                    
                    for group_files in hash_groups {
                        if group_files.len() >= 2 {
                            let total_size = group_files.iter().map(|f| f.size).sum();
                            let similarity = self.calculate_group_similarity(&group_files);
                            
                            duplicate_groups.push(DuplicateGroup {
                                files: group_files,
                                similarity,
                                total_size,
                            });
                        }
                    }
                }
            }
        }
        
        info!("Found {} duplicate groups", duplicate_groups.len());
        Ok(duplicate_groups)
    }
    
    async fn group_by_full_hash(&self, files: Vec<FileInfo>) -> Result<Vec<Vec<FileInfo>>, PipelineError> {
        let mut hash_groups: HashMap<String, Vec<FileInfo>> = HashMap::new();
        
        for mut file in files {
            // Compute full file hash if not present
            if file.checksum.is_none() {
                match self.compute_file_hash(&file.path).await {
                    Ok(hash) => {
                        file.checksum = Some(hash.clone());
                        hash_groups.entry(hash)
                            .or_insert_with(Vec::new)
                            .push(file);
                    }
                    Err(e) => {
                        warn!("Failed to compute hash for {}: {}", file.path, e);
                        // Still include file in a unique group
                        let unique_key = format!("unique_{}", file.path);
                        hash_groups.entry(unique_key)
                            .or_insert_with(Vec::new)
                            .push(file);
                    }
                }
            } else {
                let hash = file.checksum.clone().unwrap();
                hash_groups.entry(hash)
                    .or_insert_with(Vec::new)
                    .push(file);
            }
        }
        
        Ok(hash_groups.into_values().collect())
    }
    
    async fn compute_file_hash(&self, path: &camino::Utf8Path) -> Result<String, PipelineError> {
        use blake3::Hasher;
        use tokio::io::AsyncReadExt;
        
        let mut file = tokio::fs::File::open(path).await?;
        let mut hasher = Hasher::new();
        let mut buffer = [0u8; 8192];
        
        loop {
            let bytes_read = file.read(&mut buffer).await?;
            if bytes_read == 0 {
                break;
            }
            hasher.update(&buffer[..bytes_read]);
        }
        
        Ok(hasher.finalize().to_hex().to_string())
    }
    
    fn calculate_group_similarity(&self, files: &[FileInfo]) -> f64 {
        if files.len() < 2 {
            return 0.0;
        }
        
        // Calculate similarity based on available information
        let mut total_similarity = 0.0;
        let mut comparisons = 0;
        
        for i in 0..files.len() {
            for j in i + 1..files.len() {
                let mut similarity = 0.0;
                let mut factors = 0;
                
                // Size similarity (should be 100% for duplicates)
                if files[i].size == files[j].size {
                    similarity += 1.0;
                }
                factors += 1;
                
                // Quick check similarity
                if let (Some(qc1), Some(qc2)) = (&files[i].quick_check, &files[j].quick_check) {
                    if qc1.sample_checksum == qc2.sample_checksum {
                        similarity += 1.0;
                    }
                    factors += 1;
                }
                
                // Statistical similarity
                if let (Some(st1), Some(st2)) = (&files[i].statistical_info, &files[j].statistical_info) {
                    let hamming_distance = (st1.simhash ^ st2.simhash).count_ones();
                    let stat_similarity = 1.0 - (hamming_distance as f64 / 64.0);
                    similarity += stat_similarity;
                    factors += 1;
                }
                
                // Hash similarity (should be 100% for duplicates)
                if let (Some(h1), Some(h2)) = (&files[i].checksum, &files[j].checksum) {
                    if h1 == h2 {
                        similarity += 1.0;
                    }
                    factors += 1;
                }
                
                if factors > 0 {
                    total_similarity += similarity / factors as f64;
                    comparisons += 1;
                }
            }
        }
        
        if comparisons > 0 {
            (total_similarity / comparisons as f64) * 100.0
        } else {
            0.0
        }
    }
}