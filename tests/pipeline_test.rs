#[cfg(test)]
mod tests {
    use std::fs::File;
    use std::io::Write;
    use std::sync::Arc;
    use std::time::SystemTime;
    use tempfile::tempdir;
    use anyhow::Result;
    use dedupe::pipeline::{Pipeline, MetadataStage, PipelineStage, ProcessingResult};
    use dedupe::{FileInfo, Config};
    use camino::Utf8PathBuf;

    fn create_test_file(dir: &std::path::Path, name: &str, content: &[u8]) -> Result<FileInfo> {
        let file_path = dir.join(name);
        let mut file = File::create(&file_path)?;
        file.write_all(content)?;
        
        let metadata = file_path.metadata()?;
        let path = Utf8PathBuf::try_from(file_path)?;
        
        Ok(FileInfo {
            path,
            size: metadata.len(),
            file_type: None,
            modified: metadata.modified()?,
            created: metadata.created().ok(),
            readonly: false,
            hidden: false,
            checksum: None,
        })
    }

    #[tokio::test]
    async fn test_metadata_stage() -> Result<()> {
        let temp_dir = tempdir()?;
        
        // Create test files with same size - use longer content to ensure they meet minimum size requirements
        let content1 = b"this is test content for file number 1 with enough bytes";
        let content2 = b"this is test content for file number 2 with enough bytes";
        let file1 = create_test_file(temp_dir.path(), "file1.txt", content1)?;
        let file2 = create_test_file(temp_dir.path(), "file2.txt", content2)?;
        
        // Create file with different size
        let content3 = b"different sized content that is much shorter";
        let file3 = create_test_file(temp_dir.path(), "file3.txt", content3)?;

        // Use config with min file size of 0 to ensure all files are processed
        let config = Arc::new(Config {
            min_file_size: 0,
            ..Config::default()
        });
        let stage = MetadataStage::new(config);
        let files = vec![file1.clone(), file2.clone(), file3.clone()];
        let result = stage.process(files).await?;

        match result {
            ProcessingResult::Duplicates(groups) => {
                // Should have two groups: one with same-size files, one with unique file
                assert_eq!(groups.len(), 2);
                
                // Find the group with 2 files (same-size files)
                let same_size_group = groups.iter()
                    .find(|g| g.len() == 2)
                    .expect("Should have one group with 2 files");
                assert!(same_size_group.iter().any(|f| f.path == file1.path));
                assert!(same_size_group.iter().any(|f| f.path == file2.path));
                
                // Find the group with 1 file (unique file)
                let unique_group = groups.iter()
                    .find(|g| g.len() == 1)
                    .expect("Should have one group with 1 file");
                assert!(unique_group[0].path == file3.path);
            },
            _ => panic!("Expected Duplicates result with groups")
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_pipeline_execution() -> Result<()> {
        let temp_dir = tempdir()?;
        
        // Create test files with same size - use longer content to ensure they meet minimum size requirements
        let content1 = b"this is test content for file number 1a with enough bytes";
        let content2 = b"this is test content for file number 1b with enough bytes";
        let content3 = b"different sized content that is much shorter than the others";
        
        let file1a = create_test_file(temp_dir.path(), "file1a.txt", content1)?;
        let file1b = create_test_file(temp_dir.path(), "file1b.txt", content2)?;
        let file2 = create_test_file(temp_dir.path(), "file2.txt", content3)?;

        let mut pipeline = Pipeline::new();
        // Use config with min file size of 0 to ensure all files are processed
        let config = Arc::new(Config {
            min_file_size: 0,
            ..Config::default()
        });
        pipeline.add_stage(MetadataStage::new(config));

        let files = vec![file1a.clone(), file1b.clone(), file2.clone()];
        let result = pipeline.execute(files).await?;

        // Verify pipeline results - MetadataStage groups by size, so we should get 2 groups
        assert_eq!(result.len(), 2); // Two groups: one for same-size files, one for unique file
        assert!(result.iter().any(|g| g.len() == 1 && g[0].path == file2.path));
        assert!(result.iter().any(|g| {
            g.len() == 2 && 
            g.iter().any(|f| f.path == file1a.path) && 
            g.iter().any(|f| f.path == file1b.path)
        }));

        Ok(())
    }

    #[tokio::test]
    async fn test_empty_directory() -> Result<()> {
        let config = Arc::new(Config::default());
        let stage = MetadataStage::new(config);
        let result = stage.process(vec![]).await?;
        
        match result {
            ProcessingResult::Skip(files) => {
                assert_eq!(files.len(), 0);
            },
            _ => panic!("Expected Skip result for empty input")
        }
        
        Ok(())
    }

    #[tokio::test]
    async fn test_nonexistent_files() -> Result<()> {
        let config = Arc::new(Config::default());
        let stage = MetadataStage::new(config);
        let nonexistent = FileInfo {
            path: Utf8PathBuf::try_from("/nonexistent/file.txt".to_string())?,
            size: 0,
            file_type: None,
            modified: SystemTime::now(),
            created: None,
            readonly: false,
            hidden: false,
            checksum: None,
        };
        let result = stage.process(vec![nonexistent]).await?;
        
        match result {
            ProcessingResult::Skip(files) => {
                assert_eq!(files.len(), 0); // File below min_size gets filtered out
            },
            _ => panic!("Expected Skip result for nonexistent file")
        }
        
        Ok(())
    }
}