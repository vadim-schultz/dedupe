//! Integration tests for Phase 4 Additional Stages
//! 
//! Tests cover:
//! - Quick Content Check stage functionality  
//! - Statistical Analysis stage operations
//! - Memory-mapped I/O performance
//! - MIME type detection accuracy
//! - Statistical fingerprint generation
//! - Similarity scoring algorithms
//! - Parallel processing capabilities

use std::sync::Arc;
use std::fs::File;
use std::io::Write;
use anyhow::Result;
use tempfile::tempdir;

use dedupe::{Config, FileInfo};
use dedupe::pipeline::{
    QuickCheckStage, StatisticalStage, PipelineStage,
    quick_check::{QuickCheckConfig, ContentSample, ContentStructure},
    stats::{StatisticalConfig, StatisticalFingerprint},
};

fn create_test_file(dir: &std::path::Path, name: &str, content: &[u8]) -> Result<FileInfo> {
    let file_path = dir.join(name);
    let mut file = File::create(&file_path)?;
    file.write_all(content)?;
    
    let metadata = file_path.metadata()?;
    Ok(FileInfo {
        path: file_path.try_into()?,
        size: metadata.len(),
        file_type: None,
        modified: metadata.modified()?,
        created: metadata.created().ok(),
        readonly: metadata.permissions().readonly(),
        hidden: false,
        checksum: None,
    })
}

#[tokio::test]
async fn test_quick_check_stage_basic() -> Result<()> {
    let temp_dir = tempdir()?;
    let config = Arc::new(Config::default());
    let stage = QuickCheckStage::new(config);

    // Create test files with different content types
    let files = vec![
        create_test_file(temp_dir.path(), "text1.txt", b"Hello, world! This is a text file.")?,
        create_test_file(temp_dir.path(), "text2.txt", b"Hello, world! This is a text file.")?, // Identical
        create_test_file(temp_dir.path(), "binary1.bin", &[0x00, 0x01, 0x02, 0xFF, 0xFE, 0xFD])?,
        create_test_file(temp_dir.path(), "different.txt", b"Completely different content here")?,
    ];

    let result = stage.process_impl(files).await?;
    
    // Should group identical files together
    match result {
        dedupe::pipeline::ProcessingResult::Continue(files) => {
            assert!(!files.is_empty());
        }
        dedupe::pipeline::ProcessingResult::Skip(files) => {
            assert!(!files.is_empty());
        }
        dedupe::pipeline::ProcessingResult::Duplicates(groups) => {
            // Should find the two identical text files
            let text_group = groups.iter()
                .find(|group| group.len() == 2 && 
                      group.iter().all(|f| f.path.as_str().contains("text")))
                .expect("Should find identical text files");
            assert_eq!(text_group.len(), 2);
        }
    }
    
    Ok(())
}

#[tokio::test]
async fn test_quick_check_memory_mapping() -> Result<()> {
    let temp_dir = tempdir()?;
    let config = Arc::new(Config::default());
    
    // Configure for memory mapping large files
    let mut stage_config = QuickCheckConfig::default();
    stage_config.mmap_threshold = 1000; // Low threshold for testing
    stage_config.header_bytes = 100;
    stage_config.footer_bytes = 50;
    
    let stage = QuickCheckStage::with_config(config, stage_config);

    // Create a large file to trigger memory mapping
    let large_content = vec![0xAB; 2000]; // 2KB file
    let large_file = create_test_file(temp_dir.path(), "large.bin", &large_content)?;

    let sample = stage.sample_content(&large_file).await?;
    
    assert_eq!(sample.header.len(), 100);
    assert!(sample.footer.is_some());
    assert_eq!(sample.footer.as_ref().unwrap().len(), 50);
    assert!(sample.structure_hints.is_binary);
    
    Ok(())
}

#[tokio::test]
async fn test_mime_type_detection() -> Result<()> {
    let temp_dir = tempdir()?;
    let config = Arc::new(Config::default());
    let stage = QuickCheckStage::new(config);

    // Test various file types
    let test_cases = vec![
        ("image.jpg", vec![0xFF, 0xD8, 0xFF, 0xE0], Some("image/jpeg")),
        ("image.png", vec![0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A], Some("image/png")),
        ("archive.zip", vec![0x50, 0x4B, 0x03, 0x04], Some("application/zip")),
        ("text.txt", b"Plain text content".to_vec(), Some("text/plain")),
        ("binary.dat", vec![0x00, 0x01, 0x02, 0xFF], None),
    ];

    for (name, content, expected_mime) in test_cases {
        let file_info = create_test_file(temp_dir.path(), name, &content)?;
        let sample = stage.sample_content(&file_info).await?;
        
        assert_eq!(sample.mime_type.as_deref(), expected_mime, 
                   "MIME type detection failed for {}", name);
    }
    
    Ok(())
}

#[tokio::test]
async fn test_content_structure_analysis() -> Result<()> {
    let temp_dir = tempdir()?;
    let config = Arc::new(Config::default());
    let stage = QuickCheckStage::new(config);

    // Test text file
    let text_file = create_test_file(
        temp_dir.path(), 
        "text.txt", 
        b"This is plain text with normal ASCII characters."
    )?;
    let text_sample = stage.sample_content(&text_file).await?;
    assert!(text_sample.structure_hints.is_text);
    assert!(!text_sample.structure_hints.is_binary);

    // Test binary file
    let binary_content = (0..255u8).cycle().take(1000).collect::<Vec<_>>();
    let binary_file = create_test_file(temp_dir.path(), "binary.dat", &binary_content)?;
    let binary_sample = stage.sample_content(&binary_file).await?;
    assert!(binary_sample.structure_hints.is_binary);

    // Test compressed file (gzip header)
    let gzip_header = vec![0x1F, 0x8B, 0x08, 0x00];
    let gzip_file = create_test_file(temp_dir.path(), "file.gz", &gzip_header)?;
    let gzip_sample = stage.sample_content(&gzip_file).await?;
    assert!(gzip_sample.structure_hints.is_compressed);

    // Test executable (PE header)
    let pe_header = vec![0x4D, 0x5A, 0x90, 0x00];
    let exe_file = create_test_file(temp_dir.path(), "program.exe", &pe_header)?;
    let exe_sample = stage.sample_content(&exe_file).await?;
    assert!(exe_sample.structure_hints.is_executable);
    
    Ok(())
}

#[tokio::test]
async fn test_statistical_stage_basic() -> Result<()> {
    let temp_dir = tempdir()?;
    let config = Arc::new(Config::default());
    let stage = StatisticalStage::new(config);

    // Create files with different statistical properties
    let uniform_data: Vec<u8> = (0..1000).map(|i| (i % 255) as u8).collect();
    let skewed_data: Vec<u8> = vec![65u8; 1000]; // All 'A's
    let random_data1 = b"The quick brown fox jumps over the lazy dog".repeat(25);
    let random_data2 = b"The quick brown fox jumps over the lazy dog".repeat(25); // Same

    let files = vec![
        create_test_file(temp_dir.path(), "uniform.dat", &uniform_data)?,
        create_test_file(temp_dir.path(), "skewed.dat", &skewed_data)?,
        create_test_file(temp_dir.path(), "text1.txt", &random_data1)?,
        create_test_file(temp_dir.path(), "text2.txt", &random_data2)?,
    ];

    let result = stage.process_impl(files).await?;
    
    match result {
        dedupe::pipeline::ProcessingResult::Duplicates(groups) => {
            // Should detect that text1 and text2 are identical
            let text_group = groups.iter()
                .find(|group| group.len() == 2 && 
                      group.iter().all(|f| f.path.as_str().contains("text")))
                .expect("Should find identical text files");
            assert_eq!(text_group.len(), 2);
        }
        _ => panic!("Expected to find duplicates"),
    }
    
    Ok(())
}

#[tokio::test]
async fn test_statistical_fingerprint_properties() -> Result<()> {
    let temp_dir = tempdir()?;
    let config = Arc::new(Config::default());
    let stage = StatisticalStage::new(config);

    // Test entropy calculation
    let uniform_data: Vec<u8> = (0..255u8).cycle().take(10000).collect(); // High entropy
    let skewed_data = vec![65u8; 10000]; // Low entropy (all 'A's)

    let uniform_file = create_test_file(temp_dir.path(), "uniform.dat", &uniform_data)?;
    let skewed_file = create_test_file(temp_dir.path(), "skewed.dat", &skewed_data)?;

    let uniform_fp = stage.analyze_file(&uniform_file).await?;
    let skewed_fp = stage.analyze_file(&skewed_file).await?;

    // Uniform distribution should have higher entropy
    assert!(uniform_fp.entropy > skewed_fp.entropy);
    assert!(uniform_fp.entropy > 7.0); // Should be close to 8.0
    assert!(skewed_fp.entropy < 1.0);   // Should be close to 0.0

    // Frequency distributions should differ significantly  
    let uniform_sum: f64 = uniform_fp.frequency_distribution.iter().sum();
    let skewed_sum: f64 = skewed_fp.frequency_distribution.iter().sum();
    assert!((uniform_sum - 1.0).abs() < 0.01); // Should sum to 1.0
    assert!((skewed_sum - 1.0).abs() < 0.01);   // Should sum to 1.0

    Ok(())
}

#[tokio::test]
async fn test_statistical_similarity_calculation() -> Result<()> {
    let temp_dir = tempdir()?;
    let config = Arc::new(Config::default());
    let stage = StatisticalStage::new(config);

    // Create similar files
    let content1 = b"The quick brown fox jumps over the lazy dog".repeat(100);
    let content2 = b"The quick brown fox jumps over the lazy dog".repeat(100); // Identical
    let content3 = b"A completely different text with other words".repeat(100);

    let file1 = create_test_file(temp_dir.path(), "similar1.txt", &content1)?;
    let file2 = create_test_file(temp_dir.path(), "similar2.txt", &content2)?;
    let file3 = create_test_file(temp_dir.path(), "different.txt", &content3)?;

    let fp1 = stage.analyze_file(&file1).await?;
    let fp2 = stage.analyze_file(&file2).await?;
    let fp3 = stage.analyze_file(&file3).await?;

    let similarity_identical = stage.calculate_similarity(&fp1, &fp2);
    let similarity_different = stage.calculate_similarity(&fp1, &fp3);

    // Identical files should have very high similarity
    assert!(similarity_identical > 95.0);
    
    // Different files should have lower similarity
    assert!(similarity_different < similarity_identical);
    
    Ok(())
}

#[tokio::test]
async fn test_parallel_statistical_analysis() -> Result<()> {
    let temp_dir = tempdir()?;
    let config = Arc::new(Config::default());
    
    // Enable parallel analysis
    let mut stage_config = StatisticalConfig::default();
    stage_config.parallel_analysis = true;
    stage_config.block_size = 1024;
    stage_config.min_blocks = 5;
    
    let stage = StatisticalStage::with_config(config, stage_config);

    // Create a reasonably large file
    let large_content = b"Repeated pattern for statistical analysis ".repeat(1000);
    let large_file = create_test_file(temp_dir.path(), "large.txt", &large_content)?;

    let fingerprint = stage.analyze_file(&large_file).await?;
    
    assert!(fingerprint.blocks_analyzed >= 5);
    assert!(fingerprint.entropy > 0.0);
    assert_eq!(fingerprint.frequency_distribution.len(), 256);
    assert_eq!(fingerprint.file_size, large_content.len() as u64);
    
    Ok(())
}

#[tokio::test]
async fn test_stage_error_handling() -> Result<()> {
    let temp_dir = tempdir()?;
    let config = Arc::new(Config::default());
    let quick_stage = QuickCheckStage::new(config.clone());
    let stats_stage = StatisticalStage::new(config);

    // Create a very small file that might cause issues
    let tiny_file = create_test_file(temp_dir.path(), "tiny.txt", b"")?;
    
    // Both stages should handle empty files gracefully
    let quick_sample = quick_stage.sample_content(&tiny_file).await?;
    assert!(quick_sample.header.is_empty());
    
    let stats_fp = stats_stage.analyze_file(&tiny_file).await?;
    assert_eq!(stats_fp.blocks_analyzed, 0);
    assert_eq!(stats_fp.entropy, 0.0);
    
    Ok(())
}

#[tokio::test]
async fn test_integration_quick_check_to_stats() -> Result<()> {
    let temp_dir = tempdir()?;
    let config = Arc::new(Config::default());
    let quick_stage = QuickCheckStage::new(config.clone());
    let stats_stage = StatisticalStage::new(config);

    // Create files that should pass through both stages
    let content1 = b"Document content for analysis ".repeat(50);
    let content2 = b"Document content for analysis ".repeat(50); // Same
    let content3 = b"Different document content here ".repeat(50);
    
    let files = vec![
        create_test_file(temp_dir.path(), "doc1.txt", &content1)?,
        create_test_file(temp_dir.path(), "doc2.txt", &content2)?,
        create_test_file(temp_dir.path(), "doc3.txt", &content3)?,
    ];

    // First pass through Quick Check
    let quick_result = quick_stage.process_impl(files).await?;
    
    let files_for_stats = match quick_result {
        dedupe::pipeline::ProcessingResult::Continue(files) => files,
        dedupe::pipeline::ProcessingResult::Duplicates(groups) => {
            groups.into_iter().flatten().collect()
        }
        dedupe::pipeline::ProcessingResult::Skip(_) => {
            panic!("Quick check should not skip all files");
        }
    };

    // Then pass through Statistical Analysis
    let stats_result = stats_stage.process_impl(files_for_stats).await?;
    
    match stats_result {
        dedupe::pipeline::ProcessingResult::Duplicates(groups) => {
            // Should find the duplicate documents
            let doc_group = groups.iter()
                .find(|group| group.len() == 2)
                .expect("Should find duplicate documents");
            assert_eq!(doc_group.len(), 2);
        }
        _ => panic!("Statistical analysis should find duplicates"),
    }
    
    Ok(())
}