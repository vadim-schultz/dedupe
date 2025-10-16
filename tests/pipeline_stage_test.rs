use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use anyhow::Result;
use camino::Utf8PathBuf;
use dedupe::pipeline::{
    StreamingHashStage, StreamingMetadataStage, StreamingPipeline, StreamingPipelineConfig,
    StreamingQuickCheckStage, StreamingStatisticalStage,
};
use dedupe::types::FileInfo;
use dedupe::utils::progress::ProgressTracker;
use dedupe::Config;
use indicatif::ProgressDrawTarget;
use tokio::sync::mpsc;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn streaming_pipeline_enriches_all_stages() -> Result<()> {
    let fixture_root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/data/stages");
    let duplicate_files = ["duplicate_one.txt", "duplicate_two.txt"];
    let all_files = ["duplicate_one.txt", "duplicate_two.txt", "unique.txt"];

    let mut base_config = Config::default();
    base_config.min_file_size = 64;
    base_config.quick_check_sample_size = 1024;
    base_config.threads_per_stage = 2;
    base_config.parallel_scan = true;
    let config = Arc::new(base_config);

    let tracker = Arc::new(ProgressTracker::new());
    tracker
        .multi_progress()
        .set_draw_target(ProgressDrawTarget::hidden());

    let mut pipeline = StreamingPipeline::new(StreamingPipelineConfig {
        channel_capacity: 32,
        batch_size: 8,
        flush_interval: Duration::from_millis(5),
        max_concurrent_files: 32,
        thread_count: 2,
    });

    pipeline.set_progress_tracker(tracker.clone());
    pipeline.add_stage(
        StreamingMetadataStage::new(config.clone()).with_progress_tracker(tracker.clone()),
    );
    pipeline.add_stage(
        StreamingQuickCheckStage::new(config.clone()).with_progress_tracker(tracker.clone()),
    );
    pipeline.add_stage(
        StreamingStatisticalStage::new(config.clone()).with_progress_tracker(tracker.clone()),
    );
    pipeline
        .add_stage(StreamingHashStage::new(config.clone()).with_progress_tracker(tracker.clone()));

    let (tx, rx) = mpsc::channel(16);
    let mut streaming_pipeline = pipeline;
    let pipeline_handle = tokio::spawn(async move { streaming_pipeline.start_streaming(rx).await });

    for name in &all_files {
        let path = fixture_root.join(name);
        let metadata = tokio::fs::metadata(&path).await?;
        let utf8_path =
            Utf8PathBuf::from_path_buf(path.clone()).expect("fixture path must be valid UTF-8");

        let info = FileInfo {
            path: utf8_path,
            size: metadata.len(),
            file_type: None,
            modified: SystemTime::UNIX_EPOCH,
            created: None,
            readonly: metadata.permissions().readonly(),
            hidden: false,
            checksum: None,
            quick_check: None,
            statistical_info: None,
            metadata: None,
        };

        tx.send(info).await.expect("pipeline channel send");
    }
    drop(tx);

    let duplicate_groups = pipeline_handle.await??;
    tracker.finish_all();

    assert_eq!(
        duplicate_groups.len(),
        1,
        "expected exactly one duplicate group"
    );
    let group = &duplicate_groups[0];

    let expected: HashSet<&str> = duplicate_files.iter().copied().collect();
    let actual: HashSet<&str> = group
        .files
        .iter()
        .filter_map(|file| file.path.file_name())
        .collect();

    assert!(
        expected.is_subset(&actual),
        "duplicate files should be grouped together"
    );
    assert!(
        actual.len() == duplicate_files.len(),
        "unique file should not appear in duplicate group"
    );

    for file in &group.files {
        assert!(
            file.metadata.is_some(),
            "metadata stage should enrich files"
        );
        assert!(
            file.quick_check.is_some(),
            "quick-check stage should annotate files"
        );
        assert!(
            file.statistical_info.is_some(),
            "statistical stage should annotate files"
        );
        assert!(
            file.checksum.is_some(),
            "hash stage should compute checksums"
        );
    }

    Ok(())
}
