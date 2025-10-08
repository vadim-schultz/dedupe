//! Criterion benchmarks for multi-threaded pipeline performance
//! 
//! Benchmarks cover:
//! - Single-threaded vs multi-threaded performance
//! - Thread pool scaling
//! - Work stealing efficiency
//! - Memory usage patterns

use std::sync::Arc;
use std::time::Duration;
use criterion::{black_box, criterion_group, criterion_main, Criterion, BenchmarkId, Throughput};
use tempfile::tempdir;
use std::fs::File;
use std::io::Write;
use camino::Utf8PathBuf;

use dedupe::pipeline::{
    Pipeline, ParallelPipeline, ParallelConfig, MetadataStage,
    ThreadPoolManager, ThreadPoolConfig, WorkUnit, WorkPriority
};
use dedupe::{Config, FileInfo};

fn create_benchmark_file(dir: &std::path::Path, name: &str, size: usize) -> FileInfo {
    let content = "x".repeat(size);
    let file_path = dir.join(name);
    let mut file = File::create(&file_path).unwrap();
    file.write_all(content.as_bytes()).unwrap();
    
    let metadata = file_path.metadata().unwrap();
    let path = Utf8PathBuf::try_from(file_path).unwrap();
    
    FileInfo {
        path,
        size: metadata.len(),
        file_type: None,
        modified: metadata.modified().unwrap(),
        created: metadata.created().ok(),
        readonly: false,
        hidden: false,
        checksum: None,
    }
}

fn create_test_dataset(file_count: usize) -> Vec<FileInfo> {
    let temp_dir = tempdir().unwrap();
    let mut files = Vec::new();
    
    for i in 0..file_count {
        let size = if i % 10 == 0 { 10240 } else { 1024 }; // Some larger files
        let file = create_benchmark_file(temp_dir.path(), &format!("bench_file_{}.txt", i), size);
        files.push(file);
    }
    
    // Prevent temp_dir from being dropped
    std::mem::forget(temp_dir);
    files
}

fn bench_sequential_vs_parallel(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("sequential_vs_parallel");
    
    for file_count in [10, 50, 100, 500].iter() {
        let files = create_test_dataset(*file_count);
        group.throughput(Throughput::Elements(*file_count as u64));
        
        // Sequential benchmark
        group.bench_with_input(
            BenchmarkId::new("sequential", file_count),
            file_count,
            |b, _| {
                b.to_async(&rt).iter(|| async {
                    let mut pipeline = Pipeline::new();
                    let config = Arc::new(Config::default());
                    pipeline.add_stage(MetadataStage::new(config));
                    
                    let result = pipeline.execute(black_box(files.clone())).await.unwrap();
                    black_box(result);
                });
            },
        );
        
        // Parallel benchmark with 2 threads
        group.bench_with_input(
            BenchmarkId::new("parallel_2_threads", file_count),
            file_count,
            |b, _| {
                b.to_async(&rt).iter(|| async {
                    let config = ParallelConfig {
                        threads_per_stage: 2,
                        batch_size: 10,
                        ..Default::default()
                    };
                    let mut pipeline = ParallelPipeline::new(config).unwrap();
                    let stage_config = Arc::new(Config::default());
                    pipeline.add_stage(MetadataStage::new(stage_config));
                    
                    let result = pipeline.execute(black_box(files.clone())).await.unwrap();
                    black_box(result);
                });
            },
        );
        
        // Parallel benchmark with 4 threads
        group.bench_with_input(
            BenchmarkId::new("parallel_4_threads", file_count),
            file_count,
            |b, _| {
                b.to_async(&rt).iter(|| async {
                    let config = ParallelConfig {
                        threads_per_stage: 4,
                        batch_size: 10,
                        ..Default::default()
                    };
                    let mut pipeline = ParallelPipeline::new(config).unwrap();
                    let stage_config = Arc::new(Config::default());
                    pipeline.add_stage(MetadataStage::new(stage_config));
                    
                    let result = pipeline.execute(black_box(files.clone())).await.unwrap();
                    black_box(result);
                });
            },
        );
    }
    
    group.finish();
}

fn bench_thread_scaling(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("thread_scaling");
    let files = create_test_dataset(200); // Fixed dataset size
    
    group.throughput(Throughput::Elements(200));
    
    for thread_count in [1, 2, 4, 8].iter() {
        group.bench_with_input(
            BenchmarkId::new("threads", thread_count),
            thread_count,
            |b, &thread_count| {
                b.to_async(&rt).iter(|| async {
                    let config = ParallelConfig {
                        threads_per_stage: thread_count,
                        batch_size: 10,
                        max_concurrent_tasks: thread_count * 2,
                        ..Default::default()
                    };
                    
                    let mut pipeline = ParallelPipeline::new(config).unwrap();
                    let stage_config = Arc::new(Config::default());
                    pipeline.add_stage(MetadataStage::new(stage_config));
                    
                    let result = pipeline.execute(black_box(files.clone())).await.unwrap();
                    black_box(result);
                });
            },
        );
    }
    
    group.finish();
}

fn bench_batch_sizes(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("batch_sizes");
    let files = create_test_dataset(300);
    
    group.throughput(Throughput::Elements(300));
    
    for batch_size in [1, 5, 10, 25, 50, 100].iter() {
        group.bench_with_input(
            BenchmarkId::new("batch_size", batch_size),
            batch_size,
            |b, &batch_size| {
                b.to_async(&rt).iter(|| async {
                    let config = ParallelConfig {
                        threads_per_stage: 4,
                        batch_size,
                        ..Default::default()
                    };
                    
                    let mut pipeline = ParallelPipeline::new(config).unwrap();
                    let stage_config = Arc::new(Config::default());
                    pipeline.add_stage(MetadataStage::new(stage_config));
                    
                    let result = pipeline.execute(black_box(files.clone())).await.unwrap();
                    black_box(result);
                });
            },
        );
    }
    
    group.finish();
}

fn bench_work_stealing(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("work_stealing");
    
    for work_unit_count in [50, 100, 200].iter() {
        group.throughput(Throughput::Elements(*work_unit_count as u64));
        
        group.bench_with_input(
            BenchmarkId::new("steal_attempts_3", work_unit_count),
            work_unit_count,
            |b, &work_unit_count| {
                b.to_async(&rt).iter(|| async {
                    let config = ThreadPoolConfig {
                        num_threads: 4,
                        steal_attempts: 3,
                        ..Default::default()
                    };
                    
                    let pool = ThreadPoolManager::new(config).unwrap();
                    
                    // Create work units with varying sizes
                    let work_units: Vec<WorkUnit> = (0..work_unit_count).map(|i| {
                        let file_count = if i % 10 == 0 { 20 } else { 5 };
                        let files = (0..file_count).map(|j| FileInfo {
                            path: Utf8PathBuf::from(format!("/bench/file{}_{}.txt", i, j)),
                            size: 1024,
                            file_type: None,
                            modified: std::time::SystemTime::now(),
                            created: None,
                            readonly: false,
                            hidden: false,
                            checksum: None,
                        }).collect();
                        
                        WorkUnit {
                            files,
                            stage_id: format!("stage_{}", i % 3),
                            batch_id: i,
                            priority: WorkPriority::Normal,
                        }
                    }).collect();
                    
                    pool.submit_batch(work_units).unwrap();
                    
                    let results: Vec<usize> = pool.execute_work(|work_unit| {
                        Ok(work_unit.files.len())
                    }).await.unwrap();
                    
                    black_box(results);
                });
            },
        );
        
        group.bench_with_input(
            BenchmarkId::new("steal_attempts_10", work_unit_count),
            work_unit_count,
            |b, &work_unit_count| {
                b.to_async(&rt).iter(|| async {
                    let config = ThreadPoolConfig {
                        num_threads: 4,
                        steal_attempts: 10,
                        ..Default::default()
                    };
                    
                    let pool = ThreadPoolManager::new(config).unwrap();
                    
                    // Create work units with varying sizes
                    let work_units: Vec<WorkUnit> = (0..work_unit_count).map(|i| {
                        let file_count = if i % 10 == 0 { 20 } else { 5 };
                        let files = (0..file_count).map(|j| FileInfo {
                            path: Utf8PathBuf::from(format!("/bench/file{}_{}.txt", i, j)),
                            size: 1024,
                            file_type: None,
                            modified: std::time::SystemTime::now(),
                            created: None,
                            readonly: false,
                            hidden: false,
                            checksum: None,
                        }).collect();
                        
                        WorkUnit {
                            files,
                            stage_id: format!("stage_{}", i % 3),
                            batch_id: i,
                            priority: WorkPriority::Normal,
                        }
                    }).collect();
                    
                    pool.submit_batch(work_units).unwrap();
                    
                    let results: Vec<usize> = pool.execute_work(|work_unit| {
                        Ok(work_unit.files.len())
                    }).await.unwrap();
                    
                    black_box(results);
                });
            },
        );
    }
    
    group.finish();
}

fn bench_channel_capacity(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("channel_capacity");
    let files = create_test_dataset(150);
    
    group.throughput(Throughput::Elements(150));
    
    for capacity in [10, 50, 100, 500, 1000].iter() {
        group.bench_with_input(
            BenchmarkId::new("capacity", capacity),
            capacity,
            |b, &capacity| {
                b.to_async(&rt).iter(|| async {
                    let config = ParallelConfig {
                        threads_per_stage: 4,
                        channel_capacity: capacity,
                        batch_size: 10,
                        ..Default::default()
                    };
                    
                    let mut pipeline = ParallelPipeline::new(config).unwrap();
                    let stage_config = Arc::new(Config::default());
                    pipeline.add_stage(MetadataStage::new(stage_config));
                    
                    let result = pipeline.execute(black_box(files.clone())).await.unwrap();
                    black_box(result);
                });
            },
        );
    }
    
    group.finish();
}

criterion_group!(
    benches,
    bench_sequential_vs_parallel,
    bench_thread_scaling,
    bench_batch_sizes,
    bench_work_stealing,
    bench_channel_capacity
);
criterion_main!(benches);