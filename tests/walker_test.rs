use std::sync::Arc;
use rstest::rstest;
use test_case::test_case;
use std::fs::{self, File};
use std::io::Write;
use tempfile::TempDir;

// Import directly from the crate root
use dedupe::{Walker, Config};

#[rstest]
fn test_walker_skips_small_files() {
    let temp_dir = TempDir::new().unwrap();
    let small_file = temp_dir.path().join("small.txt");
    let large_file = temp_dir.path().join("large.txt");
    
    // Create test files
    File::create(&small_file).unwrap().write_all(b"small").unwrap();
    File::create(&large_file).unwrap().write_all(&vec![0; 1024]).unwrap();
    
    let config = Arc::new(Config {
        min_file_size: 100, // Skip files smaller than 100 bytes
        ..Config::default()
    });
    
    let walker = Walker::new(config);
    let files = walker.walk(temp_dir.path()).unwrap();
    
    assert_eq!(files.len(), 1);
    assert_eq!(files[0].size, 1024);
}

#[test_case("test.txt", true)]
#[test_case(".hidden", false)]
#[test_case("folder/.hidden", false)]
fn test_walker_hidden_files(name: &str, should_include: bool) {
    let temp_dir = TempDir::new().unwrap();
    let file_path = temp_dir.path().join(name);
    
    if let Some(parent) = file_path.parent() {
        fs::create_dir_all(parent).unwrap();
    }
    File::create(&file_path).unwrap().write_all(b"content").unwrap();
    
    let config = Arc::new(Config::default());
    let walker = Walker::new(config);
    let files = walker.walk(temp_dir.path()).unwrap();
    
    assert_eq!(
        files.iter().any(|f| f.path.as_str().ends_with(name)),
        should_include
    );
}

#[rstest]
fn test_walker_respects_max_depth() {
    let temp_dir = TempDir::new().unwrap();
    
    // Create a deep directory structure
    let mut current_dir = temp_dir.path().to_path_buf();
    for i in 1..=5 {
        current_dir.push(format!("level_{}", i));
        fs::create_dir(&current_dir).unwrap();
        let file_path = current_dir.join("file.txt");
        File::create(file_path).unwrap().write_all(b"content").unwrap();
    }
    
    let config = Arc::new(Config {
        max_depth: Some(2),
        ..Config::default()
    });
    
    let walker = Walker::new(config);
    let files = walker.walk(temp_dir.path()).unwrap();
    
    // Should only find files up to depth 2
    assert_eq!(files.len(), 2);
}