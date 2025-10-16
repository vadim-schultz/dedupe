use dedupe::{Config, OperationMode};
use rstest::rstest;

#[rstest]
#[case(95, true)] // Valid threshold
#[case(100, true)] // Valid threshold (edge case)
#[case(0, true)] // Valid threshold (edge case)
#[case(101, false)] // Invalid threshold
fn test_config_validation(#[case] threshold: u8, #[case] is_valid: bool) {
    let config = Config {
        similarity_threshold: threshold,
        ..Config::default()
    };

    let result = config.validate();
    assert_eq!(result.is_ok(), is_valid);
}

#[rstest]
#[case("DEDUPE_MAX_DEPTH", "5")]
#[case("DEDUPE_MIN_FILE_SIZE", "1024")]
#[case("DEDUPE_MODE", "remove")]
fn test_config_from_env(#[case] env_var: &str, #[case] env_value: &str) {
    std::env::set_var(env_var, env_value);
    let config = Config::load(None).unwrap();

    match env_var {
        "DEDUPE_MAX_DEPTH" => assert_eq!(config.max_depth, Some(5)),
        "DEDUPE_MIN_FILE_SIZE" => assert_eq!(config.min_file_size, 1024),
        "DEDUPE_MODE" => assert_eq!(config.mode, OperationMode::Remove),
        _ => panic!("Unexpected test case"),
    }

    std::env::remove_var(env_var);
}
