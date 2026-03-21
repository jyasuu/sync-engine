// sync-engine/tests/config_value_tests.rs

use sync_engine::ConfigValue;

#[test]
fn test_literal_resolves_to_itself() {
    let v = ConfigValue::Literal("hello".to_string());
    assert_eq!(v.resolve().unwrap(), "hello");
}

#[test]
fn test_env_resolves_from_environment() {
    std::env::set_var("TEST_ENGINE_VAR", "from_env");
    let v: ConfigValue = toml::from_str(r#"env = "TEST_ENGINE_VAR""#).unwrap();
    assert_eq!(v.resolve().unwrap(), "from_env");
    std::env::remove_var("TEST_ENGINE_VAR");
}

#[test]
fn test_env_missing_with_default() {
    std::env::remove_var("TEST_ENGINE_MISSING");
    let v: ConfigValue = toml::from_str(
        r#"env = "TEST_ENGINE_MISSING"
           default = "fallback""#,
    )
    .unwrap();
    assert_eq!(v.resolve().unwrap(), "fallback");
}

#[test]
fn test_env_missing_no_default_errors() {
    std::env::remove_var("TEST_ENGINE_REQUIRED");
    let v: ConfigValue = toml::from_str(r#"env = "TEST_ENGINE_REQUIRED""#).unwrap();
    assert!(
        v.resolve().is_err(),
        "missing required env var should return error"
    );
    let msg = v.resolve().unwrap_err().to_string();
    assert!(
        msg.contains("TEST_ENGINE_REQUIRED"),
        "error should name the missing var"
    );
}

#[test]
fn test_resolve_as_integer() {
    let v = ConfigValue::Literal("42".to_string());
    let n: i64 = v.resolve_as().unwrap();
    assert_eq!(n, 42);
}

#[test]
fn test_resolve_as_bad_integer_errors() {
    let v = ConfigValue::Literal("not_a_number".to_string());
    let result: Result<i64, _> = v.resolve_as();
    assert!(result.is_err());
}

#[test]
fn test_env_overrides_are_isolated() {
    // Ensure env vars set in one test don't leak to another
    let key = "TEST_ENGINE_ISOLATED";
    std::env::remove_var(key);
    let missing: ConfigValue = toml::from_str(&format!(r#"env = "{key}""#)).unwrap();
    assert!(missing.resolve().is_err());

    std::env::set_var(key, "set");
    let present: ConfigValue = toml::from_str(&format!(r#"env = "{key}""#)).unwrap();
    assert_eq!(present.resolve().unwrap(), "set");

    std::env::remove_var(key);
}
