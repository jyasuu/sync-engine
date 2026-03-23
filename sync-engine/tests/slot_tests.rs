// sync-engine/tests/slot_tests.rs

use sync_engine::slot::{SlotMap, SlotScope};

#[tokio::test]
async fn test_slot_write_and_read() {
    let mut map = SlotMap::new();
    map.declare("count", SlotScope::Job);
    map.write("count", 42usize).await.unwrap();
    let v: usize = map.read("count").await.unwrap();
    assert_eq!(v, 42);
}

#[tokio::test]
async fn test_slot_append() {
    let mut map = SlotMap::new();
    map.declare("items", SlotScope::Job);
    map.append("items", vec![1u32, 2, 3]).await.unwrap();
    map.append("items", vec![4u32, 5]).await.unwrap();
    let v: Vec<u32> = map.read("items").await.unwrap();
    assert_eq!(v, vec![1, 2, 3, 4, 5]);
}

#[tokio::test]
async fn test_window_scope_cleared() {
    let mut map = SlotMap::new();
    map.declare("batch", SlotScope::Window);
    map.declare("total", SlotScope::Job);
    map.write("batch", vec![1u32, 2]).await.unwrap();
    map.write("total", 2usize).await.unwrap();

    // Simulate window reset
    map.clear_scope(SlotScope::Window).await;

    // Window slot should be gone
    assert!(!map.is_set("batch"));
    // Job slot should survive
    assert!(map.is_set("total"));
    let total: usize = map.read("total").await.unwrap();
    assert_eq!(total, 2);
}

#[tokio::test]
async fn test_job_scope_cleared_not_pipeline() {
    let mut map = SlotMap::new();
    map.declare("job_data",      SlotScope::Job);
    map.declare("pipeline_data", SlotScope::Pipeline);
    map.write("job_data",      "job_value").await.unwrap();
    map.write("pipeline_data", "pipeline_value").await.unwrap();

    // Simulate tick reset (clears job, keeps pipeline)
    map.clear_scope(SlotScope::Job).await;

    assert!(!map.is_set("job_data"),       "job slot should be cleared between ticks");
    assert!(map.is_set("pipeline_data"),   "pipeline slot should survive across ticks");
    let v: &str = map.read("pipeline_data").await.unwrap();
    assert_eq!(v, "pipeline_value");
}

#[tokio::test]
async fn test_slot_type_mismatch_error() {
    let mut map = SlotMap::new();
    map.declare("typed", SlotScope::Job);
    map.write("typed", 99u64).await.unwrap();
    // Reading as wrong type should return Err
    let result: Result<String, _> = map.read("typed").await;
    assert!(result.is_err(), "type mismatch should return error");
}

#[tokio::test]
async fn test_undeclared_slot_error() {
    let map = SlotMap::new();
    let result: Result<u32, _> = map.read("ghost").await;
    assert!(result.is_err(), "reading undeclared slot should return error");
}

#[tokio::test]
async fn test_slot_is_set() {
    let mut map = SlotMap::new();
    map.declare("x", SlotScope::Window);
    assert!(!map.is_set("x"), "declared but unwritten slot should not be set");
    map.write("x", true).await.unwrap();
    assert!(map.is_set("x"), "written slot should be set");
}
