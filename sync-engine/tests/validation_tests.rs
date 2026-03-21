// sync-engine/tests/validation_tests.rs
//
// Tests that validate() catches misconfigurations and produces
// the correct actionable error messages.

use std::collections::HashMap;
use sync_engine::config_value::ConfigValue;
use sync_engine::pipeline_runner::{
    IteratorConfig, MainJobConfig, MainStepConfig, PipelineConfig, PostJobConfig, PostStepConfig,
    PreJobConfig, RetryConfig, SlotDef,
};
use sync_engine::{validate, TypeRegistry};

// ── Helpers ───────────────────────────────────────────────────────────────

fn minimal_cfg(retry_steps: Vec<MainStepConfig>) -> PipelineConfig {
    PipelineConfig {
        job: None,
        resources: HashMap::new(),
        slots: {
            let mut m = HashMap::new();
            m.insert(
                "api_rows".into(),
                SlotDef {
                    record_type: None,
                    scope: sync_engine::pipeline_runner::SlotScopeStr::Window,
                },
            );
            m.insert(
                "db_rows".into(),
                SlotDef {
                    record_type: None,
                    scope: sync_engine::pipeline_runner::SlotScopeStr::Window,
                },
            );
            m
        },
        queues: HashMap::new(),
        pre_job: PreJobConfig {
            init_resources: true,
            steps: vec![],
        },
        main_job: MainJobConfig {
            iterator: IteratorConfig {
                iter_type: "date_window".into(),
                start_interval: ConfigValue::Literal("30".into()),
                end_interval: ConfigValue::Literal("0".into()),
                interval_limit: ConfigValue::Literal("7".into()),
                sleep_secs: ConfigValue::Literal("60".into()),
            },
            retry: RetryConfig {
                max_attempts: 3,
                backoff_secs: 1,
            },
            retry_steps,
            post_window_steps: vec![],
            post_loop_steps: vec![],
        },
        post_job: PostJobConfig {
            steps: vec![PostStepConfig::LogSummary],
        },
    }
}

fn empty_registry() -> TypeRegistry {
    TypeRegistry::new()
}

fn step(type_: &str) -> MainStepConfig {
    MainStepConfig {
        step_type: type_.into(),
        envelope: None,
        transform: None,
        model: None,
        reads: None,
        writes: None,
        append: false,
        secs: None,
        sql: None,
        skip_if_empty: false,
        queue: None,
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────

#[test]
fn valid_empty_pipeline_passes() {
    let cfg = minimal_cfg(vec![]);
    assert!(validate(&cfg, &empty_registry()).is_ok());
}

#[test]
fn fetch_step_missing_envelope_errors() {
    let cfg = minimal_cfg(vec![step("fetch")]);
    let err = validate(&cfg, &empty_registry()).unwrap_err().to_string();
    assert!(err.contains("missing `envelope` field"), "got: {err}");
}

#[test]
fn fetch_step_unregistered_envelope_errors() {
    let mut s = step("fetch");
    s.envelope = Some("UnknownEnvelope".into());
    s.writes = Some("api_rows".into());
    let cfg = minimal_cfg(vec![s]);
    let err = validate(&cfg, &empty_registry()).unwrap_err().to_string();
    assert!(err.contains("UnknownEnvelope"), "got: {err}");
    assert!(err.contains("not registered"), "got: {err}");
    assert!(
        err.contains("main.rs"),
        "should tell user where to fix: {err}"
    );
}

#[test]
fn transform_step_missing_transform_errors() {
    let cfg = minimal_cfg(vec![step("transform")]);
    let err = validate(&cfg, &empty_registry()).unwrap_err().to_string();
    assert!(err.contains("missing `transform` field"), "got: {err}");
}

#[test]
fn tx_upsert_missing_model_errors() {
    let cfg = minimal_cfg(vec![step("tx_upsert")]);
    let err = validate(&cfg, &empty_registry()).unwrap_err().to_string();
    assert!(err.contains("missing `model` field"), "got: {err}");
}

#[test]
fn send_to_queue_undeclared_queue_errors() {
    let mut s = step("send_to_queue");
    s.model = Some("DbUser".into());
    s.reads = Some("db_rows".into());
    s.queue = Some("undeclared_queue".into());
    let cfg = minimal_cfg(vec![s]);
    let err = validate(&cfg, &empty_registry()).unwrap_err().to_string();
    // Either "not registered" (model) or "not declared" (queue)
    assert!(
        err.contains("undeclared_queue") || err.contains("not registered"),
        "got: {err}"
    );
}

#[test]
fn unknown_step_type_errors() {
    let cfg = minimal_cfg(vec![step("magic_step")]);
    let err = validate(&cfg, &empty_registry()).unwrap_err().to_string();
    assert!(err.contains("Unknown step type"), "got: {err}");
    assert!(err.contains("magic_step"), "got: {err}");
}

#[test]
fn step_reads_undeclared_slot_errors() {
    let mut s = step("tx_upsert");
    s.model = Some("DbUser".into());
    s.reads = Some("ghost_slot".into()); // not in [slots]
    let cfg = minimal_cfg(vec![s]);
    let err = validate(&cfg, &empty_registry()).unwrap_err().to_string();
    assert!(err.contains("ghost_slot"), "got: {err}");
}

#[test]
fn multiple_errors_all_reported() {
    let steps = vec![
        {
            let mut s = step("fetch");
            s.envelope = Some("MissingEnv".into());
            s.writes = Some("api_rows".into());
            s
        },
        {
            let s = step("transform");
            s
        }, // missing transform field
        {
            let s = step("tx_upsert");
            s
        }, // missing model field
    ];
    let cfg = minimal_cfg(steps);
    let err = validate(&cfg, &empty_registry()).unwrap_err().to_string();
    // All three errors should appear in a single message
    assert!(err.contains("MissingEnv"), "got: {err}");
    assert!(err.contains("`transform` field"), "got: {err}");
    assert!(err.contains("`model` field"), "got: {err}");
}
