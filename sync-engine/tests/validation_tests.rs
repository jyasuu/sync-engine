// sync-engine/tests/validation_tests.rs
//
// Tests that validate() catches misconfigurations and produces
// the correct actionable error messages.

use std::collections::HashMap;
use sync_engine::{TypeRegistry, validate};
use sync_engine::pipeline_runner::{
    PipelineConfig, PreJobConfig, MainJobConfig, MainStepConfig,
    PostJobConfig, PostStepConfig, IteratorConfig, RetryConfig,
    SlotDef, SplitTransformEntry,
};
use sync_engine::config_value::ConfigValue;

// ── Helpers ───────────────────────────────────────────────────────────────

fn minimal_cfg(retry_steps: Vec<MainStepConfig>) -> PipelineConfig {
    PipelineConfig {
        job:       None,
        init_job:  None,
        resources: HashMap::new(),
        slots: {
            let mut m = HashMap::new();
            m.insert("api_rows".into(), SlotDef {
                record_type: None,
                scope: sync_engine::pipeline_runner::SlotScopeStr::Window,
            });
            m.insert("db_rows".into(), SlotDef {
                record_type: None,
                scope: sync_engine::pipeline_runner::SlotScopeStr::Window,
            });
            m
        },
        queues:  HashMap::new(),
        pre_job: PreJobConfig { init_resources: true, steps: vec![] },
        main_job: MainJobConfig {
            iterator: Some(IteratorConfig {
                iter_type:      "date_window".into(),
                start_interval: ConfigValue::Literal("30".into()),
                end_interval:   ConfigValue::Literal("0".into()),
                interval_limit: ConfigValue::Literal("7".into()),
                sleep_secs:     ConfigValue::Literal("60".into()),
            }),
            retry: Some(RetryConfig { max_attempts: 3, backoff_secs: 1 }),
            steps:             vec![],
            retry_steps,
            post_window_steps: vec![],
            post_loop_steps:   vec![],
        },
        post_job: PostJobConfig { steps: vec![PostStepConfig::LogSummary] },
        step_groups: HashMap::new(),
    }
}

fn cfg_with_extra_slots(retry_steps: Vec<MainStepConfig>, extra: &[&str]) -> PipelineConfig {
    let mut cfg = minimal_cfg(retry_steps);
    for name in extra {
        cfg.slots.insert((*name).into(), SlotDef {
            record_type: None,
            scope: sync_engine::pipeline_runner::SlotScopeStr::Window,
        });
    }
    cfg
}

fn empty_registry() -> TypeRegistry { TypeRegistry::new() }

fn step(type_: &str) -> MainStepConfig {
    MainStepConfig {
        step_type:       type_.into(),
        envelope:        None,
        transform:       None,
        model:           None,
        reads:           None,
        writes:          None,
        append:          false,
        secs:            None,
        sql:             None,
        skip_if_empty:   false,
        queue:           None,
        commit_strategy: None,
        ack_strategy:    None,
        index:           None,
        op:              None,
        scroll_ttl:      None,
        batch_size:      None,
        topic:           None,
        key_field:       None,
        max_messages:    None,
        timeout_ms:      None,
        transforms:      vec![],
        merge_reads:     vec![],
        // wrapper fields
        steps:          vec![],
        start_interval: None,
        end_interval:   None,
        interval_limit: None,
        sleep_secs:     None,
        max_attempts:   None,
        backoff_secs:   None,
        group:          None,
    }
}

// ── Original tests ─────────────────────────────────────────────────────────

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
    s.writes   = Some("api_rows".into());
    let cfg = minimal_cfg(vec![s]);
    let err = validate(&cfg, &empty_registry()).unwrap_err().to_string();
    assert!(err.contains("UnknownEnvelope"), "got: {err}");
    assert!(err.contains("not registered"),  "got: {err}");
    assert!(err.contains("main.rs"),         "should tell user where to fix: {err}");
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
    assert!(err.contains("undeclared_queue") || err.contains("not registered"), "got: {err}");
}

#[test]
fn unknown_step_type_errors() {
    let cfg = minimal_cfg(vec![step("magic_step")]);
    let err = validate(&cfg, &empty_registry()).unwrap_err().to_string();
    assert!(err.contains("Unknown step type"), "got: {err}");
    assert!(err.contains("magic_step"),        "got: {err}");
}

#[test]
fn step_reads_undeclared_slot_errors() {
    let mut s = step("tx_upsert");
    s.model = Some("DbUser".into());
    s.reads = Some("ghost_slot".into());
    let cfg = minimal_cfg(vec![s]);
    let err = validate(&cfg, &empty_registry()).unwrap_err().to_string();
    assert!(err.contains("ghost_slot"), "got: {err}");
}

#[test]
fn multiple_errors_all_reported() {
    let steps = vec![
        { let mut s = step("fetch"); s.envelope = Some("MissingEnv".into()); s.writes = Some("api_rows".into()); s },
        { step("transform") },  // missing transform field
        { step("tx_upsert") },  // missing model field
    ];
    let cfg = minimal_cfg(steps);
    let err = validate(&cfg, &empty_registry()).unwrap_err().to_string();
    assert!(err.contains("MissingEnv"),        "got: {err}");
    assert!(err.contains("`transform` field"), "got: {err}");
    assert!(err.contains("`model` field"),     "got: {err}");
}

// ── New step type validation tests ─────────────────────────────────────────

#[test]
fn split_transform_missing_transforms_array_errors() {
    let s = step("split_transform"); // transforms vec is empty
    let cfg = minimal_cfg(vec![s]);
    let err = validate(&cfg, &empty_registry()).unwrap_err().to_string();
    assert!(err.contains("missing `transforms` array"), "got: {err}");
}

#[test]
fn split_transform_unregistered_transform_errors() {
    let mut s = step("split_transform");
    s.reads = Some("api_rows".into());
    s.transforms = vec![
        SplitTransformEntry { transform: "NoSuchTransform".into(), writes: "db_rows".into(), append: false },
    ];
    let cfg = minimal_cfg(vec![s]);
    let err = validate(&cfg, &empty_registry()).unwrap_err().to_string();
    assert!(err.contains("NoSuchTransform"), "got: {err}");
    assert!(err.contains("not registered"), "got: {err}");
}

#[test]
fn split_transform_undeclared_writes_slot_errors() {
    let mut s = step("split_transform");
    s.reads = Some("api_rows".into());
    s.transforms = vec![
        SplitTransformEntry {
            transform: "UserTransform".into(),
            writes:    "ghost_output".into(),  // not declared in [slots]
            append:    false,
        },
    ];
    let cfg = minimal_cfg(vec![s]);
    let err = validate(&cfg, &empty_registry()).unwrap_err().to_string();
    assert!(err.contains("ghost_output"), "got: {err}");
}

#[test]
fn merge_slots_missing_merge_reads_errors() {
    let mut s = step("merge_slots");
    s.model  = Some("DbUser".into());
    s.writes = Some("db_rows".into());
    // merge_reads is empty — error expected
    let cfg = minimal_cfg(vec![s]);
    let err = validate(&cfg, &empty_registry()).unwrap_err().to_string();
    assert!(err.contains("missing `merge_reads` array"), "got: {err}");
}

#[test]
fn merge_slots_missing_writes_errors() {
    let mut s = step("merge_slots");
    s.model       = Some("DbUser".into());
    s.merge_reads = vec!["api_rows".into()];
    // writes is None — error expected
    let cfg = minimal_cfg(vec![s]);
    let err = validate(&cfg, &empty_registry()).unwrap_err().to_string();
    assert!(err.contains("missing `writes` field"), "got: {err}");
}

#[test]
fn merge_slots_missing_model_errors() {
    let mut s = step("merge_slots");
    s.merge_reads = vec!["api_rows".into()];
    s.writes      = Some("db_rows".into());
    // model is None — error expected
    let cfg = minimal_cfg(vec![s]);
    let err = validate(&cfg, &empty_registry()).unwrap_err().to_string();
    assert!(err.contains("missing `model` field"), "got: {err}");
}

#[test]
fn slot_to_json_missing_fields_errors() {
    let s = step("slot_to_json"); // reads, writes, model all None
    let cfg = minimal_cfg(vec![s]);
    let err = validate(&cfg, &empty_registry()).unwrap_err().to_string();
    assert!(err.contains("missing `reads` field"),  "got: {err}");
    assert!(err.contains("missing `writes` field"), "got: {err}");
    assert!(err.contains("missing `model` field"),  "got: {err}");
}

#[test]
fn json_to_slot_missing_fields_errors() {
    let s = step("json_to_slot"); // reads, writes, model all None
    let cfg = minimal_cfg(vec![s]);
    let err = validate(&cfg, &empty_registry()).unwrap_err().to_string();
    assert!(err.contains("missing `reads` field"),  "got: {err}");
    assert!(err.contains("missing `writes` field"), "got: {err}");
    assert!(err.contains("missing `model` field"),  "got: {err}");
}

#[test]
fn slot_to_json_valid_fields_pass_slot_check() {
    let mut s = step("slot_to_json");
    s.model  = Some("DbUser".into());
    s.reads  = Some("api_rows".into());    // declared in minimal_cfg
    s.writes = Some("db_rows".into());     // declared in minimal_cfg
    let cfg = minimal_cfg(vec![s]);
    // No "ghost slot" or "missing field" errors — only possible model/registry errors
    let err = validate(&cfg, &empty_registry())
        .err().map(|e| e.to_string()).unwrap_or_default();
    assert!(!err.contains("ghost"), "slot check should pass: {err}");
    assert!(!err.contains("missing `reads`"), "reads present: {err}");
    assert!(!err.contains("missing `writes`"), "writes present: {err}");
}

// ── commit_strategy and ack_strategy validation tests ─────────────────────

#[test]
fn commit_strategy_invalid_value_errors() {
    let mut s = step("tx_upsert");
    s.model           = Some("DbUser".into());
    s.reads           = Some("db_rows".into());
    s.commit_strategy = Some("per_row_magic".into());
    let cfg = minimal_cfg(vec![s]);
    let err = validate(&cfg, &empty_registry()).unwrap_err().to_string();
    assert!(err.contains("per_row_magic"), "got: {err}");
    assert!(err.contains("commit_strategy"), "got: {err}");
}

#[test]
fn commit_strategy_valid_values_pass() {
    for strategy in &["tx_scope", "autocommit", "bulk_tx"] {
        let mut s = step("tx_upsert");
        s.model           = Some("DbUser".into());
        s.reads           = Some("db_rows".into());
        s.commit_strategy = Some((*strategy).into());
        let cfg = minimal_cfg(vec![s]);
        let err = validate(&cfg, &empty_registry())
            .err().map(|e| e.to_string()).unwrap_or_default();
        assert!(!err.contains("commit_strategy"), "valid strategy {strategy} should not error: {err}");
    }
}

#[test]
fn commit_strategy_on_wrong_step_type_errors() {
    let mut s = step("fetch");
    s.envelope        = Some("Env".into());
    s.writes          = Some("api_rows".into());
    s.commit_strategy = Some("tx_scope".into());
    let cfg = minimal_cfg(vec![s]);
    let err = validate(&cfg, &empty_registry()).unwrap_err().to_string();
    assert!(err.contains("commit_strategy") && err.contains("tx_upsert"), "got: {err}");
}

#[test]
fn ack_strategy_invalid_value_errors() {
    let mut s = step("send_to_queue");
    s.model        = Some("DbUser".into());
    s.reads        = Some("db_rows".into());
    s.queue        = Some("db_rows".into());
    s.ack_strategy = Some("unknown_ack".into());
    let cfg = cfg_with_extra_slots(vec![s], &[]);
    let err = validate(&cfg, &empty_registry()).unwrap_err().to_string();
    assert!(err.contains("unknown_ack"), "got: {err}");
}

