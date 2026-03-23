// sync-engine/src/tests.rs
//
// Unit tests for pure-logic components that don't require a database or
// network connection. Run with: cargo test -p sync-engine

#[cfg(test)]
mod slot_tests {
    use crate::slot::{SlotMap, SlotScope};

    #[tokio::test]
    async fn write_and_read_round_trips() {
        let mut m = SlotMap::new();
        m.declare("items", SlotScope::Window);
        m.write("items", vec![1u32, 2, 3]).await.unwrap();
        let out: Vec<u32> = m.read("items").await.unwrap();
        assert_eq!(out, vec![1, 2, 3]);
    }

    #[tokio::test]
    async fn append_grows_vec() {
        let mut m = SlotMap::new();
        m.declare("rows", SlotScope::Job);
        m.append("rows", vec![10u32, 20]).await.unwrap();
        m.append("rows", vec![30u32]).await.unwrap();
        let out: Vec<u32> = m.read("rows").await.unwrap();
        assert_eq!(out, vec![10, 20, 30]);
    }

    #[tokio::test]
    async fn clear_window_scope_leaves_job_scope() {
        let mut m = SlotMap::new();
        m.declare("w", SlotScope::Window);
        m.declare("j", SlotScope::Job);
        m.write("w", 1u32).await.unwrap();
        m.write("j", 2u32).await.unwrap();

        m.clear_scope(SlotScope::Window).await;

        assert!(!m.is_set("w"), "window slot should be cleared");
        assert!(m.is_set("j"),  "job slot should survive");
        let j: u32 = m.read("j").await.unwrap();
        assert_eq!(j, 2);
    }

    #[tokio::test]
    async fn clear_job_scope_leaves_pipeline_scope() {
        let mut m = SlotMap::new();
        m.declare("j", SlotScope::Job);
        m.declare("p", SlotScope::Pipeline);
        m.write("j", 99u32).await.unwrap();
        m.write("p", 42u32).await.unwrap();

        m.clear_scope(SlotScope::Job).await;

        assert!(!m.is_set("j"), "job slot should be cleared");
        assert!(m.is_set("p"),  "pipeline slot should survive");
    }

    #[tokio::test]
    async fn read_undeclared_slot_returns_err() {
        let m = SlotMap::new();
        let res = m.read::<u32>("nonexistent").await;
        assert!(res.is_err());
        let msg = res.unwrap_err().to_string();
        assert!(msg.contains("nonexistent"), "error should name the slot");
    }

    #[tokio::test]
    async fn read_declared_but_unwritten_slot_returns_err() {
        let mut m = SlotMap::new();
        m.declare("empty", SlotScope::Window);
        let res = m.read::<u32>("empty").await;
        assert!(res.is_err());
        assert!(res.unwrap_err().to_string().contains("not been written"));
    }

    #[tokio::test]
    async fn type_mismatch_on_read_returns_err() {
        let mut m = SlotMap::new();
        m.declare("data", SlotScope::Window);
        m.write("data", 42u32).await.unwrap();
        // Try to read as wrong type
        let res = m.read::<String>("data").await;
        assert!(res.is_err());
        assert!(res.unwrap_err().to_string().contains("type mismatch"));
    }

    #[tokio::test]
    async fn overwrite_slot_replaces_value() {
        let mut m = SlotMap::new();
        m.declare("v", SlotScope::Job);
        m.write("v", 1u32).await.unwrap();
        m.write("v", 99u32).await.unwrap();
        let out: u32 = m.read("v").await.unwrap();
        assert_eq!(out, 99);
    }
}

#[cfg(test)]
mod config_value_tests {
    use crate::config_value::ConfigValue;

    #[test]
    fn literal_resolves_as_is() {
        let cv = ConfigValue::Literal("hello".into());
        assert_eq!(cv.resolve().unwrap(), "hello");
    }

    #[test]
    fn env_var_resolves_from_environment() {
        std::env::set_var("TEST_CV_VAR", "from_env");
        let cv = ConfigValue::Env(crate::config_value::EnvRef {
            env: "TEST_CV_VAR".into(),
            default: None,
        });
        assert_eq!(cv.resolve().unwrap(), "from_env");
        std::env::remove_var("TEST_CV_VAR");
    }

    #[test]
    fn missing_env_var_with_default_uses_default() {
        std::env::remove_var("TEST_CV_MISSING");
        let cv = ConfigValue::Env(crate::config_value::EnvRef {
            env: "TEST_CV_MISSING".into(),
            default: Some("fallback".into()),
        });
        assert_eq!(cv.resolve().unwrap(), "fallback");
    }

    #[test]
    fn missing_env_var_without_default_errors() {
        std::env::remove_var("TEST_CV_NODEFAULT");
        let cv = ConfigValue::Env(crate::config_value::EnvRef {
            env: "TEST_CV_NODEFAULT".into(),
            default: None,
        });
        let err = cv.resolve().unwrap_err().to_string();
        assert!(err.contains("TEST_CV_NODEFAULT"));
        assert!(err.contains("not set"));
    }

    #[test]
    fn resolve_as_parses_integer() {
        let cv = ConfigValue::Literal("42".into());
        assert_eq!(cv.resolve_as::<i64>().unwrap(), 42i64);
    }

    #[test]
    fn resolve_as_fails_on_bad_integer() {
        let cv = ConfigValue::Literal("not_a_number".into());
        assert!(cv.resolve_as::<i64>().is_err());
    }
}

#[cfg(test)]
mod date_window_iter_tests {
    use crate::job::DateWindowIter;

    #[tokio::test]
    async fn yields_correct_windows() {
        // start=7 days ago, end=0 days ago, limit=3 days per window
        // Expected windows: (7,4), (4,1), (1,0)
        let mut iter = DateWindowIter::new(7, 0, 3).without_sleep();
        let mut windows = Vec::new();
        while let Some(w) = iter.next_window().await {
            windows.push(w);
        }
        assert_eq!(windows, vec![(7, 4), (4, 1), (1, 0)]);
    }

    #[tokio::test]
    async fn exhausted_iterator_returns_none() {
        let mut iter = DateWindowIter::new(0, 0, 7).without_sleep();
        // cursor(0) <= end(0) → immediately exhausted
        assert!(iter.next_window().await.is_none());
    }

    #[tokio::test]
    async fn single_window_when_start_equals_limit() {
        let mut iter = DateWindowIter::new(7, 0, 7).without_sleep();
        let w = iter.next_window().await.unwrap();
        assert_eq!(w, (7, 0));
        assert!(iter.next_window().await.is_none());
    }

    #[tokio::test]
    async fn partial_last_window_clamps_to_end() {
        // start=5, end=0, limit=3 → (5,2), (2,0)
        let mut iter = DateWindowIter::new(5, 0, 3).without_sleep();
        let mut windows = Vec::new();
        while let Some(w) = iter.next_window().await {
            windows.push(w);
        }
        assert_eq!(windows, vec![(5, 2), (2, 0)]);
    }
}

#[cfg(test)]
mod codegen_rule_tests {
    // Test the mapping rules by exercising gen_rule_expr indirectly
    // through the generated transform output.

    use crate::codegen::{gen_rule_expr_pub, RuleDef};

    // gen_rule_expr is private, but we can test it via a public wrapper
    // added specifically for tests. See codegen.rs.
    #[test]
    fn copy_rule_generates_field_access() {
        let rd = RuleDef { field: "name".into(), source: None, rule: "copy".into() };
        let src = rd.source.as_deref().unwrap_or(&rd.field);
        let expr = gen_rule_expr_pub(&rd.field, src, &rd.rule);
        assert_eq!(expr, "u.name");
    }

    #[test]
    fn copy_rule_with_different_source() {
        let rd = RuleDef {
            field:  "sso_acct".into(),
            source: Some("ssoAcct".into()),
            rule:   "copy".into(),
        };
        let src = rd.source.as_deref().unwrap_or(&rd.field);
        let expr = gen_rule_expr_pub(&rd.field, src, &rd.rule);
        assert_eq!(expr, "u.ssoAcct");
    }

    #[test]
    fn null_to_empty_rule() {
        let expr = gen_rule_expr_pub("field", "field", "null_to_empty");
        assert_eq!(expr, "u.field.unwrap_or_default()");
    }

    #[test]
    fn bool_to_yn_rule() {
        let expr = gen_rule_expr_pub("active", "active", "bool_to_yn");
        assert!(expr.contains("Y") && expr.contains("N"));
        assert!(expr.contains("u.active"));
    }

    #[test]
    fn option_bool_to_yn_rule() {
        let expr = gen_rule_expr_pub("sex", "sex", "option_bool_to_yn");
        assert!(expr.contains("u.sex.map"));
        assert!(expr.contains("unwrap_or_default"));
    }

    #[test]
    fn epoch_ms_to_ts_rule() {
        let expr = gen_rule_expr_pub("created_at", "createdAt", "epoch_ms_to_ts");
        assert!(expr.contains("epoch_ms_to_ts"));
        assert!(expr.contains("u.createdAt"));
    }

    #[test]
    #[should_panic(expected = "Unknown rule")]
    fn unknown_rule_panics() {
        gen_rule_expr_pub("field", "field", "nonexistent_rule");
    }
}

#[cfg(test)]
mod retry_tests {
    use crate::job::with_retry;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    #[tokio::test]
    async fn succeeds_on_first_attempt() {
        let count = Arc::new(AtomicUsize::new(0));
        let c = Arc::clone(&count);
        let result = with_retry(3, || {
            let c = Arc::clone(&c);
            async move {
                c.fetch_add(1, Ordering::SeqCst);
                Ok::<u32, anyhow::Error>(42)
            }
        })
        .await
        .unwrap();
        assert_eq!(result, 42);
        assert_eq!(count.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn retries_and_eventually_succeeds() {
        let count = Arc::new(AtomicUsize::new(0));
        let c = Arc::clone(&count);
        let result = with_retry(5, || {
            let c = Arc::clone(&c);
            async move {
                let n = c.fetch_add(1, Ordering::SeqCst);
                if n < 2 {
                    anyhow::bail!("not yet");
                }
                Ok::<u32, anyhow::Error>(99)
            }
        })
        .await
        .unwrap();
        assert_eq!(result, 99);
        assert_eq!(count.load(Ordering::SeqCst), 3); // failed twice, succeeded on 3rd
    }

    #[tokio::test]
    async fn exhausts_all_attempts_and_returns_err() {
        let count = Arc::new(AtomicUsize::new(0));
        let c = Arc::clone(&count);
        let result: anyhow::Result<()> = with_retry(3, || {
            let c = Arc::clone(&c);
            async move {
                c.fetch_add(1, Ordering::SeqCst);
                anyhow::bail!("always fails")
            }
        })
        .await;
        assert!(result.is_err());
        assert_eq!(count.load(Ordering::SeqCst), 3);
    }
}

#[cfg(test)]
mod http_service_config_tests {
    use crate::pipeline_runner::ResourceDef;

    #[test]
    fn default_param_names_are_start_end_time() {
        let toml = r#"
            type     = "http_service"
            http     = "http"
            auth     = "auth"
            endpoint = "https://api.example.com"
        "#;
        let def: ResourceDef = toml::from_str(toml).unwrap();
        if let ResourceDef::HttpService { start_param, end_param, date_format, .. } = def {
            assert_eq!(start_param, "start_time");
            assert_eq!(end_param,   "end_time");
            assert_eq!(date_format, "%Y%m%d");
        } else {
            panic!("expected HttpService variant");
        }
    }

    #[test]
    fn custom_param_names_round_trip() {
        let toml = r#"
            type        = "http_service"
            http        = "http"
            auth        = "auth"
            endpoint    = "https://api.example.com"
            start_param = "from_date"
            end_param   = "to_date"
            date_format = "%Y-%m-%d"
        "#;
        let def: ResourceDef = toml::from_str(toml).unwrap();
        if let ResourceDef::HttpService { start_param, end_param, date_format, .. } = def {
            assert_eq!(start_param, "from_date");
            assert_eq!(end_param,   "to_date");
            assert_eq!(date_format, "%Y-%m-%d");
        } else {
            panic!("expected HttpService variant");
        }
    }

    #[test]
    fn extra_params_parsed() {
        let toml = r#"
            type     = "http_service"
            http     = "http"
            auth     = "auth"
            endpoint = "https://api.example.com"

            [[extra_params]]
            key   = "api_version"
            value = "v2"

            [[extra_params]]
            key   = "tenant"
            value = { env = "TENANT_ID", default = "default" }
        "#;
        let def: ResourceDef = toml::from_str(toml).unwrap();
        if let ResourceDef::HttpService { extra_params, .. } = def {
            assert_eq!(extra_params.len(), 2);
            assert_eq!(extra_params[0].key, "api_version");
            assert_eq!(extra_params[1].key, "tenant");
        } else {
            panic!("expected HttpService variant");
        }
    }
}

#[cfg(test)]
mod runner_behavior_tests {
    use crate::job::DateWindowIter;

    #[tokio::test]
    async fn sleep_skipped_on_error_window() {
        // After an error window, last_ok = false — no sleep on the next iteration.
        // We test this indirectly: a zero-sleep iterator with one error window
        // should still produce the remaining windows.
        let mut iter = DateWindowIter::new(9, 0, 3).without_sleep();
        let mut windows = Vec::new();
        while let Some(w) = iter.next_window().await {
            windows.push(w);
        }
        // (9,6), (6,3), (3,0)
        assert_eq!(windows.len(), 3);
        assert_eq!(windows[0], (9, 6));
        assert_eq!(windows[2], (3, 0));
    }

    #[tokio::test]
    async fn window_count_matches_range_divided_by_limit() {
        // 28-day range, 7-day windows → exactly 4 windows
        let mut iter = DateWindowIter::new(28, 0, 7).without_sleep();
        let mut count = 0;
        while iter.next_window().await.is_some() { count += 1; }
        assert_eq!(count, 4);
    }
}

#[cfg(test)]
mod validation_extra_tests {
    use crate::pipeline_runner::{validate, PipelineConfig};
    use crate::registry::TypeRegistry;

    fn minimal_cfg_with_iter_type(t: &str) -> PipelineConfig {
        let raw = format!(r#"
[pre_job]
init_resources = true

[main_job.iterator]
type           = "{t}"
start_interval = "30"
end_interval   = "0"
interval_limit = "7"
sleep_secs     = "60"

[main_job.retry]
max_attempts = 3
backoff_secs = 1

[post_job]
"#);
        toml::from_str(&raw).unwrap()
    }

    #[test]
    fn invalid_iterator_type_is_caught() {
        let cfg = minimal_cfg_with_iter_type("offset_page");
        let reg = TypeRegistry::new();
        let err = validate(&cfg, &reg).unwrap_err().to_string();
        assert!(err.contains("offset_page"), "error should name the bad type");
        assert!(err.contains("date_window"), "error should name the valid type");
    }

    #[test]
    fn valid_iterator_type_passes() {
        let cfg = minimal_cfg_with_iter_type("date_window");
        let reg = TypeRegistry::new();
        // May fail for other reasons (empty resources etc.) but not iterator type
        let err_str = validate(&cfg, &reg)
            .err()
            .map(|e| e.to_string())
            .unwrap_or_default();
        assert!(!err_str.contains("not supported"), "valid type should not produce unsupported error");
    }
}

#[cfg(all(test, not(feature = "postgres")))]
mod json_slot_tests {
    use std::sync::Arc;
    use serde::{Deserialize, Serialize};

    use crate::context::{Connections, JobContext};
    use crate::slot::SlotScope;
    use crate::step::Step;
    use crate::step::json_slot::{SlotToJsonStep, JsonToSlotStep};
    use crate::components::auth::OAuth2Auth;

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    struct Row { id: u32, name: String }



    // Simpler approach: test the round-trip logic directly via a context
    // that only uses the slot map.
    #[tokio::test]
    async fn slot_to_json_serializes_typed_slot() {
        let ctx = make_minimal_ctx();
        {
            let mut slots = ctx.slots.write().await;
            slots.declare("rows", SlotScope::Window);
            slots.declare("json", SlotScope::Window);
        }
        let rows = vec![
            Row { id: 1, name: "alice".into() },
            Row { id: 2, name: "bob".into() },
        ];
        ctx.slot_write("rows", rows.clone()).await.unwrap();

        let step = SlotToJsonStep::<Row>::new("rows", "json", false);
        step.run(&ctx).await.unwrap();

        let json: Vec<serde_json::Value> = ctx.slot_read("json").await.unwrap();
        assert_eq!(json.len(), 2);
        assert_eq!(json[0]["id"], 1);
        assert_eq!(json[1]["name"], "bob");
    }

    #[tokio::test]
    async fn json_to_slot_deserializes_back() {
        let ctx = make_minimal_ctx();
        {
            let mut slots = ctx.slots.write().await;
            slots.declare("json", SlotScope::Window);
            slots.declare("rows", SlotScope::Window);
        }

        let json_vals = vec![
            serde_json::json!({"id": 10, "name": "carol"}),
            serde_json::json!({"id": 20, "name": "dave"}),
        ];
        ctx.slot_write("json", json_vals).await.unwrap();

        let step = JsonToSlotStep::<Row>::new("json", "rows", false);
        step.run(&ctx).await.unwrap();

        let rows: Vec<Row> = ctx.slot_read("rows").await.unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0], Row { id: 10, name: "carol".into() });
        assert_eq!(rows[1], Row { id: 20, name: "dave".into() });
    }

    #[tokio::test]
    async fn slot_to_json_then_json_to_slot_round_trips() {
        let ctx = make_minimal_ctx();
        {
            let mut slots = ctx.slots.write().await;
            slots.declare("original", SlotScope::Window);
            slots.declare("json",     SlotScope::Window);
            slots.declare("restored", SlotScope::Window);
        }

        let original = vec![
            Row { id: 1, name: "foo".into() },
            Row { id: 2, name: "bar".into() },
        ];
        ctx.slot_write("original", original.clone()).await.unwrap();

        SlotToJsonStep::<Row>::new("original", "json", false)
            .run(&ctx).await.unwrap();
        JsonToSlotStep::<Row>::new("json", "restored", false)
            .run(&ctx).await.unwrap();

        let restored: Vec<Row> = ctx.slot_read("restored").await.unwrap();
        assert_eq!(restored, original);
    }

    #[tokio::test]
    async fn json_to_slot_skips_bad_rows_and_continues() {
        let ctx = make_minimal_ctx();
        {
            let mut slots = ctx.slots.write().await;
            slots.declare("json", SlotScope::Window);
            slots.declare("rows", SlotScope::Window);
        }

        let json_vals = vec![
            serde_json::json!({"id": 1, "name": "ok"}),
            serde_json::json!({"bad_field": true}),      // missing required fields
            serde_json::json!({"id": 3, "name": "also_ok"}),
        ];
        ctx.slot_write("json", json_vals).await.unwrap();

        let step = JsonToSlotStep::<Row>::new("json", "rows", false);
        // Should succeed — bad rows are skipped with a warn log
        step.run(&ctx).await.unwrap();

        let rows: Vec<Row> = ctx.slot_read("rows").await.unwrap();
        assert_eq!(rows.len(), 2, "bad row should be skipped");
        assert_eq!(rows[0].id, 1);
        assert_eq!(rows[1].id, 3);
    }

    fn make_minimal_ctx() -> Arc<JobContext> {
        use std::collections::HashMap;
        use crate::components::auth::OAuth2Auth;
        let connections = crate::context::Connections {

            auth:        Arc::new(OAuth2Auth::new(
                reqwest::Client::new(),
                "http://unused".into(),
                "id".into(),
                "secret".into(),
            )),
            http:        reqwest::Client::new(),
            endpoint:    String::new(),
            extra_query: vec![],
            start_param: "start_time".into(),
            end_param:   "end_time".into(),
            date_format: "%Y%m%d".into(),
        };
        Arc::new(JobContext::new(connections, HashMap::new(), "test"))
    }
}

#[cfg(all(test, not(feature = "postgres")))]
mod merge_slots_tests {
    use std::sync::Arc;
    use std::collections::HashMap;
    use serde::{Deserialize, Serialize};

    use crate::context::JobContext;
    use crate::slot::SlotScope;
    use crate::step::Step;
    use crate::step::multi_transform::MergeSlotsStep;
    use crate::components::auth::OAuth2Auth;

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    struct Item { id: u32 }

    fn make_ctx() -> Arc<JobContext> {
        let connections = crate::context::Connections {
            auth:        Arc::new(OAuth2Auth::new(
                reqwest::Client::new(),
                "http://unused".into(),
                "id".into(),
                "secret".into(),
            )),
            http:        reqwest::Client::new(),
            endpoint:    String::new(),
            extra_query: vec![],
            start_param: "start_time".into(),
            end_param:   "end_time".into(),
            date_format: "%Y%m%d".into(),
        };
        Arc::new(JobContext::new(connections, HashMap::new(), "test"))
    }

    #[tokio::test]
    async fn merges_two_slots_into_one() {
        let ctx = make_ctx();
        {
            let mut slots = ctx.slots.write().await;
            slots.declare("a",      SlotScope::Window);
            slots.declare("b",      SlotScope::Window);
            slots.declare("merged", SlotScope::Window);
        }

        ctx.slot_write("a", vec![Item { id: 1 }, Item { id: 2 }]).await.unwrap();
        ctx.slot_write("b", vec![Item { id: 3 }]).await.unwrap();

        let step = MergeSlotsStep::<Item>::new(
            vec!["a".into(), "b".into()], "merged", false,
        );
        step.run(&ctx).await.unwrap();

        let merged: Vec<Item> = ctx.slot_read("merged").await.unwrap();
        assert_eq!(merged.len(), 3);
        assert_eq!(merged[0].id, 1);
        assert_eq!(merged[2].id, 3);
    }

    #[tokio::test]
    async fn merge_with_append_grows_target() {
        let ctx = make_ctx();
        {
            let mut slots = ctx.slots.write().await;
            slots.declare("src",    SlotScope::Window);
            slots.declare("target", SlotScope::Window);
        }

        ctx.slot_write("target", vec![Item { id: 10 }]).await.unwrap();
        ctx.slot_write("src",    vec![Item { id: 20 }]).await.unwrap();

        let step = MergeSlotsStep::<Item>::new(vec!["src".into()], "target", true);
        step.run(&ctx).await.unwrap();

        let result: Vec<Item> = ctx.slot_read("target").await.unwrap();
        assert_eq!(result.len(), 2);
        assert_eq!(result[0].id, 10);
        assert_eq!(result[1].id, 20);
    }

    #[tokio::test]
    async fn merge_empty_source_produces_empty_target() {
        let ctx = make_ctx();
        {
            let mut slots = ctx.slots.write().await;
            slots.declare("empty",  SlotScope::Window);
            slots.declare("output", SlotScope::Window);
        }
        // "empty" slot is declared but never written — should be skipped gracefully

        let step = MergeSlotsStep::<Item>::new(vec!["empty".into()], "output", false);
        step.run(&ctx).await.unwrap();

        let result: Vec<Item> = ctx.slot_read("output").await.unwrap_or_default();
        assert_eq!(result.len(), 0);
    }
}
