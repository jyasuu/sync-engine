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
        assert!(m.is_set("j"), "job slot should survive");
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
        assert!(m.is_set("p"), "pipeline slot should survive");
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
        let rd = RuleDef {
            field: "name".into(),
            source: None,
            rule: "copy".into(),
        };
        let src = rd.source.as_deref().unwrap_or(&rd.field);
        let expr = gen_rule_expr_pub(&rd.field, src, &rd.rule);
        assert_eq!(expr, "u.name");
    }

    #[test]
    fn copy_rule_with_different_source() {
        let rd = RuleDef {
            field: "sso_acct".into(),
            source: Some("ssoAcct".into()),
            rule: "copy".into(),
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
        if let ResourceDef::HttpService {
            start_param,
            end_param,
            date_format,
            ..
        } = def
        {
            assert_eq!(start_param, "start_time");
            assert_eq!(end_param, "end_time");
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
        if let ResourceDef::HttpService {
            start_param,
            end_param,
            date_format,
            ..
        } = def
        {
            assert_eq!(start_param, "from_date");
            assert_eq!(end_param, "to_date");
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
