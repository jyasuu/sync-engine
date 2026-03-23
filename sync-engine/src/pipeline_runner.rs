// sync-engine/src/pipeline_runner.rs
//
// TOML schema — five sections map 1:1 to the job pseudocode:
//   [resources]       — named connections (pg, auth, http, svc)
//   [slots] / [queues]— named typed data channels
//   [pre_job]         — init_resources + optional consumer spawn
//   [main_job]        — iterator, retry, [[retry_steps]], post_window, post_loop
//   [post_job]        — [[steps]]
//   [job]             — name, [job.trigger]  (alias: [job.scheduler] → cron trigger)
//
// Trigger types (set via [job.trigger] type = "..."):
//   cron       — tokio-cron-scheduler; mutex-skips overlapping ticks  (default)
//   once       — run one tick then exit cleanly
//   webhook    — tiny HTTP server; any POST fires a tick
//   pg_notify  — sqlx LISTEN; a NOTIFY on the channel fires a tick
//
// run(path, registry) — the single entry-point for a business crate's main.rs.

use anyhow::{anyhow, Context, Result};
use reqwest::Client;
use serde::Deserialize;
#[cfg(feature = "postgres")]
use sqlx::postgres::PgPoolOptions;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tokio_cron_scheduler::{Job, JobScheduler};
use tracing::info;

use crate::components::auth::OAuth2Auth;
use crate::config_value::ConfigValue;
use crate::context::{Connections, JobContext};
use crate::registry::TypeRegistry;
use crate::runner::{MainJobRunner, WindowConfig};
use crate::slot::SlotScope;
use crate::step::StepRunner;
use crate::step::Step;
#[cfg(feature = "postgres")]
use crate::step::consumer::CommitMode;
use crate::step::control::{DrainQueueStep, LogSummaryStep, RawSqlStep, SleepStep};

// ── Re-exports for backwards compat ───────────────────────────────────────
pub use crate::context::Connections as StandardConnections;

// ── TOML schema ───────────────────────────────────────────────────────────

#[derive(Debug, Deserialize)]
pub struct PipelineConfig {
    pub job:       Option<JobMeta>,
    #[serde(default)]
    pub resources: HashMap<String, ResourceDef>,
    #[serde(default)]
    pub slots:     HashMap<String, SlotDef>,
    #[serde(default)]
    pub queues:    HashMap<String, QueueDef>,
    /// Runs once at startup after build_context, before the trigger loop.
    /// Pipeline-scoped slots written here survive into every tick.
    #[serde(default)]
    pub init_job:  Option<InitJobConfig>,
    pub pre_job:   PreJobConfig,
    pub main_job:  MainJobConfig,
    pub post_job:  PostJobConfig,
}

// ── [job] ─────────────────────────────────────────────────────────────────

#[derive(Debug, Deserialize)]
pub struct JobMeta {
    pub name:      Option<String>,
    /// New key. Preferred over `scheduler`.
    pub trigger:   Option<TriggerConfig>,
    /// Legacy alias: `[job.scheduler] cron = "..."` maps to `TriggerConfig::Cron`.
    /// Kept so existing pipeline.toml files don't need changes.
    pub scheduler: Option<SchedulerConfig>,
}

/// `[job.trigger]` — selects the runtime loop that fires `run_one_tick`.
#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum TriggerConfig {
    /// Default. Fires on a cron schedule; mutex-skips overlapping ticks.
    ///
    /// ```toml
    /// [job.trigger]
    /// type = "cron"
    /// cron = { env = "SCHEDULER__CRON", default = "0 */30 * * * *" }
    /// ```
    Cron {
        cron: ConfigValue,
    },

    /// Run exactly one tick then exit with code 0.
    /// Useful for one-shot invocations (CI, k8s Job, manual backfill).
    ///
    /// ```toml
    /// [job.trigger]
    /// type = "once"
    /// ```
    Once,

    /// Listen for HTTP POST requests; each request fires one tick.
    /// Returns 202 Accepted immediately; the tick runs in the background
    /// and is mutex-skipped if a previous tick is still running.
    ///
    /// ```toml
    /// [job.trigger]
    /// type = "webhook"
    /// host = "0.0.0.0"   # optional, default 0.0.0.0
    /// port = 8080         # optional, default 8080
    /// ```
    Webhook {
        #[serde(default = "default_webhook_host")]
        host: String,
        #[serde(default = "default_webhook_port")]
        port: u16,
    },

    /// LISTEN on a Postgres NOTIFY channel; each notification fires one tick.
    /// Requires a `postgres` resource to be defined in `[resources]`.
    ///
    /// ```toml
    /// [job.trigger]
    /// type    = "pg_notify"
    /// channel = { env = "TRIGGER__PG_CHANNEL", default = "sync_trigger" }
    /// ```
    PgNotify {
        channel: ConfigValue,
    },

    /// Consume from a Kafka topic; each message fires one tick.
    /// Requires a `kafka` resource with a `topic` defined in `[resources]`.
    ///
    /// ```toml
    /// [job.trigger]
    /// type  = "kafka"
    /// topic = { env = "TRIGGER__KAFKA_TOPIC" }
    /// ```
    #[cfg(feature = "kafka")]
    Kafka {
        topic: ConfigValue,
    },
}

fn default_webhook_host() -> String { "0.0.0.0".into() }
fn default_webhook_port() -> u16    { 8080 }

/// Legacy `[job.scheduler]` block — still accepted, maps to `TriggerConfig::Cron`.
#[derive(Debug, Deserialize)]
pub struct SchedulerConfig {
    pub cron: ConfigValue,
}

// ── [resources] ───────────────────────────────────────────────────────────
// Each entry is a named, typed connection. The engine instantiates them
// in dependency order before pre_job steps run.

#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ResourceDef {
    #[cfg(feature = "postgres")]
    Postgres {
        url:             ConfigValue,
        #[serde(default = "default_max_conn")]
        max_connections: u32,
    },
    Oauth2 {
        token_url:     ConfigValue,
        client_id:     ConfigValue,
        client_secret: ConfigValue,
    },
    HttpClient {
        #[serde(default = "default_timeout")]
        timeout_secs:   u64,
        #[serde(default = "default_keepalive")]
        keepalive_secs: u64,
    },
    HttpService {
        http:        String,
        auth:        String,
        endpoint:    ConfigValue,
        #[serde(default)]
        realm_type:  Option<ConfigValue>,
        #[serde(default = "default_start_param")]
        start_param: String,
        #[serde(default = "default_end_param")]
        end_param:   String,
        #[serde(default = "default_date_fmt")]
        date_format: String,
        #[serde(default)]
        extra_params: Vec<ExtraParam>,
    },

    /// Elasticsearch cluster.
    ///
    /// ```toml
    /// [resources.es]
    /// type = "elasticsearch"
    /// url  = { env = "ES_URL", default = "http://localhost:9200" }
    /// ```
    #[cfg(feature = "elasticsearch")]
    Elasticsearch {
        url: ConfigValue,
    },

    /// Kafka cluster — used for both produce (sink) and consume (source/trigger).
    ///
    /// ```toml
    /// [resources.kafka]
    /// type     = "kafka"
    /// brokers  = { env = "KAFKA_BROKERS", default = "localhost:9092" }
    /// group_id = { env = "KAFKA_GROUP_ID", default = "sync-engine" }
    /// topic    = { env = "KAFKA_TOPIC" }   # required for consume trigger/source
    /// ```
    #[cfg(feature = "kafka")]
    Kafka {
        brokers:  ConfigValue,
        #[serde(default = "default_kafka_group")]
        group_id: ConfigValue,
        #[serde(default)]
        topic:    Option<ConfigValue>,
    },
}

fn default_start_param() -> String { "start_time".into() }
fn default_end_param()   -> String { "end_time".into() }
fn default_date_fmt()    -> String { "%Y%m%d".into() }
#[cfg(feature = "kafka")]
fn default_kafka_group() -> ConfigValue { ConfigValue::Literal("sync-engine".into()) }

#[derive(Debug, Deserialize, Clone)]
pub struct ExtraParam {
    pub key:   String,
    pub value: ConfigValue,
}

fn default_max_conn()  -> u32 { 5 }
fn default_timeout()   -> u64 { 620 }
fn default_keepalive() -> u64 { 30 }

// ── [slots] ───────────────────────────────────────────────────────────────

#[derive(Debug, Deserialize)]
pub struct SlotDef {
    /// Schema.toml record name — for documentation only at runtime.
    #[serde(rename = "type")]
    pub record_type: Option<String>,
    pub scope:       SlotScopeStr,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SlotScopeStr { Window, Job, Pipeline }

impl From<&SlotScopeStr> for SlotScope {
    fn from(s: &SlotScopeStr) -> Self {
        match s {
            SlotScopeStr::Window   => SlotScope::Window,
            SlotScopeStr::Job      => SlotScope::Job,
            SlotScopeStr::Pipeline => SlotScope::Pipeline,
        }
    }
}

// ── [queues] ──────────────────────────────────────────────────────────────

#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum QueueDef {
    Tokio {
        #[serde(default = "default_queue_cap")]
        capacity: usize,
    },
    Rabbitmq {
        url:         ConfigValue,
        exchange:    ConfigValue,
        routing_key: ConfigValue,
    },
}
fn default_queue_cap() -> usize { 256 }

// ── [pre_job] ─────────────────────────────────────────────────────────────

#[derive(Debug, Deserialize)]
pub struct PreJobConfig {
    /// If true, the engine builds all [resources] before pre_job steps.
    #[serde(default = "default_true")]
    pub init_resources: bool,
    #[serde(default)]
    pub steps: Vec<PreStepConfig>,
}
fn default_true() -> bool { true }

#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum PreStepConfig {
    SpawnConsumer {
        queue:       String,
        model:       String,
        commit_mode: CommitModeStr,
        accum_slot:  Option<String>,
    },
}

#[derive(Debug, Deserialize, Clone, Copy)]
#[serde(rename_all = "snake_case")]
pub enum CommitModeStr { PerBatch, DrainInPostJob }

// ── [main_job] ────────────────────────────────────────────────────────────

#[derive(Debug, Deserialize)]
pub struct MainJobConfig {
    pub iterator:           IteratorConfig,
    pub retry:              RetryConfig,
    #[serde(default)]
    pub retry_steps:        Vec<MainStepConfig>,
    #[serde(default)]
    pub post_window_steps:  Vec<MainStepConfig>,
    #[serde(default)]
    pub post_loop_steps:    Vec<MainStepConfig>,
}

#[derive(Debug, Deserialize)]
pub struct IteratorConfig {
    #[serde(rename = "type")]
    pub iter_type:      String,
    pub start_interval: ConfigValue,
    pub end_interval:   ConfigValue,
    pub interval_limit: ConfigValue,
    pub sleep_secs:     ConfigValue,
}

#[derive(Debug, Deserialize)]
pub struct RetryConfig {
    #[serde(default = "default_attempts")]
    pub max_attempts:  usize,
    #[serde(default = "default_backoff")]
    pub backoff_secs:  u64,
}
fn default_attempts() -> usize { 5 }
fn default_backoff()  -> u64   { 2 }

/// A step entry in [[main_job.retry_steps]] or [[main_job.post_loop_steps]].
/// The `type` field drives dispatch; all other fields are passed as `params`.
#[derive(Debug, Deserialize)]
pub struct MainStepConfig {
    #[serde(rename = "type")]
    pub step_type: String,
    /// The envelope type name for type="fetch" (e.g. "ApiUserResponse")
    pub envelope:  Option<String>,
    /// The transform type name for type="transform" (e.g. "UserTransform")
    pub transform: Option<String>,
    /// The model type name for type="tx_upsert" (e.g. "DbUser")
    pub model:     Option<String>,
    #[serde(default)]
    pub reads:     Option<String>,
    #[serde(default)]
    pub writes:    Option<String>,
    #[serde(default)]
    pub append:    bool,
    /// For type="sleep"
    pub secs:      Option<ConfigValue>,
    /// For type="raw_sql"
    pub sql:       Option<ConfigValue>,
    #[serde(default)]
    pub skip_if_empty: bool,
    /// For type="send_to_queue"
    pub queue:     Option<String>,

    // ── Commit / ack strategies ───────────────────────────────────────────
    /// Controls how Postgres writes are committed.
    /// Valid values: "tx_scope" (default), "autocommit", "bulk_tx"
    #[serde(default)]
    pub commit_strategy: Option<String>,

    /// Controls when RabbitMQ messages are acknowledged.
    /// Valid values: "ack_on_commit" (default), "ack_on_receive", "ack_per_batch"
    #[serde(default)]
    pub ack_strategy: Option<String>,

    // ── Elasticsearch fields ──────────────────────────────────────────────
    pub index:      Option<String>,
    pub op:         Option<String>,
    pub scroll_ttl: Option<String>,
    pub batch_size: Option<i64>,

    // ── Kafka fields ──────────────────────────────────────────────────────
    pub topic:        Option<ConfigValue>,
    pub key_field:    Option<String>,
    pub max_messages: Option<usize>,
    pub timeout_ms:   Option<u64>,

    // ── Complex transform fields ──────────────────────────────────────────

    /// For type="split_transform": list of (transform name, writes slot) pairs.
    /// Each entry applies a registered transform to the same `reads` slot and
    /// writes results into a different output slot.
    ///
    /// ```toml
    /// [[main_job.retry_steps]]
    /// type   = "split_transform"
    /// reads  = "api_rows"
    /// transforms = [
    ///   { transform = "UserTransform",     writes = "db_users" },
    ///   { transform = "UserRoleTransform", writes = "db_roles" },
    /// ]
    /// ```
    #[serde(default)]
    pub transforms: Vec<SplitTransformEntry>,

    /// For type="merge_slots": list of slot names to concatenate.
    /// All slots must hold the same Rust type. Writes the merged Vec to `writes`.
    ///
    /// ```toml
    /// [[main_job.retry_steps]]
    /// type        = "merge_slots"
    /// merge_reads = ["slot_a", "slot_b"]
    /// writes      = "merged"
    /// ```
    #[serde(default)]
    pub merge_reads: Vec<String>,
}

/// One entry inside `split_transform.transforms`.
#[derive(Debug, Deserialize, Clone)]
pub struct SplitTransformEntry {
    /// Name of the registered transform (e.g. "UserTransform").
    pub transform: String,
    /// Slot to write the output into.
    pub writes: String,
    /// Whether to append to the target slot rather than replace it.
    #[serde(default)]
    pub append: bool,
}

// ── [init_job] ────────────────────────────────────────────────────────────
//
// Runs ONCE after build_context and validate, before the trigger loop starts.
// Use for migrations, connectivity checks, cache warm-up, and pre-seeding
// pipeline-scoped slots that should survive across all ticks.
//
// Steps declared here share the same variants as post_job: raw_sql, custom,
// log_summary, drain_queue. Add [init_job] to pipeline.toml:
//
//   [[init_job.steps]]
//   type = "raw_sql"
//   sql  = { env = "INIT__MIGRATION_SQL", default = "" }
//   skip_if_empty = true
//
//   [[init_job.steps]]
//   type = "custom"
//   hook = "WarmCache"    # registered in main.rs

#[derive(Debug, Deserialize, Default)]
pub struct InitJobConfig {
    #[serde(default)]
    pub steps: Vec<InitStepConfig>,
}

/// Step variants available in [init_job].
/// Same capabilities as post_job — raw SQL, custom hooks, and logging.
#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum InitStepConfig {
    /// Execute a SQL statement directly (e.g. migrations, TRUNCATE, REFRESH).
    RawSql {
        sql:           ConfigValue,
        #[serde(default)]
        skip_if_empty: bool,
    },
    /// A custom async function registered in main.rs via register_post_hook.
    Custom {
        hook: String,
    },
    /// Emit a tracing log line summarising the context state. Useful for
    /// confirming connectivity at startup.
    LogSummary,
}

// ── [post_job] ────────────────────────────────────────────────────────────

#[derive(Debug, Deserialize)]
pub struct PostJobConfig {
    #[serde(default)]
    pub steps: Vec<PostStepConfig>,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum PostStepConfig {
    LogSummary,
    RawSql {
        sql:           ConfigValue,
        #[serde(default)]
        skip_if_empty: bool,
    },
    DrainQueue {
        queue: String,
    },
    Custom {
        hook: String,
    },
}

// ── Built connections ─────────────────────────────────────────────────────

/// Resolved at startup from [resources].
pub struct BuiltResources {
    pub db:          Option<sqlx::PgPool>,
    pub http:        Option<Client>,
    pub auth:        Option<Arc<OAuth2Auth>>,
    pub endpoint:    String,
    pub extra_query: Vec<(String, String)>,
}

// ── Startup validation ────────────────────────────────────────────────────

/// Validates pipeline.toml against the registry at startup.
/// Catches typos, missing registrations, and undeclared slots before
/// the first job tick runs.
pub fn validate(cfg: &PipelineConfig, registry: &TypeRegistry) -> Result<()> {
    let mut errors: Vec<String> = Vec::new();

    // Collect declared slot and queue names
    let slot_names: std::collections::HashSet<&str> =
        cfg.slots.keys().map(|s| s.as_str()).collect();
    let queue_names: std::collections::HashSet<&str> =
        cfg.queues.keys().map(|s| s.as_str()).collect();
    let all_channels: std::collections::HashSet<&str> =
        slot_names.iter().chain(queue_names.iter()).copied().collect();

    // Built-in slot names that are always available
    let builtin: std::collections::HashSet<&str> = [
        "summary.windows_processed", "summary.error_count",
        "summary.total_fetched", "summary.total_upserted", "summary.total_skipped",
        "window.fetched", "window.upserted", "window.skipped",
    ].iter().copied().collect();

    let check_slot = |name: &str, context: &str, errors: &mut Vec<String>| {
        if !all_channels.contains(name) && !builtin.contains(name) {
            errors.push(format!(
                "{context}: slot/queue \"{name}\" not declared in [slots] or [queues]"
            ));
        }
    };

    // Validate trigger config
    if let Some(trigger) = cfg.job.as_ref().and_then(|j| j.trigger.as_ref()) {
        if let TriggerConfig::PgNotify { .. } = trigger {
            #[cfg(not(feature = "postgres"))]
            errors.push(
                "trigger type=\"pg_notify\" requires feature \"postgres\" — add it to Cargo.toml".into()
            );
            #[cfg(feature = "postgres")]
            {
                let has_pg = cfg.resources.values().any(|r| matches!(r, ResourceDef::Postgres { .. }));
                if !has_pg {
                    errors.push(
                        "trigger type=\"pg_notify\" requires a [resources.*] postgres entry".into()
                    );
                }
            }
        }
        if let TriggerConfig::Webhook { port, .. } = trigger {
            if *port == 0 {
                errors.push("trigger type=\"webhook\": port must be > 0".into());
            }
        }
    }

    // Validate iterator type
    let iter_type = cfg.main_job.iterator.iter_type.as_str();
    if iter_type != "date_window" {
        errors.push(format!(
            "[main_job.iterator] type \"{iter_type}\" is not supported — only \"date_window\" is currently valid"
        ));
    }

    // Validate all step lists
    let all_steps = cfg.main_job.retry_steps.iter()
        .chain(cfg.main_job.post_window_steps.iter())
        .chain(cfg.main_job.post_loop_steps.iter());

    for step in all_steps {
        let ctx = format!("step type=\"{}\"", step.step_type);
        match step.step_type.as_str() {
            "fetch" => {
                match &step.envelope {
                    None => errors.push(format!("{ctx}: missing `envelope` field")),
                    Some(e) => if !registry.has_envelope(e) {
                        errors.push(format!(
                            "{ctx}: envelope \"{e}\" not registered — add registry.register_envelope::<{e}>(\"{e}\") in main.rs"
                        ));
                    }
                }
                if let Some(ref w) = step.writes {
                    check_slot(w, &ctx, &mut errors);
                }
            }
            "transform" => {
                match &step.transform {
                    None => errors.push(format!("{ctx}: missing `transform` field")),
                    Some(t) => if !registry.has_transform(t) {
                        errors.push(format!(
                            "{ctx}: transform \"{t}\" not registered — add registry.register_transform::<Src, Dst, {t}>(\"{t}\") in main.rs"
                        ));
                    }
                }
                if let Some(ref r) = step.reads  { check_slot(r, &ctx, &mut errors); }
                if let Some(ref w) = step.writes { check_slot(w, &ctx, &mut errors); }
            }
            "tx_upsert" => {
                #[cfg(feature = "postgres")]
                match &step.model {
                    None => errors.push(format!("{ctx}: missing `model` field")),
                    Some(m) => if !registry.has_model(m) {
                        errors.push(format!(
                            "{ctx}: model \"{m}\" not registered — add registry.register_model::<{m}>(\"{m}\") in main.rs"
                        ));
                    }
                }
                #[cfg(not(feature = "postgres"))]
                errors.push(format!("{ctx}: tx_upsert requires feature \"postgres\""));
                if let Some(ref r) = step.reads { check_slot(r, &ctx, &mut errors); }
            }
            "send_to_queue" => {
                #[cfg(feature = "postgres")]
                match &step.model {
                    None => errors.push(format!("{ctx}: missing `model` field — needed for typed queue send")),
                    Some(m) => if !registry.has_queue_send(m) {
                        errors.push(format!(
                            "{ctx}: model \"{m}\" not registered — add registry.register_model::<{m}>(\"{m}\") in main.rs"
                        ));
                    }
                }
                if let Some(ref r) = step.reads { check_slot(r, &ctx, &mut errors); }
                match &step.queue {
                    None => errors.push(format!("{ctx}: missing `queue` field")),
                    Some(q) => if !queue_names.contains(q.as_str()) {
                        errors.push(format!("{ctx}: queue \"{q}\" not declared in [queues]"));
                    }
                }
            }
            "sleep" | "raw_sql" => {} // no type-level validation needed
            "split_transform" => {
                if step.transforms.is_empty() {
                    errors.push(format!("{ctx}: missing `transforms` array"));
                }
                for entry in &step.transforms {
                    if !registry.has_transform(&entry.transform) {
                        errors.push(format!(
                            "{ctx}: transform \"{}\" not registered",
                            entry.transform
                        ));
                    }
                    check_slot(&entry.writes, &ctx, &mut errors);
                }
                if let Some(ref r) = step.reads { check_slot(r, &ctx, &mut errors); }
            }
            "merge_slots" => {
                if step.merge_reads.is_empty() {
                    errors.push(format!("{ctx}: missing `merge_reads` array"));
                }
                if step.writes.is_none() {
                    errors.push(format!("{ctx}: missing `writes` field"));
                }
                if step.model.is_none() {
                    errors.push(format!("{ctx}: missing `model` field"));
                }
                for r in &step.merge_reads { check_slot(r, &ctx, &mut errors); }
                if let Some(w) = &step.writes { check_slot(w, &ctx, &mut errors); }
            }
            "slot_to_json" | "json_to_slot" => {
                if step.reads.is_none() {
                    errors.push(format!("{ctx}: missing `reads` field"));
                }
                if step.writes.is_none() {
                    errors.push(format!("{ctx}: missing `writes` field"));
                }
                if step.model.is_none() {
                    errors.push(format!("{ctx}: missing `model` field"));
                }
                if let Some(r) = &step.reads  { check_slot(r, &ctx, &mut errors); }
                if let Some(w) = &step.writes { check_slot(w, &ctx, &mut errors); }
            }
            "es_index" => {
                #[cfg(not(feature = "elasticsearch"))]
                errors.push(format!("{ctx}: requires feature \"elasticsearch\""));
                if step.model.is_none() {
                    errors.push(format!("{ctx}: missing `model` field"));
                }
                if step.index.is_none() {
                    errors.push(format!("{ctx}: missing `index` field"));
                }
                if let Some(ref op) = step.op {
                    if !["upsert", "delete", "create"].contains(&op.as_str()) {
                        errors.push(format!("{ctx}: `op` must be upsert, delete, or create — got \"{op}\""));
                    }
                }
                if let Some(ref r) = step.reads { check_slot(r, &ctx, &mut errors); }
            }
            "es_fetch" => {
                #[cfg(not(feature = "elasticsearch"))]
                errors.push(format!("{ctx}: requires feature \"elasticsearch\""));
                if step.model.is_none() {
                    errors.push(format!("{ctx}: missing `model` field (used to select the deserialize type)"));
                }
                if step.index.is_none() {
                    errors.push(format!("{ctx}: missing `index` field"));
                }
                if let Some(ref w) = step.writes { check_slot(w, &ctx, &mut errors); }
            }
            "kafka_produce" => {
                #[cfg(not(feature = "kafka"))]
                errors.push(format!("{ctx}: requires feature \"kafka\""));
                if step.model.is_none() {
                    errors.push(format!("{ctx}: missing `model` field"));
                }
                if step.topic.is_none() {
                    errors.push(format!("{ctx}: missing `topic` field"));
                }
                if let Some(ref r) = step.reads { check_slot(r, &ctx, &mut errors); }
            }
            "kafka_consume" => {
                #[cfg(not(feature = "kafka"))]
                errors.push(format!("{ctx}: requires feature \"kafka\""));
                if step.model.is_none() {
                    errors.push(format!("{ctx}: missing `model` field"));
                }
                if step.topic.is_none() {
                    errors.push(format!("{ctx}: missing `topic` field"));
                }
                if let Some(ref w) = step.writes { check_slot(w, &ctx, &mut errors); }
            }
            other => errors.push(format!(
                "Unknown step type \"{other}\" — valid types: \
                 fetch, transform, tx_upsert, send_to_queue, sleep, raw_sql, \
                 es_index, es_fetch, kafka_produce, kafka_consume, \
                 split_transform, merge_slots, slot_to_json, json_to_slot"
            )),
        }

        // Cross-field: commit_strategy only valid on tx_upsert
        if let Some(ref cs) = step.commit_strategy {
            if step.step_type != "tx_upsert" {
                errors.push(format!(
                    "step type=\"{}\": `commit_strategy` is only valid on type=\"tx_upsert\"",
                    step.step_type
                ));
            }
            if !["tx_scope", "autocommit", "bulk_tx"].contains(&cs.as_str()) {
                errors.push(format!(
                    "step type=\"tx_upsert\": unknown commit_strategy \"{cs}\" \
                     — valid values: tx_scope, autocommit, bulk_tx"
                ));
            }
        }

        // Cross-field: ack_strategy only meaningful on send_to_queue with a rabbitmq queue
        if let Some(ref ack) = step.ack_strategy {
            if step.step_type != "send_to_queue" {
                errors.push(format!(
                    "step type=\"{}\": `ack_strategy` is only valid on type=\"send_to_queue\"",
                    step.step_type
                ));
            }
            if !["ack_on_commit", "ack_on_receive", "ack_per_batch"].contains(&ack.as_str()) {
                errors.push(format!(
                    "step type=\"send_to_queue\": unknown ack_strategy \"{ack}\" \
                     — valid values: ack_on_commit, ack_on_receive, ack_per_batch"
                ));
            }
        }
    } // end for step in all_steps

    // Validate pre_job consumer steps
    for step in &cfg.pre_job.steps {
        let PreStepConfig::SpawnConsumer { queue, model, .. } = step;
        if !queue_names.contains(queue.as_str()) {
            errors.push(format!(
                "pre_job spawn_consumer: queue \"{queue}\" not declared in [queues]"
            ));
        }
        if !registry.has_model(model.as_str()) {
            errors.push(format!(
                "pre_job spawn_consumer: model \"{model}\" not registered — add registry.register_model::<{model}>(\"{model}\") in main.rs"
            ));
        }
    }

    // Validate post_job drain_queue steps
    for step in &cfg.post_job.steps {
        if let PostStepConfig::DrainQueue { queue } = step {
            if !queue_names.contains(queue.as_str()) {
                errors.push(format!(
                    "post_job drain_queue: queue \"{queue}\" not declared in [queues]"
                ));
            }
        }
    }

    if errors.is_empty() {
        Ok(())
    } else {
        let msg = errors.join("\n  ");
        Err(anyhow!("pipeline.toml validation failed:\n  {msg}\n"))
    }
}

// ── build_context ─────────────────────────────────────────────────────────

pub async fn build_context(cfg: &PipelineConfig) -> Result<Arc<JobContext>> {
    let job_name = cfg.job.as_ref()
        .and_then(|j| j.name.as_deref())
        .unwrap_or("unnamed")
        .to_owned();

    // ── Build resources in dependency order ───────────────────────────────
    // Pass 1: http_client (no deps)
    let mut http_clients: HashMap<String, Client> = HashMap::new();
    let mut auth_clients: HashMap<String, Arc<OAuth2Auth>> = HashMap::new();
    #[cfg(feature = "postgres")]
    let mut db_pools: HashMap<String, sqlx::PgPool> = HashMap::new();
    let mut endpoint      = String::new();
    let mut extra_query: Vec<(String, String)> = Vec::new();

    for (name, def) in &cfg.resources {
        if let ResourceDef::HttpClient { timeout_secs, keepalive_secs } = def {
            info!("resource[{}]: building http_client", name);
            let client = Client::builder()
                .timeout(Duration::from_secs(*timeout_secs))
                .tcp_keepalive(Duration::from_secs(*keepalive_secs))
                .build()
                .context("HTTP client build failed")?;
            http_clients.insert(name.clone(), client);
        }
    }

    // Pass 2: oauth2
    for (name, def) in &cfg.resources {
        if let ResourceDef::Oauth2 { token_url, client_id, client_secret } = def {
            let http = http_clients.values().next()
                .cloned()
                .unwrap_or_else(|| Client::new());
            info!("resource[{}]: building oauth2 client", name);
            auth_clients.insert(name.clone(), Arc::new(OAuth2Auth::new(
                http,
                token_url.resolve()?,
                client_id.resolve()?,
                client_secret.resolve()?,
            )));
        }
    }

    // Pass 3: postgres (only when feature enabled)
    #[cfg(feature = "postgres")]
    for (name, def) in &cfg.resources {
        if let ResourceDef::Postgres { url, max_connections } = def {
            info!("resource[{}]: connecting to postgres", name);
            let pool = PgPoolOptions::new()
                .max_connections(*max_connections)
                .connect(&url.resolve()?)
                .await
                .context("Postgres connect failed")?;
            db_pools.insert(name.clone(), pool);
        }
    }

    // Pass 4: http_service
    let mut start_param = "start_time".to_owned();
    let mut end_param   = "end_time".to_owned();
    let mut date_format = "%Y%m%d".to_owned();

    for (_name, def) in &cfg.resources {
        if let ResourceDef::HttpService {
            endpoint: ep, realm_type,
            start_param: sp, end_param: ep2, date_format: df,
            extra_params, ..
        } = def {
            endpoint    = ep.resolve()?;
            start_param = sp.clone();
            end_param   = ep2.clone();
            date_format = df.clone();
            if let Some(rt) = realm_type {
                let v = rt.resolve().unwrap_or_default();
                if !v.is_empty() { extra_query.push(("realm_type".to_owned(), v)); }
            }
            for p in extra_params {
                let v = p.value.resolve().unwrap_or_default();
                if !v.is_empty() { extra_query.push((p.key.clone(), v)); }
            }
        }
    }

    // Pass 5: Elasticsearch — store the base URL; steps call the REST API
    // via ctx.connections.http (reqwest client already in context).
    #[cfg(feature = "elasticsearch")]
    let mut es_url_opt: Option<String> = None;
    #[cfg(feature = "elasticsearch")]
    for (name, def) in &cfg.resources {
        if let ResourceDef::Elasticsearch { url } = def {
            let url_str = url.resolve()?;
            info!("resource[{}]: Elasticsearch at {}", name, url_str);
            es_url_opt = Some(url_str);
            break;
        }
    }

    // Pass 6: Kafka
    #[cfg(feature = "kafka")]
    let mut kafka_prod: Option<Arc<rdkafka::producer::FutureProducer>> = None;
    #[cfg(feature = "kafka")]
    let mut kafka_cons: Option<Arc<rdkafka::consumer::StreamConsumer>> = None;
    #[cfg(feature = "kafka")]
    for (name, def) in &cfg.resources {
        if let ResourceDef::Kafka { brokers, group_id, topic } = def {
            let brokers_str  = brokers.resolve()?;
            let group_str    = group_id.resolve()?;
            info!("resource[{}]: connecting to Kafka brokers={}", name, brokers_str);

            let producer = crate::step::kafka::build_producer(&brokers_str)
                .with_context(|| format!("Kafka producer failed for resource [{name}]"))?;
            kafka_prod = Some(Arc::new(producer));

            if let Some(ref t) = topic {
                let topic_str = t.resolve()?;
                let consumer = crate::step::kafka::build_consumer(&brokers_str, &group_str, &topic_str)
                    .with_context(|| format!("Kafka consumer failed for resource [{name}]"))?;
                kafka_cons = Some(Arc::new(consumer));
            }
            break;
        }
    }

    #[cfg(feature = "postgres")]
    let db = db_pools.into_values().next()
        .ok_or_else(|| anyhow!("No postgres resource defined — add [resources.pg] or remove the postgres feature requirement"))?;

    let auth = auth_clients.into_values().next()
        .ok_or_else(|| anyhow!("No oauth2 resource defined"))?;
    let http = http_clients.into_values().next()
        .ok_or_else(|| anyhow!("No http_client resource defined"))?;

    let connections = Connections {
        #[cfg(feature = "postgres")]
        db,
        auth, http, endpoint, extra_query,
        start_param, end_param, date_format,
    };

    // ── Config map ────────────────────────────────────────────────────────
    let iter = &cfg.main_job.iterator;
    let mut config = HashMap::new();
    config.insert("iterator.start_interval".into(), iter.start_interval.resolve()?);
    config.insert("iterator.end_interval".into(),   iter.end_interval.resolve()?);
    config.insert("iterator.interval_limit".into(), iter.interval_limit.resolve()?);
    config.insert("iterator.sleep_secs".into(),     iter.sleep_secs.resolve()?);

    #[allow(unused_mut)]
    let mut ctx_inner = JobContext::new(connections, config, job_name);

    // Wire in optional backend clients
    #[cfg(feature = "elasticsearch")]
    { ctx_inner.es_url = es_url_opt; }
    #[cfg(feature = "kafka")]
    { ctx_inner.kafka_producer = kafka_prod; ctx_inner.kafka_consumer = kafka_cons; }

    let ctx = Arc::new(ctx_inner);

    // ── Declare summary slots (always present) ────────────────────────────
    {
        let mut slots = ctx.slots.write().await;
        slots.declare("summary.windows_processed", SlotScope::Job);
        slots.declare("summary.error_count",       SlotScope::Job);
        slots.declare("summary.total_fetched",     SlotScope::Job);
        slots.declare("summary.total_upserted",    SlotScope::Job);
        slots.declare("summary.total_skipped",     SlotScope::Job);
        slots.declare("window.fetched",            SlotScope::Window);
        slots.declare("window.upserted",           SlotScope::Window);
        slots.declare("window.skipped",            SlotScope::Window);

        // User-declared slots from [slots]
        for (key, def) in &cfg.slots {
            slots.declare(key, SlotScope::from(&def.scope));
        }
    }

    // ── Register queues from [queues] ─────────────────────────────────────
    for (name, def) in &cfg.queues {
        match def {
            QueueDef::Tokio { capacity } => {
                ctx.register_queue(name, *capacity).await;
                info!("queue[{}]: tokio mpsc, capacity={}", name, capacity);
            }
            QueueDef::Rabbitmq { url, exchange, routing_key } => {
                let url_str = url.resolve()?;
                let exchange_str    = exchange.resolve()?;
                let routing_key_str = routing_key.resolve()?;
                info!("queue[{}]: connecting to RabbitMQ exchange={}", name, exchange_str);
                // Store the config in the context's queue map under a special key
                // so SpawnConsumerStep and SendToQueueStep can retrieve it.
                // We use a tokio channel as the in-process bridge; the producer
                // step serializes and publishes via lapin, consumer subscribes.
                use crate::transport::rabbitmq::{RabbitmqConfig, RabbitmqQueue};
                let rmq_cfg = RabbitmqConfig {
                    url:         url_str,
                    exchange:    exchange_str,
                    routing_key: routing_key_str,
                    queue_name:  None,
                };
                // Connect producer; consumer is connected when spawn_consumer runs.
                match RabbitmqQueue::connect(rmq_cfg).await {
                    Ok(q) => {
                        // Store in context via a dedicated slot so steps can access it
                        {
                            let mut slots = ctx.slots.write().await;
                            slots.declare(
                                &format!("__rmq_queue_{name}"),
                                crate::slot::SlotScope::Pipeline,
                            );
                        }
                        ctx.slot_write(
                            &format!("__rmq_queue_{name}"),
                            std::sync::Arc::new(tokio::sync::Mutex::new(q)),
                        ).await?;
                        // Also register a tokio bridge channel for in-process coordination
                        ctx.register_queue(name, 256).await;
                        info!("queue[{}]: RabbitMQ connected", name);
                    }
                    Err(e) => {
                        return Err(e.context(format!("Failed to connect RabbitMQ queue [{name}]")));
                    }
                }
            }
        }
    }

    Ok(ctx)
}

/// Build window config from parsed iterator + retry sections.
pub fn build_window_cfg(cfg: &PipelineConfig) -> Result<WindowConfig> {
    let iter = &cfg.main_job.iterator;
    Ok(WindowConfig {
        start_interval:    iter.start_interval.resolve_as::<i64>()?,
        end_interval:      iter.end_interval.resolve_as::<i64>()?,
        interval_limit:    iter.interval_limit.resolve_as::<i64>()?,
        sleep_secs:        iter.sleep_secs.resolve_as::<u64>()?,
        max_attempts:      cfg.main_job.retry.max_attempts,
        base_backoff_secs: cfg.main_job.retry.backoff_secs,
    })
}

/// Build typed steps from a slice of MainStepConfig using the TypeRegistry.
pub fn build_steps(
    steps: &[MainStepConfig],
    registry: &Arc<TypeRegistry>,
) -> Result<StepRunner> {
    let mut runner = StepRunner::new();
    for s in steps {
        let mut params = HashMap::new();
        if let Some(ref r) = s.reads  { params.insert("reads".into(),  r.clone()); }
        if let Some(ref w) = s.writes { params.insert("writes".into(), w.clone()); }
        if s.append { params.insert("append".into(), "true".into()); }

        match s.step_type.as_str() {
            "fetch" => {
                let env = s.envelope.as_deref()
                    .ok_or_else(|| anyhow!("fetch step missing `envelope` field"))?;
                runner.push_boxed(registry.build_fetch(env, &params)?);
            }
            "transform" => {
                let xfm = s.transform.as_deref()
                    .ok_or_else(|| anyhow!("transform step missing `transform` field"))?;
                runner.push_boxed(registry.build_transform(xfm, &params)?);
            }
            "tx_upsert" => {
                #[cfg(feature = "postgres")]
                {
                    let model = s.model.as_deref()
                        .ok_or_else(|| anyhow!("tx_upsert step missing `model` field"))?;
                    let strategy = s.commit_strategy.as_deref().unwrap_or("tx_scope");
                    match strategy {
                        "autocommit" => {
                            runner.push_boxed(registry.build_autocommit_sink(model, &params)?);
                        }
                        _ => {
                            // "tx_scope" (default) and "bulk_tx" both use TxUpsertStep;
                            // bulk_tx is achieved by placing the step in post_loop_steps.
                            runner.push_boxed(registry.build_model_sink(model, &params)?);
                        }
                    }
                }
                #[cfg(not(feature = "postgres"))]
                return Err(anyhow!("tx_upsert requires feature \"postgres\" — add it to Cargo.toml"));
            }
            "es_index" => {
                #[cfg(feature = "elasticsearch")]
                {
                    let reads = s.reads.clone().unwrap_or_else(|| "db_rows".into());
                    let index = s.index.clone()
                        .ok_or_else(|| anyhow!("es_index step missing `index` field"))?;
                    let op = crate::step::elasticsearch::EsOp::from_str(
                        s.op.as_deref().unwrap_or("upsert")
                    );
                    let model = s.model.as_deref()
                        .ok_or_else(|| anyhow!("es_index step missing `model` field"))?;
                    runner.push_boxed(registry.build_es_index(model, &reads, &index, op)?);
                }
                #[cfg(not(feature = "elasticsearch"))]
                return Err(anyhow!("es_index requires feature \"elasticsearch\" — add it to Cargo.toml"));
            }
            "es_fetch" => {
                #[cfg(feature = "elasticsearch")]
                {
                    let writes     = s.writes.clone().unwrap_or_else(|| "api_rows".into());
                    let index      = s.index.clone()
                        .ok_or_else(|| anyhow!("es_fetch step missing `index` field"))?;
                    let scroll_ttl = s.scroll_ttl.clone().unwrap_or_else(|| "1m".into());
                    let batch_size = s.batch_size.unwrap_or(500);
                    let model      = s.model.as_deref()
                        .ok_or_else(|| anyhow!("es_fetch step missing `model` field"))?;
                    runner.push_boxed(registry.build_es_fetch(
                        model, &writes, &index, None, &scroll_ttl, batch_size, s.append,
                    )?);
                }
                #[cfg(not(feature = "elasticsearch"))]
                return Err(anyhow!("es_fetch requires feature \"elasticsearch\" — add it to Cargo.toml"));
            }
            "kafka_produce" => {
                #[cfg(feature = "kafka")]
                {
                    let reads = s.reads.clone().unwrap_or_else(|| "db_rows".into());
                    let topic = s.topic.as_ref()
                        .map(|t| t.resolve())
                        .transpose()?
                        .ok_or_else(|| anyhow!("kafka_produce step missing `topic` field"))?;
                    let model = s.model.as_deref()
                        .ok_or_else(|| anyhow!("kafka_produce step missing `model` field"))?;
                    runner.push_boxed(registry.build_kafka_produce(
                        model, &reads, &topic, s.key_field.clone(),
                    )?);
                }
                #[cfg(not(feature = "kafka"))]
                return Err(anyhow!("kafka_produce requires feature \"kafka\" — add it to Cargo.toml"));
            }
            "kafka_consume" => {
                #[cfg(feature = "kafka")]
                {
                    let writes       = s.writes.clone().unwrap_or_else(|| "api_rows".into());
                    let topic        = s.topic.as_ref()
                        .map(|t| t.resolve())
                        .transpose()?
                        .ok_or_else(|| anyhow!("kafka_consume step missing `topic` field"))?;
                    let max_messages = s.max_messages.unwrap_or(500);
                    let timeout_ms   = s.timeout_ms.unwrap_or(5000);
                    let model        = s.model.as_deref()
                        .ok_or_else(|| anyhow!("kafka_consume step missing `model` field"))?;
                    runner.push_boxed(registry.build_kafka_consume(
                        model, &writes, &topic, max_messages, timeout_ms, s.append,
                    )?);
                }
                #[cfg(not(feature = "kafka"))]
                return Err(anyhow!("kafka_consume requires feature \"kafka\" — add it to Cargo.toml"));
            }
            "split_transform" => {
                if s.transforms.is_empty() {
                    return Err(anyhow!("split_transform step missing `transforms` array"));
                }
                let reads = s.reads.clone().unwrap_or_else(|| "api_rows".into());
                let entries = s.transforms.iter()
                    .map(|e| (e.transform.clone(), e.writes.clone(), e.append))
                    .collect();
                use crate::step::multi_transform::SplitTransformStep;
                runner.push(SplitTransformStep::new(reads, entries, Arc::clone(registry)));
            }
            "merge_slots" => {
                if s.merge_reads.is_empty() {
                    return Err(anyhow!("merge_slots step missing `merge_reads` array"));
                }
                let writes = s.writes.clone()
                    .ok_or_else(|| anyhow!("merge_slots step missing `writes` field"))?;
                let model = s.model.as_deref()
                    .ok_or_else(|| anyhow!("merge_slots step missing `model` field"))?;
                runner.push_boxed(registry.build_merge_slots(
                    model, s.merge_reads.clone(), &writes, s.append,
                )?);
            }
            "slot_to_json" => {
                let reads = s.reads.clone()
                    .ok_or_else(|| anyhow!("slot_to_json step missing `reads` field"))?;
                let writes = s.writes.clone()
                    .ok_or_else(|| anyhow!("slot_to_json step missing `writes` field"))?;
                let model = s.model.as_deref()
                    .ok_or_else(|| anyhow!("slot_to_json step missing `model` field"))?;
                runner.push_boxed(registry.build_slot_to_json(model, &reads, &writes, s.append)?);
            }
            "json_to_slot" => {
                let reads = s.reads.clone()
                    .ok_or_else(|| anyhow!("json_to_slot step missing `reads` field"))?;
                let writes = s.writes.clone()
                    .ok_or_else(|| anyhow!("json_to_slot step missing `writes` field"))?;
                let model = s.model.as_deref()
                    .ok_or_else(|| anyhow!("json_to_slot step missing `model` field"))?;
                runner.push_boxed(registry.build_json_to_slot(model, &reads, &writes, s.append)?);
            }
            "send_to_queue" => {
                let model = s.model.as_deref()
                    .ok_or_else(|| anyhow!("send_to_queue step missing `model` field"))?;
                let reads = s.reads.clone().unwrap_or_else(|| "db_rows".into());
                let queue = s.queue.clone()
                    .ok_or_else(|| anyhow!("send_to_queue step missing `queue` field"))?;
                let mut p = params.clone();
                p.insert("reads".into(), reads);
                p.insert("queue".into(), queue);
                runner.push_boxed(registry.build_queue_send(model, &p)?);
            }
            "sleep" => {
                let secs = s.secs.as_ref()
                    .map(|v| v.resolve_as::<u64>())
                    .transpose()?
                    .unwrap_or(60);
                runner.push(SleepStep::new(secs));
            }
            "raw_sql" => {
                let sql = s.sql.as_ref()
                    .map(|v| v.resolve())
                    .transpose()?
                    .unwrap_or_default();
                runner.push(RawSqlStep::new(sql, s.skip_if_empty));
            }
            other => {
                return Err(anyhow!("Unknown step type \"{other}\" in pipeline.toml"));
            }
        }
    }
    Ok(runner)
}



// ── InitJobExecutor ───────────────────────────────────────────────────────

/// Runs once at startup — after build_context and validate, before the trigger.
pub async fn run_init_job(
    cfg:      &InitJobConfig,
    ctx:      &Arc<JobContext>,
    registry: &TypeRegistry,
) -> Result<()> {
    for step in &cfg.steps {
        match step {
            InitStepConfig::RawSql { sql, skip_if_empty } => {
                let s = sql.resolve().unwrap_or_default();
                RawSqlStep::new(s, *skip_if_empty).run(ctx).await?;
            }
            InitStepConfig::Custom { hook } => {
                if let Some(f) = registry.get_post_hook(hook) {
                    f(Arc::clone(ctx)).await?;
                } else {
                    tracing::warn!(hook = %hook, "init_job: custom hook not registered — skipping");
                }
            }
            InitStepConfig::LogSummary => {
                LogSummaryStep.run(ctx).await?;
            }
        }
    }
    Ok(())
}

// ── PostJobExecutor ───────────────────────────────────────────────────────

pub async fn run_post_job(
    cfg: &PostJobConfig,
    ctx: &Arc<JobContext>,
    registry: &TypeRegistry,
) -> Result<()> {
    for step in &cfg.steps {
        match step {
            PostStepConfig::LogSummary => {
                LogSummaryStep.run(ctx).await?;
            }
            PostStepConfig::RawSql { sql, skip_if_empty } => {
                let s = sql.resolve().unwrap_or_default();
                RawSqlStep::new(s, *skip_if_empty).run(ctx).await?;
            }
            PostStepConfig::DrainQueue { queue } => {
                DrainQueueStep::new(queue).run(ctx).await?;
            }
            PostStepConfig::Custom { hook } => {
                if let Some(f) = registry.get_post_hook(hook) {
                    f(Arc::clone(ctx)).await?;
                } else {
                    tracing::warn!(hook = %hook, "Custom hook not registered — skipping");
                }
            }
        }
    }
    Ok(())
}

// ── run() — the single entry-point ───────────────────────────────────────

/// Call from a business crate's main.rs:
///
/// ```rust,no_run
/// # use sync_engine::TypeRegistry;
/// #[tokio::main]
/// async fn main() -> anyhow::Result<()> {
///     let mut reg = TypeRegistry::new();
///     // register_envelope / register_transform / register_model
///     sync_engine::run("pipeline.toml", reg).await
/// }
/// ```
pub async fn run(path: &str, registry: TypeRegistry) -> Result<()> {
    let raw = std::fs::read_to_string(path)
        .with_context(|| format!("Cannot read {path}"))?;
    let cfg: PipelineConfig = toml::from_str(&raw)
        .with_context(|| format!("Cannot parse {path}"))?;

    let cfg = Arc::new(cfg);
    let reg = Arc::new(registry);

    validate(&cfg, &reg)?;
    let ctx = build_context(&cfg).await?;

    // Run init_job once before the trigger loop starts.
    if let Some(ref init) = cfg.init_job {
        info!("Running init_job ({} steps)", init.steps.len());
        run_init_job(init, &ctx, &reg).await
            .context("init_job failed")?;
        info!("init_job complete");
    }

    // Resolve trigger: prefer [job.trigger], fall back to legacy [job.scheduler].
    let trigger = resolve_trigger(&cfg)?;

    match trigger {
        ResolvedTrigger::Cron(cron)             => run_cron(cfg, ctx, reg, cron).await,
        ResolvedTrigger::Once                   => run_once(cfg, ctx, reg).await,
        ResolvedTrigger::Webhook { host, port } => run_webhook(cfg, ctx, reg, host, port).await,
        ResolvedTrigger::PgNotify(channel)      => run_pg_notify(cfg, ctx, reg, channel).await,
        #[cfg(feature = "kafka")]
        ResolvedTrigger::Kafka(topic)           => run_kafka_trigger(cfg, ctx, reg, topic).await,
    }
}

// ── Trigger resolution ────────────────────────────────────────────────────

enum ResolvedTrigger {
    Cron(String),
    Once,
    Webhook { host: String, port: u16 },
    PgNotify(String),
    #[cfg(feature = "kafka")]
    Kafka(String),
}

fn resolve_trigger(cfg: &PipelineConfig) -> Result<ResolvedTrigger> {
    if let Some(trigger) = cfg.job.as_ref().and_then(|j| j.trigger.as_ref()) {
        return match trigger {
            TriggerConfig::Cron { cron } => Ok(ResolvedTrigger::Cron(cron.resolve()?)),
            TriggerConfig::Once => Ok(ResolvedTrigger::Once),
            TriggerConfig::Webhook { host, port } => Ok(ResolvedTrigger::Webhook {
                host: host.clone(),
                port: *port,
            }),
            TriggerConfig::PgNotify { channel } => {
                Ok(ResolvedTrigger::PgNotify(channel.resolve()?))
            }
            #[cfg(feature = "kafka")]
            TriggerConfig::Kafka { topic } => Ok(ResolvedTrigger::Kafka(topic.resolve()?)),
        };
    }
    if let Some(cron_str) = cfg.job.as_ref()
        .and_then(|j| j.scheduler.as_ref())
        .map(|s| s.cron.resolve())
        .transpose()?
    {
        return Ok(ResolvedTrigger::Cron(cron_str));
    }
    Ok(ResolvedTrigger::Cron("0 */30 * * * *".to_owned()))
}

// ── Trigger loop: cron ────────────────────────────────────────────────────

async fn run_cron(
    cfg:  Arc<PipelineConfig>,
    ctx:  Arc<JobContext>,
    reg:  Arc<TypeRegistry>,
    cron: String,
) -> Result<()> {
    let lock: Arc<Mutex<()>> = Arc::new(Mutex::new(()));
    let mut scheduler = JobScheduler::new().await?;

    scheduler.add(Job::new_async(cron.as_str(), move |_, _| {
        let cfg  = Arc::clone(&cfg);
        let ctx  = Arc::clone(&ctx);
        let reg  = Arc::clone(&reg);
        let lock = Arc::clone(&lock);
        Box::pin(async move {
            let _guard = match lock.try_lock() {
                Ok(g)  => g,
                Err(_) => { tracing::debug!("Previous job running — skipping"); return; }
            };
            if let Err(e) = run_one_tick(&cfg, &ctx, &reg).await {
                tracing::error!(error = %e, "Job tick failed");
            }
        })
    })?).await?;

    scheduler.start().await?;
    info!("Trigger: cron ({cron}). Ctrl-C to exit.");
    tokio::signal::ctrl_c().await?;
    scheduler.shutdown().await?;
    Ok(())
}

// ── Trigger loop: once ────────────────────────────────────────────────────

async fn run_once(
    cfg: Arc<PipelineConfig>,
    ctx: Arc<JobContext>,
    reg: Arc<TypeRegistry>,
) -> Result<()> {
    info!("Trigger: once — running single tick then exiting.");
    run_one_tick(&cfg, &ctx, &reg).await?;
    info!("Once trigger complete.");
    Ok(())
}

// ── Trigger loop: webhook ─────────────────────────────────────────────────
//
// Binds a TCP listener on host:port. Any HTTP POST to any path fires a tick.
// Non-POST requests receive 405. The response is always sent before the tick
// runs so the caller is never left waiting.
//
// No new dependencies — uses tokio's raw TcpListener + minimal HTTP parsing.

async fn run_webhook(
    cfg:  Arc<PipelineConfig>,
    ctx:  Arc<JobContext>,
    reg:  Arc<TypeRegistry>,
    host: String,
    port: u16,
) -> Result<()> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    let addr = format!("{host}:{port}");
    let listener = TcpListener::bind(&addr).await
        .with_context(|| format!("Webhook: cannot bind {addr}"))?;
    info!("Trigger: webhook listening on http://{addr}  (POST any path to fire a tick)");

    let lock: Arc<Mutex<()>> = Arc::new(Mutex::new(()));

    loop {
        let (mut socket, peer) = listener.accept().await?;
        let cfg  = Arc::clone(&cfg);
        let ctx  = Arc::clone(&ctx);
        let reg  = Arc::clone(&reg);
        let lock = Arc::clone(&lock);

        tokio::spawn(async move {
            // Read just enough to determine the HTTP method.
            let mut buf = [0u8; 512];
            let n = match socket.read(&mut buf).await {
                Ok(n) => n,
                Err(_) => return,
            };
            let head = std::str::from_utf8(&buf[..n]).unwrap_or("");
            let is_post = head.starts_with("POST ");

            let (status, body) = if is_post {
                ("202 Accepted", "tick queued\n")
            } else {
                ("405 Method Not Allowed", "POST required\n")
            };

            let response = format!(
                "HTTP/1.1 {status}\r\nContent-Length: {}\r\nContent-Type: text/plain\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
            let _ = socket.write_all(response.as_bytes()).await;
            drop(socket); // close connection before running tick

            if !is_post {
                return;
            }

            tracing::info!(peer = %peer, "Webhook: received POST — firing tick");
            let _guard = match lock.try_lock() {
                Ok(g)  => g,
                Err(_) => { tracing::debug!("Previous tick still running — skipping webhook trigger"); return; }
            };
            if let Err(e) = run_one_tick(&cfg, &ctx, &reg).await {
                tracing::error!(error = %e, "Webhook tick failed");
            }
        });
    }
}

// ── Trigger loop: pg_notify ───────────────────────────────────────────────
//
// Uses sqlx::PgListener (already a transitive dep via the postgres feature).
// Listens on the configured channel; each NOTIFY fires one tick.
// Mutex-skips overlapping ticks the same way cron does.

async fn run_pg_notify(
    cfg:     Arc<PipelineConfig>,
    ctx:     Arc<JobContext>,
    reg:     Arc<TypeRegistry>,
    channel: String,
) -> Result<()> {
    #[cfg(not(feature = "postgres"))]
    return Err(anyhow!("trigger type=\"pg_notify\" requires feature \"postgres\" — add it to Cargo.toml"));

    #[cfg(feature = "postgres")]
    {
        // Reuse the postgres URL from [resources].
        let pg_url = cfg.resources.iter()
            .find_map(|(_, def)| {
                if let ResourceDef::Postgres { url, .. } = def { Some(url.resolve()) }
                else { None }
            })
            .ok_or_else(|| anyhow!(
                "trigger type=\"pg_notify\" requires a [resources.pg] postgres resource"
            ))??;

        let mut listener = sqlx::postgres::PgListener::connect(&pg_url).await
            .context("pg_notify: failed to connect PgListener")?;
        listener.listen(&channel).await
            .with_context(|| format!("pg_notify: failed to LISTEN on channel \"{channel}\""))?;

        info!("Trigger: pg_notify — LISTEN \"{channel}\". Ctrl-C to exit.");

        let lock: Arc<Mutex<()>> = Arc::new(Mutex::new(()));

        loop {
            tokio::select! {
                notify = listener.recv() => {
                    match notify {
                        Ok(n) => {
                            tracing::info!(
                                channel = %n.channel(),
                                payload = %n.payload(),
                                "pg_notify: received NOTIFY — firing tick"
                            );
                            let cfg  = Arc::clone(&cfg);
                            let ctx  = Arc::clone(&ctx);
                            let reg  = Arc::clone(&reg);
                            let lock = Arc::clone(&lock);
                            tokio::spawn(async move {
                                let _guard = match lock.try_lock() {
                                    Ok(g)  => g,
                                    Err(_) => {
                                        tracing::debug!("Previous tick still running — skipping pg_notify trigger");
                                        return;
                                    }
                                };
                                if let Err(e) = run_one_tick(&cfg, &ctx, &reg).await {
                                    tracing::error!(error = %e, "pg_notify tick failed");
                                }
                            });
                        }
                        Err(e) => {
                            tracing::warn!(error = %e, "pg_notify: listener error — reconnecting");
                            // sqlx::PgListener auto-reconnects on the next recv() call
                        }
                    }
                }
                _ = tokio::signal::ctrl_c() => {
                    info!("pg_notify: shutting down.");
                    break;
                }
            }
        }
        Ok(())
    }
}

async fn run_one_tick(
    cfg: &Arc<PipelineConfig>,
    ctx: &Arc<JobContext>,
    reg: &Arc<TypeRegistry>,
) -> Result<()> {
    // Clear job-scoped slots at the start of every tick so summary counts
    // from the previous run don't bleed into this one.
    // Pipeline-scoped slots are intentionally NOT cleared — they survive ticks.
    ctx.clear_job_slots().await;

    // Pre-job: spawn consumers if configured
    for step in &cfg.pre_job.steps {
        match step {
            PreStepConfig::SpawnConsumer { queue, model, commit_mode, accum_slot } => {
                #[cfg(feature = "postgres")]
                {
                    let mode = match commit_mode {
                        CommitModeStr::PerBatch       => CommitMode::PerBatch,
                        CommitModeStr::DrainInPostJob => CommitMode::DrainInPostJob,
                    };
                    let consumer_step = reg.build_consumer(
                        model, queue, mode, accum_slot.clone(),
                    )?;
                    consumer_step.run(ctx).await
                        .with_context(|| format!("spawn_consumer for queue \"{queue}\" failed"))?;
                }
                #[cfg(not(feature = "postgres"))]
                tracing::warn!(queue = %queue, model = %model, "spawn_consumer requires feature \"postgres\" — skipped");
            }
        }
    }

    // Main job
    let retry_steps    = build_steps(&cfg.main_job.retry_steps,       reg)?;
    let post_window    = build_steps(&cfg.main_job.post_window_steps,  reg)?;
    let post_loop      = build_steps(&cfg.main_job.post_loop_steps,    reg)?;
    let window_cfg     = build_window_cfg(cfg)?;

    let mut runner = MainJobRunner {
        window_cfg,
        retry_steps,
        post_window_steps: post_window,
        post_loop_steps:   post_loop,
    };

    let _summary = runner.run(ctx).await?;

    // Post-job
    run_post_job(&cfg.post_job, ctx, reg).await?;

    Ok(())
}

// ── Trigger loop: kafka ───────────────────────────────────────────────────
//
// Subscribes to a Kafka topic; each message received fires one tick.
// Commits the offset after the tick completes (at-least-once delivery).
// Mutex-skips overlapping ticks the same way cron and pg_notify do.

#[cfg(feature = "kafka")]
async fn run_kafka_trigger(
    cfg:   Arc<PipelineConfig>,
    ctx:   Arc<JobContext>,
    reg:   Arc<TypeRegistry>,
    topic: String,
) -> Result<()> {
    use rdkafka::consumer::Consumer;
    use rdkafka::Message;

    let consumer = ctx.kafka_consumer()
        .ok_or_else(|| anyhow!(
            "trigger type=\"kafka\" requires a [resources.*] kafka entry with `topic` set"
        ))?;

    info!("Trigger: kafka — consuming topic \"{topic}\". Ctrl-C to exit.");

    let lock: Arc<Mutex<()>> = Arc::new(Mutex::new(()));

    loop {
        tokio::select! {
            msg = async {
                use futures::StreamExt as _;
                consumer.stream().next().await
            } => {
                match msg {
                    Some(Ok(m)) => {
                        let offset  = m.offset();
                        let part    = m.partition();
                        tracing::info!(
                            topic = %topic, partition = part, offset = offset,
                            "Kafka trigger: message received — firing tick"
                        );

                        let cfg  = Arc::clone(&cfg);
                        let ctx  = Arc::clone(&ctx);
                        let reg  = Arc::clone(&reg);
                        let lock = Arc::clone(&lock);

                        let _guard = match lock.try_lock() {
                            Ok(g)  => g,
                            Err(_) => {
                                tracing::debug!(
                                    "Previous tick still running — skipping kafka trigger"
                                );
                                // Still commit so we don't re-trigger on this message
                                consumer.store_offset_from_message(&m)
                                    .unwrap_or_else(|e| tracing::warn!(error=%e, "offset store failed"));
                                continue;
                            }
                        };

                        if let Err(e) = run_one_tick(&cfg, &ctx, &reg).await {
                            tracing::error!(error = %e, "Kafka trigger tick failed");
                        }

                        consumer.store_offset_from_message(&m)
                            .unwrap_or_else(|e| tracing::warn!(error=%e, "offset store failed"));
                        consumer.commit_consumer_state(rdkafka::consumer::CommitMode::Async)
                            .unwrap_or_else(|e| tracing::warn!(error=%e, "offset commit failed"));
                    }
                    Some(Err(e)) => {
                        tracing::warn!(error = %e, "Kafka trigger: consume error");
                    }
                    None => {
                        tracing::info!("Kafka trigger: stream ended — exiting.");
                        break;
                    }
                }
            }
            _ = tokio::signal::ctrl_c() => {
                info!("Kafka trigger: shutting down.");
                break;
            }
        }
    }
    Ok(())
}
