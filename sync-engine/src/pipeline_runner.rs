// sync-engine/src/pipeline_runner.rs
//
// New TOML schema — four sections map 1:1 to the job pseudocode:
//   [resources]       — named connections (pg, auth, http, svc)
//   [slots] / [queues]— named typed data channels
//   [pre_job]         — init_resources + optional consumer spawn
//   [main_job]        — iterator, retry, [[retry_steps]], post_window, post_loop
//   [post_job]        — [[steps]]
//   [job]             — name, [job.scheduler]
//
// run(path, registry) — the single entry-point for a business crate's main.rs.

use anyhow::{anyhow, Context, Result};
use reqwest::Client;
use serde::Deserialize;
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
use crate::step::consumer::CommitMode;
use crate::step::control::{DrainQueueStep, LogSummaryStep, RawSqlStep, SleepStep};
use crate::step::Step;
use crate::step::StepRunner;

// ── Re-exports for backwards compat ───────────────────────────────────────
pub use crate::context::Connections as StandardConnections;

// ── TOML schema ───────────────────────────────────────────────────────────

#[derive(Debug, Deserialize)]
pub struct PipelineConfig {
    pub job: Option<JobMeta>,
    #[serde(default)]
    pub resources: HashMap<String, ResourceDef>,
    #[serde(default)]
    pub slots: HashMap<String, SlotDef>,
    #[serde(default)]
    pub queues: HashMap<String, QueueDef>,
    pub pre_job: PreJobConfig,
    pub main_job: MainJobConfig,
    pub post_job: PostJobConfig,
}

// ── [job] ─────────────────────────────────────────────────────────────────

#[derive(Debug, Deserialize)]
pub struct JobMeta {
    pub name: Option<String>,
    pub scheduler: Option<SchedulerConfig>,
}

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
    Postgres {
        url: ConfigValue,
        #[serde(default = "default_max_conn")]
        max_connections: u32,
    },
    Oauth2 {
        token_url: ConfigValue,
        client_id: ConfigValue,
        client_secret: ConfigValue,
    },
    HttpClient {
        #[serde(default = "default_timeout")]
        timeout_secs: u64,
        #[serde(default = "default_keepalive")]
        keepalive_secs: u64,
    },
    HttpService {
        /// Name of the http_client resource to use
        http: String,
        /// Name of the oauth2 resource to use
        auth: String,
        endpoint: ConfigValue,
        #[serde(default)]
        realm_type: Option<ConfigValue>,
    },
}

fn default_max_conn() -> u32 {
    5
}
fn default_timeout() -> u64 {
    620
}
fn default_keepalive() -> u64 {
    30
}

// ── [slots] ───────────────────────────────────────────────────────────────

#[derive(Debug, Deserialize)]
pub struct SlotDef {
    /// Schema.toml record name — for documentation only at runtime.
    #[serde(rename = "type")]
    pub record_type: Option<String>,
    pub scope: SlotScopeStr,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SlotScopeStr {
    Window,
    Job,
    Pipeline,
}

impl From<&SlotScopeStr> for SlotScope {
    fn from(s: &SlotScopeStr) -> Self {
        match s {
            SlotScopeStr::Window => SlotScope::Window,
            SlotScopeStr::Job => SlotScope::Job,
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
        url: ConfigValue,
        exchange: ConfigValue,
        routing_key: ConfigValue,
    },
}
fn default_queue_cap() -> usize {
    256
}

// ── [pre_job] ─────────────────────────────────────────────────────────────

#[derive(Debug, Deserialize)]
pub struct PreJobConfig {
    /// If true, the engine builds all [resources] before pre_job steps.
    #[serde(default = "default_true")]
    pub init_resources: bool,
    #[serde(default)]
    pub steps: Vec<PreStepConfig>,
}
fn default_true() -> bool {
    true
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum PreStepConfig {
    SpawnConsumer {
        queue: String,
        model: String,
        commit_mode: CommitModeStr,
        accum_slot: Option<String>,
    },
}

#[derive(Debug, Deserialize, Clone, Copy)]
#[serde(rename_all = "snake_case")]
pub enum CommitModeStr {
    PerBatch,
    DrainInPostJob,
}

// ── [main_job] ────────────────────────────────────────────────────────────

#[derive(Debug, Deserialize)]
pub struct MainJobConfig {
    pub iterator: IteratorConfig,
    pub retry: RetryConfig,
    pub retry_steps: Vec<MainStepConfig>,
    #[serde(default)]
    pub post_window_steps: Vec<MainStepConfig>,
    #[serde(default)]
    pub post_loop_steps: Vec<MainStepConfig>,
}

#[derive(Debug, Deserialize)]
pub struct IteratorConfig {
    #[serde(rename = "type")]
    pub iter_type: String,
    pub start_interval: ConfigValue,
    pub end_interval: ConfigValue,
    pub interval_limit: ConfigValue,
    pub sleep_secs: ConfigValue,
}

#[derive(Debug, Deserialize)]
pub struct RetryConfig {
    #[serde(default = "default_attempts")]
    pub max_attempts: usize,
    #[serde(default = "default_backoff")]
    pub backoff_secs: u64,
}
fn default_attempts() -> usize {
    5
}
fn default_backoff() -> u64 {
    2
}

/// A step entry in [[main_job.retry_steps]] or [[main_job.post_loop_steps]].
/// The `type` field drives dispatch; all other fields are passed as `params`.
#[derive(Debug, Deserialize)]
pub struct MainStepConfig {
    #[serde(rename = "type")]
    pub step_type: String,
    /// The envelope type name for type="fetch" (e.g. "ApiUserResponse")
    pub envelope: Option<String>,
    /// The transform type name for type="transform" (e.g. "UserTransform")
    pub transform: Option<String>,
    /// The model type name for type="tx_upsert" (e.g. "DbUser")
    pub model: Option<String>,
    #[serde(default)]
    pub reads: Option<String>,
    #[serde(default)]
    pub writes: Option<String>,
    #[serde(default)]
    pub append: bool,
    /// For type="sleep"
    pub secs: Option<ConfigValue>,
    /// For type="raw_sql"
    pub sql: Option<ConfigValue>,
    #[serde(default)]
    pub skip_if_empty: bool,
    /// For type="send_to_queue"
    pub queue: Option<String>,
}

// ── [post_job] ────────────────────────────────────────────────────────────

#[derive(Debug, Deserialize)]
pub struct PostJobConfig {
    pub steps: Vec<PostStepConfig>,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum PostStepConfig {
    LogSummary,
    RawSql {
        sql: ConfigValue,
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
    pub db: Option<sqlx::PgPool>,
    pub http: Option<Client>,
    pub auth: Option<Arc<OAuth2Auth>>,
    pub endpoint: String,
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
    let all_channels: std::collections::HashSet<&str> = slot_names
        .iter()
        .chain(queue_names.iter())
        .copied()
        .collect();

    // Built-in slot names that are always available
    let builtin: std::collections::HashSet<&str> = [
        "summary.windows_processed",
        "summary.error_count",
        "summary.total_fetched",
        "summary.total_upserted",
        "summary.total_skipped",
        "window.fetched",
        "window.upserted",
        "window.skipped",
    ]
    .iter()
    .copied()
    .collect();

    let check_slot = |name: &str, context: &str, errors: &mut Vec<String>| {
        if !all_channels.contains(name) && !builtin.contains(name) {
            errors.push(format!(
                "{context}: slot/queue \"{name}\" not declared in [slots] or [queues]"
            ));
        }
    };

    // Validate all step lists
    let all_steps = cfg
        .main_job
        .retry_steps
        .iter()
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
                match &step.model {
                    None => errors.push(format!("{ctx}: missing `model` field")),
                    Some(m) => if !registry.has_model(m) {
                        errors.push(format!(
                            "{ctx}: model \"{m}\" not registered — add registry.register_model::<{m}>(\"{m}\") in main.rs"
                        ));
                    }
                }
                if let Some(ref r) = step.reads { check_slot(r, &ctx, &mut errors); }
            }
            "send_to_queue" => {
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
            other => errors.push(format!("Unknown step type \"{other}\" — valid types: fetch, transform, tx_upsert, send_to_queue, sleep, raw_sql")),
        }
    }

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
    let job_name = cfg
        .job
        .as_ref()
        .and_then(|j| j.name.as_deref())
        .unwrap_or("unnamed")
        .to_owned();

    // ── Build resources in dependency order ───────────────────────────────
    // Pass 1: http_client (no deps)
    let mut http_clients: HashMap<String, Client> = HashMap::new();
    let mut auth_clients: HashMap<String, Arc<OAuth2Auth>> = HashMap::new();
    let mut db_pools: HashMap<String, sqlx::PgPool> = HashMap::new();
    let mut endpoint = String::new();
    let mut extra_query: Vec<(String, String)> = Vec::new();

    for (name, def) in &cfg.resources {
        if let ResourceDef::HttpClient {
            timeout_secs,
            keepalive_secs,
        } = def
        {
            info!("resource[{}]: building http_client", name);
            let client = Client::builder()
                .timeout(Duration::from_secs(*timeout_secs))
                .tcp_keepalive(Duration::from_secs(*keepalive_secs))
                .build()
                .context("HTTP client build failed")?;
            http_clients.insert(name.clone(), client);
        }
    }

    // Pass 2: oauth2 (needs http)
    for (name, def) in &cfg.resources {
        if let ResourceDef::Oauth2 {
            token_url,
            client_id,
            client_secret,
        } = def
        {
            // Use the first available http client
            let http = http_clients
                .values()
                .next()
                .cloned()
                .unwrap_or_else(|| Client::new());
            info!("resource[{}]: building oauth2 client", name);
            auth_clients.insert(
                name.clone(),
                Arc::new(OAuth2Auth::new(
                    http,
                    token_url.resolve()?,
                    client_id.resolve()?,
                    client_secret.resolve()?,
                )),
            );
        }
    }

    // Pass 3: postgres
    for (name, def) in &cfg.resources {
        if let ResourceDef::Postgres {
            url,
            max_connections,
        } = def
        {
            info!("resource[{}]: connecting to postgres", name);
            let pool = PgPoolOptions::new()
                .max_connections(*max_connections)
                .connect(&url.resolve()?)
                .await
                .context("Postgres connect failed")?;
            db_pools.insert(name.clone(), pool);
        }
    }

    // Pass 4: http_service (endpoint + realm_type)
    for (_name, def) in &cfg.resources {
        if let ResourceDef::HttpService {
            endpoint: ep,
            realm_type,
            ..
        } = def
        {
            endpoint = ep.resolve()?;
            if let Some(rt) = realm_type {
                let v = rt.resolve().unwrap_or_default();
                if !v.is_empty() {
                    extra_query.push(("realm_type".to_owned(), v));
                }
            }
        }
    }

    let db = db_pools
        .into_values()
        .next()
        .ok_or_else(|| anyhow!("No postgres resource defined"))?;
    let auth = auth_clients
        .into_values()
        .next()
        .ok_or_else(|| anyhow!("No oauth2 resource defined"))?;
    let http = http_clients
        .into_values()
        .next()
        .ok_or_else(|| anyhow!("No http_client resource defined"))?;

    let connections = Connections {
        db,
        auth,
        http,
        endpoint,
        extra_query,
    };

    // ── Config map ────────────────────────────────────────────────────────
    let iter = &cfg.main_job.iterator;
    let mut config = HashMap::new();
    config.insert(
        "iterator.start_interval".into(),
        iter.start_interval.resolve()?,
    );
    config.insert("iterator.end_interval".into(), iter.end_interval.resolve()?);
    config.insert(
        "iterator.interval_limit".into(),
        iter.interval_limit.resolve()?,
    );
    config.insert("iterator.sleep_secs".into(), iter.sleep_secs.resolve()?);

    let ctx = Arc::new(JobContext::new(connections, config, job_name));

    // ── Declare summary slots (always present) ────────────────────────────
    {
        let mut slots = ctx.slots.write().await;
        slots.declare("summary.windows_processed", SlotScope::Job);
        slots.declare("summary.error_count", SlotScope::Job);
        slots.declare("summary.total_fetched", SlotScope::Job);
        slots.declare("summary.total_upserted", SlotScope::Job);
        slots.declare("summary.total_skipped", SlotScope::Job);
        slots.declare("window.fetched", SlotScope::Window);
        slots.declare("window.upserted", SlotScope::Window);
        slots.declare("window.skipped", SlotScope::Window);

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
            QueueDef::Rabbitmq {
                url,
                exchange,
                routing_key,
            } => {
                let url_str = url.resolve()?;
                let exchange_str = exchange.resolve()?;
                let routing_key_str = routing_key.resolve()?;
                info!(
                    "queue[{}]: connecting to RabbitMQ exchange={}",
                    name, exchange_str
                );
                // Store the config in the context's queue map under a special key
                // so SpawnConsumerStep and SendToQueueStep can retrieve it.
                // We use a tokio channel as the in-process bridge; the producer
                // step serializes and publishes via lapin, consumer subscribes.
                use crate::transport::rabbitmq::{RabbitmqConfig, RabbitmqQueue};
                let rmq_cfg = RabbitmqConfig {
                    url: url_str,
                    exchange: exchange_str,
                    routing_key: routing_key_str,
                    queue_name: None,
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
                        )
                        .await?;
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
        start_interval: iter.start_interval.resolve_as::<i64>()?,
        end_interval: iter.end_interval.resolve_as::<i64>()?,
        interval_limit: iter.interval_limit.resolve_as::<i64>()?,
        sleep_secs: iter.sleep_secs.resolve_as::<u64>()?,
        max_attempts: cfg.main_job.retry.max_attempts,
        base_backoff_secs: cfg.main_job.retry.backoff_secs,
    })
}

/// Build typed steps from a slice of MainStepConfig using the TypeRegistry.
pub fn build_steps(steps: &[MainStepConfig], registry: &TypeRegistry) -> Result<StepRunner> {
    let mut runner = StepRunner::new();
    for s in steps {
        let mut params = HashMap::new();
        if let Some(ref r) = s.reads {
            params.insert("reads".into(), r.clone());
        }
        if let Some(ref w) = s.writes {
            params.insert("writes".into(), w.clone());
        }
        if s.append {
            params.insert("append".into(), "true".into());
        }

        match s.step_type.as_str() {
            "fetch" => {
                let env = s
                    .envelope
                    .as_deref()
                    .ok_or_else(|| anyhow!("fetch step missing `envelope` field"))?;
                runner.push_boxed(registry.build_fetch(env, &params)?);
            }
            "transform" => {
                let xfm = s
                    .transform
                    .as_deref()
                    .ok_or_else(|| anyhow!("transform step missing `transform` field"))?;
                runner.push_boxed(registry.build_transform(xfm, &params)?);
            }
            "tx_upsert" => {
                let model = s
                    .model
                    .as_deref()
                    .ok_or_else(|| anyhow!("tx_upsert step missing `model` field"))?;
                runner.push_boxed(registry.build_model_sink(model, &params)?);
            }
            "send_to_queue" => {
                let model = s.model.as_deref().ok_or_else(|| {
                    anyhow!("send_to_queue step missing `model` field — set model = \"DbUser\"")
                })?;
                let reads = s.reads.clone().unwrap_or_else(|| "db_rows".into());
                let queue = s
                    .queue
                    .clone()
                    .ok_or_else(|| anyhow!("send_to_queue step missing `queue` field"))?;
                let mut p = params.clone();
                p.insert("reads".into(), reads);
                p.insert("queue".into(), queue);
                runner.push_boxed(registry.build_queue_send(model, &p)?);
            }
            "sleep" => {
                let secs = s
                    .secs
                    .as_ref()
                    .map(|v| v.resolve_as::<u64>())
                    .transpose()?
                    .unwrap_or(60);
                runner.push(SleepStep::new(secs));
            }
            "raw_sql" => {
                let sql = s
                    .sql
                    .as_ref()
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
/// #[tokio::main]
/// async fn main() -> anyhow::Result<()> {
///     let mut reg = sync_engine::TypeRegistry::new();
///     reg.register_envelope::<ApiUserResponse>("ApiUserResponse");
///     reg.register_transform::<ApiUser, DbUser, UserTransform>("UserTransform");
///     reg.register_model::<DbUser>("DbUser");
///     sync_engine::run("pipeline.toml", reg).await
/// }
/// ```
pub async fn run(path: &str, registry: TypeRegistry) -> Result<()> {
    let raw = std::fs::read_to_string(path).with_context(|| format!("Cannot read {path}"))?;
    let cfg: PipelineConfig =
        toml::from_str(&raw).with_context(|| format!("Cannot parse {path}"))?;

    let cron = cfg
        .job
        .as_ref()
        .and_then(|j| j.scheduler.as_ref())
        .map(|s| s.cron.resolve())
        .transpose()?
        .unwrap_or_else(|| "0 */30 * * * *".to_owned());

    let cfg = Arc::new(cfg);
    let reg = Arc::new(registry);

    // Validate pipeline config against registry before starting the scheduler.
    // Catches typos and missing registrations at startup, not on first tick.
    validate(&cfg, &reg)?;

    // Build context once — pipeline-scoped slots survive cron ticks
    let ctx = build_context(&cfg).await?;

    let lock: Arc<Mutex<()>> = Arc::new(Mutex::new(()));
    let mut scheduler = JobScheduler::new().await?;

    scheduler
        .add(Job::new_async(cron.as_str(), move |_, _| {
            let cfg = Arc::clone(&cfg);
            let ctx = Arc::clone(&ctx);
            let reg = Arc::clone(&reg);
            let lock = Arc::clone(&lock);
            Box::pin(async move {
                let _guard = match lock.try_lock() {
                    Ok(g) => g,
                    Err(_) => {
                        tracing::debug!("Previous job running — skipping");
                        return;
                    }
                };
                if let Err(e) = run_one_tick(&cfg, &ctx, &reg).await {
                    tracing::error!(error = %e, "Job tick failed");
                }
            })
        })?)
        .await?;

    scheduler.start().await?;
    info!("Scheduled ({cron}). Ctrl-C to exit.");
    tokio::signal::ctrl_c().await?;
    scheduler.shutdown().await?;
    Ok(())
}

async fn run_one_tick(
    cfg: &Arc<PipelineConfig>,
    ctx: &Arc<JobContext>,
    reg: &Arc<TypeRegistry>,
) -> Result<()> {
    // Pre-job: spawn consumers if configured
    for step in &cfg.pre_job.steps {
        match step {
            PreStepConfig::SpawnConsumer {
                queue,
                model,
                commit_mode,
                accum_slot,
            } => {
                let mode = match commit_mode {
                    CommitModeStr::PerBatch => CommitMode::PerBatch,
                    CommitModeStr::DrainInPostJob => CommitMode::DrainInPostJob,
                };
                let consumer_step = reg.build_consumer(model, queue, mode, accum_slot.clone())?;
                consumer_step
                    .run(ctx)
                    .await
                    .with_context(|| format!("spawn_consumer for queue \"{queue}\" failed"))?;
            }
        }
    }

    // Main job
    let retry_steps = build_steps(&cfg.main_job.retry_steps, reg)?;
    let post_window = build_steps(&cfg.main_job.post_window_steps, reg)?;
    let post_loop = build_steps(&cfg.main_job.post_loop_steps, reg)?;
    let window_cfg = build_window_cfg(cfg)?;

    let mut runner = MainJobRunner {
        window_cfg,
        retry_steps,
        post_window_steps: post_window,
        post_loop_steps: post_loop,
    };

    let _summary = runner.run(ctx).await?;

    // Post-job
    run_post_job(&cfg.post_job, ctx, reg).await?;

    Ok(())
}
