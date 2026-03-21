// sync-engine/src/pipeline_runner.rs
//
// Parses the new pipeline.toml format (step-based, ConfigValue-aware) and
// assembles a JobContext + MainJobRunner ready to execute.
//
// run_from_pipeline_toml — the single entry-point for a business crate's
// main.rs. Reads pipeline.toml, builds everything, wires the cron scheduler
// with mutex-skip, and drives run_job on each tick.

use anyhow::{anyhow, Context, Result};
use reqwest::Client;
use serde::Deserialize;
use sqlx::postgres::PgPoolOptions;
use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tokio_cron_scheduler::{Job, JobScheduler};
use tracing::info;

use crate::components::auth::OAuth2Auth;
use crate::config_value::ConfigValue;
use crate::context::{Connections, JobContext};
use crate::job::JobSummary;
use crate::runner::WindowConfig;
use crate::slot::SlotScope;

// ── Re-export for backwards compat ────────────────────────────────────────

pub use crate::context::Connections as StandardConnections;

/// Config struct that satisfies the old HasIteratorCfg / HasRetryCfg traits.
pub struct ResolvedIteratorCfg {
    pub start_interval: i64,
    pub end_interval: i64,
    pub interval_limit: i64,
    pub window_sleep_secs: u64,
    pub max_attempts: usize,
    pub base_backoff_secs: u64,
}

impl crate::standard_job::HasIteratorCfg for ResolvedIteratorCfg {
    fn start_interval(&self) -> i64 {
        self.start_interval
    }
    fn end_interval(&self) -> i64 {
        self.end_interval
    }
    fn interval_limit(&self) -> i64 {
        self.interval_limit
    }
    fn window_sleep_secs(&self) -> u64 {
        self.window_sleep_secs
    }
}
impl crate::standard_job::HasRetryCfg for ResolvedIteratorCfg {
    fn max_attempts(&self) -> usize {
        self.max_attempts
    }
    fn base_backoff_secs(&self) -> u64 {
        self.base_backoff_secs
    }
}

// ── TOML schema ───────────────────────────────────────────────────────────

#[derive(Debug, Deserialize)]
pub struct PipelineConfig {
    pub job: Option<JobMeta>,
    pub slots: Option<HashMap<String, SlotDef>>,
    pub pre_job: PreJobConfig,
    pub main_job: MainJobConfig,
    pub post_job: PostJobConfig,
    pub scheduler: SchedulerConfig,
}

#[derive(Debug, Deserialize)]
pub struct JobMeta {
    pub name: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct SlotDef {
    pub scope: SlotScopeStr,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SlotScopeStr {
    Window,
    Job,
    Pipeline,
}

impl From<SlotScopeStr> for SlotScope {
    fn from(s: SlotScopeStr) -> Self {
        match s {
            SlotScopeStr::Window => SlotScope::Window,
            SlotScopeStr::Job => SlotScope::Job,
            SlotScopeStr::Pipeline => SlotScope::Pipeline,
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct PreJobConfig {
    pub steps: Vec<PreStepConfig>,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum PreStepConfig {
    PgConnect {
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
    SpawnConsumer {
        queue: String,
        model: String,
        commit_mode: CommitModeStr,
        accum_slot: Option<String>,
        #[serde(default = "default_queue_cap")]
        capacity: usize,
    },
    RegisterQueue {
        name: String,
        #[serde(default = "default_queue_cap")]
        capacity: usize,
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
fn default_queue_cap() -> usize {
    256
}

#[derive(Debug, Deserialize, Clone, Copy)]
#[serde(rename_all = "snake_case")]
pub enum CommitModeStr {
    PerBatch,
    DrainInPostJob,
}

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

#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum MainStepConfig {
    FetchJson {
        endpoint: ConfigValue,
        #[serde(default)]
        realm_type: Option<ConfigValue>,
        writes: String,
        #[serde(default)]
        append: bool,
    },
    Transform {
        reads: String,
        writes: String,
        mapper: String,
        #[serde(default)]
        append: bool,
    },
    TxUpsert {
        reads: String,
        model: String,
    },
    SendToQueue {
        reads: String,
        queue: String,
    },
    Sleep {
        secs: ConfigValue,
    },
    RawSql {
        sql: ConfigValue,
        #[serde(default)]
        skip_if_empty: bool,
    },
}

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

#[derive(Debug, Deserialize)]
pub struct SchedulerConfig {
    pub cron: ConfigValue,
}

// ── PostJobExecutor (kept for custom hooks) ───────────────────────────────

pub struct PostJobExecutor {
    pub steps: Vec<PostStepConfig>,
    pub custom_hooks: HashMap<
        String,
        Box<
            dyn Fn(JobSummary) -> std::pin::Pin<Box<dyn Future<Output = Result<()>> + Send>>
                + Send
                + Sync,
        >,
    >,
}

impl PostJobExecutor {
    pub fn new(steps: Vec<PostStepConfig>) -> Self {
        Self {
            steps,
            custom_hooks: HashMap::new(),
        }
    }
    pub fn register_hook<F, Fut>(&mut self, name: &str, f: F)
    where
        F: Fn(JobSummary) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<()>> + Send + 'static,
    {
        self.custom_hooks
            .insert(name.to_owned(), Box::new(move |s| Box::pin(f(s))));
    }
}

// ── Context builder ───────────────────────────────────────────────────────

/// Build a JobContext from the [pre_job] section of pipeline.toml.
/// Establishes connections and declares slots.
pub async fn build_context(cfg: &PipelineConfig) -> Result<Arc<JobContext>> {
    let job_name = cfg
        .job
        .as_ref()
        .and_then(|j| j.name.as_deref())
        .unwrap_or("unnamed")
        .to_owned();

    // ── Resolve connections from pre_job steps ────────────────────────────
    let mut db_pool: Option<sqlx::PgPool> = None;
    let mut auth_opt: Option<Arc<OAuth2Auth>> = None;
    let mut endpoint_str = String::new();
    let mut extra_query: Vec<(String, String)> = Vec::new();

    // Build http first (needed for oauth2)
    let http = {
        let mut timeout_secs = 620u64;
        let mut keepalive_secs = 30u64;
        for step in &cfg.pre_job.steps {
            if let PreStepConfig::HttpClient {
                timeout_secs: t,
                keepalive_secs: k,
            } = step
            {
                timeout_secs = *t;
                keepalive_secs = *k;
            }
        }
        Client::builder()
            .timeout(Duration::from_secs(timeout_secs))
            .tcp_keepalive(Duration::from_secs(keepalive_secs))
            .build()
            .context("HTTP client build failed")?
    };

    for step in &cfg.pre_job.steps {
        match step {
            PreStepConfig::PgConnect {
                url,
                max_connections,
            } => {
                let url_str = url.resolve()?;
                info!("pre_job: connecting to postgres");
                db_pool = Some(
                    PgPoolOptions::new()
                        .max_connections(*max_connections)
                        .connect(&url_str)
                        .await
                        .context("Postgres connect failed")?,
                );
            }
            PreStepConfig::Oauth2 {
                token_url,
                client_id,
                client_secret,
            } => {
                info!("pre_job: creating oauth2 client");
                auth_opt = Some(Arc::new(OAuth2Auth::new(
                    http.clone(),
                    token_url.resolve()?,
                    client_id.resolve()?,
                    client_secret.resolve()?,
                )));
            }
            PreStepConfig::HttpClient { .. } => { /* already handled above */ }
            _ => { /* queues + consumers handled after context is built */ }
        }
    }

    // Resolve endpoint from main_job fetch step
    for step in &cfg.main_job.retry_steps {
        if let MainStepConfig::FetchJson {
            endpoint,
            realm_type,
            ..
        } = step
        {
            endpoint_str = endpoint.resolve()?;
            if let Some(rt) = realm_type {
                let v = rt.resolve().unwrap_or_default();
                if !v.is_empty() {
                    extra_query.push(("realm_type".to_owned(), v));
                }
            }
            break;
        }
    }

    let db = db_pool.ok_or_else(|| anyhow!("No pg_connect step in pre_job"))?;
    let auth = auth_opt.ok_or_else(|| anyhow!("No oauth2 step in pre_job"))?;

    let connections = Connections {
        db,
        auth,
        http,
        endpoint: endpoint_str,
        extra_query,
    };

    // ── Config map (resolved iterator values) ─────────────────────────────
    let iter = &cfg.main_job.iterator;
    let mut config = HashMap::new();
    config.insert(
        "iterator.start_interval".to_owned(),
        iter.start_interval.resolve()?,
    );
    config.insert(
        "iterator.end_interval".to_owned(),
        iter.end_interval.resolve()?,
    );
    config.insert(
        "iterator.interval_limit".to_owned(),
        iter.interval_limit.resolve()?,
    );
    config.insert("iterator.sleep_secs".to_owned(), iter.sleep_secs.resolve()?);

    let ctx = Arc::new(JobContext::new(connections, config, job_name));

    // ── Declare slots ─────────────────────────────────────────────────────
    {
        let mut slot_map = ctx.slots.write().await;

        // Always declare summary slots — steps write/read these regardless
        // of what the user declares in pipeline.toml [slots].
        slot_map.declare("summary.windows_processed", SlotScope::Job);
        slot_map.declare("summary.error_count", SlotScope::Job);
        slot_map.declare("summary.total_fetched", SlotScope::Job);
        slot_map.declare("summary.total_upserted", SlotScope::Job);
        slot_map.declare("summary.total_skipped", SlotScope::Job);
        slot_map.declare("window.fetched", SlotScope::Window);
        slot_map.declare("window.upserted", SlotScope::Window);
        slot_map.declare("window.skipped", SlotScope::Window);

        // User-declared slots from pipeline.toml [slots]
        if let Some(slots) = &cfg.slots {
            for (key, def) in slots {
                let scope = match def.scope {
                    SlotScopeStr::Window => SlotScope::Window,
                    SlotScopeStr::Job => SlotScope::Job,
                    SlotScopeStr::Pipeline => SlotScope::Pipeline,
                };
                slot_map.declare(key, scope);
            }
        }
    }

    // ── Register queues ───────────────────────────────────────────────────
    for step in &cfg.pre_job.steps {
        match step {
            PreStepConfig::RegisterQueue { name, capacity } => {
                ctx.register_queue(name, *capacity).await;
            }
            PreStepConfig::SpawnConsumer {
                queue, capacity, ..
            } => {
                ctx.register_queue(queue, *capacity).await;
            }
            _ => {}
        }
    }

    Ok(ctx)
}

/// Build window config from the parsed iterator + retry sections.
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

// ── run_from_pipeline_toml ────────────────────────────────────────────────

/// Single entry-point for a business crate's main.rs.
///
/// The `job_factory` receives the parsed config and a pre-built `Arc<JobContext>`
/// and returns a future that drives one full job execution.
///
/// For the simple typed path (StandardJob), use the convenience wrapper
/// `run_typed_from_pipeline_toml` below.
pub async fn run_from_pipeline_toml<F, Fut>(path: &str, job_factory: F) -> Result<()>
where
    F: Fn(Arc<PipelineConfig>, Arc<JobContext>) -> Fut + Send + Sync + Clone + 'static,
    Fut: Future<Output = Result<()>> + Send + 'static,
{
    let raw = std::fs::read_to_string(path).with_context(|| format!("Cannot read {path}"))?;
    let pipeline_cfg: PipelineConfig =
        toml::from_str(&raw).with_context(|| format!("Cannot parse {path}"))?;

    let cron = pipeline_cfg.scheduler.cron.resolve()?;
    let cfg = Arc::new(pipeline_cfg);

    // Build context once per scheduler lifecycle (Pipeline-scoped slots persist)
    let ctx = build_context(&cfg).await?;

    let lock: Arc<Mutex<()>> = Arc::new(Mutex::new(()));
    let mut scheduler = JobScheduler::new().await?;

    scheduler
        .add(Job::new_async(cron.as_str(), move |_, _| {
            let cfg = Arc::clone(&cfg);
            let ctx = Arc::clone(&ctx);
            let lock = Arc::clone(&lock);
            let factory = job_factory.clone();
            Box::pin(async move {
                let _guard = match lock.try_lock() {
                    Ok(g) => g,
                    Err(_) => {
                        tracing::debug!("Previous job still running — skipping tick");
                        return;
                    }
                };
                if let Err(e) = factory(cfg, ctx).await {
                    tracing::error!(error = %e, "Job failed");
                }
            })
        })?)
        .await?;

    scheduler.start().await?;
    info!("Scheduled ({}). Ctrl-C to exit.", cron);
    tokio::signal::ctrl_c().await?;
    scheduler.shutdown().await?;
    Ok(())
}
