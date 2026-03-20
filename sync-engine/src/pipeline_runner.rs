// sync-engine/src/pipeline_runner.rs
//
// StandardConnections — builds pool + auth + http from env vars declared in
//   pipeline.toml; no hand-written connections.rs needed in business crates.
//
// run_from_pipeline_toml — reads pipeline.toml, wires cron + mutex-skip,
//   calls the user-supplied job factory on each tick.
//   main.rs in a business crate becomes a single call to this function.

use anyhow::{Context, Result};
use reqwest::Client;
use serde::Deserialize;
use sqlx::{postgres::PgPoolOptions, PgPool};
use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tokio_cron_scheduler::{Job, JobScheduler};
use tracing::info;

use crate::components::auth::OAuth2Auth;
use crate::job::{Connections, JobSummary};
use crate::standard_job::HasConnections;

// ── TOML schema ───────────────────────────────────────────────────────────

#[derive(Debug, Deserialize)]
pub struct PipelineConfig {
    pub pre_job: PreJobConfig,
    pub main_job: MainJobConfig,
    pub post_job: PostJobConfig,
    pub scheduler: SchedulerConfig,
}

#[derive(Debug, Deserialize)]
pub struct PreJobConfig {
    pub connections: ConnectionsConfig,
}

#[derive(Debug, Deserialize)]
pub struct ConnectionsConfig {
    pub db: DbConnectionConfig,
    pub auth: AuthConnectionConfig,
    pub http: HttpConnectionConfig,
}

#[derive(Debug, Deserialize)]
pub struct DbConnectionConfig {
    pub url_env: String,
    #[serde(default = "default_max_connections")]
    pub max_connections: u32,
}
fn default_max_connections() -> u32 {
    5
}

#[derive(Debug, Deserialize)]
pub struct AuthConnectionConfig {
    pub token_url_env: String,
    pub client_id_env: String,
    pub client_secret_env: String,
}

#[derive(Debug, Deserialize)]
pub struct HttpConnectionConfig {
    #[serde(default = "default_http_timeout")]
    pub timeout_secs: u64,
    #[serde(default = "default_keepalive")]
    pub keepalive_secs: u64,
}
fn default_http_timeout() -> u64 {
    620
}
fn default_keepalive() -> u64 {
    30
}

#[derive(Debug, Deserialize)]
pub struct MainJobConfig {
    pub iterator: IteratorConfig,
    pub retry: RetryConfig,
    pub fetch: FetchConfig,
    pub transform: TransformConfig,
    pub sink: SinkConfig,
}

#[derive(Debug, Deserialize)]
pub struct IteratorConfig {
    pub r#type: String,
    pub start_env: String,
    pub end_env: String,
    pub limit_env: String,
    pub sleep_secs_env: String,
}

#[derive(Debug, Deserialize)]
pub struct RetryConfig {
    #[serde(default = "default_max_attempts")]
    pub max_attempts: usize,
    #[serde(default = "default_backoff_secs")]
    pub backoff_secs: u64,
}
fn default_max_attempts() -> usize {
    5
}
fn default_backoff_secs() -> u64 {
    2
}

#[derive(Debug, Deserialize)]
pub struct FetchConfig {
    pub r#type: String,
    pub endpoint_env: String,
    pub auth: String,
    #[serde(default)]
    pub static_query: Vec<(String, String)>,
    #[serde(default)]
    pub realm_type_env: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct TransformConfig {
    /// Name of the generated Transform struct (for docs / validation).
    pub mapping: String,
}

#[derive(Debug, Deserialize)]
pub struct SinkConfig {
    pub r#type: String,
    pub db: String,
    /// Name of the generated UpsertableInTx struct (for docs / validation).
    pub model: String,
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
        db: String,
        sql_env: String,
        #[serde(default)]
        skip_if_empty: bool,
    },
    Custom {
        hook: String,
    },
}

#[derive(Debug, Deserialize)]
pub struct SchedulerConfig {
    pub cron_env: String,
}

// ── StandardConnections ───────────────────────────────────────────────────

/// Built from `[pre_job.connections]` in pipeline.toml.
/// No hand-written connections.rs required in business crates.
pub struct StandardConnections {
    pub db: PgPool,
    pub auth: OAuth2Auth,
    pub http: Client,
    pub endpoint: String,
    pub extra_query: Vec<(String, String)>,
}

impl Connections for StandardConnections {}

impl HasConnections for StandardConnections {
    fn db_pool(&self) -> &PgPool {
        &self.db
    }
    fn auth(&self) -> &OAuth2Auth {
        &self.auth
    }
    fn http_client(&self) -> &Client {
        &self.http
    }
    fn endpoint(&self) -> &str {
        &self.endpoint
    }
    fn extra_query(&self) -> Vec<(String, String)> {
        self.extra_query.clone()
    }
}

impl StandardConnections {
    pub async fn from_pipeline(cfg: &PipelineConfig) -> Result<Self> {
        let db_url = std::env::var(&cfg.pre_job.connections.db.url_env)
            .with_context(|| format!("env var {} not set", cfg.pre_job.connections.db.url_env))?;

        info!("pre_job: connecting to postgres");
        let db = PgPoolOptions::new()
            .max_connections(cfg.pre_job.connections.db.max_connections)
            .connect(&db_url)
            .await
            .context("Postgres connect failed")?;

        let http = Client::builder()
            .timeout(Duration::from_secs(
                cfg.pre_job.connections.http.timeout_secs,
            ))
            .tcp_keepalive(Duration::from_secs(
                cfg.pre_job.connections.http.keepalive_secs,
            ))
            .build()
            .context("HTTP client build failed")?;

        let token_url =
            std::env::var(&cfg.pre_job.connections.auth.token_url_env).with_context(|| {
                format!(
                    "env var {} not set",
                    cfg.pre_job.connections.auth.token_url_env
                )
            })?;
        let client_id =
            std::env::var(&cfg.pre_job.connections.auth.client_id_env).with_context(|| {
                format!(
                    "env var {} not set",
                    cfg.pre_job.connections.auth.client_id_env
                )
            })?;
        let client_secret = std::env::var(&cfg.pre_job.connections.auth.client_secret_env)
            .with_context(|| {
                format!(
                    "env var {} not set",
                    cfg.pre_job.connections.auth.client_secret_env
                )
            })?;

        info!("pre_job: creating oauth2 client");
        let auth = OAuth2Auth::new(http.clone(), token_url, client_id, client_secret);

        let endpoint = std::env::var(&cfg.main_job.fetch.endpoint_env)
            .with_context(|| format!("env var {} not set", cfg.main_job.fetch.endpoint_env))?;

        let mut extra_query = cfg.main_job.fetch.static_query.clone();
        if let Some(ref env_key) = cfg.main_job.fetch.realm_type_env {
            if let Ok(v) = std::env::var(env_key) {
                if !v.is_empty() {
                    extra_query.push(("realm_type".to_owned(), v));
                }
            }
        }

        info!("pre_job: all connections ready");
        Ok(Self {
            db,
            auth,
            http,
            endpoint,
            extra_query,
        })
    }
}

// ── ResolvedIteratorCfg ───────────────────────────────────────────────────

/// Resolved at startup from `[main_job.iterator]` env vars.
/// Implements `HasIteratorCfg` and `HasRetryCfg` so it can be passed
/// directly to `StandardJob`.
pub struct ResolvedIteratorCfg {
    pub start_interval: i64,
    pub end_interval: i64,
    pub interval_limit: i64,
    pub window_sleep_secs: u64,
    pub max_attempts: usize,
    pub base_backoff_secs: u64,
}

impl ResolvedIteratorCfg {
    pub fn from_pipeline(cfg: &PipelineConfig) -> Result<Self> {
        fn get_i64(env: &str) -> Result<i64> {
            std::env::var(env)
                .with_context(|| format!("env var {env} not set"))?
                .parse::<i64>()
                .with_context(|| format!("env var {env} must be an integer"))
        }
        fn get_u64(env: &str) -> Result<u64> {
            std::env::var(env)
                .with_context(|| format!("env var {env} not set"))?
                .parse::<u64>()
                .with_context(|| format!("env var {env} must be a non-negative integer"))
        }
        let iter = &cfg.main_job.iterator;
        Ok(Self {
            start_interval: get_i64(&iter.start_env)?,
            end_interval: get_i64(&iter.end_env)?,
            interval_limit: get_i64(&iter.limit_env)?,
            window_sleep_secs: get_u64(&iter.sleep_secs_env)?,
            max_attempts: cfg.main_job.retry.max_attempts,
            base_backoff_secs: cfg.main_job.retry.backoff_secs,
        })
    }
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

// ── PostJobExecutor ───────────────────────────────────────────────────────

type HookFn = Box<
    dyn Fn(JobSummary) -> std::pin::Pin<Box<dyn Future<Output = Result<()>> + Send>> + Send + Sync,
>;

/// Executes the post_job steps declared in pipeline.toml.
/// Custom hooks are registered by the business crate before calling run.
pub struct PostJobExecutor {
    steps: Vec<PostStepConfig>,
    custom_hooks: HashMap<String, HookFn>,
}

impl PostJobExecutor {
    pub fn new(steps: Vec<PostStepConfig>) -> Self {
        Self {
            steps,
            custom_hooks: HashMap::new(),
        }
    }

    /// Register a named custom post-job hook.
    pub fn register_hook<F, Fut>(&mut self, name: &str, f: F)
    where
        F: Fn(JobSummary) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<()>> + Send + 'static,
    {
        self.custom_hooks
            .insert(name.to_owned(), Box::new(move |s| Box::pin(f(s))));
    }

    pub async fn run(&self, summary: JobSummary, cx: &StandardConnections) -> Result<()> {
        info!(
            windows = summary.windows_processed,
            fetched = summary.records_fetched,
            upserted = summary.records_upserted,
            skipped = summary.records_skipped,
            errors = summary.errors.len(),
            "Job complete"
        );
        for err in &summary.errors {
            tracing::warn!(error = %err, "Recorded error");
        }

        for step in &self.steps {
            match step {
                PostStepConfig::LogSummary => { /* already logged above */ }

                PostStepConfig::RawSql {
                    sql_env,
                    skip_if_empty,
                    ..
                } => {
                    let sql = std::env::var(sql_env).unwrap_or_default();
                    if sql.trim().is_empty() && *skip_if_empty {
                        continue;
                    }
                    info!("post_job: running raw SQL");
                    sqlx::raw_sql(&sql)
                        .execute(&cx.db)
                        .await
                        .context("Post-sync SQL failed")?;
                }

                PostStepConfig::Custom { hook } => {
                    let f = self
                        .custom_hooks
                        .get(hook)
                        .with_context(|| format!("No custom hook registered for \"{hook}\""))?;
                    f(JobSummary::default()).await?;
                }
            }
        }
        Ok(())
    }
}

// ── run_from_pipeline_toml ────────────────────────────────────────────────

/// Single entry-point for a business crate's main.rs.
///
/// Reads `pipeline.toml`, validates env vars, wires the cron scheduler
/// (with mutex-skip so concurrent runs are prevented), and calls
/// `job_factory` on each tick.
///
/// Example `main.rs`:
///
/// ```rust,no_run
/// #[tokio::main]
/// async fn main() -> anyhow::Result<()> {
///     sync_engine::run_from_pipeline_toml(
///         "pipeline.toml",
///         |cx, cfg| async move {
///             sync_engine::run_job::<MyJob, _, _>(cfg.as_ref()).await
///         },
///     ).await
/// }
/// ```
pub async fn run_from_pipeline_toml<F, Fut>(path: &str, job_factory: F) -> Result<()>
where
    F: Fn(Arc<StandardConnections>, Arc<ResolvedIteratorCfg>) -> Fut
        + Send
        + Sync
        + Clone
        + 'static,
    Fut: Future<Output = Result<()>> + Send + 'static,
{
    let raw = std::fs::read_to_string(path).with_context(|| format!("Cannot read {path}"))?;
    let pipeline_cfg: PipelineConfig =
        toml::from_str(&raw).with_context(|| format!("Cannot parse {path}"))?;

    let cron = std::env::var(&pipeline_cfg.scheduler.cron_env)
        .with_context(|| format!("env var {} not set", pipeline_cfg.scheduler.cron_env))?;

    info!("Building connections from pipeline config");
    let cx = Arc::new(StandardConnections::from_pipeline(&pipeline_cfg).await?);
    let iter_cfg = Arc::new(ResolvedIteratorCfg::from_pipeline(&pipeline_cfg)?);

    let lock: Arc<Mutex<()>> = Arc::new(Mutex::new(()));
    let mut scheduler = JobScheduler::new().await?;

    scheduler
        .add(Job::new_async(cron.as_str(), move |_, _| {
            let cx = Arc::clone(&cx);
            let cfg = Arc::clone(&iter_cfg);
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
                if let Err(e) = factory(cx, cfg).await {
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
