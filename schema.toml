// user-sync/src/job.rs
//
// Before: ~130 lines of manual fetch/transform/tx/upsert/retry loop.
// After:  PreJob wires StandardConnections; MainJob delegates to StandardJob;
//         PostJob is the only custom logic that remains.

use anyhow::{Context, Result};
use async_trait::async_trait;

use sync_engine::{
    Connections, HasConnections, HasIteratorCfg, HasRetryCfg, JobSummary, MainJob, OAuth2Auth,
    PostJob, PreJob, StandardConnections, StandardJob,
};

use crate::config::AppConfig;
use crate::generated::{envelopes::ApiUserResponse, records::DbUser, transforms::UserTransform};

// ── Connections wrapper ───────────────────────────────────────────────────

pub struct UserConnections(pub StandardConnections);

impl Connections for UserConnections {}

impl HasConnections for UserConnections {
    fn db_pool(&self) -> &sqlx::PgPool {
        self.0.db_pool()
    }
    fn auth(&self) -> &OAuth2Auth {
        self.0.auth()
    }
    fn http_client(&self) -> &reqwest::Client {
        self.0.http_client()
    }
    fn endpoint(&self) -> &str {
        self.0.endpoint()
    }
    fn extra_query(&self) -> Vec<(String, String)> {
        self.0.extra_query()
    }
}

// ── HasIteratorCfg / HasRetryCfg delegated from AppConfig ────────────────

impl HasIteratorCfg for AppConfig {
    fn start_interval(&self) -> i64 {
        self.source.start_interval
    }
    fn end_interval(&self) -> i64 {
        self.source.end_interval
    }
    fn interval_limit(&self) -> i64 {
        self.source.interval_limit
    }
    fn window_sleep_secs(&self) -> u64 {
        self.source.window_sleep_secs
    }
}

impl HasRetryCfg for AppConfig {
    fn max_attempts(&self) -> usize {
        5
    }
    fn base_backoff_secs(&self) -> u64 {
        2
    }
}

// ── Job struct ────────────────────────────────────────────────────────────

pub struct UserSyncJob;

// PreJob — build connections (no duplicate OAuth2Client; uses library type)
#[async_trait]
impl PreJob for UserSyncJob {
    type Cx = UserConnections;
    type Cfg = AppConfig;

    async fn run(cfg: &AppConfig) -> Result<UserConnections> {
        let http = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(620))
            .tcp_keepalive(std::time::Duration::from_secs(30))
            .build()?;

        let db = sqlx::postgres::PgPoolOptions::new()
            .max_connections(5)
            .connect(&cfg.sink.database_url)
            .await
            .context("Postgres connect failed")?;

        let auth = OAuth2Auth::new(
            http.clone(),
            &cfg.auth.token_url,
            &cfg.auth.client_id,
            &cfg.auth.client_secret,
        );

        let extra_query = if cfg.source.include_realm_types.is_empty() {
            vec![]
        } else {
            vec![(
                "realm_type".to_owned(),
                cfg.source.include_realm_types.clone(),
            )]
        };

        Ok(UserConnections(StandardConnections {
            db,
            auth,
            http,
            endpoint: cfg.source.user_endpoint.clone(),
            extra_query,
        }))
    }
}

// MainJob — entirely provided by StandardJob; one delegating line
#[async_trait]
impl MainJob for UserSyncJob {
    type Cx = UserConnections;
    type Cfg = AppConfig;

    async fn run(cx: &UserConnections, cfg: &AppConfig) -> Result<JobSummary> {
        StandardJob::<UserConnections, AppConfig, ApiUserResponse, DbUser, UserTransform>::run(
            cx, cfg,
        )
        .await
    }
}

// PostJob — the only genuinely custom code left in user-sync
#[async_trait]
impl PostJob for UserSyncJob {
    type Cx = UserConnections;
    type Cfg = AppConfig;

    async fn run(summary: JobSummary, cx: &UserConnections, cfg: &AppConfig) -> Result<()> {
        tracing::info!(
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
        if !cfg.sink.sync_sql.trim().is_empty() {
            tracing::info!("Running post-sync SQL");
            sqlx::raw_sql(&cfg.sink.sync_sql)
                .execute(&cx.0.db)
                .await
                .context("Post-sync SQL failed")?;
        }
        Ok(())
    }
}
