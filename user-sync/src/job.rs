// user-sync/src/job.rs
//
// Wires the step-based pipeline for user-sync.
// Pattern 1 (active): tx_upsert per window, slot scope = window.
// Switch patterns by editing pipeline.toml only — no Rust changes needed.

use anyhow::{Context, Result};
use async_trait::async_trait;
use std::sync::Arc;

use sync_engine::{
    context::JobContext,
    job::{Connections as JobConnTrait, JobSummary, MainJob, PostJob, PreJob},
    pipeline_runner::{build_context, build_window_cfg, PipelineConfig, PostStepConfig},
    runner::MainJobRunner,
    step::{
        control::SleepStep, fetch::FetchJsonStep, sink::TxUpsertStep, transform::TransformStep,
        StepRunner,
    },
};

use crate::generated::{
    envelopes::ApiUserResponse,
    records::{ApiUser, DbUser},
    transforms::UserTransform,
};

// ── Connections wrapper ───────────────────────────────────────────────────

pub struct UserConnections(pub Arc<JobContext>);
impl JobConnTrait for UserConnections {}

// ── Job ───────────────────────────────────────────────────────────────────

pub struct UserSyncJob;

#[async_trait]
impl PreJob for UserSyncJob {
    type Cx = UserConnections;
    type Cfg = PipelineConfig;

    async fn run(cfg: &PipelineConfig) -> Result<UserConnections> {
        Ok(UserConnections(build_context(cfg).await?))
    }
}

#[async_trait]
impl MainJob for UserSyncJob {
    type Cx = UserConnections;
    type Cfg = PipelineConfig;

    async fn run(cx: &UserConnections, cfg: &PipelineConfig) -> Result<JobSummary> {
        let ctx = Arc::clone(&cx.0);

        // ── Retry steps ───────────────────────────────────────────────────
        let mut retry = StepRunner::new();

        // fetch: HTTP GET → writes Vec<ApiUser> into "api_rows" slot
        retry.push(FetchJsonStep::<ApiUserResponse>::new("api_rows", false));

        // transform: Vec<ApiUser> → Vec<DbUser>
        retry.push(TransformStep::<ApiUser, DbUser, UserTransform>::new(
            "api_rows",
            "db_rows",
            false, // replace, not append (window scope)
            UserTransform,
        ));

        // sink: open tx, upsert each DbUser, commit
        retry.push(TxUpsertStep::<DbUser>::new("db_rows"));

        // ── Post-window steps (outside retry) ─────────────────────────────
        let mut post_window = StepRunner::new();
        let sleep_secs = cfg
            .main_job
            .iterator
            .sleep_secs
            .resolve_as::<u64>()
            .unwrap_or(60);
        post_window.push(SleepStep::new(sleep_secs));

        // ── Post-loop steps (pattern 2: bulk tx after all windows) ─────────
        let post_loop = StepRunner::new();
        // Uncomment for pattern 2:
        // post_loop.push(TxUpsertStep::<DbUser>::new("db_rows"));

        let mut runner = MainJobRunner {
            window_cfg: build_window_cfg(cfg)?,
            retry_steps: retry,
            post_window_steps: post_window,
            post_loop_steps: post_loop,
        };

        runner.run(&ctx).await
    }
}

#[async_trait]
impl PostJob for UserSyncJob {
    type Cx = UserConnections;
    type Cfg = PipelineConfig;

    async fn run(summary: JobSummary, cx: &UserConnections, cfg: &PipelineConfig) -> Result<()> {
        tracing::info!(
            windows = summary.windows_processed,
            fetched = summary.records_fetched,
            upserted = summary.records_upserted,
            skipped = summary.records_skipped,
            errors = summary.errors.len(),
            "Job complete"
        );
        for err in &summary.errors {
            tracing::warn!(error = %err, "Error");
        }

        for step in &cfg.post_job.steps {
            match step {
                PostStepConfig::LogSummary => {}
                PostStepConfig::RawSql { sql, skip_if_empty } => {
                    let s = sql.resolve().unwrap_or_default();
                    if s.trim().is_empty() && *skip_if_empty {
                        continue;
                    }
                    tracing::info!("Running post-sync SQL");
                    sqlx::raw_sql(&s)
                        .execute(&cx.0.connections.db)
                        .await
                        .context("Post-sync SQL failed")?;
                }
                PostStepConfig::DrainQueue { .. } | PostStepConfig::Custom { .. } => {}
            }
        }
        Ok(())
    }
}
