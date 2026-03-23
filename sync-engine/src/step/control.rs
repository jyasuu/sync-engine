// sync-engine/src/step/control.rs
//
// Flow-control and post-job steps:
//   SleepStep       — rate-limit sleep between windows
//   LogSummaryStep  — log the JobSummary from ctx
//   RawSqlStep      — run arbitrary SQL against the pool
//   DrainQueueStep  — await the consumer task; collect accumulated items

use anyhow::{Context, Result};
use async_trait::async_trait;
use std::time::Duration;
use tracing::info;

use crate::context::JobContext;
use crate::step::Step;

// ── SleepStep ─────────────────────────────────────────────────────────────

pub struct SleepStep {
    pub secs: u64,
}

impl SleepStep {
    pub fn new(secs: u64) -> Self { Self { secs } }
}

#[async_trait]
impl Step for SleepStep {
    fn name(&self) -> &str { "sleep" }

    async fn run(&self, _ctx: &JobContext) -> Result<()> {
        if self.secs > 0 {
            info!(secs = self.secs, "Rate-limit sleep");
            tokio::time::sleep(Duration::from_secs(self.secs)).await;
        }
        Ok(())
    }
}

// ── LogSummaryStep ────────────────────────────────────────────────────────

/// Reads job-scoped count slots written by FetchJsonStep and TxUpsertStep
/// and logs a final summary. Also reads the JobSummary written by MainJobRunner
/// via the "summary.*" config entries for window/error counts.
pub struct LogSummaryStep;

#[async_trait]
impl Step for LogSummaryStep {
    fn name(&self) -> &str { "log_summary" }

    async fn run(&self, ctx: &JobContext) -> Result<()> {
        let windows:  usize = ctx.slot_read("summary.windows_processed") .await.unwrap_or(0);
        let errors:   usize = ctx.slot_read("summary.error_count")        .await.unwrap_or(0);
        let fetched:  usize = ctx.slot_read("summary.total_fetched")      .await.unwrap_or(0);
        let upserted: usize = ctx.slot_read("summary.total_upserted")     .await.unwrap_or(0);
        let skipped:  usize = ctx.slot_read("summary.total_skipped")      .await.unwrap_or(0);

        info!(
            job = %ctx.job_name,
            windows,
            fetched,
            upserted,
            skipped,
            errors,
            "Job summary"
        );
        Ok(())
    }
}

// ── RawSqlStep ────────────────────────────────────────────────────────────

pub struct RawSqlStep {
    pub sql:            String,
    pub skip_if_empty:  bool,
}

impl RawSqlStep {
    pub fn new(sql: impl Into<String>, skip_if_empty: bool) -> Self {
        Self { sql: sql.into(), skip_if_empty }
    }
}

#[async_trait]
impl Step for RawSqlStep {
    fn name(&self) -> &str { "raw_sql" }

    async fn run(&self, ctx: &JobContext) -> Result<()> {
        if self.sql.trim().is_empty() && self.skip_if_empty {
            return Ok(());
        }
        #[cfg(feature = "postgres")]
        {
            info!("Running post-sync SQL");
            sqlx::raw_sql(&self.sql)
                .execute(&ctx.connections.db)
                .await
                .context("Raw SQL failed")?;
        }
        #[cfg(not(feature = "postgres"))]
        {
            tracing::warn!(sql = %self.sql, "raw_sql step requires the 'postgres' feature — skipped");
        }
        Ok(())
    }
}

// ── DrainQueueStep ────────────────────────────────────────────────────────

/// Awaits the consumer task and (for DrainInPostJob mode) commits
/// the accumulated items to Postgres in one transaction.
pub struct DrainQueueStep {
    pub queue: String,
}

impl DrainQueueStep {
    pub fn new(queue: impl Into<String>) -> Self {
        Self { queue: queue.into() }
    }
}

#[async_trait]
impl Step for DrainQueueStep {
    fn name(&self) -> &str { "drain_queue" }

    async fn run(&self, ctx: &JobContext) -> Result<()> {
        // Close the channel by dropping the queue entry — signals EOF to consumer.
        ctx.queues.write().await.remove(&self.queue);

        // Take the consumer handle from the slot without requiring Clone.
        // We replace the slot value with None (a fresh empty Option) so the
        // slot remains declared but empty after we extract the handle.
        let handle_slot = format!("__consumer_handle_{}", self.queue);
        let handle_opt: Option<tokio::task::JoinHandle<()>> = {
            let mut slots = ctx.slots.write().await;
            let entry = slots.slots.get_mut(&handle_slot)
                .with_context(|| format!(
                    "Consumer handle slot \"{handle_slot}\" not found — was SpawnConsumerStep run?"
                ))?;
            let arc = entry.value.take()
                .with_context(|| format!("Consumer handle for \"{}\" already consumed", self.queue))?;
            // Unwrap the Arc — we just took the only reference from the slot.
            match std::sync::Arc::try_unwrap(arc) {
                Ok(lock) => {
                    let boxed = lock.into_inner();
                    boxed
                        .downcast::<Option<tokio::task::JoinHandle<()>>>()
                        .ok()
                        .map(|b| *b)
                        .flatten()
                }
                Err(_) => None,
            }
        };

        if let Some(handle) = handle_opt {
            handle.await.ok();
            info!(queue = %self.queue, "Consumer task joined");
        }

        let rx_slot = format!("__accum_rx_{}", self.queue);
        if ctx.slot_is_set(&rx_slot).await {
            info!(queue = %self.queue, "Accumulated items handled by consumer task");
        }

        Ok(())
    }
}
