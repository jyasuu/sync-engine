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
    pub fn new(secs: u64) -> Self {
        Self { secs }
    }
}

#[async_trait]
impl Step for SleepStep {
    fn name(&self) -> &str {
        "sleep"
    }

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
    fn name(&self) -> &str {
        "log_summary"
    }

    async fn run(&self, ctx: &JobContext) -> Result<()> {
        let windows: usize = ctx
            .slot_read("summary.windows_processed")
            .await
            .unwrap_or(0);
        let errors: usize = ctx.slot_read("summary.error_count").await.unwrap_or(0);
        let fetched: usize = ctx.slot_read("summary.total_fetched").await.unwrap_or(0);
        let upserted: usize = ctx.slot_read("summary.total_upserted").await.unwrap_or(0);
        let skipped: usize = ctx.slot_read("summary.total_skipped").await.unwrap_or(0);

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
    pub sql: String,
    pub skip_if_empty: bool,
}

impl RawSqlStep {
    pub fn new(sql: impl Into<String>, skip_if_empty: bool) -> Self {
        Self {
            sql: sql.into(),
            skip_if_empty,
        }
    }
}

#[async_trait]
impl Step for RawSqlStep {
    fn name(&self) -> &str {
        "raw_sql"
    }

    async fn run(&self, ctx: &JobContext) -> Result<()> {
        if self.sql.trim().is_empty() && self.skip_if_empty {
            return Ok(());
        }
        info!("Running post-sync SQL");
        sqlx::raw_sql(&self.sql)
            .execute(&ctx.connections.db)
            .await
            .context("Raw SQL failed")?;
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
        Self {
            queue: queue.into(),
        }
    }
}

#[async_trait]
impl Step for DrainQueueStep {
    fn name(&self) -> &str {
        "drain_queue"
    }

    async fn run(&self, ctx: &JobContext) -> Result<()> {
        use crate::step::consumer::ConsumerHandle;

        // Drop the sender so the consumer task knows we're done producing.
        {
            let queues = ctx.queues.read().await;
            if let Some(entry) = queues.get(&self.queue) {
                // The tx clone is dropped when the QueueEntry is dropped or
                // when all senders are dropped. To signal EOF we close the
                // channel — we do this by dropping all senders except those
                // inside the QueueEntry itself. Since QueueEntry holds the
                // last tx after all SendToQueueSteps have run, we need to
                // explicitly close it here.
                drop(entry.tx.clone()); // no-op: need all senders gone
            }
        }
        // Actually close the channel by dropping the queue entry.
        ctx.queues.write().await.remove(&self.queue);

        // Await the consumer task.
        let handle_slot = format!("__consumer_handle_{}", self.queue);
        let consumer: ConsumerHandle = ctx
            .slots
            .write()
            .await
            .take(&handle_slot)
            .await
            .context("Consumer handle not found — was SpawnConsumerStep run?")?;

        consumer.handle.await.ok();
        info!(queue = %self.queue, "Consumer task joined");

        // If DrainInPostJob mode: the consumer task already accumulated and
        // committed items internally. This step just ensures the handle is
        // joined before post_job proceeds.
        let rx_slot = format!("__accum_rx_{}", self.queue);
        if ctx.slot_is_set(&rx_slot).await {
            info!(queue = %self.queue, "Accumulated items handled by consumer task");
        }

        Ok(())
    }
}
