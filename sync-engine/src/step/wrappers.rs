// sync-engine/src/step/wrappers.rs
//
// Wrapper steps: steps that own a child StepRunner and add orchestration
// around it.  They satisfy the Step trait so they can appear anywhere in a
// pipeline — any nesting depth, any number of times, in any order.
//
//   WindowLoopStep  — iterates over date windows; runs child steps per window
//   RetryStep       — re-runs child steps on failure with exponential back-off
//   TxStep          — wraps child steps in a Postgres transaction
//                     (requires feature "postgres")

use anyhow::Result;
use async_trait::async_trait;
use chrono::Utc;
use std::time::Duration;
use tracing::{error, info, warn};

use crate::context::{JobContext, WindowMeta};
use crate::step::{Step, StepRunner};

// ── WindowLoopStep ────────────────────────────────────────────────────────

/// Iterates over date windows and runs child steps for each window.
///
/// ```toml
/// [[main_job.steps]]
/// type           = "window_loop"
/// start_interval = { env = "SOURCE__START_INTERVAL", default = "30" }
/// end_interval   = { env = "SOURCE__END_INTERVAL",   default = "0"  }
/// interval_limit = { env = "SOURCE__INTERVAL_LIMIT", default = "7"  }
/// sleep_secs     = "60"
///
///   [[main_job.steps.steps]]
///   type = "retry"
///   # ...
/// ```
pub struct WindowLoopStep {
    pub start_interval: i64,
    pub end_interval:   i64,
    pub interval_limit: i64,
    /// Seconds to sleep between windows (0 = no sleep).
    pub sleep_secs:     u64,
    pub child:          StepRunner,
}

#[async_trait]
impl Step for WindowLoopStep {
    fn name(&self) -> &str { "window_loop" }

    async fn run(&self, ctx: &JobContext) -> Result<()> {
        let mut cursor = self.start_interval;
        let end        = self.end_interval;
        let limit      = self.interval_limit;
        let mut first  = true;

        loop {
            if cursor <= end {
                info!("Window iterator exhausted");
                break;
            }

            if !first && self.sleep_secs > 0 {
                info!(secs = self.sleep_secs, "Sleeping between windows");
                tokio::time::sleep(Duration::from_secs(self.sleep_secs)).await;
            }
            first = false;

            let window_end = (cursor - limit).max(end);
            let fmt        = &ctx.connections.date_format;
            let now        = Utc::now();
            let start_str  = (now - chrono::Duration::days(cursor)).format(fmt).to_string();
            let end_str    = (now - chrono::Duration::days(window_end)).format(fmt).to_string();

            info!(start = %start_str, end = %end_str, "Processing window");

            {
                let prev_index: usize = ctx.slot_read("summary.windows_processed")
                    .await.unwrap_or(0);
                let mut w = ctx.window.write().await;
                *w = WindowMeta {
                    start_day: cursor,
                    end_day:   window_end,
                    start_str,
                    end_str,
                    index: prev_index,
                };
            }

            ctx.clear_window_slots().await;

            for step in &self.child.steps {
                step.run(ctx).await.map_err(|e| {
                    error!(step = step.name(), error = %e, "Window child step failed");
                    e
                })?;
            }

            let prev: usize = ctx.slot_read("summary.windows_processed").await.unwrap_or(0);
            ctx.slot_write("summary.windows_processed", prev + 1).await.ok();

            cursor = window_end;
        }
        Ok(())
    }
}

// ── RetryStep ─────────────────────────────────────────────────────────────

/// Runs child steps in a retry loop with exponential back-off.
///
/// ```toml
/// [[main_job.steps.steps]]      # inside a window_loop
/// type         = "retry"
/// max_attempts = 5
/// backoff_secs = 2
///
///   [[main_job.steps.steps.steps]]
///   type     = "fetch"
///   envelope = "ApiUserResponse"
///   writes   = "api_rows"
/// ```
pub struct RetryStep {
    pub max_attempts:      usize,
    pub base_backoff_secs: u64,
    pub child:             StepRunner,
}

#[async_trait]
impl Step for RetryStep {
    fn name(&self) -> &str { "retry" }

    async fn run(&self, ctx: &JobContext) -> Result<()> {
        let mut delay = Duration::from_secs(self.base_backoff_secs);

        for attempt in 1..=self.max_attempts {
            // Clear window-scoped slots before each attempt so stale data
            // from a previous failed attempt does not leak into the next one.
            ctx.clear_window_slots().await;

            let mut failed: Option<anyhow::Error> = None;
            for step in &self.child.steps {
                match step.run(ctx).await {
                    Ok(()) => {}
                    Err(e) => {
                        error!(step = step.name(), error = %e, "Step failed inside retry");
                        failed = Some(e);
                        break;
                    }
                }
            }

            match failed {
                None => return Ok(()),
                Some(e) if attempt == self.max_attempts => {
                    error!(attempt, error = %e, "All retries exhausted");
                    return Err(e);
                }
                Some(e) => {
                    warn!(attempt, delay_secs = delay.as_secs(), error = %e, "Retrying");
                    tokio::time::sleep(delay).await;
                    delay = (delay * 2).min(Duration::from_secs(30));
                }
            }
        }
        unreachable!()
    }
}

// ── TxStep ────────────────────────────────────────────────────────────────

/// Opens a Postgres transaction, runs child steps inside it, then commits.
/// On any error the transaction is rolled back (dropped without commit).
///
/// Child upsert steps detect the active transaction via ctx.take_tx() and
/// ctx.put_tx(): they borrow it, do their work, and return it so TxStep can
/// commit after all children finish.
///
/// ```toml
/// [[main_job.steps.steps.steps]]   # inside a retry
/// type = "tx"
///
///   [[main_job.steps.steps.steps.steps]]
///   type  = "tx_upsert"
///   model = "DbUser"
///   reads = "db_rows"
/// ```
#[cfg(feature = "postgres")]
pub struct TxStep {
    pub child: StepRunner,
}

#[cfg(feature = "postgres")]
#[async_trait]
impl Step for TxStep {
    fn name(&self) -> &str { "tx" }

    async fn run(&self, ctx: &JobContext) -> Result<()> {
        use anyhow::Context as _;

        let tx = ctx.connections.db.begin().await.context("TxStep: begin")?;
        ctx.put_tx(tx).await;

        let mut failed: Option<anyhow::Error> = None;
        for step in &self.child.steps {
            match step.run(ctx).await {
                Ok(()) => {}
                Err(e) => {
                    error!(step = step.name(), error = %e, "Step failed inside tx");
                    failed = Some(e);
                    break;
                }
            }
        }

        match failed {
            None => {
                if let Some(tx) = ctx.take_tx().await {
                    tx.commit().await.context("TxStep: commit")?;
                    info!("Transaction committed");
                }
                Ok(())
            }
            Some(e) => {
                // Drop the transaction — sqlx rolls back automatically on drop.
                let _ = ctx.take_tx().await;
                error!(error = %e, "Transaction rolled back");
                Err(e)
            }
        }
    }
}
