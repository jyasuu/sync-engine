// sync-engine/src/runner.rs
//
// MainJobRunner owns the date-window iterator and the retry logic.
// For each window tick it:
//   1. Updates ctx.window with the current window's dates.
//   2. Clears window-scoped slots.
//   3. Runs the retry_steps in a retry loop (fetch, transform, sink).
//   4. Runs the post_window_steps (sleep, optional bulk write).
//
// After all windows: runs post_loop_steps (bulk tx for pattern 2).

use anyhow::Result;
use chrono::Utc;
use std::sync::Arc;
use std::time::Duration;
use tracing::{error, info, warn};

use crate::context::{JobContext, WindowMeta};
use crate::job::JobSummary;
use crate::step::StepRunner;

pub struct WindowConfig {
    pub start_interval:    i64,
    pub end_interval:      i64,
    pub interval_limit:    i64,
    pub sleep_secs:        u64,
    pub max_attempts:      usize,
    pub base_backoff_secs: u64,
}

pub struct MainJobRunner {
    pub window_cfg:        WindowConfig,
    /// Steps inside the retry loop: fetch, transform, sink (or send_to_queue).
    pub retry_steps:       StepRunner,
    /// Steps after each window (outside retry): typically sleep.
    pub post_window_steps: StepRunner,
    /// Steps after all windows: bulk tx for pattern 2, noop for pattern 1/3.
    pub post_loop_steps:   StepRunner,
}

impl MainJobRunner {
    pub async fn run(&mut self, ctx: &Arc<JobContext>) -> Result<JobSummary> {
        let mut summary  = JobSummary::default();
        let mut cursor   = self.window_cfg.start_interval;
        let end          = self.window_cfg.end_interval;
        let limit        = self.window_cfg.interval_limit;
        let mut first    = true;
        let mut last_ok  = true;

        loop {
            if cursor <= end {
                info!("Window iterator exhausted");
                break;
            }

            // Sleep between windows (skip before first, skip after an error window)
            // The built-in sleep runs here so it cannot be skipped by a failing step.
            // If sleep_secs = 0 AND the user has a SleepStep in post_window_steps,
            // only the step-level sleep runs — avoiding double-sleep.
            if !first && last_ok && self.window_cfg.sleep_secs > 0 {
                info!(secs = self.window_cfg.sleep_secs, "Sleeping between windows");
                tokio::time::sleep(Duration::from_secs(self.window_cfg.sleep_secs)).await;
            }
            first = false;

            let window_end = (cursor - limit).max(end);

            // Compute formatted date strings using the configured format
            let fmt       = &ctx.connections.date_format;
            let now       = Utc::now();
            let start_str = (now - chrono::Duration::days(cursor))
                .format(fmt).to_string();
            let end_str   = (now - chrono::Duration::days(window_end))
                .format(fmt).to_string();

            info!(start = %start_str, end = %end_str, "Processing window");
            summary.windows_processed += 1;

            // Update window metadata in context
            {
                let mut w = ctx.window.write().await;
                *w = WindowMeta {
                    start_day: cursor,
                    end_day:   window_end,
                    start_str: start_str.clone(),
                    end_str:   end_str.clone(),
                    index:     summary.windows_processed - 1,
                };
            }

            // Clear window-scoped slots
            ctx.clear_window_slots().await;

            // Retry loop
            let result = self.run_with_retry(ctx, cursor, window_end).await;

            match result {
                Ok((fetched, upserted, skipped)) => {
                    summary.records_fetched   += fetched;
                    summary.records_upserted  += upserted;
                    summary.records_skipped   += skipped;
                    last_ok = true;
                }
                Err(e) => {
                    let msg = format!("Window [{cursor}..{window_end}] failed: {e}");
                    error!("{msg}");
                    summary.errors.push(msg);
                    last_ok = false;
                }
            }

            // Update job-scoped summary slots so LogSummaryStep reads live totals
            ctx.slot_write("summary.windows_processed", summary.windows_processed).await.ok();
            ctx.slot_write("summary.error_count",       summary.errors.len()).await.ok();

            // Post-window steps (sleep already handled above, but explicit
            // SleepStep in the runner is also fine — it will no-op if 0)
            if let Err(e) = self.post_window_steps.run_all(ctx).await {
                warn!(error = %e, "Post-window step failed — continuing");
            }

            cursor = window_end;
        }

        // Post-loop steps (bulk write for pattern 2)
        self.post_loop_steps.run_all(ctx).await?;

        Ok(summary)
    }

    async fn run_with_retry(
        &self,
        ctx: &Arc<JobContext>,
        start_day: i64,
        end_day: i64,
    ) -> Result<(usize, usize, usize)> {
        let mut delay = Duration::from_secs(self.window_cfg.base_backoff_secs);

        for attempt in 1..=self.window_cfg.max_attempts {
            // Clear window slots before each attempt so stale data doesn't leak
            ctx.clear_window_slots().await;

            match self.retry_steps.run_all(ctx).await {
                Ok(()) => {
                    // Read counts from summary slots written by sink steps
                    let fetched  = read_count(ctx, "window.fetched").await;
                    let upserted = read_count(ctx, "window.upserted").await;
                    let skipped  = read_count(ctx, "window.skipped").await;
                    return Ok((fetched, upserted, skipped));
                }
                Err(e) if attempt == self.window_cfg.max_attempts => {
                    error!(attempt, error = %e, "All retries exhausted");
                    return Err(e);
                }
                Err(e) => {
                    warn!(
                        attempt,
                        error = %e,
                        delay_secs = delay.as_secs(),
                        start_day,
                        end_day,
                        "Retrying window"
                    );
                    tokio::time::sleep(delay).await;
                    delay = (delay * 2).min(Duration::from_secs(30));
                }
            }
        }
        unreachable!()
    }
}

async fn read_count(ctx: &JobContext, key: &str) -> usize {
    ctx.slot_read::<usize>(key).await.unwrap_or(0)
}
