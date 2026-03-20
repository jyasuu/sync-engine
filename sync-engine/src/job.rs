// sync-engine/src/job.rs
//
// Talend-style job phase traits.
//
// A job is declared by implementing three traits on a unit struct:
//
//   struct UserSyncJob;
//
//   impl PreJob  for UserSyncJob { type Connections = ...; async fn run() }
//   impl MainJob for UserSyncJob { async fn run(cx, cfg) -> JobSummary }
//   impl PostJob for UserSyncJob { async fn run(summary, cx, cfg) }
//
// The runner calls them in order. Connections are created once in pre_job
// and passed explicitly — nothing is hidden inside components.

use anyhow::Result;
use async_trait::async_trait;
use std::time::Duration;

/// Everything created in pre_job and kept alive for the whole job.
/// Business crates define their own concrete type.
pub trait Connections: Send + Sync {}

/// Outcome collected during main_job, passed to post_job.
#[derive(Debug, Default)]
pub struct JobSummary {
    pub windows_processed: usize,
    pub records_fetched: usize,
    pub records_upserted: usize,
    pub records_skipped: usize,
    pub errors: Vec<String>,
}

// ── Phase traits ──────────────────────────────────────────────────────────

#[async_trait]
pub trait PreJob: Send + Sync {
    type Cx: Connections;
    type Cfg: Send + Sync;

    /// Create all connections. Called once before main_job.
    async fn run(cfg: &Self::Cfg) -> Result<Self::Cx>;
}

#[async_trait]
pub trait MainJob: Send + Sync {
    type Cx: Connections;
    type Cfg: Send + Sync;

    /// Execute the data pipeline. Receives connections from pre_job.
    async fn run(cx: &Self::Cx, cfg: &Self::Cfg) -> Result<JobSummary>;
}

#[async_trait]
pub trait PostJob: Send + Sync {
    type Cx: Connections;
    type Cfg: Send + Sync;

    /// Custom summary / cleanup. Receives the summary from main_job.
    async fn run(summary: JobSummary, cx: &Self::Cx, cfg: &Self::Cfg) -> Result<()>;
}

// ── Job runner ────────────────────────────────────────────────────────────

/// Executes pre → main → post in order.
/// On pre_job failure the whole job aborts (nothing to clean up yet).
/// On main_job failure post_job still runs so connections can be closed.
pub async fn run_job<J, Cx, Cfg>(cfg: &Cfg) -> Result<()>
where
    J: PreJob<Cx = Cx, Cfg = Cfg> + MainJob<Cx = Cx, Cfg = Cfg> + PostJob<Cx = Cx, Cfg = Cfg>,
    Cx: Connections,
    Cfg: Send + Sync,
{
    let cx = <J as PreJob>::run(cfg).await?;

    let summary = match <J as MainJob>::run(&cx, cfg).await {
        Ok(s) => s,
        Err(e) => {
            tracing::error!(error = %e, "main_job failed");
            let mut s = JobSummary::default();
            s.errors.push(format!("main_job fatal: {e}"));
            s
        }
    };

    <J as PostJob>::run(summary, &cx, cfg).await?;
    Ok(())
}

// ── Built-in iterator + retry template ───────────────────────────────────

/// Yields successive date windows until the full range is covered.
/// Sleeps `sleep` between yields to avoid overloading upstream.
pub struct DateWindowIter {
    pub cursor: i64,
    pub end: i64,
    pub limit: i64,
    pub sleep: Duration,
    pub first: bool,
}

impl DateWindowIter {
    pub fn new(start: i64, end: i64, limit: i64) -> Self {
        Self {
            cursor: start,
            end,
            limit,
            sleep: Duration::from_secs(60),
            first: true,
        }
    }

    pub async fn next_window(&mut self) -> Option<(i64, i64)> {
        if self.cursor <= self.end {
            return None;
        }
        if !self.first {
            tracing::info!("Sleeping {:?} between windows", self.sleep);
            tokio::time::sleep(self.sleep).await;
        }
        self.first = false;
        let window_end = (self.cursor - self.limit).max(self.end);
        let result = (self.cursor, window_end);
        self.cursor = window_end;
        Some(result)
    }
}

/// Exponential back-off retry helper.
/// Returns Ok(T) on first success, Err on exhaustion.
pub async fn with_retry<F, Fut, T>(max_attempts: usize, mut f: F) -> Result<T>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<T>>,
{
    let mut delay = Duration::from_secs(2);
    for attempt in 1..=max_attempts {
        match f().await {
            Ok(v) => return Ok(v),
            Err(e) if attempt == max_attempts => return Err(e),
            Err(e) => {
                tracing::warn!(attempt, error = %e, "Retrying in {:?}", delay);
                tokio::time::sleep(delay).await;
                delay = (delay * 2).min(Duration::from_secs(30));
            }
        }
    }
    unreachable!()
}
