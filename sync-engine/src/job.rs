// sync-engine/src/job.rs
use anyhow::Result;
use async_trait::async_trait;
use std::time::Duration;

pub trait Connections: Send + Sync {}

#[derive(Debug, Default)]
pub struct JobSummary {
    pub windows_processed: usize,
    pub records_fetched: usize,
    pub records_upserted: usize,
    pub records_skipped: usize,
    pub errors: Vec<String>,
}

#[async_trait]
pub trait PreJob: Send + Sync {
    type Cx: Connections;
    type Cfg: Send + Sync;
    async fn run(cfg: &Self::Cfg) -> Result<Self::Cx>;
}

#[async_trait]
pub trait MainJob: Send + Sync {
    type Cx: Connections;
    type Cfg: Send + Sync;
    async fn run(cx: &Self::Cx, cfg: &Self::Cfg) -> Result<JobSummary>;
}

#[async_trait]
pub trait PostJob: Send + Sync {
    type Cx: Connections;
    type Cfg: Send + Sync;
    async fn run(summary: JobSummary, cx: &Self::Cx, cfg: &Self::Cfg) -> Result<()>;
}

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

// ── Date window iterator ──────────────────────────────────────────────────

pub struct DateWindowIter {
    pub cursor: i64,
    pub end: i64,
    pub limit: i64,
    pub sleep: Duration,
    first: bool,
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

    pub fn without_sleep(mut self) -> Self {
        self.sleep = Duration::ZERO;
        self
    }

    pub async fn next_window(&mut self) -> Option<(i64, i64)> {
        self.next_window_with_sleep(true).await
    }

    pub async fn next_window_with_sleep(&mut self, do_sleep: bool) -> Option<(i64, i64)> {
        tracing::debug!(cursor = self.cursor, end = self.end, "next_window called");
        if self.cursor <= self.end {
            tracing::info!(cursor = self.cursor, end = self.end, "Iterator exhausted");
            return None;
        }
        if !self.first && do_sleep {
            tracing::info!("Sleeping {:?} between windows", self.sleep);
            tokio::time::sleep(self.sleep).await;
        }
        self.first = false;
        let window_end = (self.cursor - self.limit).max(self.end);
        let result = (self.cursor, window_end);
        self.cursor = window_end;
        tracing::debug!(next_cursor = self.cursor, "Window yielded");
        Some(result)
    }
}

// ── Retry helper ──────────────────────────────────────────────────────────

pub async fn with_retry<F, Fut, T>(max_attempts: usize, mut f: F) -> Result<T>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<T>>,
{
    let mut delay = Duration::from_secs(2);
    for attempt in 1..=max_attempts {
        match f().await {
            Ok(v) => return Ok(v),
            Err(e) if attempt == max_attempts => {
                tracing::error!(attempt, error = %e, "All retries exhausted");
                return Err(e);
            }
            Err(e) => {
                tracing::warn!(attempt, error = %e, "Retrying in {:?}", delay);
                tokio::time::sleep(delay).await;
                delay = (delay * 2).min(Duration::from_secs(30));
            }
        }
    }
    unreachable!()
}
