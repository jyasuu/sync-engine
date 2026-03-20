// sync-engine/src/standard_job.rs
//
// A generic MainJob implementation that replaces the hand-written
// fetch→transform→tx-upsert→commit loop in every business crate.
//
// Business crates implement three lightweight traits:
//   HasConnections  — exposes db pool, auth client, http endpoint
//   HasIteratorCfg  — exposes window iteration parameters
//   HasRetryCfg     — exposes retry knobs (has sensible defaults)
//
// Then StandardJob<Env, DbM, Xfm> drives the whole loop.

use anyhow::{Context, Result};
use async_trait::async_trait;
use reqwest::StatusCode;
use std::time::Duration;
use tracing::{debug, error, info, warn};

use crate::components::auth::OAuth2Auth;
use crate::components::writer::{TxWriter, UpsertableInTx};
use crate::job::{DateWindowIter, JobSummary, MainJob};
use crate::pipeline::primitives::Auth;
use crate::{HasEnvelope, Transform};

// ── Traits the business crate implements ─────────────────────────────────

/// Exposes the pieces of Connections that StandardJob needs.
pub trait HasConnections: Send + Sync {
    fn db_pool(&self) -> &sqlx::PgPool;
    fn auth(&self) -> &OAuth2Auth;
    fn http_client(&self) -> &reqwest::Client;
    fn endpoint(&self) -> &str;
    /// Optional extra static query params (e.g. realm_type).
    fn extra_query(&self) -> Vec<(String, String)> {
        vec![]
    }
}

/// Iterator window parameters, typically forwarded from the config struct.
pub trait HasIteratorCfg: Send + Sync {
    fn start_interval(&self) -> i64;
    fn end_interval(&self) -> i64;
    fn interval_limit(&self) -> i64;
    fn window_sleep_secs(&self) -> u64;
}

/// Retry knobs — defaults are 5 attempts / 2 s base delay.
pub trait HasRetryCfg: Send + Sync {
    fn max_attempts(&self) -> usize {
        5
    }
    fn base_backoff_secs(&self) -> u64 {
        2
    }
}

// ── StandardJob ───────────────────────────────────────────────────────────

/// Zero-boilerplate MainJob.
///
/// Type parameters:
///   `Cx`  — the Connections type (implements `HasConnections`)
///   `Cfg` — the config type (implements `HasIteratorCfg + HasRetryCfg`)
///   `Env` — the API envelope type (implements `HasEnvelope`; Item = ApiRecord)
///   `DbM` — the DB model type (implements `UpsertableInTx`)
///   `Xfm` — the transform (implements `Transform<Input=ApiRecord, Output=DbM>`)
///
/// All five parameters must be provided so Rust can anchor the `MainJob` impl.
///
/// Typical usage in a business crate:
/// ```rust,ignore
/// impl MainJob for UserSyncJob {
///     type Cx  = UserConnections;
///     type Cfg = AppConfig;
///     async fn run(cx: &UserConnections, cfg: &AppConfig) -> Result<JobSummary> {
///         StandardJob::<UserConnections, AppConfig, ApiUserResponse, DbUser, UserTransform>::run(cx, cfg).await
///     }
/// }
/// ```
// PhantomData<fn() -> T> is always Send + Sync regardless of T's variance —
// it models "this type is generic over T" without inheriting T's Send/Sync
// requirements, which is correct here because we never actually store any T.
pub struct StandardJob<Cx, Cfg, Env, DbM, Xfm> {
    _phantom: std::marker::PhantomData<fn() -> (Cx, Cfg, Env, DbM, Xfm)>,
}

impl<Cx, Cfg, Env, DbM, Xfm> Default for StandardJob<Cx, Cfg, Env, DbM, Xfm> {
    fn default() -> Self {
        Self {
            _phantom: std::marker::PhantomData,
        }
    }
}

#[async_trait]
impl<Cx, Cfg, Env, DbM, Xfm> MainJob for StandardJob<Cx, Cfg, Env, DbM, Xfm>
where
    Cx: HasConnections + crate::job::Connections,
    Cfg: HasIteratorCfg + HasRetryCfg + Send + Sync,
    Env: HasEnvelope + Send + 'static,
    Env::Item: Send + 'static,
    Xfm: Transform<Input = Env::Item, Output = DbM> + Default + Send + Sync,
    DbM: UpsertableInTx + Send + Sync + 'static,
{
    type Cx = Cx;
    type Cfg = Cfg;

    async fn run(cx: &Cx, cfg: &Cfg) -> Result<JobSummary> {
        let mut summary = JobSummary::default();
        let transform = Xfm::default();
        let writer = TxWriter::<DbM>::new(cx.db_pool().clone());

        let mut iter = DateWindowIter::new(
            cfg.start_interval(),
            cfg.end_interval(),
            cfg.interval_limit(),
        );
        iter.sleep = Duration::from_secs(cfg.window_sleep_secs());

        let mut last_ok = true;

        while let Some((start, end)) = iter.next_window_with_sleep(last_ok).await {
            summary.windows_processed += 1;
            info!(start, end, "Processing window");

            let now = chrono::Utc::now();
            let start_str = (now - chrono::Duration::days(start))
                .format("%Y%m%d")
                .to_string();
            let end_str = (now - chrono::Duration::days(end))
                .format("%Y%m%d")
                .to_string();

            let result = run_window_with_retry::<Cx, Env, DbM, Xfm>(
                cfg.max_attempts(),
                Duration::from_secs(cfg.base_backoff_secs()),
                cx,
                &transform,
                &writer,
                &start_str,
                &end_str,
            )
            .await;

            match result {
                Ok((fetched, upserted, skipped)) => {
                    summary.records_fetched += fetched;
                    summary.records_upserted += upserted;
                    summary.records_skipped += skipped;
                    last_ok = true;
                }
                Err(e) => {
                    let msg = format!("Window [{start}..{end}] failed: {e}");
                    error!("{msg}");
                    summary.errors.push(msg);
                    last_ok = false;
                }
            }
        }

        Ok(summary)
    }
}

// ── Inner retry loop ──────────────────────────────────────────────────────

async fn run_window_with_retry<Cx, Env, DbM, Xfm>(
    max_attempts: usize,
    base_delay: Duration,
    cx: &Cx,
    transform: &Xfm,
    writer: &TxWriter<DbM>,
    start_str: &str,
    end_str: &str,
) -> Result<(usize, usize, usize)>
where
    Cx: HasConnections,
    Env: HasEnvelope + Send + 'static,
    Env::Item: Send + 'static,
    Xfm: Transform<Input = Env::Item, Output = DbM>,
    DbM: UpsertableInTx + Send + Sync + 'static,
{
    let mut delay = base_delay;
    for attempt in 1..=max_attempts {
        match fetch_transform_upsert::<Cx, Env, DbM, Xfm>(cx, transform, writer, start_str, end_str)
            .await
        {
            Ok(counts) => return Ok(counts),
            Err(e) if attempt == max_attempts => {
                error!(attempt, error = %e, "All retries exhausted");
                return Err(e);
            }
            Err(e) => {
                warn!(attempt, error = %e, delay_secs = delay.as_secs(), "Retrying");
                tokio::time::sleep(delay).await;
                delay = (delay * 2).min(Duration::from_secs(30));
            }
        }
    }
    unreachable!()
}

async fn fetch_transform_upsert<Cx, Env, DbM, Xfm>(
    cx: &Cx,
    transform: &Xfm,
    writer: &TxWriter<DbM>,
    start_str: &str,
    end_str: &str,
) -> Result<(usize, usize, usize)>
where
    Cx: HasConnections,
    Env: HasEnvelope + Send + 'static,
    Env::Item: Send + 'static,
    Xfm: Transform<Input = Env::Item, Output = DbM>,
    DbM: UpsertableInTx + Send + Sync + 'static,
{
    // 1. acquire token (cached / auto-refreshed)
    let token = cx.auth().get_token().await?;
    debug!(endpoint = cx.endpoint(), start_str, end_str, "HTTP fetch");

    // 2. build request
    let mut req = cx
        .http_client()
        .get(cx.endpoint())
        .bearer_auth(&token)
        .query(&[("start_time", start_str), ("end_time", end_str)])
        .timeout(Duration::from_secs(600));

    for (k, v) in cx.extra_query() {
        req = req.query(&[(&k, &v)]);
    }

    // 3. send
    let resp = req.send().await.context("HTTP send failed")?;
    let status = resp.status();

    if status == StatusCode::UNAUTHORIZED {
        cx.auth().invalidate().await;
        anyhow::bail!("401 — token invalidated, will retry");
    }

    let resp = resp.error_for_status().context("API error status")?;
    let bytes = resp.bytes().await.context("Failed to read body")?;
    info!(bytes = bytes.len(), "Body received");

    // 4. parse envelope
    let envelope: Env = serde_json::from_slice(&bytes).map_err(|e| {
        let preview = std::str::from_utf8(&bytes)
            .unwrap_or("<binary>")
            .chars()
            .take(500)
            .collect::<String>();
        error!(error = %e, preview = %preview, "JSON parse failed");
        anyhow::anyhow!("JSON parse: {e}")
    })?;

    let api_items = envelope.into_items();
    let fetched = api_items.len();
    info!(fetched, "Parse OK");

    // 5. transform
    let db_items: Vec<DbM> = api_items
        .into_iter()
        .filter_map(|item| match transform.apply(item) {
            Ok(d) => Some(d),
            Err(e) => {
                warn!(error = %e, "Transform skip");
                None
            }
        })
        .collect();
    debug!(mapped = db_items.len(), "Transform done");

    // 6. transactional upsert (single tx per window)
    let (upserted, skipped) = writer.write_batch(&db_items).await?;

    Ok((fetched, upserted, skipped))
}
