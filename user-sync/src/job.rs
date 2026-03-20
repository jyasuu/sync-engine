// user-sync/src/job.rs
use anyhow::{Context, Result};
use async_trait::async_trait;
use chrono::Utc;
use reqwest::StatusCode;
use std::time::Duration;

use sync_engine::{with_retry, DateWindowIter, JobSummary, MainJob, PostJob, PreJob, Transform};

use crate::config::AppConfig;
use crate::connections::JobConnections;
use crate::generated::{envelopes::ApiUserResponse, records::DbUser, transforms::UserTransform};

pub struct UserSyncJob;

#[async_trait]
impl PreJob for UserSyncJob {
    type Cx = JobConnections;
    type Cfg = AppConfig;
    async fn run(cfg: &AppConfig) -> Result<JobConnections> {
        JobConnections::create(cfg).await
    }
}

#[async_trait]
impl MainJob for UserSyncJob {
    type Cx = JobConnections;
    type Cfg = AppConfig;

    async fn run(cx: &JobConnections, cfg: &AppConfig) -> Result<JobSummary> {
        let mut summary = JobSummary::default();

        let mut iter = DateWindowIter::new(
            cfg.source.start_interval,
            cfg.source.end_interval,
            cfg.source.interval_limit,
        );

        let mut last_window_ok = true;

        while let Some((start, end)) = iter.next_window_with_sleep(last_window_ok).await {
            summary.windows_processed += 1;
            tracing::info!(start, end, "Processing window");

            // Clone everything out of cx so the closure owns it fully —
            // borrowing cx across await points inside FnMut causes the future
            // to be silently dropped in some executor configurations.
            let endpoint = cx.user_service.endpoint.clone();
            let realm_type = cx.user_service.realm_type.clone();
            let http = cx.user_service.http.clone();
            let db = cx.db.clone(); // PgPool is Arc-backed, cheap clone
            let auth = cx.auth.clone(); // Arc<Mutex<>> inside, cheap clone

            let now = Utc::now();
            let start_str = (now - chrono::Duration::days(start))
                .format("%Y%m%d")
                .to_string();
            let end_str = (now - chrono::Duration::days(end))
                .format("%Y%m%d")
                .to_string();

            let result =
                with_retry(5, || {
                    let endpoint = endpoint.clone();
                    let realm_type = realm_type.clone();
                    let http = http.clone();
                    let db = db.clone();
                    let start_str = start_str.clone();
                    let end_str = end_str.clone();
                    let auth = auth.clone(); // clone per attempt so async move can own it

                    async move {
                        // 1. token
                        let token = auth.get_token().await?;
                        tracing::debug!("Calling {endpoint} [{start_str}..{end_str}]");

                        // 2. fetch
                        let mut req = http
                            .get(&endpoint)
                            .bearer_auth(&token)
                            .query(&[("start_time", &start_str), ("end_time", &end_str)])
                            .timeout(Duration::from_secs(600));

                        if let Some(ref rt) = realm_type {
                            req = req.query(&[("realm_type", rt)]);
                        }

                        let resp = req.send().await.context("HTTP send failed")?;
                        let status = resp.status();
                        tracing::debug!("HTTP {status}");

                        if status == StatusCode::UNAUTHORIZED {
                            auth.invalidate().await;
                            anyhow::bail!("401 — token refreshed, retrying");
                        }

                        let resp = resp.error_for_status().context("API error status")?;

                        // 3. read body
                        let bytes = resp.bytes().await.context("Failed to read body")?;
                        tracing::info!(bytes = bytes.len(), "Body received");

                        // 4. parse — log preview on failure
                        let envelope: ApiUserResponse = serde_json::from_slice(&bytes)
                        .map_err(|e| {
                            let preview = std::str::from_utf8(&bytes)
                                .unwrap_or("<binary>")
                                .chars().take(500).collect::<String>();
                            tracing::error!(error = %e, preview = %preview, "JSON parse failed");
                            anyhow::anyhow!("JSON parse: {e}")
                        })?;

                        let fetched = envelope.data.len();
                        tracing::info!(fetched, "Parse OK");

                        // 5. transform
                        let transform = UserTransform;
                        let db_users: Vec<DbUser> = envelope
                            .data
                            .into_iter()
                            .filter_map(|u| match transform.apply(u) {
                                Ok(d) => Some(d),
                                Err(e) => {
                                    tracing::warn!(error = %e, "Transform skip");
                                    None
                                }
                            })
                            .collect();
                        tracing::debug!(mapped = db_users.len(), "Transform done");

                        // 6. begin tx
                        tracing::debug!("Beginning tx");
                        let mut tx = db.begin().await.context("begin tx failed")?;
                        tracing::debug!("Tx open");

                        // 7. upsert
                        let mut upserted = 0usize;
                        let mut skipped = 0usize;
                        for user in &db_users {
                            match user.upsert_in_tx(&mut tx).await {
                                Ok(_) => upserted += 1,
                                Err(e) => {
                                    tracing::error!(error = %e, pccuid = user.pccuid, "Upsert err");
                                    skipped += 1;
                                }
                            }
                        }
                        tracing::debug!(upserted, skipped, "Committing");

                        // 8. commit
                        tx.commit().await.context("commit failed")?;
                        tracing::info!(upserted, skipped, "Window committed");
                        Ok((fetched, upserted, skipped))
                    }
                })
                .await;

            match result {
                Ok((fetched, upserted, skipped)) => {
                    summary.records_fetched += fetched;
                    summary.records_upserted += upserted;
                    summary.records_skipped += skipped;
                    last_window_ok = true;
                }
                Err(e) => {
                    let msg = format!("Window [{start}..{end}] failed: {e}");
                    tracing::error!("{msg}");
                    summary.errors.push(msg);
                    last_window_ok = false;
                }
            }
        }

        Ok(summary)
    }
}

#[async_trait]
impl PostJob for UserSyncJob {
    type Cx = JobConnections;
    type Cfg = AppConfig;

    async fn run(summary: JobSummary, cx: &JobConnections, cfg: &AppConfig) -> Result<()> {
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
                .execute(&cx.db)
                .await
                .context("Post-sync SQL failed")?;
        }
        Ok(())
    }
}
