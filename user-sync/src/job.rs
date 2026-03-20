// user-sync/src/job.rs
use anyhow::{Context, Result};
use async_trait::async_trait;
use chrono::Utc;
use reqwest::StatusCode;

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
            cfg.source.start_interval, // nested
            cfg.source.end_interval,
            cfg.source.interval_limit,
        );

        while let Some((start, end)) = iter.next_window().await {
            summary.windows_processed += 1;
            tracing::info!(start, end, "Processing window");

            let result = with_retry(5, || async {
                let token = cx.auth.get_token().await?;
                let now = Utc::now();
                let fmt = "%Y%m%d";
                let start_str = (now - chrono::Duration::days(start))
                    .format(fmt)
                    .to_string();
                let end_str = (now - chrono::Duration::days(end)).format(fmt).to_string();

                let mut req = cx
                    .user_service
                    .http
                    .get(&cx.user_service.endpoint)
                    .bearer_auth(&token)
                    .query(&[("start_time", &start_str), ("end_time", &end_str)])
                    .timeout(std::time::Duration::from_secs(600));

                if let Some(ref rt) = cx.user_service.realm_type {
                    req = req.query(&[("realm_type", rt)]);
                }

                let resp = req.send().await.context("HTTP request failed")?;
                if resp.status() == StatusCode::UNAUTHORIZED {
                    cx.auth.invalidate().await;
                    anyhow::bail!("401 Unauthorized — token refreshed, will retry");
                }

                let envelope: ApiUserResponse = resp
                    .error_for_status()
                    .context("API error")?
                    .json()
                    .await
                    .context("JSON parse failed")?;

                tracing::info!("Fetched {} users", envelope.data.len());

                let transform = UserTransform;
                let db_users: Vec<DbUser> = envelope
                    .data
                    .into_iter()
                    .filter_map(|u| match transform.apply(u) {
                        Ok(d) => Some(d),
                        Err(e) => {
                            tracing::warn!(error = %e, "Transform skipped");
                            None
                        }
                    })
                    .collect();

                let mut tx = cx.db.begin().await.context("Failed to begin tx")?;

                let mut upserted = 0usize;
                let mut skipped = 0usize;
                for user in &db_users {
                    match user.upsert_in_tx(&mut tx).await {
                        Ok(_) => upserted += 1,
                        Err(e) => {
                            tracing::error!(error = %e, pccuid = user.pccuid, "Upsert failed");
                            skipped += 1;
                        }
                    }
                }

                tx.commit().await.context("Failed to commit tx")?;
                tracing::info!(upserted, skipped, "Window committed");
                Ok((db_users.len(), upserted, skipped))
            })
            .await;

            match result {
                Ok((fetched, upserted, skipped)) => {
                    summary.records_fetched += fetched;
                    summary.records_upserted += upserted;
                    summary.records_skipped += skipped;
                }
                Err(e) => {
                    let msg = format!("Window [{start}..{end}] failed: {e}");
                    tracing::error!("{msg}");
                    summary.errors.push(msg);
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
            tracing::warn!(error = %err, "Error during job");
        }

        if !cfg.sink.sync_sql.trim().is_empty() {
            // nested
            tracing::info!("Running post-sync SQL");
            sqlx::raw_sql(&cfg.sink.sync_sql)
                .execute(&cx.db)
                .await
                .context("Post-sync SQL failed")?;
        }

        Ok(())
    }
}
