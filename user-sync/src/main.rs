// user-sync/src/main.rs
//
// Loads pipeline.toml, parses it into PipelineConfig, and drives the
// scheduled job. The cron expression, all connections, and all step wiring
// come from pipeline.toml — this file never needs to change.

mod generated;
mod job;

use anyhow::Result;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio_cron_scheduler::{Job, JobScheduler};

use job::UserSyncJob;
use sync_engine::{pipeline_runner::PipelineConfig, run_job};

#[tokio::main]
async fn main() -> Result<()> {
    dotenvy::dotenv().ok();

    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let raw = std::fs::read_to_string("pipeline.toml").expect("Cannot read pipeline.toml");
    let pipeline_cfg: PipelineConfig = toml::from_str(&raw).expect("Cannot parse pipeline.toml");

    let cron = pipeline_cfg
        .scheduler
        .cron
        .resolve()
        .expect("SCHEDULER__CRON not set");

    let cfg_arc = Arc::new(pipeline_cfg);
    let lock: Arc<Mutex<()>> = Arc::new(Mutex::new(()));

    let mut scheduler = JobScheduler::new().await?;

    scheduler
        .add(Job::new_async(cron.as_str(), move |_, _| {
            let cfg = Arc::clone(&cfg_arc);
            let lock = Arc::clone(&lock);
            Box::pin(async move {
                let _guard = match lock.try_lock() {
                    Ok(g) => g,
                    Err(_) => {
                        tracing::debug!("Previous job still running — skipping tick");
                        return;
                    }
                };
                if let Err(e) = run_job::<UserSyncJob, _, _>(cfg.as_ref()).await {
                    tracing::error!(error = %e, "Job failed");
                }
            })
        })?)
        .await?;

    scheduler.start().await?;
    tracing::info!("user-sync scheduled ({})", cron);
    tokio::signal::ctrl_c().await?;
    scheduler.shutdown().await?;
    Ok(())
}
