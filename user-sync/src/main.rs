// user-sync/src/main.rs
mod config;
mod connections;
mod generated;
mod job;

use anyhow::Result;
use std::sync::Arc;
use tokio_cron_scheduler::{Job, JobScheduler};
use tracing_subscriber::EnvFilter;

use config::AppConfig;
use connections::JobConnections;
use job::UserSyncJob;
use sync_engine::run_job;

#[tokio::main]
async fn main() -> Result<()> {
    let cfg = AppConfig::from_env()?;

    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("user_sync=info")),
        )
        .init();

    let cron = cfg.cron.clone(); // clone before Arc::new consumes cfg
    let cfg_arc: Arc<AppConfig> = Arc::new(cfg);

    // clone for the logging line BEFORE moving into the closure
    let cfg_for_log = Arc::clone(&cfg_arc);

    let mut scheduler = JobScheduler::new().await?; // mut for shutdown()

    scheduler
        .add(Job::new_async(cron.as_str(), move |_, _| {
            let cfg: Arc<AppConfig> = Arc::clone(&cfg_arc);
            Box::pin(async move {
                if let Err(e) =
                    run_job::<UserSyncJob, JobConnections, AppConfig>(cfg.as_ref()).await
                {
                    tracing::error!(error = %e, "Job failed");
                }
            })
        })?)
        .await?;

    scheduler.start().await?;
    tracing::info!("user-sync scheduled ({})", cfg_for_log.cron);

    tokio::signal::ctrl_c().await?;
    scheduler.shutdown().await?;
    Ok(())
}
