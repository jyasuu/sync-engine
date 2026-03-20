// user-sync/src/main.rs
mod config;
mod connections;
mod generated;
mod job;

use anyhow::Result;
use std::sync::Arc;
use tokio::sync::Mutex;
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
        .with_env_filter(EnvFilter::new(&cfg.log.rust_log))
        .init();

    let cron = cfg.scheduler.cron.clone();
    let cfg_arc: Arc<AppConfig> = Arc::new(cfg);
    let cfg_for_log = Arc::clone(&cfg_arc);

    // Mutex ensures only one job runs at a time — if a tick fires while
    // the previous job is still running, it skips rather than stacking.
    let lock: Arc<Mutex<()>> = Arc::new(Mutex::new(()));

    let mut scheduler = JobScheduler::new().await?;

    scheduler
        .add(Job::new_async(cron.as_str(), move |_, _| {
            let cfg = Arc::clone(&cfg_arc);
            let lock = Arc::clone(&lock);
            Box::pin(async move {
                // try_lock: if already running, skip this tick
                let _guard = match lock.try_lock() {
                    Ok(g) => g,
                    Err(_) => {
                        tracing::debug!("Previous job still running — skipping this tick");
                        return;
                    }
                };
                if let Err(e) =
                    run_job::<UserSyncJob, JobConnections, AppConfig>(cfg.as_ref()).await
                {
                    tracing::error!(error = %e, "Job failed");
                }
            })
        })?)
        .await?;

    scheduler.start().await?;
    tracing::info!("user-sync scheduled ({})", cfg_for_log.scheduler.cron);

    tokio::signal::ctrl_c().await?;
    scheduler.shutdown().await?;
    Ok(())
}
