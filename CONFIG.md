// user-sync/src/main.rs
//
// Scheduler wiring, mutex-skip, and signal handling are now in the engine.
// This file loads config and calls run_job on each cron tick.
//
// To go fully zero-code and drive everything from pipeline.toml, see the
// commented-out alternative at the bottom.

mod config;
mod generated;
mod job;

use anyhow::Result;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio_cron_scheduler::{Job, JobScheduler};
use tracing_subscriber::EnvFilter;

use config::AppConfig;
use job::UserSyncJob;
use sync_engine::run_job;

#[tokio::main]
async fn main() -> Result<()> {
    let cfg = AppConfig::from_env()?;

    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::new(&cfg.log.rust_log))
        .init();

    let cron = cfg.scheduler.cron.clone();
    let cfg_arc = Arc::new(cfg);
    let cfg_for_log = Arc::clone(&cfg_arc);
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
    tracing::info!("user-sync scheduled ({})", cfg_for_log.scheduler.cron);
    tokio::signal::ctrl_c().await?;
    scheduler.shutdown().await?;
    Ok(())
}

// ── Alternative: fully pipeline.toml-driven (no AppConfig needed) ─────────
//
// #[tokio::main]
// async fn main() -> anyhow::Result<()> {
//     dotenvy::dotenv().ok();
//     tracing_subscriber::fmt()
//         .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
//         .init();
//
//     sync_engine::run_from_pipeline_toml(
//         "pipeline.toml",
//         |_cx, _cfg| async move {
//             // With the fully dynamic path the job type parameters are resolved
//             // inside run_from_pipeline_toml via the registered component names.
//             // For the typed path keep using run_job::<UserSyncJob,_,_> above.
//             Ok(())
//         },
//     )
//     .await
// }
