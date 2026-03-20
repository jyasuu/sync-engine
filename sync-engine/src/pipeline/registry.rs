// sync-engine/src/pipeline/registry.rs
use std::any::Any;
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use anyhow::{anyhow, Context, Result};
use async_trait::async_trait;
use serde::Deserialize;
use tracing::info;

// ── Type-erased trait objects ─────────────────────────────────────────────

#[async_trait]
pub trait AnySource: Send + Sync {
    async fn next_batch(&mut self) -> Option<Vec<Box<dyn Any + Send>>>;
}

pub trait AnyTransform: Send + Sync {
    fn apply_batch(&self, items: Vec<Box<dyn Any + Send>>) -> Vec<Box<dyn Any + Send>>;
}

#[async_trait]
pub trait AnySink: Send + Sync {
    async fn write(&self, items: Vec<Box<dyn Any + Send>>);
    async fn on_complete(&self) {}
}

// ── Factory types ─────────────────────────────────────────────────────────

type SourceFactory =
    Box<dyn Fn() -> Pin<Box<dyn Future<Output = Result<Box<dyn AnySource>>> + Send>> + Send + Sync>;
type TransformFactory = Box<
    dyn Fn() -> Pin<Box<dyn Future<Output = Result<Box<dyn AnyTransform>>> + Send>> + Send + Sync,
>;
type SinkFactory =
    Box<dyn Fn() -> Pin<Box<dyn Future<Output = Result<Box<dyn AnySink>>> + Send>> + Send + Sync>;

// ── TOML schema ───────────────────────────────────────────────────────────

#[derive(Debug, Deserialize)]
pub struct PipelineToml {
    pub pipeline: Vec<PipelineEntry>,
}

#[derive(Debug, Deserialize)]
pub struct PipelineEntry {
    pub name: Option<String>,
    pub source: String,
    pub transform: String,
    pub sink: String,
    pub cron: String,
}

// ── Registry ─────────────────────────────────────────────────────────────

#[derive(Default)]
pub struct Registry {
    sources: HashMap<String, SourceFactory>,
    transforms: HashMap<String, TransformFactory>,
    sinks: HashMap<String, SinkFactory>,
}

impl Registry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register_source<F, Fut>(&mut self, key: &str, factory: F)
    where
        F: Fn() -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Box<dyn AnySource>>> + Send + 'static,
    {
        self.sources
            .insert(key.to_owned(), Box::new(move || Box::pin(factory())));
    }

    pub fn register_transform<F, Fut>(&mut self, key: &str, factory: F)
    where
        F: Fn() -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Box<dyn AnyTransform>>> + Send + 'static,
    {
        self.transforms
            .insert(key.to_owned(), Box::new(move || Box::pin(factory())));
    }

    pub fn register_sink<F, Fut>(&mut self, key: &str, factory: F)
    where
        F: Fn() -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Box<dyn AnySink>>> + Send + 'static,
    {
        self.sinks
            .insert(key.to_owned(), Box::new(move || Box::pin(factory())));
    }

    pub async fn run_from_toml(self, path: &str) -> Result<()> {
        let raw = std::fs::read_to_string(path).with_context(|| format!("Cannot read {path}"))?;
        let def: PipelineToml =
            toml::from_str(&raw).with_context(|| format!("Cannot parse {path}"))?;

        use tokio_cron_scheduler::{Job, JobScheduler};
        let reg = Arc::new(self);
        let mut scheduler = JobScheduler::new().await?;

        for entry in def.pipeline {
            let label = entry
                .name
                .clone()
                .unwrap_or_else(|| format!("{}/{}/{}", entry.source, entry.transform, entry.sink));

            if !reg.sources.contains_key(&entry.source) {
                return Err(anyhow!("Unknown source: \"{}\"", entry.source));
            }
            if !reg.transforms.contains_key(&entry.transform) {
                return Err(anyhow!("Unknown transform: \"{}\"", entry.transform));
            }
            if !reg.sinks.contains_key(&entry.sink) {
                return Err(anyhow!("Unknown sink: \"{}\"", entry.sink));
            }

            info!(pipeline = %label, cron = %entry.cron, "Scheduling pipeline");

            let (reg2, sk, tk, kk, lbl) = (
                Arc::clone(&reg),
                entry.source.clone(),
                entry.transform.clone(),
                entry.sink.clone(),
                label.clone(),
            );

            scheduler
                .add(Job::new_async(entry.cron.as_str(), move |_, _| {
                    let (reg, sk, tk, kk, lbl) = (
                        Arc::clone(&reg2),
                        sk.clone(),
                        tk.clone(),
                        kk.clone(),
                        lbl.clone(),
                    );
                    Box::pin(async move {
                        info!(pipeline = %lbl, "Cycle starting");
                        let mut source = match reg.sources[&sk]().await {
                            Ok(s) => s,
                            Err(e) => {
                                tracing::error!(error = %e, "Source build failed");
                                return;
                            }
                        };
                        let transform = match reg.transforms[&tk]().await {
                            Ok(t) => t,
                            Err(e) => {
                                tracing::error!(error = %e, "Transform build failed");
                                return;
                            }
                        };
                        let sink = match reg.sinks[&kk]().await {
                            Ok(s) => s,
                            Err(e) => {
                                tracing::error!(error = %e, "Sink build failed");
                                return;
                            }
                        };
                        let mut total = 0usize;
                        while let Some(batch) = source.next_batch().await {
                            sink.write(transform.apply_batch(batch)).await;
                            total += 1;
                        }
                        sink.on_complete().await;
                        info!(pipeline = %lbl, total_batches = total, "Cycle complete");
                    })
                })?)
                .await?;
        }

        scheduler.start().await?;
        info!("All pipelines scheduled — Ctrl-C to exit");
        tokio::signal::ctrl_c().await?;
        scheduler.shutdown().await?;
        Ok(())
    }
}
