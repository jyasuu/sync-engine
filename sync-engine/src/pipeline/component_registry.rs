// sync-engine/src/pipeline/component_registry.rs
use std::any::Any;
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use anyhow::{anyhow, Context, Result};
use async_trait::async_trait;
use serde::Deserialize;
use tracing::info;

use super::primitives::{Auth, Chunker, FetchParams};
use super::registry::{AnySink, AnySource, AnyTransform, Registry};

// ── Erased fetcher ────────────────────────────────────────────────────────

#[async_trait]
pub trait AnyFetcher: Send + Sync {
    async fn fetch_erased(
        &self,
        params: &FetchParams,
        token: &str,
    ) -> Result<Vec<Box<dyn Any + Send>>>;
}

#[async_trait]
impl<F: super::primitives::Fetcher + Send + Sync> AnyFetcher for F
where
    F::Item: 'static,
{
    async fn fetch_erased(
        &self,
        params: &FetchParams,
        token: &str,
    ) -> Result<Vec<Box<dyn Any + Send>>> {
        self.fetch(params, token).await.map(|v| {
            v.into_iter()
                .map(|i| Box::new(i) as Box<dyn Any + Send>)
                .collect()
        })
    }
}

// ── Erased writer / post-hook ─────────────────────────────────────────────

#[async_trait]
pub trait AnyWriter: Send + Sync {
    async fn write_erased(&self, items: Vec<Box<dyn Any + Send>>);
}

#[async_trait]
pub trait AnyPostHook: Send + Sync {
    async fn on_complete(&self);
}

// ── TOML schema ───────────────────────────────────────────────────────────

#[derive(Debug, Deserialize)]
pub struct ComponentToml {
    #[serde(default)]
    pub source: HashMap<String, SourceDef>,
    #[serde(default)]
    pub transform: HashMap<String, TransformDef>,
    #[serde(default)]
    pub sink: HashMap<String, SinkDef>,
}

#[derive(Debug, Deserialize)]
pub struct SourceDef {
    pub chunker: String,
    pub auth: String,
    pub fetcher: String,
}
#[derive(Debug, Deserialize)]
pub struct TransformDef {
    pub mapper: String,
}
#[derive(Debug, Deserialize)]
pub struct SinkDef {
    pub writer: String,
    pub post_hook: Option<String>,
}

// ── Factory types ─────────────────────────────────────────────────────────

type AsyncFactory<T> =
    Box<dyn Fn() -> Pin<Box<dyn Future<Output = Result<T>> + Send>> + Send + Sync>;

// ── PrimitiveRegistry ─────────────────────────────────────────────────────

#[derive(Default)]
pub struct PrimitiveRegistry {
    chunkers: HashMap<String, AsyncFactory<Box<dyn Chunker>>>,
    auths: HashMap<String, AsyncFactory<Box<dyn Auth>>>,
    fetchers: HashMap<String, AsyncFactory<Box<dyn AnyFetcher>>>,
    mappers: HashMap<String, AsyncFactory<Box<dyn AnyTransform>>>,
    writers: HashMap<String, AsyncFactory<Box<dyn AnyWriter>>>,
    post_hooks: HashMap<String, AsyncFactory<Box<dyn AnyPostHook>>>,
}

macro_rules! register_fn {
    ($map:expr, $key:expr, $f:expr) => {
        $map.insert($key.into(), Box::new(move || Box::pin($f())));
    };
}

impl PrimitiveRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register_chunker<F, Fut>(&mut self, key: &str, f: F)
    where
        F: Fn() -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Box<dyn Chunker>>> + Send + 'static,
    {
        register_fn!(self.chunkers, key, f);
    }

    pub fn register_auth<F, Fut>(&mut self, key: &str, f: F)
    where
        F: Fn() -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Box<dyn Auth>>> + Send + 'static,
    {
        register_fn!(self.auths, key, f);
    }

    pub fn register_fetcher<F, Fut>(&mut self, key: &str, f: F)
    where
        F: Fn() -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Box<dyn AnyFetcher>>> + Send + 'static,
    {
        register_fn!(self.fetchers, key, f);
    }

    pub fn register_mapper<F, Fut>(&mut self, key: &str, f: F)
    where
        F: Fn() -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Box<dyn AnyTransform>>> + Send + 'static,
    {
        register_fn!(self.mappers, key, f);
    }

    pub fn register_writer<F, Fut>(&mut self, key: &str, f: F)
    where
        F: Fn() -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Box<dyn AnyWriter>>> + Send + 'static,
    {
        register_fn!(self.writers, key, f);
    }

    pub fn register_post_hook<F, Fut>(&mut self, key: &str, f: F)
    where
        F: Fn() -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Box<dyn AnyPostHook>>> + Send + 'static,
    {
        register_fn!(self.post_hooks, key, f);
    }

    pub async fn build_pipeline_registry(self, component_path: &str) -> Result<Registry> {
        let raw = std::fs::read_to_string(component_path)
            .with_context(|| format!("Cannot read {component_path}"))?;
        let def: ComponentToml =
            toml::from_str(&raw).with_context(|| format!("Cannot parse {component_path}"))?;

        let prim = Arc::new(self);
        let mut registry = Registry::new();

        for (name, src) in def.source {
            let p = Arc::clone(&prim);
            let (ck, ak, fk) = (src.chunker.clone(), src.auth.clone(), src.fetcher.clone());
            if !p.chunkers.contains_key(&ck) {
                return Err(anyhow!("Unknown chunker: \"{ck}\" in [source.{name}]"));
            }
            if !p.auths.contains_key(&ak) {
                return Err(anyhow!("Unknown auth: \"{ak}\" in [source.{name}]"));
            }
            if !p.fetchers.contains_key(&fk) {
                return Err(anyhow!("Unknown fetcher: \"{fk}\" in [source.{name}]"));
            }
            info!(source = %name, %ck, %ak, %fk, "Assembling source");
            registry.register_source(&name, move || {
                let (p, ck, ak, fk) = (Arc::clone(&p), ck.clone(), ak.clone(), fk.clone());
                async move {
                    let chunker = p.chunkers[&ck]().await?;
                    let auth = p.auths[&ak]().await?;
                    let fetcher = p.fetchers[&fk]().await?;
                    Ok(Box::new(ErasedComposedSource {
                        chunker,
                        auth,
                        fetcher,
                    }) as Box<dyn AnySource>)
                }
            });
        }

        for (name, xf) in def.transform {
            let p = Arc::clone(&prim);
            let mk = xf.mapper.clone();
            if !p.mappers.contains_key(&mk) {
                return Err(anyhow!("Unknown mapper: \"{mk}\" in [transform.{name}]"));
            }
            info!(transform = %name, %mk, "Assembling transform");
            registry.register_transform(&name, move || {
                let (p, mk) = (Arc::clone(&p), mk.clone());
                async move { p.mappers[&mk]().await }
            });
        }

        for (name, sk) in def.sink {
            let p = Arc::clone(&prim);
            let (wk, hk) = (sk.writer.clone(), sk.post_hook.clone());
            if !p.writers.contains_key(&wk) {
                return Err(anyhow!("Unknown writer: \"{wk}\" in [sink.{name}]"));
            }
            if let Some(ref h) = hk {
                if !p.post_hooks.contains_key(h) {
                    return Err(anyhow!("Unknown post_hook: \"{h}\" in [sink.{name}]"));
                }
            }
            info!(sink = %name, %wk, "Assembling sink");
            registry.register_sink(&name, move || {
                let (p, wk, hk) = (Arc::clone(&p), wk.clone(), hk.clone());
                async move {
                    let writer = p.writers[&wk]().await?;
                    let post_hook = if let Some(ref k) = hk {
                        Some(p.post_hooks[k]().await?)
                    } else {
                        None
                    };
                    Ok(Box::new(AssembledSink { writer, post_hook }) as Box<dyn AnySink>)
                }
            });
        }

        Ok(registry)
    }
}

// ── Internal assembled types ──────────────────────────────────────────────

struct ErasedComposedSource {
    chunker: Box<dyn Chunker>,
    auth: Box<dyn Auth>,
    fetcher: Box<dyn AnyFetcher>,
}

#[async_trait]
impl AnySource for ErasedComposedSource {
    async fn next_batch(&mut self) -> Option<Vec<Box<dyn Any + Send>>> {
        let params = self.chunker.next_window().await?;
        let result = async {
            let token = self.auth.get_token().await?;
            match self.fetcher.fetch_erased(&params, &token).await {
                Ok(items) => Ok(items),
                Err(e) => {
                    if e.to_string().contains("401") {
                        self.auth.invalidate().await;
                    }
                    Err(e)
                }
            }
        }
        .await;
        match result {
            Ok(items) => Some(items),
            Err(e) => {
                tracing::error!(error = %e, "Fetch failed");
                Some(vec![])
            }
        }
    }
}

struct AssembledSink {
    writer: Box<dyn AnyWriter>,
    post_hook: Option<Box<dyn AnyPostHook>>,
}

#[async_trait]
impl AnySink for AssembledSink {
    async fn write(&self, items: Vec<Box<dyn Any + Send>>) {
        self.writer.write_erased(items).await;
    }
    async fn on_complete(&self) {
        if let Some(ref h) = self.post_hook {
            h.on_complete().await;
        }
    }
}
