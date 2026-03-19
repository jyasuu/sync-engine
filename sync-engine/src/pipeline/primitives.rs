// sync-engine/src/pipeline/primitives.rs
use anyhow::Result;
use async_trait::async_trait;
use std::collections::HashMap;

#[derive(Debug, Clone, Default)]
pub struct FetchParams {
    pub query: HashMap<String, String>,
}

impl FetchParams {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn with(mut self, k: impl Into<String>, v: impl Into<String>) -> Self {
        self.query.insert(k.into(), v.into());
        self
    }
}

#[async_trait]
pub trait Chunker: Send + Sync {
    async fn next_window(&mut self) -> Option<FetchParams>;
}

#[async_trait]
pub trait Auth: Send + Sync {
    async fn get_token(&self) -> Result<String>;
    async fn invalidate(&self);
}

#[async_trait]
pub trait Fetcher: Send + Sync {
    type Item: Send + 'static;
    async fn fetch(&self, params: &FetchParams, token: &str) -> Result<Vec<Self::Item>>;
}

#[async_trait]
pub trait AnyPostHook: Send + Sync {
    async fn on_complete(&self);
}
