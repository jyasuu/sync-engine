// sync-engine/src/pipeline/mod.rs
pub mod adapters;
pub mod component_registry;
pub mod composed_source;
pub mod primitives;
pub mod registry;

use anyhow::Result;
use async_trait::async_trait;

#[async_trait]
pub trait Source: Send + Sync {
    type Item: Send;
    async fn next_batch(&mut self) -> Option<Vec<Self::Item>>;
}

pub trait Transform: Send + Sync {
    type Input: Send;
    type Output: Send;
    fn apply(&self, input: Self::Input) -> Result<Self::Output>;
}

#[async_trait]
pub trait Sink: Send + Sync {
    type Item: Send;
    async fn write(&self, items: Vec<Self::Item>);
    async fn on_complete(&self) {}
}
