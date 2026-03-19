// sync-engine/src/pipeline/adapters.rs
use async_trait::async_trait;
use std::any::Any;
use tracing::warn;

use super::registry::{AnySink, AnySource, AnyTransform};
use super::{Sink, Source, Transform};

pub struct SourceAdapter<S>(pub S);

#[async_trait]
impl<S> AnySource for SourceAdapter<S>
where
    S: Source + Send + Sync,
    S::Item: Any + Send + 'static,
{
    async fn next_batch(&mut self) -> Option<Vec<Box<dyn Any + Send>>> {
        self.0.next_batch().await.map(|batch| {
            batch
                .into_iter()
                .map(|item| Box::new(item) as Box<dyn Any + Send>)
                .collect()
        })
    }
}

pub struct TransformAdapter<T>(pub T);

impl<T, I, O> AnyTransform for TransformAdapter<T>
where
    T: Transform<Input = I, Output = O> + Send + Sync,
    I: Any + Send + 'static,
    O: Any + Send + 'static,
{
    fn apply_batch(&self, items: Vec<Box<dyn Any + Send>>) -> Vec<Box<dyn Any + Send>> {
        items
            .into_iter()
            .filter_map(|boxed| {
                let typed = match boxed.downcast::<I>() {
                    Ok(v) => *v,
                    Err(_) => {
                        warn!(
                            "Type mismatch: source item ≠ transform input — check component.toml"
                        );
                        return None;
                    }
                };
                match self.0.apply(typed) {
                    Ok(out) => Some(Box::new(out) as Box<dyn Any + Send>),
                    Err(e) => {
                        warn!(error = %e, "Transform failed — item skipped");
                        None
                    }
                }
            })
            .collect()
    }
}

pub struct SinkAdapter<K>(pub K);

#[async_trait]
impl<K> AnySink for SinkAdapter<K>
where
    K: Sink + Send + Sync,
    K::Item: Any + Send + 'static,
{
    async fn write(&self, items: Vec<Box<dyn Any + Send>>) {
        let typed: Vec<K::Item> = items
            .into_iter()
            .filter_map(|b| match b.downcast::<K::Item>() {
                Ok(v) => Some(*v),
                Err(_) => {
                    warn!("Type mismatch: transform output ≠ sink input");
                    None
                }
            })
            .collect();
        self.0.write(typed).await;
    }
    async fn on_complete(&self) {
        self.0.on_complete().await;
    }
}
