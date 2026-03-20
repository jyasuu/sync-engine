// sync-engine/src/pipeline/composed_source.rs
use async_trait::async_trait;
use backon::{ExponentialBuilder, Retryable};
use std::time::Duration;
use tracing::{error, info, warn};

use super::primitives::{Auth, Chunker, Fetcher};
use super::Source;

pub struct ComposedSource<C, A, F> {
    pub chunker: C,
    pub auth: A,
    pub fetcher: F,
}

impl<C, A, F> ComposedSource<C, A, F> {
    pub fn new(chunker: C, auth: A, fetcher: F) -> Self {
        Self {
            chunker,
            auth,
            fetcher,
        }
    }
}

#[async_trait]
impl<C, A, F> Source for ComposedSource<C, A, F>
where
    C: Chunker + Send + Sync,
    A: Auth + Send + Sync,
    F: Fetcher + Send + Sync,
    F::Item: Send + 'static,
{
    type Item = F::Item;

    async fn next_batch(&mut self) -> Option<Vec<Self::Item>> {
        let params = self.chunker.next_window().await?;

        let result = (|| async {
            let token = self.auth.get_token().await?;
            match self.fetcher.fetch(&params, &token).await {
                Ok(items) => Ok(items),
                Err(e) => {
                    if e.to_string().contains("401") {
                        warn!("401 — invalidating token before retry");
                        self.auth.invalidate().await;
                    }
                    Err(e)
                }
            }
        })
        .retry(
            ExponentialBuilder::default()
                .with_min_delay(Duration::from_secs(2))
                .with_max_delay(Duration::from_secs(30))
                .with_factor(2.0)
                .with_max_times(5),
        )
        .await;

        match result {
            Ok(items) => {
                info!("Batch: {} items", items.len());
                Some(items)
            }
            Err(e) => {
                error!(error = %e, "All retries exhausted — empty batch");
                Some(vec![])
            }
        }
    }
}
