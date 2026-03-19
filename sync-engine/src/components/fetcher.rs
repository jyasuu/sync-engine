// sync-engine/src/components/fetcher.rs
use anyhow::{bail, Context, Result};
use async_trait::async_trait;
use reqwest::{Client, StatusCode};
use serde::de::DeserializeOwned;
use std::time::Duration;
use tracing::info;

use crate::pipeline::primitives::{FetchParams, Fetcher};

/// Implemented by generated *Response envelope types.
pub trait HasEnvelope: DeserializeOwned + Send {
    type Item: Send + 'static;
    fn into_items(self) -> Vec<Self::Item>;
}

pub struct HttpJsonFetcher<T> {
    client: Client,
    endpoint: String,
    realm_type: Option<String>,
    _phantom: std::marker::PhantomData<T>,
}

impl<T: HasEnvelope> HttpJsonFetcher<T> {
    pub fn new(client: Client, endpoint: impl Into<String>, realm_type: Option<String>) -> Self {
        Self {
            client,
            endpoint: endpoint.into(),
            realm_type,
            _phantom: std::marker::PhantomData,
        }
    }
}

#[async_trait]
impl<T> Fetcher for HttpJsonFetcher<T>
where
    T: HasEnvelope + Send + Sync,
    T::Item: Send + 'static,
{
    type Item = T::Item;

    async fn fetch(&self, params: &FetchParams, token: &str) -> Result<Vec<T::Item>> {
        let mut req = self
            .client
            .get(&self.endpoint)
            .bearer_auth(token)
            .timeout(Duration::from_secs(600));
        for (k, v) in &params.query {
            req = req.query(&[(k, v)]);
        }
        if let Some(rt) = &self.realm_type {
            req = req.query(&[("realm_type", rt)]);
        }
        info!(endpoint = %self.endpoint, "HTTP fetch");
        let resp = req.send().await.context("HTTP request failed")?;
        if resp.status() == StatusCode::UNAUTHORIZED {
            bail!("401 Unauthorized");
        }
        Ok(resp
            .error_for_status()
            .context("API error")?
            .json::<T>()
            .await
            .context("JSON parse failed")?
            .into_items())
    }
}
