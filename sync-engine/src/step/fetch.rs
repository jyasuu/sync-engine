// sync-engine/src/step/fetch.rs
//
// FetchJsonStep: reads the current window dates from ctx.window,
// calls the configured endpoint with bearer auth, deserialises the
// JSON envelope, and writes the raw items into a named slot.

use anyhow::{Context, Result};
use async_trait::async_trait;
use reqwest::StatusCode;
use serde::de::DeserializeOwned;
use std::marker::PhantomData;
use std::time::Duration;
use tracing::{debug, error, info};

use crate::components::fetcher::HasEnvelope;
use crate::context::JobContext;
use crate::pipeline::primitives::Auth;
use crate::step::Step;

pub struct FetchJsonStep<Env>
where
    Env: HasEnvelope + Send + Sync + 'static,
    Env::Item: Send + Sync + Clone + 'static,
{
    /// Name of the slot to write fetched items into.
    pub writes: String,
    /// Append to an existing Vec (job-scope accumulation) rather than replace.
    pub append: bool,
    _phantom: PhantomData<Env>,
}

impl<Env> FetchJsonStep<Env>
where
    Env: HasEnvelope + Send + Sync + 'static,
    Env::Item: Send + Sync + Clone + 'static,
{
    pub fn new(writes: impl Into<String>, append: bool) -> Self {
        Self {
            writes: writes.into(),
            append,
            _phantom: PhantomData,
        }
    }
}

#[async_trait]
impl<Env> Step for FetchJsonStep<Env>
where
    Env: HasEnvelope + DeserializeOwned + Send + Sync + 'static,
    Env::Item: Send + Sync + Clone + std::any::Any + 'static,
{
    fn name(&self) -> &str {
        "fetch_json"
    }

    async fn run(&self, ctx: &JobContext) -> Result<()> {
        let window = ctx.window.read().await;
        let start_str = window.start_str.clone();
        let end_str = window.end_str.clone();
        drop(window);

        let endpoint = ctx.connections.endpoint.clone();
        let start_param = ctx.connections.start_param.as_str();
        let end_param = ctx.connections.end_param.as_str();
        let token = ctx.connections.auth.get_token().await?;

        debug!(endpoint = %endpoint, start = %start_str, end = %end_str, "HTTP fetch");

        let mut req = ctx
            .connections
            .http
            .get(&endpoint)
            .bearer_auth(&token)
            .query(&[(start_param, &start_str), (end_param, &end_str)])
            .timeout(Duration::from_secs(600));

        for (k, v) in &ctx.connections.extra_query {
            req = req.query(&[(k, v)]);
        }

        let resp = req.send().await.context("HTTP send failed")?;
        let status = resp.status();

        if status == StatusCode::UNAUTHORIZED {
            ctx.connections.auth.invalidate().await;
            anyhow::bail!("401 — token invalidated, will retry");
        }

        let resp = resp.error_for_status().context("API error status")?;
        let bytes = resp.bytes().await.context("Failed to read body")?;
        info!(bytes = bytes.len(), "Body received");

        let envelope: Env = serde_json::from_slice(&bytes).map_err(|e| {
            let preview = std::str::from_utf8(&bytes)
                .unwrap_or("<binary>")
                .chars()
                .take(500)
                .collect::<String>();
            error!(error = %e, preview = %preview, "JSON parse failed");
            anyhow::anyhow!("JSON parse: {e}")
        })?;

        let items = envelope.into_items();
        info!(fetched = items.len(), slot = %self.writes, "Fetch OK");

        // Write count so LogSummaryStep can aggregate across windows
        let count = items.len();
        if self.append {
            ctx.slot_append(&self.writes, items).await?;
        } else {
            ctx.slot_write(&self.writes, items).await?;
        }
        // Accumulate into job-scoped total
        let prev: usize = ctx.slot_read("summary.total_fetched").await.unwrap_or(0);
        ctx.slot_write("summary.total_fetched", prev + count)
            .await?;

        // Window-scoped count for this attempt
        ctx.slot_write("window.fetched", count).await?;

        Ok(())
    }
}
