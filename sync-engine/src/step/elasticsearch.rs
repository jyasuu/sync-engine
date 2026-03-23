// sync-engine/src/step/elasticsearch.rs
//
// EsIndexStep  — sink: index (upsert / delete / create) documents into ES
// EsFetchStep  — source: scroll query from ES into a slot
//
// Implemented directly over reqwest (already a dependency) — no external
// Elasticsearch crate required. Requires feature "elasticsearch".

#![cfg(feature = "elasticsearch")]

use anyhow::{anyhow, Context, Result};
use async_trait::async_trait;
use serde::{de::DeserializeOwned, Serialize};
use std::marker::PhantomData;
use tracing::info;

use crate::context::JobContext;
use crate::step::Step;

// ── EsOp ─────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EsOp {
    /// Create or update — uses _id from `es_id()`. Default.
    Upsert,
    /// Delete documents whose _id matches `es_id()`.
    Delete,
    /// Create only — fails if document already exists.
    Create,
}

impl EsOp {
    pub fn from_str(s: &str) -> Self {
        match s {
            "delete" => EsOp::Delete,
            "create" => EsOp::Create,
            _        => EsOp::Upsert,
        }
    }
}

// ── EsIndexable ──────────────────────────────────────────────────────────

/// Implement on any model type that will be indexed into Elasticsearch.
/// `es_id()` returns the document `_id` string.
pub trait EsIndexable: Serialize + Send + Sync + 'static {
    fn es_id(&self) -> String;
}

// ── helper ────────────────────────────────────────────────────────────────

fn es_base_url(ctx: &JobContext) -> Result<&str> {
    ctx.es_url.as_deref().ok_or_else(|| anyhow!(
        "No Elasticsearch resource configured — \
         add [resources.*] type=\"elasticsearch\" to pipeline.toml"
    ))
}

// ── EsIndexStep ──────────────────────────────────────────────────────────

pub struct EsIndexStep<T: EsIndexable> {
    pub reads:  String,
    pub index:  String,
    pub op:     EsOp,
    _phantom:   PhantomData<T>,
}

impl<T: EsIndexable> EsIndexStep<T> {
    pub fn new(reads: impl Into<String>, index: impl Into<String>, op: EsOp) -> Self {
        Self { reads: reads.into(), index: index.into(), op, _phantom: PhantomData }
    }
}

#[async_trait]
impl<T> Step for EsIndexStep<T>
where
    T: EsIndexable + std::any::Any + Clone + 'static,
{
    fn name(&self) -> &str { "es_index" }

    async fn run(&self, ctx: &JobContext) -> Result<()> {
        let items: Vec<T> = ctx.slot_read(&self.reads).await?;
        if items.is_empty() {
            info!(slot = %self.reads, "Nothing to index");
            return Ok(());
        }

        let base_url = es_base_url(ctx)?;
        let client   = &ctx.connections.http;

        // Build NDJSON bulk body
        let mut body = String::new();
        for item in &items {
            let id = item.es_id();
            let action = match self.op {
                EsOp::Upsert => format!(
                    "{{\"index\":{{\"_index\":\"{}\",\"_id\":\"{}\"}}}}\n",
                    self.index, id
                ),
                EsOp::Create => format!(
                    "{{\"create\":{{\"_index\":\"{}\",\"_id\":\"{}\"}}}}\n",
                    self.index, id
                ),
                EsOp::Delete => format!(
                    "{{\"delete\":{{\"_index\":\"{}\",\"_id\":\"{}\"}}}}\n",
                    self.index, id
                ),
            };
            body.push_str(&action);
            if self.op != EsOp::Delete {
                let doc = serde_json::to_string(item)
                    .context("Failed to serialize document for ES bulk")?;
                body.push_str(&doc);
                body.push('\n');
            }
        }

        let resp = client
            .post(format!("{base_url}/_bulk"))
            .header("Content-Type", "application/x-ndjson")
            .body(body)
            .send()
            .await
            .context("ES bulk request failed")?;

        let status = resp.status();
        if !status.is_success() {
            let text = resp.text().await.unwrap_or_default();
            return Err(anyhow!("ES bulk returned HTTP {status}: {text}"));
        }

        let resp_body: serde_json::Value = resp.json().await
            .context("Failed to parse ES bulk response")?;

        let (mut indexed, mut skipped) = (0usize, 0usize);
        if resp_body["errors"].as_bool().unwrap_or(false) {
            for item_resp in resp_body["items"].as_array().unwrap_or(&vec![]) {
                let op_key = ["index", "create", "delete", "update"]
                    .iter().find(|k| item_resp.get(*k).is_some())
                    .copied().unwrap_or("index");
                if item_resp[op_key]["status"].as_u64().unwrap_or(200) >= 400 {
                    skipped += 1;
                } else {
                    indexed += 1;
                }
            }
        } else {
            indexed = items.len();
        }

        info!(indexed, skipped, index = %self.index, op = ?self.op, "ES bulk done");
        ctx.slot_write("window.upserted", indexed).await?;
        ctx.slot_write("window.skipped",  skipped).await?;
        let prev_u: usize = ctx.slot_read("summary.total_upserted").await.unwrap_or(0);
        let prev_s: usize = ctx.slot_read("summary.total_skipped").await.unwrap_or(0);
        ctx.slot_write("summary.total_upserted", prev_u + indexed).await?;
        ctx.slot_write("summary.total_skipped",  prev_s + skipped).await?;
        Ok(())
    }
}

// ── EsFetchStep ──────────────────────────────────────────────────────────

pub struct EsFetchStep<T: DeserializeOwned + Send + Sync + Clone + 'static> {
    pub writes:     String,
    pub index:      String,
    pub query:      Option<serde_json::Value>,
    pub scroll_ttl: String,
    pub batch_size: i64,
    pub append:     bool,
    _phantom:       PhantomData<T>,
}

impl<T: DeserializeOwned + Send + Sync + Clone + 'static> EsFetchStep<T> {
    pub fn new(
        writes:     impl Into<String>,
        index:      impl Into<String>,
        query:      Option<serde_json::Value>,
        scroll_ttl: impl Into<String>,
        batch_size: i64,
        append:     bool,
    ) -> Self {
        Self {
            writes:     writes.into(),
            index:      index.into(),
            query,
            scroll_ttl: scroll_ttl.into(),
            batch_size,
            append,
            _phantom:   PhantomData,
        }
    }
}

#[async_trait]
impl<T> Step for EsFetchStep<T>
where
    T: DeserializeOwned + std::any::Any + Send + Sync + Clone + 'static,
{
    fn name(&self) -> &str { "es_fetch" }

    async fn run(&self, ctx: &JobContext) -> Result<()> {
        let base_url = es_base_url(ctx)?;
        let client   = &ctx.connections.http;
        let query    = self.query.clone()
            .unwrap_or_else(|| serde_json::json!({ "match_all": {} }));

        let init_url = format!(
            "{base_url}/{index}/_search?scroll={ttl}&size={size}",
            index = self.index, ttl = self.scroll_ttl, size = self.batch_size,
        );
        let mut resp: serde_json::Value = client
            .post(&init_url)
            .json(&serde_json::json!({ "query": query }))
            .send().await.context("ES initial scroll failed")?
            .json().await.context("ES initial scroll parse failed")?;

        let mut all_items: Vec<T> = Vec::new();

        loop {
            let scroll_id = resp["_scroll_id"].as_str().map(str::to_owned);

            let hits = resp["hits"]["hits"]
                .as_array()
                .ok_or_else(|| anyhow!("ES response missing hits.hits"))?;

            if hits.is_empty() {
                if let Some(ref sid) = scroll_id {
                    let _ = client
                        .delete(format!("{base_url}/_search/scroll"))
                        .json(&serde_json::json!({ "scroll_id": sid }))
                        .send().await;
                }
                break;
            }

            for hit in hits {
                let item: T = serde_json::from_value(hit["_source"].clone())
                    .context("Failed to deserialize ES hit _source")?;
                all_items.push(item);
            }

            let sid = match scroll_id { Some(s) => s, None => break };
            resp = client
                .post(format!("{base_url}/_search/scroll"))
                .json(&serde_json::json!({ "scroll": self.scroll_ttl, "scroll_id": sid }))
                .send().await.context("ES scroll continuation failed")?
                .json().await.context("ES scroll continuation parse failed")?;
        }

        let fetched = all_items.len();
        info!(fetched, index = %self.index, "ES scroll complete");

        if self.append {
            let mut existing: Vec<T> = ctx.slot_read(&self.writes).await.unwrap_or_default();
            existing.extend(all_items);
            ctx.slot_write(&self.writes, existing).await?;
        } else {
            ctx.slot_write(&self.writes, all_items).await?;
        }

        ctx.slot_write("window.fetched", fetched).await?;
        let prev: usize = ctx.slot_read("summary.total_fetched").await.unwrap_or(0);
        ctx.slot_write("summary.total_fetched", prev + fetched).await?;
        Ok(())
    }
}
