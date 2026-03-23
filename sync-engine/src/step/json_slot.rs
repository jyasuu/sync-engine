// sync-engine/src/step/json_slot.rs
//
// SlotToJsonStep — serialize any typed slot to Vec<serde_json::Value>.
//   The resulting JSON slot can be passed to raw_sql via ctx, logged,
//   forwarded to ES without EsIndexable, or inspected for debugging.
//
// JsonToSlotStep<T> — deserialize a Vec<serde_json::Value> slot back to Vec<T>.
//   Use after receiving JSON from a Kafka consumer or ES fetch when the
//   payload type differs from the registered model.

use anyhow::{Context, Result};
use async_trait::async_trait;
use serde::{de::DeserializeOwned, Serialize};
use std::any::Any;
use std::marker::PhantomData;
use tracing::info;

use crate::context::JobContext;
use crate::step::Step;

// ── SlotToJsonStep ────────────────────────────────────────────────────────

/// Serialize Vec<T> slot → Vec<serde_json::Value> slot.
///
/// pipeline.toml:
/// ```toml
/// [[main_job.retry_steps]]
/// type   = "slot_to_json"
/// model  = "DbUser"        # registered type — determines T
/// reads  = "db_rows"
/// writes = "db_rows_json"
/// ```
pub struct SlotToJsonStep<T: Serialize + Any + Send + Sync + Clone + 'static> {
    pub reads:  String,
    pub writes: String,
    pub append: bool,
    _phantom: PhantomData<T>,
}

impl<T: Serialize + Any + Send + Sync + Clone + 'static> SlotToJsonStep<T> {
    pub fn new(reads: impl Into<String>, writes: impl Into<String>, append: bool) -> Self {
        Self { reads: reads.into(), writes: writes.into(), append, _phantom: PhantomData }
    }
}

#[async_trait]
impl<T> Step for SlotToJsonStep<T>
where
    T: Serialize + Any + Send + Sync + Clone + 'static,
{
    fn name(&self) -> &str { "slot_to_json" }

    async fn run(&self, ctx: &JobContext) -> Result<()> {
        let items: Vec<T> = ctx.slot_read(&self.reads).await?;

        let json_items: Vec<serde_json::Value> = items
            .iter()
            .map(|item| serde_json::to_value(item)
                .with_context(|| format!("slot_to_json: serialize failed for slot {}", self.reads)))
            .collect::<Result<_>>()?;

        let count = json_items.len();

        if self.append {
            let mut existing: Vec<serde_json::Value> =
                ctx.slot_read(&self.writes).await.unwrap_or_default();
            existing.extend(json_items);
            ctx.slot_write(&self.writes, existing).await?;
        } else {
            ctx.slot_write(&self.writes, json_items).await?;
        }

        info!(count, reads = %self.reads, writes = %self.writes,
              "slot_to_json: done");
        Ok(())
    }
}

// ── JsonToSlotStep<T> ─────────────────────────────────────────────────────

/// Deserialize Vec<serde_json::Value> slot → Vec<T> slot.
///
/// pipeline.toml:
/// ```toml
/// [[main_job.retry_steps]]
/// type   = "json_to_slot"
/// model  = "DbUser"        # registered type — determines T
/// reads  = "db_rows_json"
/// writes = "db_rows"
/// ```
pub struct JsonToSlotStep<T: DeserializeOwned + Any + Send + Sync + Clone + 'static> {
    pub reads:  String,
    pub writes: String,
    pub append: bool,
    _phantom: PhantomData<T>,
}

impl<T: DeserializeOwned + Any + Send + Sync + Clone + 'static> JsonToSlotStep<T> {
    pub fn new(reads: impl Into<String>, writes: impl Into<String>, append: bool) -> Self {
        Self { reads: reads.into(), writes: writes.into(), append, _phantom: PhantomData }
    }
}

#[async_trait]
impl<T> Step for JsonToSlotStep<T>
where
    T: DeserializeOwned + Any + Send + Sync + Clone + 'static,
{
    fn name(&self) -> &str { "json_to_slot" }

    async fn run(&self, ctx: &JobContext) -> Result<()> {
        let json_items: Vec<serde_json::Value> = ctx.slot_read(&self.reads).await?;

        let mut items: Vec<T> = Vec::with_capacity(json_items.len());
        let mut skipped = 0usize;

        for value in json_items {
            match serde_json::from_value::<T>(value) {
                Ok(item) => items.push(item),
                Err(e)   => {
                    tracing::warn!(error = %e, "json_to_slot: deserialize failed — skipping row");
                    skipped += 1;
                }
            }
        }

        let count = items.len();

        if self.append {
            let mut existing: Vec<T> =
                ctx.slot_read(&self.writes).await.unwrap_or_default();
            existing.extend(items);
            ctx.slot_write(&self.writes, existing).await?;
        } else {
            ctx.slot_write(&self.writes, items).await?;
        }

        info!(count, skipped, reads = %self.reads, writes = %self.writes,
              "json_to_slot: done");
        Ok(())
    }
}
