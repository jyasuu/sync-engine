// sync-engine/src/step/multi_transform.rs
//
// SplitTransformStep — fan-out: apply multiple registered transforms to the
//   same source slot, each writing to a different output slot.
//   Use for syncing one API record type to multiple DB tables.
//
// MergeSlotsStep<T> — fan-in: concatenate multiple same-typed slots into one.
//   A pure slot operation — no transform needed. All named slots must hold
//   Vec<T> with the same T.

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use std::any::Any;
use std::collections::HashMap;
use std::marker::PhantomData;
use std::sync::Arc;
use tracing::info;

use crate::context::JobContext;
use crate::registry::TypeRegistry;
use crate::step::Step;

// ── SplitTransformStep ────────────────────────────────────────────────────

/// Reads one slot, applies multiple transforms in sequence, each writing to
/// its own output slot.  The registry builds each transform step at runtime.
///
/// pipeline.toml:
/// ```toml
/// [[main_job.retry_steps]]
/// type   = "split_transform"
/// reads  = "api_rows"
/// transforms = [
///   { transform = "UserTransform",     writes = "db_users" },
///   { transform = "UserRoleTransform", writes = "db_roles" },
/// ]
/// ```
pub struct SplitTransformStep {
    pub reads:      String,
    /// (transform_name, writes_slot, append)
    pub transforms: Vec<(String, String, bool)>,
    pub registry:   Arc<TypeRegistry>,
}

impl SplitTransformStep {
    pub fn new(
        reads:      impl Into<String>,
        transforms: Vec<(String, String, bool)>,
        registry:   Arc<TypeRegistry>,
    ) -> Self {
        Self { reads: reads.into(), transforms, registry }
    }
}

#[async_trait]
impl Step for SplitTransformStep {
    fn name(&self) -> &str { "split_transform" }

    async fn run(&self, ctx: &JobContext) -> Result<()> {
        for (transform_name, writes, append) in &self.transforms {
            let mut params = HashMap::new();
            params.insert("reads".into(),  self.reads.clone());
            params.insert("writes".into(), writes.clone());
            if *append {
                params.insert("append".into(), "true".into());
            }

            let step = self.registry.build_transform(transform_name, &params)
                .map_err(|e| anyhow!(
                    "split_transform: transform \"{transform_name}\" build failed: {e}"
                ))?;

            step.run(ctx).await
                .map_err(|e| anyhow!(
                    "split_transform: transform \"{transform_name}\" → \"{writes}\": {e}"
                ))?;

            info!(transform = %transform_name, writes = %writes,
                  "split_transform: branch done");
        }
        Ok(())
    }
}

// ── MergeSlotsStep<T> ─────────────────────────────────────────────────────

/// Reads multiple slots of the same type T and writes a concatenated Vec<T>
/// into a single output slot. Source slots are left unchanged.
///
/// pipeline.toml:
/// ```toml
/// [[main_job.retry_steps]]
/// type        = "merge_slots"
/// model       = "DbUser"       # registered type — determines T
/// merge_reads = ["slot_a", "slot_b"]
/// writes      = "merged"
/// ```
pub struct MergeSlotsStep<T: Any + Send + Sync + Clone + 'static> {
    pub merge_reads: Vec<String>,
    pub writes:      String,
    pub append:      bool,
    _phantom: PhantomData<T>,
}

impl<T: Any + Send + Sync + Clone + 'static> MergeSlotsStep<T> {
    pub fn new(
        merge_reads: Vec<String>,
        writes:      impl Into<String>,
        append:      bool,
    ) -> Self {
        Self { merge_reads, writes: writes.into(), append, _phantom: PhantomData }
    }
}

#[async_trait]
impl<T> Step for MergeSlotsStep<T>
where
    T: Any + Send + Sync + Clone + 'static,
{
    fn name(&self) -> &str { "merge_slots" }

    async fn run(&self, ctx: &JobContext) -> Result<()> {
        let mut merged: Vec<T> = if self.append {
            ctx.slot_read(&self.writes).await.unwrap_or_default()
        } else {
            Vec::new()
        };

        for slot_name in &self.merge_reads {
            match ctx.slot_read::<Vec<T>>(slot_name).await {
                Ok(items) => {
                    info!(slot = %slot_name, count = items.len(),
                          "merge_slots: merging source");
                    merged.extend(items);
                }
                Err(_) => {
                    tracing::warn!(slot = %slot_name,
                        "merge_slots: slot empty or wrong type — skipping");
                }
            }
        }

        let total = merged.len();
        ctx.slot_write(&self.writes, merged).await?;
        info!(total, writes = %self.writes, "merge_slots: done");
        Ok(())
    }
}
