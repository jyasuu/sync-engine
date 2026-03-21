// sync-engine/src/step/setup.rs
//
// Pre-job setup steps. These run once before main_job starts.
// They populate ctx.connections and declare slots in ctx.slots.

use anyhow::{Context, Result};
use async_trait::async_trait;
use sqlx::postgres::PgPoolOptions;
use std::sync::Arc;
use std::time::Duration;

use crate::context::JobContext;
use crate::slot::SlotScope;
use crate::step::Step;

// ── DeclareSlotStep ───────────────────────────────────────────────────────

/// Declares one slot with a given scope.
/// Must run before any step reads or writes that slot.
pub struct DeclareSlotStep {
    pub key: String,
    pub scope: SlotScope,
}

impl DeclareSlotStep {
    pub fn new(key: impl Into<String>, scope: SlotScope) -> Self {
        Self {
            key: key.into(),
            scope,
        }
    }
}

#[async_trait]
impl Step for DeclareSlotStep {
    fn name(&self) -> &str {
        "declare_slot"
    }

    async fn run(&self, ctx: &JobContext) -> Result<()> {
        ctx.slots.write().await.declare(&self.key, self.scope);
        tracing::debug!(slot = %self.key, scope = ?self.scope, "Slot declared");
        Ok(())
    }
}

// ── RegisterQueueStep ─────────────────────────────────────────────────────

/// Registers a named async channel used by producer/consumer steps.
pub struct RegisterQueueStep {
    pub name: String,
    pub capacity: usize,
}

impl RegisterQueueStep {
    pub fn new(name: impl Into<String>, capacity: usize) -> Self {
        Self {
            name: name.into(),
            capacity,
        }
    }
}

#[async_trait]
impl Step for RegisterQueueStep {
    fn name(&self) -> &str {
        "register_queue"
    }

    async fn run(&self, ctx: &JobContext) -> Result<()> {
        ctx.register_queue(&self.name, self.capacity).await;
        tracing::debug!(queue = %self.name, capacity = self.capacity, "Queue registered");
        Ok(())
    }
}
