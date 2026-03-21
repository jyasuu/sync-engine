// sync-engine/src/step/mod.rs

pub mod consumer;
pub mod control;
pub mod fetch;
pub mod setup;
pub mod sink;
pub mod transform;

use anyhow::Result;
use async_trait::async_trait;
use std::sync::Arc;

use crate::context::JobContext;

// ── Step trait ────────────────────────────────────────────────────────────

/// The fundamental unit of work in a pipeline.
///
/// Every built-in operation (fetch, transform, upsert, sleep, …) implements
/// this trait. Business crates implement it for custom logic.
///
/// Steps communicate exclusively through `JobContext` — they never call
/// each other directly. This makes each step independently testable.
#[async_trait]
pub trait Step: Send + Sync {
    /// Human-readable name shown in logs.
    fn name(&self) -> &str;

    /// Execute the step. Read from ctx.slots / ctx.connections as needed,
    /// write results back into ctx.slots.
    async fn run(&self, ctx: &JobContext) -> Result<()>;
}

// ── StepRunner ────────────────────────────────────────────────────────────

/// Runs a sequence of steps, sharing a single JobContext.
/// Used for pre_job, post_job, and (with extra logic) main_job.
pub struct StepRunner {
    pub steps: Vec<Box<dyn Step>>,
}

impl StepRunner {
    pub fn new() -> Self {
        Self { steps: vec![] }
    }

    pub fn push(&mut self, step: impl Step + 'static) {
        self.steps.push(Box::new(step));
    }

    pub fn push_boxed(&mut self, step: Box<dyn Step>) {
        self.steps.push(step);
    }

    pub async fn run_all(&self, ctx: &Arc<JobContext>) -> Result<()> {
        for step in &self.steps {
            tracing::debug!(step = step.name(), "Running step");
            step.run(ctx).await.map_err(|e| {
                tracing::error!(step = step.name(), error = %e, "Step failed");
                e
            })?;
        }
        Ok(())
    }
}

impl Default for StepRunner {
    fn default() -> Self {
        Self::new()
    }
}
