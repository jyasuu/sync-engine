// sync-engine/src/step/sink.rs
//
// TxUpsertStep  — reads a Vec<T> from a slot, opens a Postgres transaction,
//                 upserts each row, commits.
//
// SendToQueueStep — reads a Vec<T> from a slot, sends each item to a named
//                   channel for the consumer task to process.

use anyhow::{Context, Result};
use async_trait::async_trait;
use std::marker::PhantomData;
use tracing::{error, info};

use crate::components::writer::UpsertableInTx;
use crate::context::JobContext;
use crate::step::Step;

// ── TxUpsertStep ──────────────────────────────────────────────────────────

pub struct TxUpsertStep<T: UpsertableInTx> {
    pub reads: String,
    _phantom: PhantomData<T>,
}

impl<T: UpsertableInTx> TxUpsertStep<T> {
    pub fn new(reads: impl Into<String>) -> Self {
        Self {
            reads: reads.into(),
            _phantom: PhantomData,
        }
    }
}

#[async_trait]
impl<T> Step for TxUpsertStep<T>
where
    T: UpsertableInTx + std::any::Any + Send + Sync + Clone + 'static,
{
    fn name(&self) -> &str {
        "tx_upsert"
    }

    async fn run(&self, ctx: &JobContext) -> Result<()> {
        let items: Vec<T> = ctx.slot_read(&self.reads).await?;
        if items.is_empty() {
            info!(slot = %self.reads, "Nothing to upsert");
            return Ok(());
        }

        let mut tx = ctx.connections.db.begin().await.context("begin tx")?;
        let mut upserted = 0usize;
        let mut skipped = 0usize;

        for item in &items {
            match item.upsert_in_tx(&mut tx).await {
                Ok(_) => upserted += 1,
                Err(e) => {
                    error!(error = %e, "Upsert skipped");
                    skipped += 1;
                }
            }
        }

        tx.commit().await.context("commit tx")?;
        info!(upserted, skipped, slot = %self.reads, "Tx committed");

        // Window-scoped counts for runner.rs to read per-window
        ctx.slot_write("window.upserted", upserted).await?;
        ctx.slot_write("window.skipped", skipped).await?;

        // Accumulate into job-scoped totals readable in post_job
        let prev_u: usize = ctx.slot_read("summary.total_upserted").await.unwrap_or(0);
        let prev_s: usize = ctx.slot_read("summary.total_skipped").await.unwrap_or(0);
        ctx.slot_write("summary.total_upserted", prev_u + upserted)
            .await?;
        ctx.slot_write("summary.total_skipped", prev_s + skipped)
            .await?;
        Ok(())
    }
}

// ── SendToQueueStep ───────────────────────────────────────────────────────

pub struct SendToQueueStep<T: Send + 'static> {
    /// Slot to read items from.
    pub reads: String,
    /// Queue to send items into.
    pub queue: String,
    _phantom: PhantomData<T>,
}

impl<T: Send + 'static> SendToQueueStep<T> {
    pub fn new(reads: impl Into<String>, queue: impl Into<String>) -> Self {
        Self {
            reads: reads.into(),
            queue: queue.into(),
            _phantom: PhantomData,
        }
    }
}

#[async_trait]
impl<T> Step for SendToQueueStep<T>
where
    T: std::any::Any + Send + Sync + Clone + 'static,
{
    fn name(&self) -> &str {
        "send_to_queue"
    }

    async fn run(&self, ctx: &JobContext) -> Result<()> {
        let items: Vec<T> = ctx.slot_read(&self.reads).await?;
        let count = items.len();

        for item in items {
            ctx.queue_send(&self.queue, item).await?;
        }

        info!(sent = count, queue = %self.queue, "Items queued");
        Ok(())
    }
}
