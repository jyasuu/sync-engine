// sync-engine/src/step/sink.rs
//
// TxUpsertStep     — postgres feature — transactional upsert
// SendToQueueStep  — always available (tokio mpsc, no feature gate)

use anyhow::Result;
use async_trait::async_trait;
use std::marker::PhantomData;
use tracing::info;

use crate::context::JobContext;
use crate::step::Step;

// ── TxUpsertStep — requires feature "postgres" ────────────────────────────

#[cfg(feature = "postgres")]
use anyhow::Context as _;
#[cfg(feature = "postgres")]
use tracing::error;
#[cfg(feature = "postgres")]
use crate::components::writer::UpsertableInTx;

#[cfg(feature = "postgres")]
pub struct TxUpsertStep<T: UpsertableInTx> {
    pub reads: String,
    _phantom: PhantomData<T>,
}

#[cfg(feature = "postgres")]
impl<T: UpsertableInTx> TxUpsertStep<T> {
    pub fn new(reads: impl Into<String>) -> Self {
        Self { reads: reads.into(), _phantom: PhantomData }
    }
}

#[cfg(feature = "postgres")]
#[async_trait]
impl<T> Step for TxUpsertStep<T>
where
    T: UpsertableInTx + std::any::Any + Send + Sync + Clone + 'static,
{
    fn name(&self) -> &str { "tx_upsert" }

    async fn run(&self, ctx: &JobContext) -> Result<()> {
        let items: Vec<T> = ctx.slot_read(&self.reads).await?;
        if items.is_empty() {
            info!(slot = %self.reads, "Nothing to upsert");
            return Ok(());
        }

        // If a parent TxStep placed an active transaction, borrow it.
        // Otherwise open a self-contained transaction (legacy / standalone use).
        let borrowed = ctx.take_tx().await;
        let standalone = borrowed.is_none();
        let mut tx = match borrowed {
            Some(t) => t,
            None    => ctx.connections.db.begin().await.context("begin tx")?,
        };

        let mut upserted = 0usize;
        let mut skipped  = 0usize;

        for item in &items {
            match item.upsert_in_tx(&mut tx).await {
                Ok(_)  => upserted += 1,
                Err(e) => {
                    error!(error = %e, "Upsert skipped");
                    skipped += 1;
                }
            }
        }

        if standalone {
            // No parent TxStep — commit immediately.
            tx.commit().await.context("commit tx")?;
            info!(upserted, skipped, slot = %self.reads, "Tx committed (standalone)");
        } else {
            // Return the transaction to the context so TxStep can commit
            // after all sibling steps have completed.
            ctx.put_tx(tx).await;
            info!(upserted, skipped, slot = %self.reads, "Upsert done (tx borrowed)");
        }

        ctx.slot_write("window.upserted", upserted).await?;
        ctx.slot_write("window.skipped",  skipped).await?;

        let prev_u: usize = ctx.slot_read("summary.total_upserted").await.unwrap_or(0);
        let prev_s: usize = ctx.slot_read("summary.total_skipped").await.unwrap_or(0);
        ctx.slot_write("summary.total_upserted", prev_u + upserted).await?;
        ctx.slot_write("summary.total_skipped",  prev_s + skipped).await?;
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
            reads:    reads.into(),
            queue:    queue.into(),
            _phantom: PhantomData,
        }
    }
}

#[async_trait]
impl<T> Step for SendToQueueStep<T>
where
    T: std::any::Any + Send + Sync + Clone + 'static,
{
    fn name(&self) -> &str { "send_to_queue" }

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
