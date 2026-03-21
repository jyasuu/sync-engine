// sync-engine/src/step/consumer.rs
//
// SpawnConsumerStep runs in pre_job. It:
//   1. Takes the receiver side of a named queue.
//   2. Spawns a tokio task that processes items as they arrive.
//   3. commit_mode controls when the task commits to Postgres:
//        PerBatch      — one tx per received batch (safer)
//        DrainInPostJob — accumulates; the post_job DrainQueueStep commits
//
// The spawned handle is stored in ctx so post_job can await it.

use anyhow::{Context, Result};
use async_trait::async_trait;
use std::marker::PhantomData;
use tokio::task::JoinHandle;
use tracing::{error, info};

use crate::components::writer::UpsertableInTx;
use crate::context::JobContext;
use crate::slot::SlotScope;
use crate::step::Step;

// ── CommitMode ────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CommitMode {
    /// Commit to Postgres after each batch received from the queue.
    PerBatch,
    /// Accumulate items into a job-scoped slot; post_job commits them.
    DrainInPostJob,
}

// ── ConsumerHandle ────────────────────────────────────────────────────────

/// Stored in a job-scoped slot so post_job can await the task.
pub struct ConsumerHandle {
    pub handle: JoinHandle<()>,
    pub queue_name: String,
}

// ── SpawnConsumerStep ─────────────────────────────────────────────────────

pub struct SpawnConsumerStep<T>
where
    T: UpsertableInTx + Send + Sync + Clone + 'static,
{
    pub queue: String,
    pub commit_mode: CommitMode,
    /// Job-scoped slot to accumulate into when commit_mode = DrainInPostJob.
    pub accum_slot: Option<String>,
    _phantom: PhantomData<T>,
}

impl<T> SpawnConsumerStep<T>
where
    T: UpsertableInTx + Send + Sync + Clone + 'static,
{
    pub fn per_batch(queue: impl Into<String>) -> Self {
        Self {
            queue: queue.into(),
            commit_mode: CommitMode::PerBatch,
            accum_slot: None,
            _phantom: PhantomData,
        }
    }

    pub fn drain_in_post_job(queue: impl Into<String>, accum_slot: impl Into<String>) -> Self {
        Self {
            queue: queue.into(),
            commit_mode: CommitMode::DrainInPostJob,
            accum_slot: Some(accum_slot.into()),
            _phantom: PhantomData,
        }
    }
}

#[async_trait]
impl<T> Step for SpawnConsumerStep<T>
where
    T: UpsertableInTx + std::any::Any + Send + Sync + Clone + 'static,
{
    fn name(&self) -> &str {
        "spawn_consumer"
    }

    async fn run(&self, ctx: &JobContext) -> Result<()> {
        let queue_entry = ctx.get_queue(&self.queue).await?;
        let mut rx = queue_entry
            .take_rx()
            .await
            .context("Consumer already spawned for this queue")?;

        let db = ctx.connections.db.clone();
        let commit_mode = self.commit_mode;
        let queue_name = self.queue.clone();

        // For DrainInPostJob mode we need to send accumulated items back to
        // a job-scoped slot. We do this by storing them on the task's stack
        // and writing via a channel back to the context — but since we can't
        // hold an Arc<JobContext> across the spawn (it would create a cycle),
        // we use a dedicated accumulation sender instead.
        //
        // For simplicity here: DrainInPostJob stores items in a Vec that gets
        // written back via a oneshot when the queue closes.
        let (accum_tx, accum_rx) = tokio::sync::oneshot::channel::<Vec<T>>();

        let handle = tokio::spawn(async move {
            let mut total_upserted = 0usize;
            let mut total_skipped = 0usize;
            let mut accumulated: Vec<T> = Vec::new();

            while let Some(boxed) = rx.recv().await {
                let item = match boxed.downcast::<T>() {
                    Ok(v) => *v,
                    Err(_) => {
                        error!(queue = %queue_name, "Type mismatch in queue — item skipped");
                        continue;
                    }
                };

                match commit_mode {
                    CommitMode::PerBatch => match db.begin().await {
                        Ok(mut tx) => match item.upsert_in_tx(&mut tx).await {
                            Ok(_) => {
                                let _ = tx.commit().await;
                                total_upserted += 1;
                            }
                            Err(e) => {
                                error!(error = %e, "Consumer upsert skipped");
                                total_skipped += 1;
                            }
                        },
                        Err(e) => {
                            error!(error = %e, "Consumer: begin tx failed");
                            total_skipped += 1;
                        }
                    },
                    CommitMode::DrainInPostJob => {
                        accumulated.push(item);
                    }
                }
            }

            match commit_mode {
                CommitMode::PerBatch => {
                    info!(
                        queue       = %queue_name,
                        upserted    = total_upserted,
                        skipped     = total_skipped,
                        "Consumer task finished"
                    );
                }
                CommitMode::DrainInPostJob => {
                    info!(
                        queue      = %queue_name,
                        accumulated = accumulated.len(),
                        "Consumer task finished — items queued for post_job commit"
                    );
                    let _ = accum_tx.send(accumulated);
                }
            }
        });

        // Store the join handle as Option<JoinHandle<()>> so DrainQueueStep
        // can extract it without needing Clone (JoinHandle isn't Clone).
        let handle_slot = format!("__consumer_handle_{}", self.queue);
        {
            let mut slots = ctx.slots.write().await;
            slots.declare(&handle_slot, SlotScope::Job);
        }
        ctx.slot_write(&handle_slot, Some(handle)).await?;

        // For DrainInPostJob: store the oneshot receiver so DrainQueueStep
        // can collect the accumulated items.
        if self.commit_mode == CommitMode::DrainInPostJob {
            let accum_slot = self
                .accum_slot
                .clone()
                .unwrap_or_else(|| format!("__accum_{}", self.queue));
            let rx_slot = format!("__accum_rx_{}", self.queue);
            {
                let mut slots = ctx.slots.write().await;
                slots.declare(&rx_slot, SlotScope::Job);
            }
            ctx.slot_write(&rx_slot, accum_rx).await?;
            // Also declare the target accum slot so DrainQueueStep can write it.
            {
                let mut slots = ctx.slots.write().await;
                slots.declare(&accum_slot, SlotScope::Job);
            }
        }

        info!(queue = %self.queue, mode = ?commit_mode, "Consumer task spawned");
        Ok(())
    }
}
