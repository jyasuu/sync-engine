// sync-engine/src/step/autocommit.rs
//
// AutocommitUpsertStep — postgres strategy: autocommit
//
// Each row is wrapped in its own minimal transaction (begin→upsert→commit).
// This is the practical "autocommit" equivalent using the UpsertableInTx trait
// that all generated models already implement — no extra trait needed.
//
// Use this when:
//   - Window-level atomicity is not required
//   - Partial success is acceptable (failed rows are skipped, rest continue)
//   - You want to avoid long-held transactions on large windows
//
// Requires feature "postgres".

#![cfg(feature = "postgres")]

use anyhow::{Context, Result};
use async_trait::async_trait;
use std::marker::PhantomData;
use tracing::{error, info};

use crate::components::writer::UpsertableInTx;
use crate::context::JobContext;
use crate::step::Step;

pub struct AutocommitUpsertStep<T: UpsertableInTx> {
    pub reads: String,
    _phantom: PhantomData<T>,
}

impl<T: UpsertableInTx> AutocommitUpsertStep<T> {
    pub fn new(reads: impl Into<String>) -> Self {
        Self { reads: reads.into(), _phantom: PhantomData }
    }
}

#[async_trait]
impl<T> Step for AutocommitUpsertStep<T>
where
    T: UpsertableInTx + std::any::Any + Send + Sync + Clone + 'static,
{
    fn name(&self) -> &str { "autocommit_upsert" }

    async fn run(&self, ctx: &JobContext) -> Result<()> {
        let items: Vec<T> = ctx.slot_read(&self.reads).await?;
        if items.is_empty() {
            info!(slot = %self.reads, "Nothing to upsert");
            return Ok(());
        }

        let pool = &ctx.connections.db;
        let mut upserted = 0usize;
        let mut skipped  = 0usize;

        // Each row gets its own tiny tx — commit immediately after each row.
        // This is the closest approximation to autocommit when only
        // UpsertableInTx is available.
        for item in &items {
            match pool.begin().await.context("begin autocommit tx") {
                Ok(mut tx) => match item.upsert_in_tx(&mut tx).await {
                    Ok(_) => {
                        let _ = tx.commit().await;
                        upserted += 1;
                    }
                    Err(e) => {
                        error!(error = %e, "Autocommit upsert skipped");
                        skipped += 1;
                    }
                },
                Err(e) => {
                    error!(error = %e, "Autocommit: begin tx failed");
                    skipped += 1;
                }
            }
        }

        info!(upserted, skipped, slot = %self.reads, strategy = "autocommit", "Upsert done");

        ctx.slot_write("window.upserted", upserted).await?;
        ctx.slot_write("window.skipped",  skipped).await?;
        let prev_u: usize = ctx.slot_read("summary.total_upserted").await.unwrap_or(0);
        let prev_s: usize = ctx.slot_read("summary.total_skipped").await.unwrap_or(0);
        ctx.slot_write("summary.total_upserted", prev_u + upserted).await?;
        ctx.slot_write("summary.total_skipped",  prev_s + skipped).await?;
        Ok(())
    }
}
