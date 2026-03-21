// sync-engine/src/step/transform.rs
//
// TransformStep: reads a Vec<Src> from one slot, applies a Transform,
// and writes Vec<Dst> into another slot (or appends for job-scope).

use anyhow::Result;
use async_trait::async_trait;
use std::marker::PhantomData;
use tracing::{debug, warn};

use crate::context::JobContext;
use crate::pipeline::Transform;
use crate::step::Step;

pub struct TransformStep<Src, Dst, Xfm>
where
    Xfm: Transform<Input = Src, Output = Dst>,
{
    pub reads: String,
    pub writes: String,
    /// Append to an existing Vec rather than replace (for job-scope slots).
    pub append: bool,
    pub xfm: Xfm,
    _phantom: PhantomData<(Src, Dst)>,
}

impl<Src, Dst, Xfm> TransformStep<Src, Dst, Xfm>
where
    Xfm: Transform<Input = Src, Output = Dst>,
{
    pub fn new(
        reads: impl Into<String>,
        writes: impl Into<String>,
        append: bool,
        xfm: Xfm,
    ) -> Self {
        Self {
            reads: reads.into(),
            writes: writes.into(),
            append,
            xfm,
            _phantom: PhantomData,
        }
    }
}

#[async_trait]
impl<Src, Dst, Xfm> Step for TransformStep<Src, Dst, Xfm>
where
    Src: std::any::Any + Send + Sync + Clone + 'static,
    Dst: std::any::Any + Send + Sync + Clone + 'static,
    Xfm: Transform<Input = Src, Output = Dst> + Send + Sync,
{
    fn name(&self) -> &str {
        "transform"
    }

    async fn run(&self, ctx: &JobContext) -> Result<()> {
        let items: Vec<Src> = ctx.slot_read(&self.reads).await?;
        debug!(count = items.len(), from = %self.reads, to = %self.writes, "Transforming");

        let mapped: Vec<Dst> = items
            .into_iter()
            .filter_map(|item| match self.xfm.apply(item) {
                Ok(out) => Some(out),
                Err(e) => {
                    warn!(error = %e, "Transform skip");
                    None
                }
            })
            .collect();

        debug!(mapped = mapped.len(), "Transform done");

        if self.append {
            ctx.slot_append(&self.writes, mapped).await?;
        } else {
            ctx.slot_write(&self.writes, mapped).await?;
        }

        Ok(())
    }
}
