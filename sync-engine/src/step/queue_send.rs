// sync-engine/src/step/queue_send.rs
//
// RabbitmqSendStep — reads Vec<T> from a slot and publishes each item
// via the lapin RabbitmqProducer stored in the context.
// Used automatically when [queues.*] type = "rabbitmq" in pipeline.toml.

use anyhow::Result;
use async_trait::async_trait;
use serde::Serialize;
use std::any::Any;
use std::marker::PhantomData;
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::info;

use crate::context::JobContext;
use crate::step::Step;
use crate::transport::rabbitmq::RabbitmqQueue;

pub struct RabbitmqSendStep<T> {
    pub reads:      String,
    pub queue_name: String,
    _phantom: PhantomData<T>,
}

impl<T> RabbitmqSendStep<T> {
    pub fn new(reads: impl Into<String>, queue_name: impl Into<String>) -> Self {
        Self {
            reads:      reads.into(),
            queue_name: queue_name.into(),
            _phantom:   PhantomData,
        }
    }
}

#[async_trait]
impl<T> Step for RabbitmqSendStep<T>
where
    T: Any + Send + Sync + Clone + Serialize + 'static,
{
    fn name(&self) -> &str { "rmq_publish" }

    async fn run(&self, ctx: &JobContext) -> Result<()> {
        let items: Vec<T> = ctx.slot_read(&self.reads).await?;
        if items.is_empty() {
            return Ok(());
        }

        let slot_key = format!("__rmq_queue_{}", self.queue_name);
        let queue_arc: Arc<Mutex<RabbitmqQueue>> = ctx.slot_read(&slot_key).await?;
        let queue = queue_arc.lock().await;

        queue.producer.publish_batch(&items).await?;
        info!(published = items.len(), queue = %self.queue_name, "RabbitMQ publish OK");
        Ok(())
    }
}
