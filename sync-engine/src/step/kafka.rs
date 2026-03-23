// sync-engine/src/step/kafka.rs
//
// KafkaProduceStep — sink: publish serialized records to a Kafka topic
// KafkaConsumeStep — source: consume a batch from a Kafka topic into a slot
//
// Requires feature "kafka" (rdkafka).

#![cfg(feature = "kafka")]

use anyhow::{anyhow, Context, Result};
use async_trait::async_trait;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::message::OwnedMessage;
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::Message;
use futures::StreamExt as _;
use serde::{de::DeserializeOwned, Serialize};
use std::marker::PhantomData;
use std::time::Duration;
use tracing::{error, info, warn};

use crate::context::JobContext;
use crate::step::Step;

// ── KafkaProduceStep ─────────────────────────────────────────────────────

pub struct KafkaProduceStep<T: Serialize + Send + 'static> {
    pub reads:   String,
    pub topic:   String,
    /// Optional field name to use as Kafka message key. If None, key is empty.
    pub key_field: Option<String>,
    _phantom: PhantomData<T>,
}

impl<T: Serialize + Send + 'static> KafkaProduceStep<T> {
    pub fn new(
        reads:     impl Into<String>,
        topic:     impl Into<String>,
        key_field: Option<String>,
    ) -> Self {
        Self {
            reads:     reads.into(),
            topic:     topic.into(),
            key_field,
            _phantom:  PhantomData,
        }
    }
}

#[async_trait]
impl<T> Step for KafkaProduceStep<T>
where
    T: Serialize + std::any::Any + Send + Sync + Clone + 'static,
{
    fn name(&self) -> &str { "kafka_produce" }

    async fn run(&self, ctx: &JobContext) -> Result<()> {
        let items: Vec<T> = ctx.slot_read(&self.reads).await?;
        if items.is_empty() {
            info!(slot = %self.reads, "Nothing to produce");
            return Ok(());
        }

        let producer: &FutureProducer = ctx.kafka_producer()
            .ok_or_else(|| anyhow!("No Kafka resource configured — add [resources.*] type=\"kafka\""))?;

        let mut produced = 0usize;
        let mut skipped  = 0usize;

        for item in &items {
            let payload = serde_json::to_vec(item)
                .context("Failed to serialize Kafka message")?;

            // Extract key from item if key_field is set
            let key: String = if let Some(ref kf) = self.key_field {
                serde_json::to_value(item)
                    .ok()
                    .and_then(|v| v.get(kf).and_then(|k| k.as_str().map(str::to_owned)))
                    .unwrap_or_default()
            } else {
                String::new()
            };

            let record = FutureRecord::to(&self.topic)
                .payload(&payload)
                .key(key.as_str());

            match producer.send(record, Duration::from_secs(5)).await {
                Ok(_)  => produced += 1,
                Err((e, _)) => {
                    error!(error = %e, topic = %self.topic, "Kafka produce failed — skipping");
                    skipped += 1;
                }
            }
        }

        info!(produced, skipped, topic = %self.topic, "Kafka produce done");

        ctx.slot_write("window.upserted", produced).await?;
        ctx.slot_write("window.skipped",  skipped).await?;
        let prev_u: usize = ctx.slot_read("summary.total_upserted").await.unwrap_or(0);
        let prev_s: usize = ctx.slot_read("summary.total_skipped").await.unwrap_or(0);
        ctx.slot_write("summary.total_upserted", prev_u + produced).await?;
        ctx.slot_write("summary.total_skipped",  prev_s + skipped).await?;
        Ok(())
    }
}

// ── KafkaConsumeStep ─────────────────────────────────────────────────────
//
// Consumes up to `max_messages` messages from a Kafka topic in one step
// execution, writing deserialized records into a slot. Commits offsets after
// the batch is written.
//
// For use as a continuous trigger (run_kafka_consume loop), see
// pipeline_runner.rs `run_kafka_trigger`. For use as a pipeline source step
// (batch pull per window), use this step in [[main_job.retry_steps]].

pub struct KafkaConsumeStep<T: DeserializeOwned + Send + 'static> {
    pub writes:       String,
    pub topic:        String,
    pub max_messages: usize,
    pub timeout_ms:   u64,
    pub append:       bool,
    _phantom: PhantomData<T>,
}

impl<T: DeserializeOwned + Send + 'static> KafkaConsumeStep<T> {
    pub fn new(
        writes:       impl Into<String>,
        topic:        impl Into<String>,
        max_messages: usize,
        timeout_ms:   u64,
        append:       bool,
    ) -> Self {
        Self {
            writes:       writes.into(),
            topic:        topic.into(),
            max_messages,
            timeout_ms,
            append,
            _phantom:     PhantomData,
        }
    }
}

#[async_trait]
impl<T> Step for KafkaConsumeStep<T>
where
    T: DeserializeOwned + std::any::Any + Send + Sync + Clone + 'static,
{
    fn name(&self) -> &str { "kafka_consume" }

    async fn run(&self, ctx: &JobContext) -> Result<()> {
        let consumer: &StreamConsumer = ctx.kafka_consumer()
            .ok_or_else(|| anyhow!("No Kafka resource configured"))?;

        let mut items: Vec<T> = if self.append {
            ctx.slot_read(&self.writes).await.unwrap_or_default()
        } else {
            Vec::new()
        };

        let deadline = tokio::time::Instant::now()
            + Duration::from_millis(self.timeout_ms);

        let mut count = 0usize;

        while count < self.max_messages {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                break;
            }

            let msg: Result<OwnedMessage, _> = tokio::time::timeout(
                remaining,
                async {
                    consumer.stream().next().await
                        .ok_or_else(|| anyhow!("Kafka stream ended"))
                        .and_then(|r| r.map(|m| m.detach()).map_err(|e| anyhow!("{e}")))
                },
            )
            .await
            .unwrap_or_else(|_| Err(anyhow!("timeout")));

            match msg {
                Ok(m) => {
                    if let Some(payload) = m.payload() {
                        match serde_json::from_slice::<T>(payload) {
                            Ok(item) => {
                                items.push(item);
                                count += 1;
                            }
                            Err(e) => {
                                warn!(error = %e, topic = %self.topic, "Failed to deserialize Kafka message — skipping");
                            }
                        }
                    }
                    // Commit offset for this message
                    consumer.store_offset_from_message(&m)
                        .unwrap_or_else(|e| warn!(error = %e, "Failed to store offset"));
                }
                Err(e) if e.to_string() == "timeout" => break,
                Err(e) => {
                    warn!(error = %e, "Kafka consume error — stopping batch");
                    break;
                }
            }
        }

        // Commit all stored offsets
        if count > 0 {
            consumer.commit_consumer_state(rdkafka::consumer::CommitMode::Async)
                .unwrap_or_else(|e| warn!(error = %e, "Kafka offset commit failed"));
        }

        let fetched = count;
        info!(fetched, topic = %self.topic, "Kafka consume done");

        ctx.slot_write(&self.writes, items).await?;
        ctx.slot_write("window.fetched", fetched).await?;
        let prev: usize = ctx.slot_read("summary.total_fetched").await.unwrap_or(0);
        ctx.slot_write("summary.total_fetched", prev + fetched).await?;
        Ok(())
    }
}

// ── Helper: build producers and consumers from config ─────────────────────

pub fn build_producer(brokers: &str) -> Result<FutureProducer> {
    ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .set("message.timeout.ms", "5000")
        .create()
        .context("Failed to create Kafka producer")
}

pub fn build_consumer(brokers: &str, group_id: &str, topic: &str) -> Result<StreamConsumer> {
    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .set("group.id", group_id)
        .set("enable.auto.commit", "false")
        .set("auto.offset.reset", "earliest")
        .create()
        .context("Failed to create Kafka consumer")?;

    consumer
        .subscribe(&[topic])
        .with_context(|| format!("Failed to subscribe to Kafka topic \"{topic}\""))?;

    Ok(consumer)
}
