// sync-engine/src/transport/rabbitmq.rs — requires feature "rabbitmq"
#![cfg(feature = "rabbitmq")]
// RabbitMQ transport for the producer/consumer pipeline pattern (case 4).
//
// RabbitmqProducer  — publishes serialized items to an exchange.
// RabbitmqConsumer  — subscribes to a queue, deserializes items, calls a
//                     user-supplied handler (typically tx_upsert).
//
// Items are serialized as JSON. The exchange type is "direct" by default.
// The routing key is user-configured in pipeline.toml [queues.*].

use anyhow::{Context, Result};
use futures_lite::StreamExt;
use lapin::{
    options::{
        BasicAckOptions, BasicConsumeOptions, BasicPublishOptions,
        ExchangeDeclareOptions, QueueBindOptions, QueueDeclareOptions,
    },
    types::FieldTable,
    BasicProperties, Channel, Connection, ConnectionProperties,
    ExchangeKind,
};
use serde::{de::DeserializeOwned, Serialize};
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::{error, info, warn};

// ── RabbitmqConfig ────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct RabbitmqConfig {
    pub url:         String,
    pub exchange:    String,
    pub routing_key: String,
    /// Consumer queue name — defaults to "{exchange}.{routing_key}"
    pub queue_name:  Option<String>,
}

impl RabbitmqConfig {
    pub fn queue(&self) -> String {
        self.queue_name
            .clone()
            .unwrap_or_else(|| format!("{}.{}", self.exchange, self.routing_key))
    }
}

// ── Connection helper ─────────────────────────────────────────────────────

async fn open_channel(url: &str) -> Result<Channel> {
    let conn = Connection::connect(url, ConnectionProperties::default())
        .await
        .with_context(|| format!("RabbitMQ connect failed: {url}"))?;
    let channel = conn.create_channel().await.context("RabbitMQ create channel")?;
    Ok(channel)
}

// ── RabbitmqProducer ──────────────────────────────────────────────────────

/// Publishes JSON-serialized items to a RabbitMQ exchange.
pub struct RabbitmqProducer {
    channel:     Arc<Mutex<Channel>>,
    config:      RabbitmqConfig,
}

impl RabbitmqProducer {
    pub async fn connect(config: RabbitmqConfig) -> Result<Self> {
        let channel = open_channel(&config.url).await?;

        // Declare the exchange as durable direct — idempotent, safe to repeat.
        channel
            .exchange_declare(
                &config.exchange,
                ExchangeKind::Direct,
                ExchangeDeclareOptions { durable: true, ..Default::default() },
                FieldTable::default(),
            )
            .await
            .context("RabbitMQ exchange_declare")?;

        info!(exchange = %config.exchange, routing_key = %config.routing_key,
              "RabbitMQ producer connected");

        Ok(Self {
            channel: Arc::new(Mutex::new(channel)),
            config,
        })
    }

    /// Publish a single item as a JSON message.
    pub async fn publish<T: Serialize>(&self, item: &T) -> Result<()> {
        let payload = serde_json::to_vec(item)
            .context("RabbitMQ: JSON serialization failed")?;

        let ch = self.channel.lock().await;
        ch.basic_publish(
            &self.config.exchange,
            &self.config.routing_key,
            BasicPublishOptions::default(),
            &payload,
            BasicProperties::default()
                .with_content_type("application/json".into())
                .with_delivery_mode(2), // persistent
        )
        .await
        .context("RabbitMQ basic_publish")?
        .await
        .context("RabbitMQ publish confirm")?;

        Ok(())
    }

    /// Publish a batch of items.
    pub async fn publish_batch<T: Serialize>(&self, items: &[T]) -> Result<()> {
        for item in items {
            self.publish(item).await?;
        }
        Ok(())
    }
}

// ── RabbitmqConsumer ──────────────────────────────────────────────────────

/// Subscribes to a RabbitMQ queue and calls a handler for each message.
/// Runs until the shutdown signal is received.
pub struct RabbitmqConsumer {
    config: RabbitmqConfig,
}

impl RabbitmqConsumer {
    pub fn new(config: RabbitmqConfig) -> Self {
        Self { config }
    }

    /// Start consuming. For each received message, call `handler(item)`.
    /// Acks the message on success; rejects (no requeue) on handler error.
    /// Returns when the connection is closed or `shutdown` is signalled.
    pub async fn run<T, F, Fut>(
        &self,
        handler:  F,
        shutdown: tokio::sync::oneshot::Receiver<()>,
    ) -> Result<()>
    where
        T: DeserializeOwned + Send + 'static,
        F: Fn(T) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = Result<()>> + Send + 'static,
    {
        let channel   = open_channel(&self.config.url).await?;
        let queue_name = self.config.queue();

        // Declare exchange + queue + binding — idempotent
        channel
            .exchange_declare(
                &self.config.exchange,
                ExchangeKind::Direct,
                ExchangeDeclareOptions { durable: true, ..Default::default() },
                FieldTable::default(),
            )
            .await
            .context("RabbitMQ consumer exchange_declare")?;

        channel
            .queue_declare(
                &queue_name,
                QueueDeclareOptions { durable: true, ..Default::default() },
                FieldTable::default(),
            )
            .await
            .context("RabbitMQ queue_declare")?;

        channel
            .queue_bind(
                &queue_name,
                &self.config.exchange,
                &self.config.routing_key,
                QueueBindOptions::default(),
                FieldTable::default(),
            )
            .await
            .context("RabbitMQ queue_bind")?;

        let mut consumer = channel
            .basic_consume(
                &queue_name,
                "sync-engine-consumer",
                BasicConsumeOptions::default(),
                FieldTable::default(),
            )
            .await
            .context("RabbitMQ basic_consume")?;

        info!(queue = %queue_name, exchange = %self.config.exchange, "RabbitMQ consumer started");

        let handler = Arc::new(handler);
        let mut shutdown = shutdown;

        loop {
            tokio::select! {
                delivery = consumer.next() => {
                    match delivery {
                        Some(Ok(delivery)) => {
                            match serde_json::from_slice::<T>(&delivery.data) {
                                Ok(item) => {
                                    let h = Arc::clone(&handler);
                                    match h(item).await {
                                        Ok(_) => {
                                            delivery
                                                .ack(BasicAckOptions::default())
                                                .await
                                                .unwrap_or_else(|e| warn!(error = %e, "RabbitMQ ack failed"));
                                        }
                                        Err(e) => {
                                            error!(error = %e, "Handler failed — message rejected");
                                            delivery
                                                .nack(lapin::options::BasicNackOptions {
                                                    requeue: false,
                                                    ..Default::default()
                                                })
                                                .await
                                                .unwrap_or_else(|e| warn!(error = %e, "RabbitMQ nack failed"));
                                        }
                                    }
                                }
                                Err(e) => {
                                    error!(error = %e, "JSON deserialization failed — message rejected");
                                    delivery
                                        .nack(lapin::options::BasicNackOptions {
                                            requeue: false,
                                            ..Default::default()
                                        })
                                        .await
                                        .unwrap_or_else(|e| warn!(error = %e, "RabbitMQ nack failed"));
                                }
                            }
                        }
                        Some(Err(e)) => {
                            error!(error = %e, "RabbitMQ delivery error");
                            break;
                        }
                        None => {
                            info!("RabbitMQ consumer stream ended");
                            break;
                        }
                    }
                }
                _ = &mut shutdown => {
                    info!(queue = %queue_name, "RabbitMQ consumer shutdown signal received");
                    break;
                }
            }
        }

        Ok(())
    }
}

// ── RabbitmqQueue ─────────────────────────────────────────────────────────

/// Registered in the engine's queue map when [queues.*] type = "rabbitmq".
/// Holds the producer and a shutdown sender for the consumer task.
pub struct RabbitmqQueue {
    pub producer:      RabbitmqProducer,
    pub shutdown_tx:   Option<tokio::sync::oneshot::Sender<()>>,
}

impl RabbitmqQueue {
    pub async fn connect(config: RabbitmqConfig) -> Result<Self> {
        let producer = RabbitmqProducer::connect(config).await?;
        Ok(Self { producer, shutdown_tx: None })
    }

    /// Signal the consumer task to stop gracefully.
    pub fn shutdown(&mut self) {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
    }
}
