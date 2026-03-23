// sync-engine/src/transport/mod.rs
#[cfg(feature = "rabbitmq")]
pub mod rabbitmq;

#[cfg(feature = "rabbitmq")]
pub use rabbitmq::{RabbitmqConfig, RabbitmqConsumer, RabbitmqProducer, RabbitmqQueue};
