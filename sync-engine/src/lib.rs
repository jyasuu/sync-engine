// sync-engine/src/lib.rs
pub mod codegen;
pub use codegen::{generate, generate_architecture_svg, generate_architecture_svg_file, generate_config_doc, generate_pipeline_skeleton};
pub mod components;
pub mod config_value;
pub mod context;
pub mod job;
pub mod pipeline;
pub mod pipeline_runner;
pub mod registry;
pub mod runner;
pub mod sinks;
pub mod slot;
pub mod standard_job;
pub mod step;
pub mod transport;

#[cfg(test)]
mod tests;

// ── Components ────────────────────────────────────────────────────────────
pub use components::{
    auth::OAuth2Auth,
    chunker::DateWindowChunker,
    fetcher::{HasEnvelope, HttpJsonFetcher},
};
#[cfg(feature = "postgres")]
pub use components::writer::{
    NoopHook, PostgresWriter, RawSqlHook, TxWriter,
    Upsertable, UpsertableInTx, WriterAdapter,
};

// ── Job traits ────────────────────────────────────────────────────────────
pub use job::{run_job, with_retry, DateWindowIter, JobSummary, MainJob, PostJob, PreJob};
#[cfg(feature = "postgres")]
pub use job::Connections;

// ── Step system ───────────────────────────────────────────────────────────
pub use config_value::{ConfigValue, EnvRef};
pub use context::{Connections as JobConnections, JobContext, WindowMeta};
pub use runner::{MainJobRunner, WindowConfig};
pub use slot::{SlotMap, SlotScope};
pub use step::Step;
pub use step::consumer::{CommitMode, ConsumerHandle};
#[cfg(feature = "postgres")]
pub use step::consumer::SpawnConsumerStep;
pub use step::control::{DrainQueueStep, LogSummaryStep, RawSqlStep, SleepStep};
pub use step::fetch::FetchJsonStep;
pub use step::setup::{DeclareSlotStep, RegisterQueueStep};
pub use step::sink::SendToQueueStep;
#[cfg(feature = "postgres")]
pub use step::sink::TxUpsertStep;
pub use step::transform::TransformStep;

// ── New backend steps ─────────────────────────────────────────────────────
#[cfg(feature = "postgres")]
pub use step::autocommit::AutocommitUpsertStep;
#[cfg(feature = "elasticsearch")]
pub use step::elasticsearch::{EsOp, EsIndexable, EsIndexStep, EsFetchStep};
#[cfg(feature = "kafka")]
pub use step::kafka::{KafkaProduceStep, KafkaConsumeStep};

// ── File sinks ────────────────────────────────────────────────────────────
pub use sinks::FileSink;
#[cfg(feature = "csv")]
pub use sinks::CsvSink;
#[cfg(feature = "excel")]
pub use sinks::ExcelSink;

// ── Transport ─────────────────────────────────────────────────────────────
#[cfg(feature = "rabbitmq")]
pub use transport::{RabbitmqConfig, RabbitmqConsumer, RabbitmqProducer, RabbitmqQueue};

// ── TypeRegistry + run() entry-point ─────────────────────────────────────
pub use registry::TypeRegistry;
// register_model_es is a method on TypeRegistry, available when features
// "elasticsearch" + "postgres" are both enabled. No separate re-export needed.
pub use pipeline_runner::{
    run, validate, build_context, build_window_cfg, build_steps,
    PipelineConfig, ResourceDef, SlotDef, SlotScopeStr, QueueDef,
    PreJobConfig, MainJobConfig, MainStepConfig, IteratorConfig, RetryConfig,
    PostJobConfig, PostStepConfig, SchedulerConfig, TriggerConfig,
};

// ── StandardJob (legacy) ──────────────────────────────────────────────────
pub use standard_job::{HasConnections, HasIteratorCfg, HasRetryCfg, StandardJob};

// ── Pipeline primitives ───────────────────────────────────────────────────
pub use pipeline::adapters::TransformAdapter;
pub use pipeline::component_registry::{AnyFetcher, AnyPostHook, AnyWriter, PrimitiveRegistry};
pub use pipeline::primitives::{Auth, Chunker, FetchParams, Fetcher};
pub use pipeline::registry::{AnySink, AnySource, AnyTransform, Registry};
pub use pipeline::{Sink, Source, Transform};

#[macro_export]
macro_rules! include_schema {
    ($name:literal) => {
        include!(concat!(env!("OUT_DIR"), "/", $name, ".rs"));
    };
}
