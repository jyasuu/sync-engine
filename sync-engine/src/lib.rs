// sync-engine/src/lib.rs
pub mod codegen;
pub mod components;
pub mod config_value;
pub mod context;
pub mod job;
pub mod pipeline;
pub mod pipeline_runner;
pub mod runner;
pub mod slot;
pub mod standard_job;
pub mod step;

// ── Components ────────────────────────────────────────────────────────────
pub use components::{
    auth::OAuth2Auth,
    chunker::DateWindowChunker,
    fetcher::{HasEnvelope, HttpJsonFetcher},
    writer::{
        NoopHook, PostgresWriter, RawSqlHook, TxWriter, Upsertable, UpsertableInTx, WriterAdapter,
    },
};

// ── Job traits ────────────────────────────────────────────────────────────
pub use job::{
    run_job, with_retry, Connections, DateWindowIter, JobSummary, MainJob, PostJob, PreJob,
};

// ── New slot/context/step system ──────────────────────────────────────────
pub use config_value::{ConfigValue, EnvRef};
pub use context::{Connections as JobConnections, JobContext, WindowMeta};
pub use runner::{MainJobRunner, WindowConfig};
pub use slot::{SlotMap, SlotScope};
pub use step::consumer::{CommitMode, ConsumerHandle, SpawnConsumerStep};
pub use step::control::{DrainQueueStep, LogSummaryStep, RawSqlStep, SleepStep};
pub use step::fetch::FetchJsonStep;
pub use step::setup::{DeclareSlotStep, RegisterQueueStep};
pub use step::sink::{SendToQueueStep, TxUpsertStep};
pub use step::transform::TransformStep;
pub use step::Step;

// ── StandardJob (existing, kept for backwards compat) ─────────────────────
pub use standard_job::{HasConnections, HasIteratorCfg, HasRetryCfg, StandardJob};

// ── Pipeline runner ───────────────────────────────────────────────────────
pub use pipeline_runner::{
    run_from_pipeline_toml, PipelineConfig, PostJobExecutor, ResolvedIteratorCfg,
    StandardConnections,
};

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
