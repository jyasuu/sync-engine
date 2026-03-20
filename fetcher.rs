// sync-engine/src/lib.rs
pub mod codegen;
pub mod components;
pub mod job;
pub mod pipeline;
pub mod pipeline_runner;
pub mod standard_job;

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

// ── StandardJob (Gap 2) ───────────────────────────────────────────────────
pub use standard_job::{HasConnections, HasIteratorCfg, HasRetryCfg, StandardJob};

// ── Pipeline runner (Gaps 4 + 5) ─────────────────────────────────────────
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

/// Tonic-style include macro for generated code. Use in generated/mod.rs:
///
/// ```rust
/// pub mod records {
///     use chrono::{DateTime, Utc};
///     sync_engine::include_schema!("records");
/// }
/// ```
#[macro_export]
macro_rules! include_schema {
    ($name:literal) => {
        include!(concat!(env!("OUT_DIR"), "/", $name, ".rs"));
    };
}
