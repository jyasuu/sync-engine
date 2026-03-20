// sync-engine/src/lib.rs
pub mod codegen; // available when used as build-dependency
pub mod components;
pub mod job;
pub mod pipeline;

pub use components::{
    auth::OAuth2Auth,
    chunker::DateWindowChunker,
    fetcher::{HasEnvelope, HttpJsonFetcher},
    writer::{NoopHook, PostgresWriter, RawSqlHook, Upsertable, WriterAdapter},
};
pub use job::{
    run_job, with_retry, Connections, DateWindowIter, JobSummary, MainJob, PostJob, PreJob,
};
pub use pipeline::adapters::TransformAdapter;
pub use pipeline::component_registry::{AnyFetcher, AnyPostHook, AnyWriter, PrimitiveRegistry};
pub use pipeline::primitives::{Auth, Chunker, FetchParams, Fetcher};
pub use pipeline::registry::{AnySink, AnySource, AnyTransform, Registry};
pub use pipeline::{Sink, Source, Transform};

/// Tonic-style include macro. Use in generated/mod.rs:
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
