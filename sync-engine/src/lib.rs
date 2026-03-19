// sync-engine/src/lib.rs
pub mod components;
pub mod pipeline;

pub use components::{
    auth::OAuth2Auth,
    chunker::DateWindowChunker,
    fetcher::{HasEnvelope, HttpJsonFetcher},
    writer::{NoopHook, PostgresWriter, RawSqlHook, Upsertable, WriterAdapter},
};
pub use pipeline::adapters::TransformAdapter;
pub use pipeline::component_registry::{AnyFetcher, AnyPostHook, AnyWriter, PrimitiveRegistry};
pub use pipeline::primitives::{Auth, Chunker, FetchParams, Fetcher};
pub use pipeline::registry::{AnySink, AnySource, AnyTransform, Registry};
pub use pipeline::{Sink, Source, Transform};
