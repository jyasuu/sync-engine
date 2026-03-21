// user-sync/src/main.rs
//
// The ONLY Rust file in user-sync.
// Registers generated types by name so the engine can instantiate them
// from pipeline.toml. Everything else is configuration.

mod generated;

use generated::{
    envelopes::ApiUserResponse,
    records::{ApiUser, DbUser},
    transforms::UserTransform,
};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenvy::dotenv().ok();

    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let mut registry = sync_engine::TypeRegistry::new();

    // Register every generated type by its schema.toml name.
    // These names must match what's written in pipeline.toml:
    //   envelope = "ApiUserResponse"
    //   transform = "UserTransform"
    //   model = "DbUser"
    registry.register_envelope::<ApiUserResponse>("ApiUserResponse");
    registry.register_transform::<ApiUser, DbUser, UserTransform>("UserTransform");
    registry.register_model::<DbUser>("DbUser");

    // Optional: register a custom post-job hook
    // registry.register_post_hook("MyCustomHook", |ctx| async move {
    //     tracing::info!("Custom hook running");
    //     Ok(())
    // });

    sync_engine::run("pipeline.toml", registry).await
}
