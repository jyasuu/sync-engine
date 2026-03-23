// user-sync/build.rs
fn main() {
    sync_engine::codegen::generate("schema.toml");
    sync_engine::codegen::generate_pipeline_skeleton("schema.toml", "pipeline.toml");
    sync_engine::codegen::generate_config_doc("config.toml", "schema.toml", "CONFIG.md");
    sync_engine::codegen::generate_architecture_svg(
        "schema.toml",
        "pipeline.toml",
        "ARCHITECTURE.md",
    );
}
