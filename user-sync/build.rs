// user-sync/build.rs
fn main() {
    sync_engine::codegen::generate("schema.toml");
    // Generates pipeline.toml skeleton only if it doesn't already exist.
    // Safe to call every build — never overwrites user edits.
    sync_engine::codegen::generate_pipeline_skeleton("schema.toml", "pipeline.toml");
    sync_engine::codegen::generate_config_doc("config.toml", "schema.toml", "CONFIG.md");
}
