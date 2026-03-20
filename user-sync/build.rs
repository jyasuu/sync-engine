// user-sync/build.rs
fn main() {
    sync_engine::codegen::generate("schema.toml");
    sync_engine::codegen::generate_config_doc("config.toml", "schema.toml", "CONFIG.md");
}
