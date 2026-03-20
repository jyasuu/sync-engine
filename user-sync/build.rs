// user-sync/build.rs
fn main() {
    sync_engine::codegen::generate("schema.toml");
}
