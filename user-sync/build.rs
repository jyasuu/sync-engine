// user-sync/build.rs
fn main() {
    sync_engine::codegen::generate("schema.toml");
    sync_engine::codegen::generate_pipeline_skeleton("schema.toml", "pipeline.toml");
    // Write the standalone SVG first (viewable in browser, diffable in git).
    sync_engine::codegen::generate_architecture_svg_file(
        "schema.toml",
        "pipeline.toml",
        "ARCHITECTURE.svg",
    );
    // Write the Markdown wrapper that embeds the SVG via a relative img tag.
    sync_engine::codegen::generate_architecture_svg(
        "schema.toml",
        "pipeline.toml",
        "ARCHITECTURE.md",
        "ARCHITECTURE.svg",
    );
}
