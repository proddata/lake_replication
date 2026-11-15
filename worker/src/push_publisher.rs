//! Optional schema-aware publisher; normal logical-pull binaries keep their SQL path.
#[allow(dead_code)]
#[path = "publisher.rs"]
mod publisher;
fn main() {
    publisher::main_with_function("lake_replication.publish_push_schema_batch");
}
