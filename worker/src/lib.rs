//! Core protocol and journal types for the external replication worker.
//!
//! Network transport and Iceberg publication are intentionally separate from
//! this crate's first slice. The decoder accepts payloads emitted by `pgoutput`;
//! callers must not acknowledge WAL until the decoded transaction is durable.

pub mod credentials;
pub mod journal;
pub mod logging;
pub mod pgoutput;
pub mod wire;
