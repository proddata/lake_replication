//! Shared protocol, credentials, logging, and journal types for the receiver,
//! initial sync, publisher, and supervisor executables. The receiver must not
//! acknowledge WAL until the decoded transaction is durable in the journal.

pub mod credentials;
pub mod journal;
pub mod logging;
pub mod managed;
pub mod pgoutput;
pub mod wire;
