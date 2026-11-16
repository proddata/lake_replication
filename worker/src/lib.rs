//! Shared protocol, credentials, logging, and journal types for the receiver,
//! initial sync, publisher, and supervisor executables. The default receiver
//! acknowledges WAL only after durable journal admission. Experimental direct
//! publication owns a separate final-table/checkpoint acknowledgement boundary.

pub mod credentials;
pub mod events;
pub mod journal;
pub mod logging;
pub mod managed;
pub mod pgoutput;
pub mod push_import;
pub mod push_protocol;
pub mod push_schema;
pub mod sql;
pub mod staging;
mod trace;
pub mod wire;
