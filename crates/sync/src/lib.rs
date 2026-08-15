//! zeron-sync — local persistence for session and workspace documents.
//!
//! This crate intentionally contains only the SQLite snapshot store and the
//! processed-command ledger. No network transport is part of the local fork.

mod store;
pub use store::{DocsStore, StoreError};
