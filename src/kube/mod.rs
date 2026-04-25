pub mod cache;
pub mod client_session;

/// How long a watcher (or local resource source) stays alive after the last
/// subscriber drops. Shared between the K8s `LiveQuery` grace-period task
/// and the `LocalSubscription` grace-period task so the two systems use the
/// same window and can't silently drift.
pub(crate) const GRACE_PERIOD_SECS: u64 = 300;
pub mod mux;
pub mod daemon;
pub mod describe;
pub mod live_query;
pub mod live_query_dynamic;
pub mod local;
pub mod metrics;
pub mod ops;
pub mod overlay;
pub mod protocol;
pub mod resource_def;
pub mod resource_defs;
pub mod resources;
pub mod server_session;
pub mod table;
pub mod session;
pub mod session_actions;
pub mod session_commands;
pub mod session_events;
