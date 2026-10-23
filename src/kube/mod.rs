
// `tokio::time::Instant`, not `std::time::Instant`: identical monotonic clock
// in production, but it moves with `tokio::time::pause()`, so a test can walk
// a 30-minute window without waiting 30 minutes. A `std::time::Instant` here
// would leave the deadline pinned to real time while the sleeps jumped ahead,
// and the gate would never open.

// Extracted crates, re-exported under their old paths. Every
// `crate::kube::live_query::…`, `::resources::…`, `::local::…` call site in
// the tree keeps resolving; the extraction does not get to rewrite them too.
// Deleted in a final sweep once the last crate is out.
pub use k9rs_mux as mux;
pub use k9rs_kube::cache;
pub use k9rs_kube::daemon_config;
pub use k9rs_kube::describe;
pub use k9rs_kube::live_query;
pub use k9rs_kube::live_query_dynamic;
pub use k9rs_kube::local;
pub use k9rs_kube::metrics;
pub use k9rs_kube::ops;
pub use k9rs_kube::repaint;
pub use k9rs_kube::resource_defs;
pub use k9rs_kube::resources;
pub use k9rs_kube::session_env;
pub use k9rs_kube::shared_poller;
pub use k9rs_daemon::daemon;
// Extracted crates, re-exported under their old paths so the hundreds of
// existing `crate::kube::protocol::…` / `::overlay::…` / `::resource_def::…`
// call sites keep resolving. Deleted in a final sweep once every crate is out.
pub use k9rs_proto::protocol;
pub use k9rs_proto::overlay;
pub use k9rs_proto::resource_def;
pub use k9rs_daemon::server_session;

// The grace machinery moved to `k9rs_kube::grace`; re-exported so
// `crate::kube::GraceGate` still names it.
pub use k9rs_kube::grace::{locals_grace, spawn_grace, GraceGate};
