//! Live query system for watching Kubernetes resources.
//!
//! A `LiveQuery` is a running watcher for one resource type. The k8s watch
//! stream already IS a delta stream — the watcher task keeps a store of
//! converted [`ResourceRow`](k9rs_proto::row::ResourceRow)s and, every flush tick, broadcasts only the
//! rows that actually changed ([`WatcherMsg::Delta`]). Full state
//! ([`TableBaseline`](k9rs_proto::protocol::TableBaseline)) is built on demand via an actor-ask, only when a
//! subscriber joins, lags, or a bridge resubscribes. Nothing diffs: the
//! only set arithmetic anywhere is the relist tombstone (`store − seen`).
//!
//! `WatcherCache` manages a per-process cache of live queries using `Weak`
//! references so watchers die naturally when no `Subscription` holds them.
//!
//! Watcher dispatch uses the trait-based registry: each built-in resource
//! has a type-erased `WatcherSpawner` closure captured at startup. CRDs
//! go through `subscribe_dynamic` and `run_dynamic_live_watcher`.

//! # Two halves, one edge
//!
//! Split 2026-10-27 on a measured seam. `watcher` is the task: the
//! messages it publishes, the health it reports, and the loop that turns a
//! kube watch stream into deltas and baselines. `cache` is the registry:
//! who is watching what, and the handles that keep a watcher alive.
//!
//! The edge runs one way. The cache names the watcher twenty times — it
//! builds them, and its handles carry the channels the watcher writes to —
//! and the watcher names the cache ZERO times in code. That is what made
//! the split mechanical rather than a negotiation.
//!
//! Re-exported flat (`pub use watcher::*`), so no call site moved: this was
//! one 1088-line module and `crate::live_query::WatcherCache` still means
//! what it meant. Same arrangement as `k9rs_app`'s `element/`.

mod cache;
mod watcher;

pub use cache::*;
pub use watcher::*;

// ---------------------------------------------------------------------------
// Watcher-loop tests — the injected stream lets these drive raw
// WatcherEvents and assert the delta/baseline contract without a cluster.
// ---------------------------------------------------------------------------

#[cfg(test)]
#[path = "../tests/live_query.rs"]
mod cache_tests;
