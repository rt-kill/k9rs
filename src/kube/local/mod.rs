//! Daemon-owned "local" resources.
//!
//! Local resources are things that live in the daemon (not fetched from the
//! Kubernetes API) but flow through the same Subscribe → Snapshot pipeline as
//! real K8s resources. Port-forwards are the first example; future examples
//! might include saved queries, benchmark results, or alert history.
//!
//! # Design
//!
//! Each local resource type implements [`LocalResourceSource`]. Sources:
//! - Own their own typed state (the trait is object-safe via `Arc<dyn ...>`).
//! - Publish `ResourceUpdate::Rows` snapshots via a `watch::Sender` whenever
//!   their state changes.
//! - Are registered on [`LocalRegistry`] (lives on `SessionSharedState`) so
//!   they're shared across every TUI session connected to the daemon.
//! - Are always alive for the daemon's lifetime once registered — subscribing
//!   to a local resource just hands out a fresh `watch::Receiver`.
//!
//! # Identity
//!
//! Local resources use the sentinel group `"k9rs.local"` (see
//! [`crate::kube::protocol::LOCAL_GROUP`]). Metadata lives in a parallel table
//! ([`LOCAL_RESOURCE_TYPES`]) that the existing alias lookup consults after
//! the K8s `RESOURCE_TYPES` table, so `:pf` resolves the same way as `:pods`.
//!
//! # Per-context lifetime
//!
//! Local sources are **per context**, not process-wide. The
//! [`registry::LocalRegistry`] caches them with the same `Weak<Arc>` TTL
//! pattern as the K8s watcher cache: each session that subscribes holds
//! a strong [`LocalSubscription`] containing an `Arc` keepalive, and the
//! registry only stores `Weak` refs. When the last subscription drops,
//! its `Drop` impl spawns a grace-period task that holds the `Arc` for
//! a few minutes — re-subscribes within that window upgrade the existing
//! `Weak` and reuse the same source.
//!
//! For port-forwards: switching context starts the grace period for the
//! old context's `PortForwardSource`, leaving its kubectl subprocesses
//! running until the window expires. Switching back within the window
//! reuses them; otherwise the source drops and `kill_on_drop` cleans up.
//!
//! # Adding a new local resource type
//!
//! 1. Implement [`LocalResourceSource`] on a new struct with a
//!    `for_context(name: String)` constructor.
//! 2. Write a converter `*_to_row` next to the source that turns entries
//!    into [`crate::kube::resources::row::ResourceRow`].
//! 3. Add a [`LocalTypeMeta`] entry to [`LOCAL_RESOURCE_TYPES`] with aliases,
//!    short label, and scope.
//! 4. Add a per-context cache field on [`registry::LocalRegistry`] and a
//!    `*_for(context)` accessor (mirror `port_forwards_for`); plug the new
//!    plural into the match arm in `LocalRegistry::get`.

pub mod port_forward;
pub mod registry;
pub mod subscription;
pub mod types;

pub use registry::LocalRegistry;
pub use subscription::LocalSubscription;
pub use types::{find_by_alias, find_by_plural, LocalTypeMeta, LOCAL_RESOURCE_TYPES};

use std::sync::Arc;
use tokio::sync::watch;

use crate::event::ResourceUpdate;
use crate::kube::protocol::{ResourceCapabilities, ResourceId};

/// A daemon-owned source of [`ResourceRow`](crate::kube::resources::row::ResourceRow)
/// snapshots. See the module docs for the contract.
pub trait LocalResourceSource: Send + Sync + 'static {
    /// The stable [`ResourceId`] this source serves. Its `group` field must
    /// be [`crate::kube::protocol::LOCAL_GROUP`].
    fn resource_id(&self) -> &ResourceId;

    /// Runtime column headers (matches the order of `ResourceRow::cells`).
    fn headers(&self) -> Vec<String>;

    /// Capabilities this resource exposes (can_log, can_delete, etc.).
    /// Emitted to the client right after subscription succeeds.
    fn capabilities(&self) -> ResourceCapabilities;

    /// Get a `watch::Receiver` for snapshot updates. Local sources are
    /// infallible by construction — the receiver always carries the
    /// current snapshot, and "the source went away" is modeled by the
    /// subscription being dropped, not by publishing a sentinel.
    fn subscribe(&self) -> watch::Receiver<ResourceUpdate>;

    /// Delete a logical entry by its row name. The row name is chosen by the
    /// converter and carries whatever encoded id the source needs (e.g.
    /// `"pf-42"`). Returns `Err` with a user-visible message if the name is
    /// invalid or the entry doesn't exist.
    ///
    /// Default: unsupported.
    fn delete(&self, _name: &str) -> Result<(), String> {
        Err("delete not supported on this resource".into())
    }

    /// Render a human-readable describe of a single entry by row name.
    /// Mirrors `kubectl describe`'s formatting role for K8s resources.
    /// Returns `None` if the source doesn't expose a describe view, or
    /// `Some(Err(msg))` if the entry is missing/invalid.
    ///
    /// Default: not supported.
    fn describe(&self, _name: &str) -> Option<Result<String, String>> {
        None
    }

    /// Serialize a single entry as YAML. Mirrors `kubectl get -o yaml`.
    /// Returns `None` if the source doesn't expose a yaml view, or
    /// `Some(Err(msg))` if the entry is missing/invalid.
    ///
    /// Default: not supported.
    fn yaml(&self, _name: &str) -> Option<Result<String, String>> {
        None
    }
}

/// Convenience: downcast-free typed handle to the daemon's PortForwardSource.
pub use port_forward::PortForwardSource;

/// Helper wrapper for `Arc<dyn LocalResourceSource>`.
pub type SharedLocalSource = Arc<dyn LocalResourceSource>;
