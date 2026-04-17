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
//! [`crate::kube::protocol::LOCAL_GROUP`]). Metadata lives on the closed
//! [`LocalResourceKind`] enum via exhaustive-match `const fn` accessors;
//! the alias lookup ([`find_by_alias`]) consults it after the K8s `REGISTRY`
//! so `:pf` resolves the same way as `:pods`. No parallel static table.
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
//! 1. Add a variant to [`LocalResourceKind`] — every `const fn` accessor
//!    becomes a compile error on the missing arm; fill in name, version,
//!    kind_str, plural, scope, aliases, short_label, and add the variant
//!    to [`LocalResourceKind::all`].
//! 2. Implement [`LocalResourceSource`] on a new struct with a
//!    `for_context(name: ContextName)` constructor.
//! 3. Write a converter `*_to_row` next to the source that turns entries
//!    into [`crate::kube::resources::row::ResourceRow`].
//! 4. Add a per-context cache field on [`registry::LocalRegistry`] and a
//!    `*_for(context)` accessor (mirror `port_forwards_for`); add a match
//!    arm in `LocalRegistry::get` for the new kind.

pub mod port_forward;
pub mod registry;
pub mod subscription;
pub mod types;

pub use registry::LocalRegistry;
pub use subscription::LocalSubscription;
pub use types::{find_by_alias, LocalResourceKind};

use std::sync::Arc;
use tokio::sync::watch;

use crate::event::ResourceUpdate;
use crate::kube::protocol::ResourceId;

/// A daemon-owned source of [`ResourceRow`](crate::kube::resources::row::ResourceRow)
/// snapshots. See the module docs for the contract.
pub trait LocalResourceSource: Send + Sync + 'static {
    /// The stable [`ResourceId`] this source serves. Its `group` field must
    /// be [`crate::kube::protocol::LOCAL_GROUP`].
    fn resource_id(&self) -> &ResourceId;

    /// Runtime column headers (matches the order of `ResourceRow::cells`).
    fn headers(&self) -> Vec<String>;

    // Capabilities used to live on this trait as `fn capabilities() ->
    // ResourceCapabilities`. Removed: the client computes them from the
    // typed `ResourceId::capabilities()` method, so nothing on the server
    // consumed this trait method after the wire send was dropped.

    /// Get a `watch::Receiver` for snapshot updates. Local sources are
    /// infallible by construction — the receiver always carries the
    /// current snapshot, and "the source went away" is modeled by the
    /// subscription being dropped, not by publishing a sentinel.
    fn subscribe(&self) -> watch::Receiver<ResourceUpdate>;

    /// Delete a logical entry by its row name. The row name is chosen by the
    /// converter and carries whatever encoded id the source needs (e.g.
    /// `"pf-42"`). Returns `Err` with a user-visible message if the name is
    /// invalid or the entry doesn't exist.
    fn delete(&self, name: &str) -> Result<(), String>;

    /// Render a human-readable describe of a single entry by row name.
    /// Mirrors `kubectl describe`'s formatting role for K8s resources.
    /// Returns `None` if the source doesn't expose a describe view, or
    /// `Some(Err(msg))` if the entry is missing/invalid.
    fn describe(&self, name: &str) -> Option<Result<String, String>>;

    /// Serialize a single entry as YAML. Mirrors `kubectl get -o yaml`.
    /// Returns `None` if the source doesn't expose a yaml view, or
    /// `Some(Err(msg))` if the entry is missing/invalid.
    fn yaml(&self, name: &str) -> Option<Result<String, String>>;

    /// Apply a new YAML representation for an entry. Mirrors `kubectl apply
    /// -f`. Implementations parse the YAML, diff against the current entry,
    /// and reconcile (in PortForwardSource: stop the existing kubectl
    /// subprocess and create a new one with the new ports). Returns the
    /// user-facing message on success, or an error string on failure.
    fn apply_yaml(&self, name: &str, yaml: &str) -> Result<String, String>;

    /// Try to claim the "grace task in flight" slot. Returns `true` if the
    /// caller won the race and SHOULD spawn a fresh grace task; `false`
    /// if a grace task is already running for this source and the caller
    /// should just drop its Arc immediately. Implementations typically
    /// CAS-flip an `AtomicBool`.
    fn try_begin_grace(&self) -> bool;

    /// Reset the "grace task in flight" slot. Called by the grace task
    /// just before it drops its Arc, so that a subsequent subscribe/drop
    /// cycle can spawn a fresh grace task.
    fn end_grace(&self);
}

/// Convenience: downcast-free typed handle to the daemon's PortForwardSource.
pub use port_forward::PortForwardSource;

/// Helper wrapper for `Arc<dyn LocalResourceSource>`.
pub type SharedLocalSource = Arc<dyn LocalResourceSource>;
