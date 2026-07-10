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
//! - Live inside their context's [`ContextLocals`] slice, resolved through
//!   [`LocalRegistry`] (on `SessionSharedState`) so they're shared across
//!   every TUI session attached to that context.
//! - Background work (a port-forward's kubectl child, an exec source's
//!   poll) runs as a supervised [`supervise::LocalOperator`] — restart with
//!   backoff for crash-shaped work, fixed interval for poll-shaped work —
//!   torn down by dropping its [`supervise::OperatorGuard`].
//!
//! # Identity
//!
//! Local resources use the sentinel group `"k9rs.local"` (see
//! [`crate::kube::protocol::LOCAL_GROUP`]). Metadata lives on the closed
//! [`LocalResourceKind`] enum via exhaustive-match `fn` accessors;
//! the alias lookup ([`find_by_alias`]) consults it after the K8s `REGISTRY`
//! so `:pf` resolves the same way as `:pods`. No parallel static table.
//!
//! # Per-context lifetime
//!
//! One ownership structure bounds everything: each context's local
//! resources live in its [`ContextLocals`] slice, held `Weak` by the
//! registry and kept alive by the sessions attached to that context (with
//! a grace window after the last one leaves — see
//! [`context_locals`]'s module docs). Context teardown drops the slice and
//! everything in it.
//!
//! Within that bound, two deliberate ownership policies coexist, expressed
//! through strong-vs-`Weak` inside the slice:
//!
//! - **Port forwards (strong)** — user-created side effects. They run for
//!   the slice's whole life, independent of whether anyone is viewing the
//!   `:pf` list, and die only on explicit stop or context teardown.
//!
//! - **Exec resources (`Weak`)** — derived views (caches of a command's
//!   output, no user-created state). Demand-driven within the context
//!   bound: subscribers hold `Arc` keepalives, and when the last
//!   subscription drops a grace task extends the lifetime briefly before
//!   the source (and its poll) is released.
//!
//! # Adding a new local resource type
//!
//! 1. Add a variant to [`LocalResourceKind`] — every `fn` accessor
//!    becomes a compile error on the missing arm; fill in name, version,
//!    kind_str, plural, scope, aliases, short_label, and add the variant
//!    to [`LocalResourceKind::all`].
//! 2. Implement [`LocalResourceSource`] on a new struct with a
//!    `for_context(...)` constructor. If it has background work, model
//!    each unit as a [`supervise::LocalOperator`] and hold its
//!    [`supervise::OperatorGuard`] where the unit's lifetime lives.
//! 3. Write a converter `*_to_row` next to the source that turns entries
//!    into [`crate::kube::resources::row::ResourceRow`].
//! 4. Add a field on [`context_locals::ContextLocals`] (strong for
//!    side-effect resources, `Weak` for derived views) and a match arm in
//!    `ContextLocals::get` for the new kind.

pub mod context_locals;
pub mod exec_source;
pub mod pf_resolve;
pub mod port_forward;
pub mod registry;
pub mod subscription;
pub mod supervise;
pub mod types;

pub use context_locals::{ContextKeepalive, ContextLocals};
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
    ///
    /// Paired with [`Self::end_grace`]: together they are the `claim`/`reset`
    /// closures driving [`crate::kube::spawn_grace`]. The pair MUST target the
    /// exact same slot — the type system can't verify it, so an implementation
    /// that claims one flag and resets another would silently wedge the grace
    /// coalescing (a permanently-set flag means no future grace task spawns).
    fn try_begin_grace(&self) -> bool;

    /// Reset the "grace task in flight" slot claimed by
    /// [`Self::try_begin_grace`] — the *same* slot (see its note). Called by
    /// the grace task just before it drops its Arc, so that a subsequent
    /// subscribe/drop cycle can spawn a fresh grace task.
    fn end_grace(&self);
}

/// Convenience: downcast-free typed handle to the daemon's PortForwardSource.
pub use port_forward::PortForwardSource;

/// Helper wrapper for `Arc<dyn LocalResourceSource>`.
pub type SharedLocalSource = Arc<dyn LocalResourceSource>;
