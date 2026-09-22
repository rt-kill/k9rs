//! Derived (client-side projected) view types.
//!
//! A `DerivedViewKind` is a view built by reshaping a row the client already
//! has, rather than by subscribing to anything: the containers of a pod, and
//! nothing else so far. The kind owns its behaviour — label, columns,
//! operations, projection — so adding one never touches `DrillTarget`.
//!
//! (This module used to also hold `ViewId`, a parallel identity for "what a
//! nav step is looking at". The 2026-07 redesign made `Element` the nav-stack
//! node and `ViewId` had no callers outside this file afterwards — it was a
//! SECOND answer to questions `Element` already answers, sitting in a file
//! the next person would reach for. Its doc still referenced the `NavStep`
//! that redesign deleted.)

use crate::kube::protocol::OperationKind;
use crate::kube::resources::row::ResourceRow;

// ---------------------------------------------------------------------------
// DerivedViewKind — closed enum of client-side derived views
// ---------------------------------------------------------------------------

/// A client-side derived view type. The DATA enum is defined in
/// [`crate::kube::resources::row`] (it rides the wire inside
/// `DrillTarget::Derived`, WIRE-FROZEN there); the BEHAVIOR lives here via
/// exhaustive-match accessors — same pattern as [`crate::kube::resource_def::BuiltInKind`]
/// and [`crate::kube::local::LocalResourceKind`]. Adding a variant
/// forces filling in all metadata arms.
pub use crate::kube::resources::row::DerivedViewKind;

impl DerivedViewKind {
    /// Short label for breadcrumbs and tab bar.
    pub fn short_label(&self) -> &'static str {
        match self {
            Self::Containers => "Containers",
        }
    }

    /// Plural name (for overlay lookup, display).
    pub fn plural(&self) -> &'static str {
        match self {
            Self::Containers => "containers",
        }
    }

    /// Whether this view is cluster-scoped.
    pub fn is_cluster_scoped(&self) -> bool {
        match self {
            Self::Containers => false,
        }
    }

    /// Operations available on rows in this view.
    pub fn operations(&self) -> Vec<OperationKind> {
        match self {
            Self::Containers => vec![
                OperationKind::StreamLogs,
                OperationKind::PreviousLogs,
                OperationKind::Shell,
                OperationKind::Describe,
                // The container's pod has a node, and the projection carries
                // it onto every row (see `project_containers`), so `o` works
                // here for the same reason it works on the pod itself.
                OperationKind::ShowNode,
            ],
        }
    }

    /// Project a parent row into derived child rows. Each variant knows
    /// how to extract and reshape the parent's data. Implementation
    /// functions live in [`crate::app::derived`].
    pub fn project(&self, parent: &ResourceRow) -> Vec<ResourceRow> {
        match self {
            Self::Containers => crate::app::derived::project_containers(parent),
        }
    }

    /// Default column headers for this view's table.
    pub fn default_headers(&self) -> Vec<String> {
        match self {
            Self::Containers => {
                ["NAME", "IMAGE", "STATUS", "READY", "RESTARTS"]
                    .into_iter().map(String::from).collect()
            }
        }
    }
}

