//! View identity for the navigation stack.
//!
//! A `ViewId` is what a nav step is "looking at." It's either a resource
//! (K8s or local, backed by a daemon subscription) or a derived view
//! (client-side projection of existing data, like containers of a pod).
//!
//! `ViewId` identifies the VIEW TYPE, not the view instance. Two container
//! views from different pods have the same `ViewId` (Derived(Containers)) —
//! the specific pod is instance context on `NavStep.source`, not type identity.

use crate::kube::protocol::{OperationKind, ResourceCapabilities, ResourceId, ResourceScope};
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

// ---------------------------------------------------------------------------
// ViewId — what type of view a nav step is showing
// ---------------------------------------------------------------------------

/// Identity of a navigation view TYPE. Either a resource type backed by a
/// daemon subscription, or a derived view type (client-side projection).
///
/// Identifies the VIEW TYPE, not the view instance. Instance context (which
/// specific pod's containers, which namespace's pods) lives on `NavStep`,
/// not on `ViewId`. This means `ViewId::Derived(Containers)` is the same
/// regardless of which pod it was derived from — same as
/// `ViewId::Resource(Pod)` is the same regardless of namespace.
///
/// Methods delegate to `ResourceId` for resources and `DerivedViewKind`
/// for derived views. Most callers don't care which variant they have.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ViewId {
    /// K8s or local resource type backed by a daemon subscription.
    Resource(ResourceId),
    /// Client-side derived view type projected from existing data.
    Derived(DerivedViewKind),
}

impl ViewId {
    /// True if this is a derived view (not backed by a daemon subscription).
    pub fn is_derived(&self) -> bool {
        matches!(self, Self::Derived(_))
    }

    /// The underlying `ResourceId`, if this is a resource view.
    /// Returns `None` for derived views.
    pub fn resource_id(&self) -> Option<&ResourceId> {
        match self {
            Self::Resource(rid) => Some(rid),
            Self::Derived(_) => None,
        }
    }

    /// Short label for breadcrumbs and tab bar.
    pub fn short_label(&self) -> &str {
        match self {
            Self::Resource(rid) => rid.short_label(),
            Self::Derived(kind) => kind.short_label(),
        }
    }

    /// Plural name (for overlay lookup, display).
    pub fn plural(&self) -> &str {
        match self {
            Self::Resource(rid) => rid.plural(),
            Self::Derived(kind) => kind.plural(),
        }
    }

    /// Whether this view is cluster-scoped.
    pub fn is_cluster_scoped(&self) -> bool {
        match self {
            Self::Resource(rid) => rid.is_cluster_scoped(),
            Self::Derived(kind) => kind.is_cluster_scoped(),
        }
    }

    /// Build the capability manifest for this view.
    pub fn capabilities(&self) -> ResourceCapabilities {
        match self {
            Self::Resource(rid) => rid.capabilities(),
            Self::Derived(kind) => ResourceCapabilities {
                operations: kind.operations(),
            },
        }
    }

    /// Scope of this view (Cluster or Namespaced).
    pub fn scope(&self) -> ResourceScope {
        match self {
            Self::Resource(rid) => rid.scope(),
            Self::Derived(kind) => {
                if kind.is_cluster_scoped() {
                    ResourceScope::Cluster
                } else {
                    ResourceScope::Namespaced
                }
            }
        }
    }

    /// Display label for the view.
    pub fn display_label(&self) -> &str {
        match self {
            Self::Resource(rid) => rid.display_label(),
            Self::Derived(kind) => kind.short_label(),
        }
    }
}

/// Convenience: wrap a `ResourceId` into a `ViewId::Resource`.
impl From<ResourceId> for ViewId {
    fn from(rid: ResourceId) -> Self {
        Self::Resource(rid)
    }
}
