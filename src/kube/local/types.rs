//! Metadata for local resource types — lives directly on the closed
//! [`LocalResourceKind`] enum as exhaustive-match `fn` accessors.
//! No parallel static table: adding a variant forces a compile error
//! in every method arm until you fill it in.

use serde::{Deserialize, Serialize};

use crate::kube::protocol::{ResourceId, ResourceScope};

/// Closed enum of every local resource type the daemon serves. All
/// metadata lives on the enum itself via `fn` accessors — no
/// parallel metadata table to keep in sync. Dispatch is compile-time
/// checked: adding a variant forces an update to every exhaustive
/// match below, and any call site that branches on `LocalResourceKind`
/// will equally fail to compile until the new arm is covered.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum LocalResourceKind {
    PortForward,
    ExecResource,
    /// User-defined local resource from daemon config.
    Custom(String),
    /// The kubeconfig's contexts. The one kind the daemon does NOT serve:
    /// the rows are a fact about the CLIENT's kubeconfig, and the whole
    /// point of the view is to be usable when there is no session at all
    /// (no `current-context`, or the one you have is unreachable). Its
    /// store is seeded in-process — see [`crate::app::element::LiveQuery::client`].
    ///
    /// APPENDED, not slotted in next to its siblings: this enum rides
    /// `ResourceId` on the wire, so inserting mid-enum silently re-tags
    /// `Custom`. (`local_resource_kind_wire_tags_are_stable` caught exactly
    /// that when this variant was first written in the obvious place.)
    Context,
}

impl LocalResourceKind {
    /// The well-known (non-Custom) variants. Custom kinds are registered
    /// at runtime and are not part of this list — callers that need to
    /// enumerate customs maintain their own registry.
    pub fn all() -> Vec<Self> {
        vec![Self::PortForward, Self::ExecResource, Self::Context]
    }

    /// Canonical singular name (e.g. `"portforward"`).
    pub fn name(&self) -> &str {
        match self {
            Self::PortForward => "portforward",
            Self::ExecResource => "execresource",
            Self::Context => "context",
            Self::Custom(name) => name.as_str(),
        }
    }

    /// API version token — free-form for local resources but
    /// conventionally `"v1"`.
    pub fn version(&self) -> &str {
        match self {
            Self::PortForward => "v1",
            Self::ExecResource => "v1",
            Self::Context => "v1",
            Self::Custom(_) => "v1",
        }
    }

    /// K8s-style kind string (e.g. `"PortForward"`) — for display.
    /// For custom resources, returns the raw name (caller-provided).
    pub fn kind_str(&self) -> &str {
        match self {
            Self::PortForward => "PortForward",
            Self::ExecResource => "ExecResource",
            Self::Context => "Context",
            Self::Custom(name) => name.as_str(),
        }
    }

    /// Plural name used as the identifier in the URL-like form.
    /// For custom resources, returns the raw name (custom resources
    /// are registered by their exact identifier).
    pub fn plural(&self) -> &str {
        match self {
            Self::PortForward => "portforwards",
            Self::ExecResource => "execresources",
            Self::Context => "contexts",
            Self::Custom(name) => name.as_str(),
        }
    }

    /// Scope — most local resources are cluster-scoped (no namespace).
    pub fn scope(&self) -> ResourceScope {
        match self {
            Self::PortForward => ResourceScope::Cluster,
            Self::ExecResource => ResourceScope::Cluster,
            Self::Context => ResourceScope::Cluster,
            Self::Custom(_) => ResourceScope::Cluster,
        }
    }

    /// Short aliases for command mode and tab bar lookup.
    /// Custom resources have no aliases — they are referenced by
    /// their exact name.
    pub fn aliases(&self) -> &'static [&'static str] {
        match self {
            Self::PortForward => &["pf", "portforward", "portforwards", "port-forwards"],
            Self::ExecResource => &["exec", "execresource", "execresources"],
            Self::Context => &["ctx", "context", "contexts"],
            Self::Custom(_) => &[],
        }
    }

    /// Short label for tab/breadcrumb display (e.g. `"PF"`).
    /// For custom resources, returns the raw name.
    pub fn short_label(&self) -> &str {
        match self {
            Self::PortForward => "PF",
            Self::ExecResource => "EXEC",
            Self::Context => "CTX",
            Self::Custom(name) => name.as_str(),
        }
    }

    /// Column metadata, same shape built-ins get from their `ResourceDef`.
    /// Consulted by `ColumnPolicy` for display level AND width ceiling, so a
    /// local resource is no more special about its columns than any other.
    /// An empty slice means "infer everything", which is what the built-in
    /// path already does for columns a def doesn't mention.
    pub fn column_defs(&self) -> &'static [crate::kube::resource_def::ColumnDef] {
        use crate::kube::resource_def::ColumnDef;
        match self {
            // Cluster and user are usually long and rarely the thing you are
            // reading — on EKS they are full ARNs, which at the global cap
            // push NAME and ACTIVE off the screen entirely.
            Self::Context => {
                static COLS: &[ColumnDef] = &[
                    ColumnDef::new("NAME").max_width(40),
                    ColumnDef::new("CLUSTER").max_width(28),
                    ColumnDef::new("USER").max_width(24),
                    ColumnDef::new("ACTIVE").max_width(8),
                ];
                COLS
            }
            Self::PortForward | Self::ExecResource | Self::Custom(_) => &[],
        }
    }

    /// Build the `ResourceId` that identifies this local resource type.
    pub fn to_resource_id(self) -> ResourceId {
        ResourceId::Local(self)
    }

    /// The operation set this local resource supports. Single source
    /// of truth — both the client-side `ResourceId::capabilities()`
    /// and the server-side `LocalResourceSource::capabilities()` impls
    /// reach for this so they can't drift.
    pub fn operations(&self) -> Vec<crate::kube::protocol::OperationKind> {
        use crate::kube::protocol::OperationKind;
        match self {
            // Port-forward: describe (show config), yaml (edit config),
            // delete (stop the forward). Nothing else applies.
            Self::PortForward => vec![
                OperationKind::Describe,
                OperationKind::Yaml,
                OperationKind::Delete,
            ],
            // Exec resource: describe (show raw JSON), yaml (show raw JSON
            // as yaml), delete (remove entry from internal store).
            Self::ExecResource => vec![
                OperationKind::Describe,
                OperationKind::Yaml,
                OperationKind::Delete,
            ],
            // A context is switched to (Enter, via `DrillTarget`), not
            // described or deleted — every other operation would have to ask
            // a daemon about the client's own kubeconfig.
            Self::Context => vec![],
            // Custom resources get the standard trio.
            Self::Custom(_) => vec![
                OperationKind::Describe,
                OperationKind::Yaml,
                OperationKind::Delete,
            ],
        }
    }
}

/// Look up a local resource kind by any alias (case-insensitive).
pub fn find_by_alias(alias: &str) -> Option<LocalResourceKind> {
    let lower = alias.to_lowercase();
    LocalResourceKind::all().into_iter().find(|k| {
        k.aliases().iter().any(|a| a.eq_ignore_ascii_case(&lower))
    })
}

#[cfg(test)]
#[path = "../../tests/kube/local/types.rs"]
mod tests;
