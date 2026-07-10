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
}

impl LocalResourceKind {
    /// The well-known (non-Custom) variants. Custom kinds are registered
    /// at runtime and are not part of this list — callers that need to
    /// enumerate customs maintain their own registry.
    pub fn all() -> Vec<Self> {
        vec![Self::PortForward, Self::ExecResource]
    }

    /// Canonical singular name (e.g. `"portforward"`).
    pub fn name(&self) -> &str {
        match self {
            Self::PortForward => "portforward",
            Self::ExecResource => "execresource",
            Self::Custom(name) => name.as_str(),
        }
    }

    /// API version token — free-form for local resources but
    /// conventionally `"v1"`.
    pub fn version(&self) -> &str {
        match self {
            Self::PortForward => "v1",
            Self::ExecResource => "v1",
            Self::Custom(_) => "v1",
        }
    }

    /// K8s-style kind string (e.g. `"PortForward"`) — for display.
    /// For custom resources, returns the raw name (caller-provided).
    pub fn kind_str(&self) -> &str {
        match self {
            Self::PortForward => "PortForward",
            Self::ExecResource => "ExecResource",
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
            Self::Custom(name) => name.as_str(),
        }
    }

    /// Scope — most local resources are cluster-scoped (no namespace).
    pub fn scope(&self) -> ResourceScope {
        match self {
            Self::PortForward => ResourceScope::Cluster,
            Self::ExecResource => ResourceScope::Cluster,
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
            Self::Custom(_) => &[],
        }
    }

    /// Short label for tab/breadcrumb display (e.g. `"PF"`).
    /// For custom resources, returns the raw name.
    pub fn short_label(&self) -> &str {
        match self {
            Self::PortForward => "PF",
            Self::ExecResource => "EXEC",
            Self::Custom(name) => name.as_str(),
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
mod tests {
    use super::*;

    #[test]
    fn port_forward_is_findable() {
        let kind = find_by_alias("pf").expect("pf alias should resolve");
        assert_eq!(kind, LocalResourceKind::PortForward);
        assert_eq!(kind.plural(), "portforwards");
        assert_eq!(kind.kind_str(), "PortForward");
        let rid = kind.to_resource_id();
        assert_eq!(rid.group(), crate::kube::protocol::LOCAL_GROUP);
        assert!(rid.is_local());
    }

    #[test]
    fn unknown_alias_returns_none() {
        assert!(find_by_alias("definitely-not-a-thing").is_none());
    }

    // The former `kind_table_complete` drift-guard test is deleted: the
    // exhaustive match inside every metadata accessor above enforces
    // "every variant has a definition" at compile time, so a separate
    // runtime test would only be testing the compiler.

    #[test]
    fn custom_kind_metadata() {
        let kind = LocalResourceKind::Custom("my-resource".into());
        assert_eq!(kind.name(), "my-resource");
        assert_eq!(kind.version(), "v1");
        assert_eq!(kind.scope(), ResourceScope::Cluster);
        assert!(kind.aliases().is_empty());
    }

    /// Wire-tag stability for the local-resource identity enum (bincode encodes
    /// the variant as its u32 declaration-index, LE). Reordering/inserting
    /// remaps existing wire values; appending is safe.
    #[test]
    fn local_resource_kind_wire_tags_are_stable() {
        assert_eq!(bincode::serialize(&LocalResourceKind::PortForward).unwrap(), 0u32.to_le_bytes());
        assert_eq!(bincode::serialize(&LocalResourceKind::ExecResource).unwrap(), 1u32.to_le_bytes());
        // Custom carries a payload; pin only its 4-byte tag.
        let custom = bincode::serialize(&LocalResourceKind::Custom("x".into())).unwrap();
        assert_eq!(&custom[..4], 2u32.to_le_bytes());
    }
}
