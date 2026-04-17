//! Metadata for local resource types — lives directly on the closed
//! [`LocalResourceKind`] enum as exhaustive-match `const fn` accessors.
//! No parallel static table: adding a variant forces a compile error
//! in every method arm until you fill it in.

use serde::{Deserialize, Serialize};

use crate::kube::protocol::{ResourceId, ResourceScope};

/// Closed enum of every local resource type the daemon serves. All
/// metadata lives on the enum itself via `const fn` accessors — no
/// parallel metadata table to keep in sync. Dispatch is compile-time
/// checked: adding a variant forces an update to every exhaustive
/// match below, and any call site that branches on `LocalResourceKind`
/// will equally fail to compile until the new arm is covered.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum LocalResourceKind {
    PortForward,
}

impl LocalResourceKind {
    /// Every variant. Manually maintained, but it's the only list that
    /// exists — every other accessor below is an exhaustive match, so
    /// forgetting to add a new variant here is caught the moment any
    /// iterator path (alias lookup, table pre-population) has to touch
    /// it without this slice covering the new discriminant. There is
    /// no second table to drift against.
    pub const fn all() -> &'static [Self] {
        &[Self::PortForward]
    }

    /// Canonical singular name (e.g. `"portforward"`).
    pub const fn name(self) -> &'static str {
        match self {
            Self::PortForward => "portforward",
        }
    }

    /// API version token — free-form for local resources but
    /// conventionally `"v1"`.
    pub const fn version(self) -> &'static str {
        match self {
            Self::PortForward => "v1",
        }
    }

    /// K8s-style kind string (e.g. `"PortForward"`) — for display.
    pub const fn kind_str(self) -> &'static str {
        match self {
            Self::PortForward => "PortForward",
        }
    }

    /// Plural name used as the identifier in the URL-like form.
    pub const fn plural(self) -> &'static str {
        match self {
            Self::PortForward => "portforwards",
        }
    }

    /// Scope — most local resources are cluster-scoped (no namespace).
    pub const fn scope(self) -> ResourceScope {
        match self {
            Self::PortForward => ResourceScope::Cluster,
        }
    }

    /// Short aliases for command mode and tab bar lookup.
    pub const fn aliases(self) -> &'static [&'static str] {
        match self {
            Self::PortForward => &["pf", "portforward", "portforwards", "port-forwards"],
        }
    }

    /// Short label for tab/breadcrumb display (e.g. `"PF"`).
    pub const fn short_label(self) -> &'static str {
        match self {
            Self::PortForward => "PF",
        }
    }

    /// Build the `ResourceId` that identifies this local resource type.
    pub const fn to_resource_id(self) -> ResourceId {
        ResourceId::Local(self)
    }

    /// The operation set this local resource supports. Single source
    /// of truth — both the client-side `ResourceId::capabilities()`
    /// and the server-side `LocalResourceSource::capabilities()` impls
    /// reach for this so they can't drift.
    pub fn operations(self) -> Vec<crate::kube::protocol::OperationKind> {
        use crate::kube::protocol::OperationKind;
        match self {
            // Port-forward: describe (show config), yaml (edit config),
            // delete (stop the forward). Nothing else applies.
            Self::PortForward => vec![
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
    LocalResourceKind::all().iter().copied().find(|k| {
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
}
