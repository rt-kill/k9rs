//! Metadata table for local resource types, parallel to
//! `crate::kube::resource_types::RESOURCE_TYPES`.
//!
//! The alias lookup in `ResourceId::from_alias` consults this table after the
//! K8s one, so `:pf` resolves to the port-forward local resource id without
//! touching the K8s table.

use crate::kube::protocol::{ResourceId, ResourceScope, LOCAL_GROUP};

/// Metadata for a local resource type.
pub struct LocalTypeMeta {
    /// Canonical singular name (e.g. `"portforward"`).
    pub name: &'static str,
    /// API version token — free-form for local resources but conventionally `"v1"`.
    pub version: &'static str,
    /// Kind (e.g. `"PortForward"`) — used for display.
    pub kind: &'static str,
    /// Plural name (used as the identifier in the URL-like form).
    pub plural: &'static str,
    /// Scope — most local resources are cluster-scoped (no namespace concept).
    pub scope: ResourceScope,
    /// Short aliases for command mode and tab bar lookup.
    pub aliases: &'static [&'static str],
    /// Short label for tab/breadcrumb display (e.g. `"PF"`).
    pub short_label: &'static str,
}

impl LocalTypeMeta {
    /// Build the `ResourceId` that identifies this local resource type.
    pub fn to_resource_id(&self) -> ResourceId {
        ResourceId::new(LOCAL_GROUP, self.version, self.kind, self.plural, self.scope)
    }
}

pub const LOCAL_RESOURCE_TYPES: &[LocalTypeMeta] = &[
    LocalTypeMeta {
        name: "portforward",
        version: "v1",
        kind: "PortForward",
        plural: "portforwards",
        scope: ResourceScope::Cluster,
        aliases: &["pf", "portforward", "portforwards", "port-forwards"],
        short_label: "PF",
    },
];

/// Look up a local type by any alias (case-insensitive).
pub fn find_by_alias(alias: &str) -> Option<&'static LocalTypeMeta> {
    let lower = alias.to_lowercase();
    LOCAL_RESOURCE_TYPES
        .iter()
        .find(|r| r.aliases.iter().any(|a| a.eq_ignore_ascii_case(&lower)))
}

/// Look up a local type by its plural name.
pub fn find_by_plural(plural: &str) -> Option<&'static LocalTypeMeta> {
    LOCAL_RESOURCE_TYPES.iter().find(|r| r.plural == plural)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn port_forward_is_findable() {
        let meta = find_by_alias("pf").expect("pf alias should resolve");
        assert_eq!(meta.plural, "portforwards");
        assert_eq!(meta.kind, "PortForward");
        let rid = meta.to_resource_id();
        assert_eq!(rid.group, LOCAL_GROUP);
        assert!(rid.is_local());
    }

    #[test]
    fn unknown_alias_returns_none() {
        assert!(find_by_alias("definitely-not-a-thing").is_none());
    }
}
