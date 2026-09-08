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
