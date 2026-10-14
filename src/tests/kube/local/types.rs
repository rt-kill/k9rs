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
    assert_eq!(bincode::serialize(&LocalResourceKind::Context).unwrap(), 3u32.to_le_bytes());
    assert_eq!(bincode::serialize(&LocalResourceKind::Keybinding).unwrap(), 4u32.to_le_bytes());
    assert_eq!(bincode::serialize(&LocalResourceKind::Alias).unwrap(), 5u32.to_le_bytes());
    // EVERY variant, counted — not a prefix of them. This pin stopped one
    // short THREE TIMES: `Context`, `Keybinding` and `Alias`, each time with
    // the explanation sitting directly above it. Asserting the COUNT is what
    // makes the next append fail here instead of being silently unpinned.
    assert_eq!(
        LocalResourceKind::all().len() + 1,
        6,
        "a variant was added without pinning its wire tag above \
         (`all()` omits `Custom`, hence the +1)",
    );
}

/// `?` is not a `:cmd` resource, and must not become one by having aliases.
///
/// `find_by_alias` feeds `ResourceId::from_alias`, which feeds the
/// resource-command path — and that path calls `nav.reset()`. So giving the
/// keybindings kind the obvious aliases made `:keys` destroy the user's whole
/// drill stack to land on a table that only `Action::Help` ever seeds (so:
/// empty, permanently "Initializing") at depth 1, where `pop` refuses and
/// neither `q` nor `Esc` can get out.
///
/// Every other alias here names a noun you browse. Help is a sheet you
/// consult ABOUT the view you are in — which is why it floats over it.
#[test]
fn the_keybindings_view_is_not_reachable_as_a_command() {
    for word in ["key", "keys", "keybindings", "help", "KEYS", "Help"] {
        assert_eq!(
            super::find_by_alias(word),
            None,
            "`:{word}` must not resolve to a resource view",
        );
    }
    assert!(
        LocalResourceKind::Keybinding.aliases().is_empty(),
        "the aliases are what make it typeable — there must be none",
    );
    // The sibling client-owned kind still IS browsable, so this is a
    // statement about help specifically, not about local kinds in general.
    assert_eq!(super::find_by_alias("ctx"), Some(LocalResourceKind::Context));
}

/// `:alias`, `:aliases` and `:a` all land on the aliases view, through the
/// ordinary resource path — and none of them is a word some other resource
/// had already claimed.
#[test]
fn the_aliases_view_is_reachable_by_every_word_it_advertises() {
    for word in LocalResourceKind::Alias.aliases() {
        assert_eq!(
            super::find_by_alias(word),
            Some(LocalResourceKind::Alias),
            "`:{word}` is advertised by the Alias kind",
        );
        assert_eq!(
            crate::kube::protocol::ResourceId::from_alias(word),
            Some(LocalResourceKind::Alias.to_resource_id()),
            "`:{word}` must not be intercepted by a built-in with the same alias",
        );
    }
}
