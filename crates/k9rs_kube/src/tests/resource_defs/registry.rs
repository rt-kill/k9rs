use crate::resource_defs::REGISTRY;

/// Every registered def's `gvr().plural` matches the key it was
/// registered under in `by_plural`, and `kind()` round-trips through
/// `by_kind`. Walks the real registry (not a hand-maintained slice),
/// so there's no drift surface: adding a new resource means writing
/// a def + calling `register_*`, and both sides of the round-trip
/// check kick in as soon as the def exists.
#[test]
fn registry_consistency() {
    for def in k9rs_proto::resource_def::all_defs() {
        let kind = def.kind();
        let by_kind_def = kind.def();
        assert_eq!(by_kind_def.gvr().plural, def.gvr().plural);
        let by_plural_def = k9rs_proto::resource_def::def_by_plural(def.gvr().plural).expect("registered");
        assert_eq!(by_plural_def.kind(), kind);
    }
}

/// Every def's `column_defs()` (when it tags any) must align positionally
/// with its `default_headers()` — same length, same header text in order.
/// These two lists, plus each converter's `cells` vec, are hand-authored
/// and coupled by index; a drift silently renders data under the wrong
/// header (and, for metrics resources, misdirects the by-index metrics
/// overlay). This walks the real registry so the guard covers every
/// resource, present and future, not just the ones with a bespoke test.
/// (The third list — per-row `cells` — needs a typed default object per
/// resource, so it's checked per-converter; see e.g. `nodes`/`pods`.)
#[test]
fn column_defs_align_with_headers() {
    for def in k9rs_proto::resource_def::all_defs() {
        let cols = def.column_defs();
        if cols.is_empty() {
            continue; // no explicit tagging — headers are used directly
        }
        let headers = def.default_headers();
        assert_eq!(
            cols.len(), headers.len(),
            "{:?}: column_defs ({}) vs default_headers ({}) length",
            def.kind(), cols.len(), headers.len(),
        );
        for (col, header) in cols.iter().zip(&headers) {
            assert_eq!(
                col.header, header.as_str(),
                "{:?}: column_defs/default_headers header text mismatch", def.kind(),
            );
        }
    }
}

/// The third leg of the positional column triple: every converter's per-row
/// `cells` must be the same length as its `default_headers()`. Walks the
/// real registry via each converter's `convert(K::default())` row, so the
/// guard covers every resource present and future — previously only nodes
/// and pods had a hand-written cells check, leaving the other converters'
/// `cells`↔`headers` coupling untested (a drift silently shifts every
/// column past the drift point under the wrong header).
#[test]
fn default_cells_align_with_headers() {
    for def in k9rs_proto::resource_def::all_defs() {
        let row = REGISTRY.default_row(def.kind());
        let headers = def.default_headers();
        assert_eq!(
            row.cells.len(), headers.len(),
            "{:?}: convert(default) cells ({}) vs default_headers ({})",
            def.kind(), row.cells.len(), headers.len(),
        );
    }
}

/// Every alias of every registered resource resolves to THAT resource, in
/// either case, deterministically.
///
/// `by_alias` used to scan `by_kind.values()` — a `HashMap` with the default
/// randomized hasher — so the first alias collision anyone introduced would
/// have resolved to a different resource on different launches of the same
/// binary, with no panic, no test failure and no way to reproduce the report.
/// It is an index now and `insert` rejects a collision at registration, so
/// this walks the live registry to prove the index is total and faithful.
#[test]
fn every_alias_resolves_to_its_own_resource() {
    for def in k9rs_proto::resource_def::all_defs() {
        for alias in def.aliases() {
            let found = k9rs_proto::resource_def::def_by_alias(alias)
                .unwrap_or_else(|| panic!("alias {alias:?} of {:?} resolves to nothing", def.kind()));
            assert_eq!(
                found.kind(),
                def.kind(),
                "alias {alias:?} belongs to {:?} but resolved to {:?}",
                def.kind(),
                found.kind(),
            );
            let shouty = alias.to_uppercase();
            assert_eq!(
                k9rs_proto::resource_def::def_by_alias(&shouty).map(|d| d.kind()),
                Some(def.kind()),
                "alias lookup is case-insensitive ({shouty})",
            );
        }
    }
}

/// EVERY `BuiltInKind` resolves in the registry.
///
/// `registry_consistency` above walks `REGISTRY.all()` — i.e. only what was
/// registered — so a variant nobody called `register_*` for was invisible to
/// it. `by_kind` panics on a missing entry, so the first user to navigate
/// there crashed the TUI, and the enum's own doc conceded the gap: "caught
/// during any test that exercises the new variant", which required somebody
/// to write that test.
///
/// This is that test, and it cannot go stale: `BuiltInKind::ALL` is generated
/// from the enum's declaration by `built_in_kinds!`, so a new variant is in
/// it the moment it exists.
#[test]
fn every_built_in_kind_is_registered() {
    use k9rs_proto::resource_def::BuiltInKind;

    assert!(BuiltInKind::ALL.len() > 30, "the generated list is really the whole enum");
    for &kind in BuiltInKind::ALL {
        // Panics with "missing entry for {kind}" if unregistered — here,
        // where it is a test failure naming the variant, instead of under a
        // user's cursor.
        let def = kind.def();
        assert_eq!(def.kind(), kind, "{kind:?} is registered under its own kind");
        // And reachable the way every caller reaches it.
        assert!(
            k9rs_proto::resource_def::def_by_plural(def.gvr().plural).is_some(),
            "{kind:?} is reachable by plural, which is how the command bar finds it",
        );
    }
    // The two indexes agree in size: no kind registered twice, none lost.
    assert_eq!(
        k9rs_proto::resource_def::all_defs().count(),
        BuiltInKind::ALL.len(),
        "the registry holds exactly the enum's kinds",
    );
}

/// Every kind has a WATCHER, which is the half the compiler cannot hold.
///
/// `BuiltInKind::def` is generated from the enum's declaration, so a variant
/// without a definition no longer compiles. The spawner table is still built
/// by hand — 34 `register_*` calls — so a variant nobody registered has a def,
/// answers every metadata question, and then panics the first time a user
/// navigates to it and a watcher is spawned.
///
/// This is the guard that used to come bundled with the def check, back when
/// one registry held both. Splitting them split it, and a check that is not
/// re-written is a check that was deleted.
#[test]
fn every_built_in_kind_has_a_watcher() {
    use k9rs_proto::resource_def::BuiltInKind;

    assert_eq!(
        BuiltInKind::ALL.len(),
        k9rs_proto::resource_def::all_defs().count(),
        "the generated list and the generated table are the same length",
    );
    for &kind in BuiltInKind::ALL {
        assert!(
            REGISTRY.has_watcher(kind),
            "{kind:?} has a definition but no registered watcher — it would \
             answer every metadata question and then panic on first subscribe",
        );
    }
    assert_eq!(
        REGISTRY.watcher_count(),
        BuiltInKind::ALL.len(),
        "no kind registered twice, none left over",
    );
}
