use crate::kube::resource_defs::REGISTRY;

/// Every registered def's `gvr().plural` matches the key it was
/// registered under in `by_plural`, and `kind()` round-trips through
/// `by_kind`. Walks the real registry (not a hand-maintained slice),
/// so there's no drift surface: adding a new resource means writing
/// a def + calling `register_*`, and both sides of the round-trip
/// check kick in as soon as the def exists.
#[test]
fn registry_consistency() {
    for def in REGISTRY.all() {
        let kind = def.kind();
        let by_kind_def = REGISTRY.by_kind(kind);
        assert_eq!(by_kind_def.gvr().plural, def.gvr().plural);
        let by_plural_def = REGISTRY.by_plural(def.gvr().plural).expect("registered");
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
    for def in REGISTRY.all() {
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
    for def in REGISTRY.all() {
        let row = REGISTRY.default_row(def.kind());
        let headers = def.default_headers();
        assert_eq!(
            row.cells.len(), headers.len(),
            "{:?}: convert(default) cells ({}) vs default_headers ({})",
            def.kind(), row.cells.len(), headers.len(),
        );
    }
}
