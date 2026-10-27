use super::*;

// ---------------------------------------------------------------------------
// Connection — the unified link/reconnect lifecycle (2026-08 audit: the
// five-field constellation whose missed edges shipped two real bugs)
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// delta_tracker_tests (mirrored out of types.rs)
// ---------------------------------------------------------------------------

    use k9rs_proto::protocol::{ObjectKey, RowChange};
    use k9rs_proto::row::{CellValue, ResourceRow};

    fn row(name: &str, val: &str) -> ResourceRow {
        ResourceRow {
            name: name.into(),
            namespace: Some("ns".into()),
            cells: vec![CellValue::Text(val.into())],
            ..Default::default()
        }
    }
    fn key(name: &str) -> ObjectKey {
        ObjectKey::new("ns".to_string(), name.to_string())
    }

    /// E4: a metadata-only upsert (identical cells) must NOT flash — kube
    /// heartbeat churn would otherwise light the table permanently. A real
    /// change flashes; an insert doesn't (matches the baseline path);
    /// removes clear their state.
    #[test]
    fn apply_changes_flashes_only_genuine_changes() {
        let mut d = DeltaTracker::new();
        d.rebaseline(&[row("a", "1")]);
        assert!(d.changed_rows().is_empty(), "baseline seeds hashes, no flash");

        d.apply_changes(&[RowChange::Upsert(row("a", "1"))]); // heartbeat
        assert!(d.changed_rows().is_empty(), "identical cells: no flash");

        d.apply_changes(&[RowChange::Upsert(row("a", "2"))]); // real change
        assert!(d.changed_rows().contains_key(&key("a")));

        d.apply_changes(&[RowChange::Upsert(row("new", "1"))]); // insert
        assert!(!d.changed_rows().contains_key(&key("new")), "inserts don't flash");

        d.apply_changes(&[RowChange::Remove(key("a"))]);
        assert!(!d.changed_rows().contains_key(&key("a")), "remove clears state");
    }

    /// Cross-recovery continuity: hashes survive a re-baseline, so a row
    /// that changed while a watcher was down flashes after the recovery
    /// baseline — and unchanged rows do NOT mass-flash.
    #[test]
    fn rebaseline_keeps_continuity_without_mass_flash() {
        let mut d = DeltaTracker::new();
        d.rebaseline(&[row("a", "1"), row("b", "1")]);
        d.rebaseline(&[row("a", "1"), row("b", "2")]); // recovery baseline
        assert!(!d.changed_rows().contains_key(&key("a")), "unchanged: quiet");
        assert!(d.changed_rows().contains_key(&key("b")), "changed-during-gap: flash");
    }
