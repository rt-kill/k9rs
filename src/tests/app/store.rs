use super::*;
use crate::app::nav::CompiledGrep;
use crate::kube::protocol::{TableBaseline, TableDelta};
use crate::kube::resource_def::BuiltInKind;

fn rid() -> ResourceId {
    ResourceId::BuiltIn(BuiltInKind::Pod)
}

fn row(name: &str, ns: &str, cells: &[&str]) -> ResourceRow {
    ResourceRow {
        name: name.into(),
        namespace: Some(ns.into()),
        cells: cells.iter().map(|c| CellValue::Text((*c).to_string())).collect(),
        ..Default::default()
    }
}

fn baseline(rows: Vec<ResourceRow>) -> StorePayload {
    StorePayload::Baseline(TableBaseline {
        resource: rid(),
        headers: vec!["NAME".into(), "STATUS".into()],
        rows,
    })
}

fn delta(changes: Vec<RowChange>) -> StorePayload {
    StorePayload::Delta(TableDelta { changes })
}

fn key(ns: &str, name: &str) -> ObjectKey {
    ObjectKey::new(ns.to_string(), name.to_string())
}

fn spec<'a>(
    predicates: &'a [Arc<RowPredicate>],
    visible: &'a [usize],
    headers: &'a [&'a str],
) -> DeriveSpec<'a> {
    DeriveSpec {
        predicates,
        draft: None,
        sort: SortSpec::default(),
        visible_cols: visible,
        headers,
        max_col_widths: CAPS,
    }
}

/// Generous per-column caps for the derivation tests — they assert row
/// content and ordering, not layout.
const CAPS: &[u16] = &[40; 16];

#[test]
fn baseline_replaces_and_delta_edits_in_wire_order() {
    let store = RowStore::new("pods");
    store.apply(1, baseline(vec![row("b", "ns", &["b", "ok"]), row("a", "ns", &["a", "ok"])]));
    // Wire order preserved — the store never sorts.
    store.with_read(|i| {
        assert_eq!(i.rows[0].name, "b");
        assert_eq!(i.state, TableDataState::Ready);
    });
    let g1 = store.generation();
    store.apply(
        1,
        delta(vec![
            RowChange::Upsert(row("a", "ns", &["a", "changed"])),
            RowChange::Upsert(row("c", "ns", &["c", "new"])),
            RowChange::Remove(key("ns", "b")),
            RowChange::Remove(key("ns", "ghost")), // absent: no-op
        ]),
    );
    store.with_read(|i| {
        let names: Vec<&str> = i.rows.iter().map(|r| r.name.as_str()).collect();
        assert_eq!(names, ["a", "c"]); // replaced in place, compacted, appended
        assert_eq!(i.rows[0].cells[1].to_string(), "changed");
    });
    assert!(store.generation() > g1);
}

#[test]
fn epoch_floor_gates_stale_streams() {
    let store = RowStore::new("pods");
    store.apply(5, baseline(vec![row("live", "ns", &["live", "ok"])]));
    // A successor stream exists the moment expect_epoch runs.
    store.expect_epoch(9);
    // Predecessor events (epoch < 9) are rejected — baseline AND delta.
    store.apply(5, baseline(vec![row("stale", "ns", &["stale", "old"])]));
    store.apply(8, delta(vec![RowChange::Remove(key("ns", "live"))]));
    store.with_read(|i| assert_eq!(i.rows[0].name, "live"));
    // The successor's baseline lands.
    store.apply(9, baseline(vec![row("fresh", "ns", &["fresh", "ok"])]));
    store.with_read(|i| assert_eq!(i.rows[0].name, "fresh"));
}

#[test]
fn marks_prune_on_baseline_and_remove() {
    let store = RowStore::new("pods");
    store.apply(1, baseline(vec![row("a", "ns", &["a", "ok"]), row("b", "ns", &["b", "ok"])]));
    assert_eq!(store.toggle_mark(&key("ns", "a")), Some(true));
    assert_eq!(store.toggle_mark(&key("ns", "b")), Some(true));
    store.apply(1, delta(vec![RowChange::Remove(key("ns", "a"))]));
    assert_eq!(store.marked_keys(), vec![key("ns", "b")]);
    // Baseline without b prunes it too.
    store.apply(2, baseline(vec![row("c", "ns", &["c", "ok"])]));
    assert!(store.marked_keys().is_empty());
}

/// Ghost-mark guard: a mark can never name a row the store doesn't
/// hold. Toggle through a stale frame refuses (`None`); span-style
/// bulk marking silently drops absent keys; unmarking a marked row
/// still works regardless of presence.
#[test]
fn mark_inserts_verify_row_presence() {
    let store = RowStore::new("pods");
    store.apply(1, baseline(vec![row("a", "ns", &["a", "ok"])]));
    assert_eq!(store.toggle_mark(&key("ns", "gone")), None);
    assert!(!store.has_marks());
    store.mark_keys(vec![key("ns", "a"), key("ns", "gone")]);
    assert_eq!(store.marked_keys(), vec![key("ns", "a")]);
    // Toggling an EXISTING mark off never needs presence.
    assert_eq!(store.toggle_mark(&key("ns", "a")), Some(false));
    assert!(!store.has_marks());
}

/// Ctrl-R continuity: marks survive `clear()` (like flash hashes);
/// the recovery baseline re-anchors them, pruning non-returners.
/// While the window is open the store is `Initializing` — use-time
/// intersection (get_marked_resource_infos) sees zero present rows.
#[test]
fn clear_keeps_marks_and_recovery_baseline_reanchors() {
    let store = RowStore::new("pods");
    store.apply(1, baseline(vec![row("a", "ns", &["a", "ok"]), row("b", "ns", &["b", "ok"])]));
    store.toggle_mark(&key("ns", "a"));
    store.toggle_mark(&key("ns", "b"));
    store.clear(); // Ctrl-R
    store.with_read(|i| {
        assert!(i.rows.is_empty());
        assert_eq!(i.state, TableDataState::Initializing);
    });
    assert!(store.has_marks(), "marks survive the refresh window");
    // Recovery baseline: only `a` returned — `b`'s mark prunes.
    store.apply(2, baseline(vec![row("a", "ns", &["a", "ok"])]));
    assert_eq!(store.marked_keys(), vec![key("ns", "a")]);
}

#[test]
fn clear_keeps_flash_continuity_across_refresh() {
    let store = RowStore::new("pods");
    store.apply(1, baseline(vec![row("a", "ns", &["a", "Running"])]));
    store.clear(); // Ctrl-R
    store.with_read(|i| {
        assert!(i.rows.is_empty());
        assert_eq!(i.state, TableDataState::Initializing);
    });
    // Recovery baseline: the row changed while we weren't looking — it
    // must flash (hash continuity survived the clear).
    store.apply(2, baseline(vec![row("a", "ns", &["a", "CrashLoopBackOff"])]));
    store.with_read(|i| assert!(i.flash.changed_rows().contains_key(&key("ns", "a"))));
}

#[test]
fn derive_filters_sorts_and_projects() {
    let store = RowStore::new("pods");
    store.apply(
        1,
        baseline(vec![
            row("web-2", "ns", &["web-2", "Running"]),
            row("web-1", "ns", &["web-1", "Failed"]),
            row("db-1", "ns", &["db-1", "Running"]),
        ]),
    );
    let preds = [Arc::new(RowPredicate::Grep(CompiledGrep::new("web")))];
    let visible = [0usize, 1usize];
    let headers = ["NAME", "STATUS"];
    let mut sp = spec(&preds, &visible, &headers);
    sp.sort = SortSpec { col: 0, ascending: false };
    let view = store.with_read(|i| derive_view(&i.rows, &i.column_rules,None, &sp));
    // db-1 filtered out; descending by NAME.
    assert_eq!(view.rows.iter().map(|r| r[0].as_str()).collect::<Vec<_>>(), ["web-2", "web-1"]);
    assert_eq!(view.keys[0], key("ns", "web-2"));
    assert_eq!(view.total_rows, 3);
    assert_eq!(view.col_widths.len(), 2);
}

#[test]
fn narrowed_composes_grep_on_grep() {
    let store = RowStore::new("pods");
    store.apply(
        1,
        baseline(vec![
            row("web-api", "ns", &["web-api", "ok"]),
            row("web-cache", "ns", &["web-cache", "ok"]),
            row("db-api", "ns", &["db-api", "ok"]),
        ]),
    );
    let base = RowSource::new(Arc::clone(&store), None);
    let first = base.narrowed(Arc::new(RowPredicate::Grep(CompiledGrep::new("web"))));
    let second = first.narrowed(Arc::new(RowPredicate::Grep(CompiledGrep::new("api"))));
    assert_eq!(second.predicates().len(), 2);
    let visible = [0usize];
    let headers = ["NAME"];
    let view = store.with_read(|i| derive_view(&i.rows, &i.column_rules,None, &spec(second.predicates(), &visible, &headers)));
    assert_eq!(view.rows.len(), 1);
    assert_eq!(view.rows[0][0], "web-api");
    // The parent chain is untouched — sources are values.
    assert_eq!(first.predicates().len(), 1);
}

#[test]
fn fault_and_draft_predicates() {
    let store = RowStore::new("pods");
    let mut bad = row("bad", "ns", &["bad", "CrashLoop"]);
    bad.health = RowHealth::Failed;
    store.apply(1, baseline(vec![row("good", "ns", &["good", "Running"]), bad]));
    let preds = [Arc::new(RowPredicate::Fault)];
    let visible = [0usize];
    let headers = ["NAME"];
    let mut sp = spec(&preds, &visible, &headers);
    let draft = SearchPattern::new("ba");
    sp.draft = Some(&draft);
    let view = store.with_read(|i| derive_view(&i.rows, &i.column_rules,None, &sp));
    assert_eq!(view.rows.iter().map(|r| r[0].as_str()).collect::<Vec<_>>(), ["bad"]);
}

#[test]
fn pod_metrics_overlay_display_and_sort() {
    let hub = MetricsHub::new();
    let binding = MetricsBinding::for_rid(&rid(), &hub).expect("pods have metrics columns");
    // Resolve the real CPU column index from the registry so the test
    // rows can size their cells accordingly.
    let MetricsCols::Pod(cols) = binding.cols else { panic!("pod binding") };
    let cpu_col = cols.cpu.expect("pod def has a CPU column");

    let mk_row = |name: &str| {
        let mut r = ResourceRow {
            name: name.into(),
            namespace: Some("ns".into()),
            ..Default::default()
        };
        r.cells = (0..=cpu_col).map(|_| CellValue::Text(String::new())).collect();
        r.cells[0] = CellValue::Text(name.into());
        r
    };
    let store = RowStore::new("pods");
    store.apply(
        1,
        StorePayload::Baseline(TableBaseline {
            resource: rid(),
            headers: (0..=cpu_col).map(|i| format!("H{i}")).collect(),
            rows: vec![mk_row("low"), mk_row("high")],
        }),
    );

    // Before any poll: stored (empty) cells win.
    let visible = [cpu_col];
    let headers = ["CPU"];
    let preds: [Arc<RowPredicate>; 0] = [];
    let sp_plain = spec(&preds, &visible, &headers);
    let lens = binding.lens();
    let view = store.with_read(|i| derive_view(&i.rows, &i.column_rules,Some(&lens), &sp_plain));
    assert_eq!(view.rows[0][0], "");
    drop(lens);

    let mut usage = HashMap::new();
    usage.insert(key("ns", "high"), MetricsUsage { cpu_milli: 900, mem_bytes: 0, ..Default::default() });
    usage.insert(key("ns", "low"), MetricsUsage { cpu_milli: 100, mem_bytes: 0, ..Default::default() });
    hub.set_pods(usage);

    // Overlaid values display AND drive the sort (descending by CPU).
    let mut sp = spec(&preds, &visible, &headers);
    sp.sort = SortSpec { col: cpu_col, ascending: false };
    let lens = binding.lens();
    let view = store.with_read(|i| derive_view(&i.rows, &i.column_rules,Some(&lens), &sp));
    assert_eq!(view.rows[0][0], "900m");
    assert_eq!(view.keys[0], key("ns", "high"));
    assert_eq!(view.rows[1][0], "100m");
}

#[test]
fn node_metrics_absent_vs_never_polled() {
    use crate::kube::protocol::NodeName;
    let hub = MetricsHub::new();
    let node_rid = ResourceId::BuiltIn(BuiltInKind::Node);
    let binding = MetricsBinding::for_rid(&node_rid, &hub).expect("nodes have metrics columns");
    let MetricsCols::Node(cols) = binding.cols else { panic!("node binding") };
    let cpu_col = cols.cpu.expect("node def has a CPU column");

    let mut r = ResourceRow { name: "worker-1".into(), namespace: None, ..Default::default() };
    r.cells = (0..=cpu_col).map(|_| CellValue::Text("stored".into())).collect();
    let store = RowStore::new("nodes");
    store.apply(
        1,
        StorePayload::Baseline(TableBaseline {
            resource: node_rid.clone(),
            headers: (0..=cpu_col).map(|i| format!("H{i}")).collect(),
            rows: vec![r],
        }),
    );
    let visible = [cpu_col];
    let headers = ["CPU"];
    let preds: [Arc<RowPredicate>; 0] = [];
    let sp = spec(&preds, &visible, &headers);

    // Never polled → stored cell shows.
    let lens = binding.lens();
    let view = store.with_read(|i| derive_view(&i.rows, &i.column_rules,Some(&lens), &sp));
    assert_eq!(view.rows[0][0], "stored");
    drop(lens);

    // Polled, node absent → n/a placeholder, not a frozen stale value.
    hub.set_nodes(HashMap::<NodeName, MetricsUsage>::new());
    let lens = binding.lens();
    let view = store.with_read(|i| derive_view(&i.rows, &i.column_rules,Some(&lens), &sp));
    assert_eq!(view.rows[0][0], CellValue::Placeholder.to_string());
}

#[test]
fn line_store_pushes_evicts_and_counts() {
    use crate::kube::protocol::LogLine;
    let store = LineStore::new(3);
    for i in 0..5 {
        store.push(1, LogLine { content: format!("l{i}"), container: None });
    }
    store.with_read(|inner| {
        assert_eq!(inner.lines.len(), 3);
        assert_eq!(inner.evicted, 2, "front evictions are counted for scroll healing");
        assert_eq!(inner.lines.front().unwrap().content, "l2");
    });
    // clear keeps the counter monotonic.
    store.clear();
    store.with_read(|inner| {
        assert!(inner.lines.is_empty());
        assert_eq!(inner.evicted, 5);
    });
}

#[test]
fn line_store_epoch_floor_gates_stale_streams() {
    use crate::kube::protocol::LogLine;
    let store = LineStore::new(10);
    store.push(1, LogLine { content: "old".into(), container: None });
    // A range restart raised the floor before the old stream's queued
    // lines drained.
    store.expect_epoch(5);
    store.push(1, LogLine { content: "stale".into(), container: None });
    store.mark_ended(1); // the old stream's EOF must not mark the new one dead
    store.with_read(|inner| {
        assert_eq!(inner.lines.len(), 1);
        assert!(inner.live);
    });
    store.push(5, LogLine { content: "fresh".into(), container: None });
    store.mark_ended(5);
    store.with_read(|inner| {
        assert_eq!(inner.lines.back().unwrap().content, "fresh");
        assert!(!inner.live);
    });
}

#[test]
fn failed_is_epoch_gated_and_sets_state() {
    let store = RowStore::new("pods");
    store.apply(3, baseline(vec![row("a", "ns", &["a", "ok"])]));
    store.expect_epoch(7);
    // A superseded stream's death must not mark the successor failed.
    store.apply(3, StorePayload::Failed("old stream died".into()));
    store.with_read(|i| assert_eq!(i.state, TableDataState::Ready));
    store.apply(7, StorePayload::Failed("real failure".into()));
    store.with_read(|i| {
        assert_eq!(i.state, TableDataState::Failed("real failure".into()));
        // Rows are retained — the UI decides what to show for Failed.
        assert_eq!(i.rows.len(), 1);
    });
}

#[test]
fn a_stale_watch_freezes_the_rows_without_dropping_them() {
    // Staleness is a statement ABOUT resident rows, not a reason to discard
    // them: the link is up, so they're still the last known truth and every
    // operation on them still works.
    let store = RowStore::new("pods");
    store.apply(1, baseline(vec![row("a", "ns", &["a", "ok"])]));
    store.apply(1, StorePayload::Stale("watch died".into()));
    store.with_read(|i| {
        assert_eq!(i.state, TableDataState::Stale("watch died".into()));
        assert_eq!(i.rows.len(), 1, "stale rows stay resident");
    });
}

#[test]
fn anything_arriving_from_the_cluster_clears_staleness() {
    // Data arriving IS liveness — a delta can only come from a watch that is
    // feeding again, so it must not need a separate all-clear to be believed.
    for recovery in [
        StorePayload::Live,
        StorePayload::Delta(TableDelta { changes: vec![] }),
        baseline(vec![row("a", "ns", &["a", "ok"])]),
    ] {
        let store = RowStore::new("pods");
        store.apply(1, baseline(vec![row("a", "ns", &["a", "ok"])]));
        store.apply(1, StorePayload::Stale("watch died".into()));
        store.apply(1, recovery);
        store.with_read(|i| assert_eq!(i.state, TableDataState::Ready));
    }
}

#[test]
fn staleness_never_overwrites_a_louder_state() {
    // `Initializing` has no rows to freeze (it is already showing a loading
    // screen) and `Failed` is terminal — softening either into "stale" would
    // hide the stronger message behind a weaker one.
    let store = RowStore::new("pods");
    store.apply(1, StorePayload::Stale("watch died".into()));
    store.with_read(|i| assert_eq!(i.state, TableDataState::Initializing));

    store.apply(1, baseline(vec![row("a", "ns", &["a", "ok"])]));
    store.apply(1, StorePayload::Failed("forbidden".into()));
    store.apply(1, StorePayload::Stale("watch died".into()));
    store.with_read(|i| assert_eq!(i.state, TableDataState::Failed("forbidden".into())));

    // …and the quiet all-clear doesn't resurrect a failed stream either.
    store.apply(1, StorePayload::Live);
    store.with_read(|i| assert_eq!(i.state, TableDataState::Failed("forbidden".into())));
}
