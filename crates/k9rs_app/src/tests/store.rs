use super::*;
use k9rs_util::CompiledGrep;
use k9rs_proto::protocol::{LogLine, TableBaseline, TableDelta};
use k9rs_proto::resource_def::BuiltInKind;

fn rid() -> ResourceId {
    BuiltInKind::Pod.to_resource_id()
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
    use k9rs_proto::protocol::NodeName;
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
    use k9rs_proto::protocol::LogLine;
    let store = LineStore::new(3);
    for i in 0..5 {
        store.push(1, LogLine { content: format!("l{i}"), source: None, timestamp: None });
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
    use k9rs_proto::protocol::LogLine;
    let store = LineStore::new(10);
    store.push(1, LogLine { content: "old".into(), source: None, timestamp: None });
    // A range restart raised the floor before the old stream's queued
    // lines drained.
    store.expect_epoch(5);
    store.push(1, LogLine { content: "stale".into(), source: None, timestamp: None });
    store.mark_ended(1); // the old stream's EOF must not mark the new one dead
    store.with_read(|inner| {
        assert_eq!(inner.lines.len(), 1);
        assert!(inner.live);
    });
    store.push(5, LogLine { content: "fresh".into(), source: None, timestamp: None });
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

// ---------------------------------------------------------------------------
// A typed percentage colours itself
// ---------------------------------------------------------------------------

/// Colouring used to come from ONE place: overlay YAML matching a column by
/// header STRING. That is why `CPU%` instead of `%CPU` silently coloured
/// nothing — and why every new percent column was born invisible. The cell
/// already knows it is a percentage and already holds the number.
#[test]
fn a_percentage_cell_colours_itself_without_an_overlay() {
    use k9rs_proto::row::{CellValue, RowHealth};
    assert_eq!(CellValue::Percentage(Some(95)).utilization_health(), Some(RowHealth::Failed));
    assert_eq!(CellValue::Percentage(Some(80)).utilization_health(), Some(RowHealth::Pending));
    assert_eq!(CellValue::Percentage(Some(10)).utilization_health(), None);
    // Nothing to report is not a health state.
    assert_eq!(CellValue::Percentage(None).utilization_health(), None);
    // And it is a percentage rule, not a number rule: a count of 95 is a
    // count.
    assert_eq!(CellValue::Count(95).utilization_health(), None);
    assert_eq!(CellValue::Text("95%".into()).utilization_health(), None);
}

/// The derive wires it up, and an explicit overlay rule still wins — the
/// built-in is a default, not a policy.
#[test]
fn an_overlay_rule_outranks_the_built_in_threshold() {
    use k9rs_proto::row::{CellValue, RowHealth};

    let row = ResourceRow {
        name: "n1".into(),
        cells: vec![CellValue::Text("n1".into()), CellValue::Percentage(Some(95))],
        ..Default::default()
    };
    let headers = ["NAME", "%CPU"];
    let visible: Vec<usize> = vec![0, 1];
    let widths = vec![40u16, 40u16];
    let spec = DeriveSpec {
        predicates: &[],
        draft: None,
        sort: SortSpec::default(),
        visible_cols: &visible,
        headers: &headers,
        max_col_widths: &widths,
    };

    // No overlay at all: 95% is critical by construction.
    let view = derive_view(std::slice::from_ref(&row), &[], None, &spec);
    assert_eq!(view.cell_style[0][1], Some(RowHealth::Failed));

    // An overlay that calls 95% merely pending is obeyed: an explicit rule
    // for THIS column beats the type's default.
    let rules = vec![
        k9rs_proto::overlay::ColumnRenderRules::default(),
        k9rs_proto::overlay::ColumnRenderRules::from_when_pairs(&[(">= 50", RowHealth::Pending)])
            .expect("a well-formed `when`"),
    ];
    let view = derive_view(&[row], &rules, None, &spec);
    assert_eq!(
        view.cell_style[0][1],
        Some(RowHealth::Pending),
        "the overlay rule wins, or it isn't an override",
    );
}

// ---------------------------------------------------------------------------
// Which end of an over-long stream a store keeps
// ---------------------------------------------------------------------------

/// A history request keeps its HEAD. The apiserver streams forward from the
/// range's start, so the oldest lines arrive first — and a tail-anchored ring
/// evicted them with the newer ones behind, leaving the newest N of a range
/// asked for precisely to see the oldest. Asking for MORE history made it
/// strictly worse.
#[test]
fn a_head_store_keeps_the_oldest_and_counts_what_it_refused() {
    use crate::store::Retain;
    use k9rs_proto::protocol::LogLine;

    let store = LineStore::new(3);
    store.set_retain(Retain::Head);
    for i in 0..10 {
        store.push(1, LogLine::untagged(format!("line-{i}")));
    }

    store.with_read(|i| {
        let got: Vec<&str> = i.lines.iter().map(|l| l.content.as_str()).collect();
        assert_eq!(got, ["line-0", "line-1", "line-2"], "the OLDEST three");
        assert_eq!(i.evicted, 0, "nothing was evicted — arrivals were refused");
        assert_eq!(i.truncated, 7, "…and the refusals are counted, not silent");
    });
    assert!(store.head_is_full(), "full means: there is more behind this window");
}

/// The default is unchanged: a follow keeps the newest.
#[test]
fn a_tail_store_still_keeps_the_newest() {
    use k9rs_proto::protocol::LogLine;

    let store = LineStore::new(3);
    for i in 0..10 {
        store.push(1, LogLine::untagged(format!("line-{i}")));
    }
    store.with_read(|i| {
        let got: Vec<&str> = i.lines.iter().map(|l| l.content.as_str()).collect();
        assert_eq!(got, ["line-7", "line-8", "line-9"]);
        assert_eq!(i.evicted, 7);
        assert_eq!(i.truncated, 0);
    });
    assert!(!store.head_is_full());
}

/// The palette assignment is the STREAM's, recorded as lines arrive, and it
/// survives a range restart — a pod changing colour because you pressed `5`
/// would be the hash problem wearing a different hat.
#[test]
fn source_slots_are_first_seen_order_and_outlive_a_clear() {
    use k9rs_proto::protocol::LogLine;
    let store = LineStore::new(100);
    for src in ["pod-b", "pod-a", "pod-b", "pod-c"] {
        store.push(1, LogLine {
            source: Some(src.to_string()),
            timestamp: None,
            content: "x".into(),
        });
    }
    assert_eq!(store.sources(), vec!["pod-b", "pod-a", "pod-c"], "first seen, no duplicates");

    store.clear();
    assert_eq!(
        store.sources(),
        vec!["pod-b", "pod-a", "pod-c"],
        "a restart re-reads the same pods; their colours must not shuffle",
    );

    // An untagged stream records nothing — there is no tag to colour.
    let store = LineStore::new(100);
    store.push(1, LogLine { source: None, timestamp: None, content: "x".into() });
    assert!(store.sources().is_empty());
}

/// The committed predicate obeys the same rule as the draft — they are two
/// code paths for one statement, and the draft is the one you see while
/// typing. If they disagreed, a pattern would match rows while you typed it
/// and lose them the moment you pressed Enter.
#[test]
fn the_committed_grep_also_scans_only_visible_columns() {
    use k9rs_proto::row::{CellValue, ResourceRow};
    let row = ResourceRow {
        cells: vec![
            CellValue::Text("fetcher-playwright".into()),
            CellValue::Text("1/1".into()),
            CellValue::Text("registry/pimp-base:v2".into()), // hidden column
        ],
        name: "fetcher-playwright".into(),
        ..Default::default()
    };
    let cells: Vec<String> =
        row.cells.iter().map(|c| c.to_string()).collect();
    let grep = RowPredicate::Grep(k9rs_util::CompiledGrep::new("pimp"));

    assert!(
        !grep.matches(&row, &cells, &[0, 1]),
        "the needle is only in column 2, which is not displayed",
    );
    assert!(
        grep.matches(&row, &cells, &[0, 1, 2]),
        "…and Ctrl-w, which shows it, must also search it",
    );

    // A column-restricted grep (`~`) indexes ABSOLUTE data columns, so it is
    // unaffected by which happen to be on screen.
    let col = RowPredicate::ColumnGrep {
        pattern: k9rs_util::CompiledGrep::new("pimp"),
        col: 2,
        header: "IMAGES".into(),
    };
    assert!(col.matches(&row, &cells, &[0, 1]));
}

// ---------------------------------------------------------------------------
// StoredLine — a log line plus the one form everything derives from it
// ---------------------------------------------------------------------------

fn stored(ts: Option<&str>, source: Option<&str>, content: &str) -> StoredLine {
    StoredLine::from(LogLine {
        timestamp: ts.map(str::to_string),
        source: source.map(str::to_string),
        content: content.to_string(),
    })
}

#[test]
fn logline_flat_text() {
    // Export carries the line's SUBSTANCE, not a transcript of the drawn
    // cells: both pieces of provenance ride along whatever `t` and `p` are
    // set to. All FOUR shapes are pinned, because the one the daemon
    // actually produces — timestamped — is the one the old test left out,
    // and it was the one being silently dropped.
    assert_eq!(StoredLine::from(LogLine::untagged("hi")).flat_text().as_ref(), "hi");
    assert_eq!(
        stored(None, Some("web"), "msg").flat_text().as_ref(),
        "[web] msg",
    );
    let ts = "2026-09-18T20:29:40.6Z";
    assert_eq!(
        stored(Some(ts), None, "msg").flat_text().as_ref(),
        format!("{ts} msg"),
    );
    assert_eq!(
        stored(Some(ts), Some("web"), "msg").flat_text().as_ref(),
        format!("{ts} [web] msg"),
    );
}

/// A file is not a terminal: the export form carries the line's visible
/// text, and nothing can hand it a colour code.
///
/// This used to be the callers' job — three of them, all saying
/// `strip_ansi(&l.flat_text())` through a tokenizer that was not the
/// renderer's. With the decision inside `flat_text` there is no way to ask
/// for the export form and get escapes, so the fourth caller cannot get it
/// wrong, and a `grep` over a saved log sees what the screen showed.
#[test]
fn an_export_is_never_a_transcript_of_colour() {
    let coloured = "\u{1b}[2mts\u{1b}[0m \u{1b}[32m INFO\u{1b}[0m \u{1b}[2mworkers\u{1b}[0m=0";
    let line = stored(Some("2026-09-18T20:29:40.6Z"), Some("web"), coloured);
    assert_eq!(
        line.flat_text().as_ref(),
        "2026-09-18T20:29:40.6Z [web] ts  INFO workers=0",
        "the body is the VISIBLE text — and `workers=0` is contiguous in it",
    );

    // The untagged shape is the one that can stay borrowed, so it is the one
    // that could have skipped the cleaning by accident.
    let bare = StoredLine::from(LogLine::untagged(coloured));
    assert!(
        !bare.flat_text().contains('\u{1b}'),
        "even the zero-copy arm cleans: {:?}",
        bare.flat_text(),
    );
    assert_eq!(
        StoredLine::from(LogLine::untagged("plain")).flat_text(),
        std::borrow::Cow::Borrowed("plain"),
        "and a clean line still costs nothing",
    );
}

/// The derived form is derived ONCE, from the line, and cannot drift from it.
///
/// This is the whole point of the type: four consumers wanted the visible
/// text and each derived it at the point of use, so one of them (the grep)
/// could be — and was — still looking at the raw bytes. A stored line has no
/// spelling that yields the raw body to a consumer that wanted the visible
/// one.
#[test]
fn a_stored_line_derives_its_visible_text_once() {
    use k9rs_proto::protocol::Role;

    let coloured = "\u{1b}[2mts\u{1b}[0m \u{1b}[32m INFO\u{1b}[0m \u{1b}[2mworkers\u{1b}[0m=0";
    let line = stored(Some("2026-09-18T20:29:40.6Z"), Some("web"), coloured);

    // The one definition, not a second copy of its rules.
    assert_eq!(line.visible(), k9rs_util::text::visible_text(coloured).as_ref());
    assert_eq!(line.visible(), "ts  INFO workers=0");
    // `Deref`: the RAW content is still there, because the renderer's colour
    // comes from it. Both forms, one line, no third view.
    assert_eq!(line.content, coloured);

    // The body segment IS the visible text — which is what makes a grep,
    // a highlight and an export index one offset space.
    let body = line
        .segments()
        .find(|(r, ..)| *r == Role::Body)
        .expect("a line always has a body");
    assert_eq!(body.1, "ts  INFO workers=0");
    assert!(
        line.segments().all(|(_, text, _)| !text.contains('\u{1b}')),
        "no segment carries an escape",
    );

    // The roles and their order are the wire fields', unchanged.
    let roles: Vec<Role> = line.segments().map(|(r, ..)| r).collect();
    assert_eq!(roles, vec![Role::Source, Role::Timestamp, Role::Body]);

    // A line with nothing to strip keeps no second copy of itself.
    let clean = stored(None, None, "workers=0");
    assert_eq!(clean.visible(), "workers=0");
    assert!(
        std::ptr::eq(clean.visible(), clean.content.as_str()),
        "a clean line's visible text IS its content, not a duplicate of it",
    );
}

/// Arrival is where the derivation happens — so it happens once per line in
/// its whole lifetime, not once per line per frame.
///
/// Measuring a wrapped 50,000-line buffer cost 49 ms a frame, nearly all of
/// it re-deriving a value that cannot change. `push` is the only door a wire
/// line comes through, which is why that is where it is made.
#[test]
fn the_store_materializes_on_arrival() {
    use k9rs_proto::protocol::LogLine;

    let store = LineStore::new(4);
    store.push(0, LogLine::untagged("\u{1b}[32mok\u{1b}[0m yes"));
    store.push(0, LogLine::untagged("plain"));

    store.with_read(|inner| {
        assert_eq!(inner.lines[0].visible(), "ok yes", "already visible text");
        assert_eq!(inner.lines[0].content, "\u{1b}[32mok\u{1b}[0m yes", "raw kept for colour");
        assert_eq!(inner.lines[1].visible(), "plain");
    });

    // A derived store (a grep's own retention) takes ALREADY-materialized
    // lines: the filter clones what the parent derived rather than deriving
    // the same bytes a second time.
    let derived = LineStore::new(4);
    let batch: Vec<StoredLine> = store.with_read(|inner| inner.lines.iter().cloned().collect());
    derived.extend_derived(batch);
    derived.with_read(|inner| {
        assert_eq!(inner.lines.len(), 2);
        assert_eq!(inner.lines[0].visible(), "ok yes");
    });
}

/// The two column spaces convert in ONE place, in both directions.
///
/// A data column indexes `ResourceRow.cells`; a visible column indexes the
/// view's `visible_cols`/`headers`/`col_widths`. `TableInteraction` holds one
/// of each as a bare `usize` — `sort.col` is data, `selected_col` is visible —
/// so the two conversions are where a mix-up would land. They were hand-rolled
/// (`position()` here, `get()` there); now they are named, so the direction is
/// readable at the call site.
#[test]
fn the_two_column_spaces_convert_in_one_place() {
    let view = PreparedView {
        rows: vec![vec!["a".into(), "c".into()]],
        health: vec![k9rs_proto::row::RowHealth::Normal],
        cell_style: vec![vec![None, None]],
        keys: vec![],
        col_widths: vec![10, 10],
        total_rows: 1,
        // Column 1 is HIDDEN: the visible set is data columns 0 and 2.
        visible_cols: vec![0, 2],
        headers: vec!["NAME".into(), "AGE".into()],
    };

    // visible -> data, with the header, and the hidden column skipped.
    assert_eq!(view.data_col(0), Some((0, "NAME".to_string())));
    assert_eq!(view.data_col(1), Some((2, "AGE".to_string())));
    // A cursor past the end CLAMPS rather than failing: the visible set
    // shrinks under a column-level change while the cursor is still the old
    // one, and display-time clamping is the rule here.
    assert_eq!(view.data_col(99), Some((2, "AGE".to_string())));

    // data -> visible, and `None` for a column that is not on screen —
    // which is exactly when a sort indicator must not be drawn.
    assert_eq!(view.visible_col(0), Some(0));
    assert_eq!(view.visible_col(2), Some(1));
    assert_eq!(view.visible_col(1), None, "data column 1 is hidden");

    // Round-trip: every visible column maps back to itself.
    for v in 0..view.visible_cols.len() {
        let (data, _) = view.data_col(v).expect("in range");
        assert_eq!(view.visible_col(data), Some(v), "visible {v} round-trips");
    }

    // An empty view has no columns to convert, and says so rather than
    // panicking on `len() - 1`.
    let empty = PreparedView { visible_cols: vec![], headers: vec![], ..view };
    assert_eq!(empty.data_col(0), None);
    assert_eq!(empty.visible_col(0), None);
}

/// How a tagged log line EXPORTS: the source becomes a `[tag] ` prefix, and
/// an untagged line gets none.
///
/// These two assertions were in `k9rs_daemon`'s `server_session` mirror,
/// bundled with the tests for the TAGGING. Tagging is the daemon's job and
/// rendering is `StoredLine`'s, so the daemon's tests were reaching one crate
/// up — which costs `cargo test -p k9rs_daemon` the entire TUI.
#[test]
fn a_tagged_line_exports_with_its_source_as_a_prefix() {
    use k9rs_proto::protocol::LogLine;
    let untagged = LogLine { source: None, timestamp: None, content: "hello".to_string() };
    assert_eq!(StoredLine::from(untagged).flat_text(), "hello");
    let tagged = LogLine {
        source: Some("pod-b".to_string()),
        timestamp: None,
        content: "hello".to_string(),
    };
    assert_eq!(StoredLine::from(tagged).flat_text(), "[pod-b] hello");
}
