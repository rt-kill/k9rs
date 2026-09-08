use crate::app::element::{Element, LogSession};
use crate::app::store::StorePayload;
use crate::app::{App, ContainerRef, Overlay, OverlayExtent, ShellState};
use crate::kube::protocol::{LogContainer, LogLine, TableBaseline};
use crate::kube::resource_def::BuiltInKind;
use crate::kube::resources::row::{CellValue, ResourceRow};
use ratatui::backend::TestBackend;
use ratatui::Terminal;

// ---------------------------------------------------------------------------
// "A loading/connecting state owns its screen" — the invariant the user has
// reported broken three times. A state that says the data is not live must
// not leave the not-live data on screen behind it.
// ---------------------------------------------------------------------------

/// Everything currently painted, as one string — what the terminal shows.
fn screen(term: &Terminal<TestBackend>) -> String {
    term.backend().buffer().content.iter().map(|c| c.symbol()).collect()
}

fn term() -> Terminal<TestBackend> {
    Terminal::new(TestBackend::new(100, 30)).expect("test terminal")
}

fn paint(term: &mut Terminal<TestBackend>, app: &mut App) -> String {
    term.draw(|f| crate::ui::draw(f, app)).expect("draw");
    screen(term)
}

/// Seed the root pod table with one recognizable row.
fn seed_row(app: &App, name: &str) {
    let store = app.nav.top().data_store().expect("root is a table");
    store.apply(
        1,
        StorePayload::Baseline(TableBaseline {
            resource: crate::app::nav::rid(BuiltInKind::Pod),
            headers: vec!["NAME".into(), "STATUS".into()],
            rows: vec![ResourceRow {
                name: name.into(),
                namespace: Some("default".into()),
                cells: vec![
                    CellValue::Text(name.to_string()),
                    CellValue::Text("Running".to_string()),
                ],
                ..Default::default()
            }],
        }),
    );
}

fn shell_overlay() -> Overlay {
    Overlay::Shell(Box::new(ShellState {
        title: "canarypod".to_string(),
        stream: None,
        connect_state: crate::app::ShellConnectState::Connecting,
        pending_output: Vec::new(),
    }))
}

#[test]
fn a_full_frame_overlay_erases_the_view_beneath() {
    // THE bug: the shell connect screen claims the whole frame and draws a
    // titled border, but a `Block` paints only its edges — so every interior
    // cell still held the resource table, and the connect screen rendered as
    // a frame AROUND the stale rows ("background persistent on ssh").
    let mut app = App::new_for_test();
    app.conn.established();
    seed_row(&app, "canary-row-marker");
    let mut t = term();

    let before = paint(&mut t, &mut app);
    assert!(before.contains("canary-row-marker"), "table paints its rows normally");

    app.ui.overlay = Some(shell_overlay());
    let during = paint(&mut t, &mut app);
    assert!(
        !during.contains("canary-row-marker"),
        "a full-frame overlay must not leave the view inside its border:\n{}",
        during
    );
    assert!(during.contains("shell:"), "the connect screen itself still paints");
}

#[test]
fn a_dialog_overlay_keeps_the_live_view_around_it() {
    // The other half of the rule: a dialog claims a box, not the frame, and
    // the view around it is live and honest — erasing it would be wrong.
    let mut app = App::new_for_test();
    app.conn.established();
    seed_row(&app, "canary-row-marker");
    let mut t = term();
    app.ui.overlay = Some(Overlay::Help {
        viewport: crate::app::viewport::Viewport::default(),
    });

    let painted = paint(&mut t, &mut app);
    assert!(
        painted.contains("canary-row-marker"),
        "a Dialog-extent overlay leaves the surrounding view alone"
    );
}

#[test]
fn overlay_extents_are_declared_not_improvised() {
    // Extent drives the central erase; a new overlay kind must choose one.
    assert_eq!(shell_overlay().extent(), OverlayExtent::FullFrame);
    assert_eq!(
        Overlay::Help { viewport: crate::app::viewport::Viewport::default() }.extent(),
        OverlayExtent::Dialog
    );
    assert_eq!(
        Overlay::Edit {
            target: crate::kube::protocol::ObjectRef {
                resource: crate::app::nav::rid(BuiltInKind::Pod),
                namespace: crate::kube::protocol::Namespace::Named("default".into()),
                name: "p".into(),
            },
            state: crate::app::EditState::AwaitingYaml,
        }
        .extent(),
        OverlayExtent::Dialog
    );
}

#[test]
fn a_dead_link_hides_the_frozen_log_tail() {
    // Logs had no link gate at all: on a daemon drop the lines stayed fully
    // painted — and the store can't self-report, since `live` stays true
    // across the gap. The view kept advertising a tail that had stopped.
    let mut app = App::new_for_test();
    app.conn.established();
    let session = LogSession::for_test(ContainerRef::new(
        "canarypod",
        "default",
        LogContainer::Named("app".to_string()),
    ));
    session.store().push(
        0,
        LogLine { container: None, content: "canary-log-marker".to_string() },
    );
    app.nav.push(Element::LogSession(Box::new(session)));
    let mut t = term();

    let live = paint(&mut t, &mut app);
    assert!(live.contains("canary-log-marker"), "a live tail shows its lines");

    app.conn.disconnected();
    let gap = paint(&mut t, &mut app);
    assert!(
        !gap.contains("canary-log-marker"),
        "a dead link must not leave a frozen tail on screen:\n{}",
        gap
    );
    assert!(gap.contains("Connecting"), "the log view states why it's empty");
}

#[test]
fn a_dead_link_hides_the_resource_table() {
    // The table's half of the reported bug. A dead LINK outranks anything
    // the store believes — the store still says Ready, because "Ready" is
    // itself the last thing that arrived over the connection that just died.
    let mut app = App::new_for_test();
    app.conn.established();
    seed_row(&app, "canary-row-marker");
    let mut t = term();
    assert!(paint(&mut t, &mut app).contains("canary-row-marker"));

    app.conn.disconnected();
    let gap = paint(&mut t, &mut app);
    assert!(
        !gap.contains("canary-row-marker"),
        "a dead link must not leave a frozen table on screen:\n{}",
        gap
    );
    assert!(gap.contains("Connecting"), "the table states why it's empty");
}

#[test]
fn a_dead_cluster_watch_shows_its_rows_but_never_silently() {
    // The other half of the rule. Under Stale the LINK is up, so every
    // operation on these rows still reaches the apiserver — blanking the
    // table would throw away the cursor, the marks and the scroll position
    // over a cluster hiccup that heals itself. What must never happen is
    // showing them with nothing to say.
    let mut app = App::new_for_test();
    app.conn.established();
    seed_row(&app, "canary-row-marker");
    let store = app.nav.top().data_store().expect("root is a table");
    store.apply(1, StorePayload::Stale("watch for pods failed".into()));
    let mut t = term();

    let painted = paint(&mut t, &mut app);
    assert!(painted.contains("canary-row-marker"), "stale rows are still worth acting on");
    assert!(painted.contains("STALE"), "…but the frame says so:\n{}", painted);
}

#[test]
fn a_failed_subscription_is_visible_even_when_its_rows_survive() {
    // Pre-existing hole, independent of the reported bug: the resource gate
    // was `connecting || total_rows == 0 || initializing`, so a store in
    // Failed WITH resident rows fell straight through to the normal table —
    // a terminal subscription error was invisible whenever rows outlived it.
    let mut app = App::new_for_test();
    app.conn.established();
    seed_row(&app, "canary-row-marker");
    let store = app.nav.top().data_store().expect("root is a table");
    store.apply(1, StorePayload::Failed("forbidden: pods is denied".into()));
    let mut t = term();

    let painted = paint(&mut t, &mut app);
    assert!(
        painted.contains("ERROR"),
        "a failed subscription must not render as a normal table:\n{}",
        painted
    );
}

#[test]
fn the_reconnect_gap_does_not_eat_the_log_scroll_position() {
    // The gate must not publish geometry from a frame that measured no
    // content: a zero extent clamps the offset to 0, so the user would come
    // back from a blip scrolled to the top of a long buffer.
    let mut app = App::new_for_test();
    app.conn.established();
    let session = LogSession::for_test(ContainerRef::new(
        "canarypod",
        "default",
        LogContainer::Named("app".to_string()),
    ));
    for i in 0..200 {
        session.store().push(0, LogLine { container: None, content: format!("line {}", i) });
    }
    app.nav.push(Element::LogSession(Box::new(session)));
    let mut t = term();
    paint(&mut t, &mut app);

    // Park somewhere in the middle of the buffer, then lose the connection.
    // (`line_down` is bounded by the extent the paint above published.)
    let view = app.nav.top_mut().log_view_mut().expect("log view");
    view.viewport.line_down(40);
    let parked = view.viewport.offset();
    assert!(parked > 0, "test scrolled somewhere to preserve");

    app.conn.disconnected();
    paint(&mut t, &mut app);
    assert_eq!(
        app.nav.top().log_view().expect("log view").viewport.offset(),
        parked,
        "the connecting frame must not republish geometry"
    );
}


#[test]
fn the_contexts_view_paints_as_an_ordinary_table() {
    // It used to have its own 214-line renderer. Now it goes through the
    // resource-table widget like everything else, so the columns, the
    // title/count chrome and the liveness gate all come for free — and the
    // one thing that is genuinely its own (rows from disk, not a stream)
    // still paints with no daemon connected at all.
    let mut app = App::new_for_test();
    crate::kube::session_events::apply_event(&mut app, crate::event::AppEvent::KubeconfigLoaded {
        contexts: vec![
            crate::app::KubeContext {
                name: crate::kube::protocol::ContextName::new("canary-ctx").unwrap(),
                identity: crate::kube::protocol::ClusterIdentity::new(
                    "canary-cluster".into(), "canary-user".into(),
                ),
                is_current: true,
            },
        ],
        current_context: None,
        current_identity: crate::kube::protocol::ClusterIdentity::default(),
    });
    crate::kube::session_events::apply_event(&mut app, crate::event::AppEvent::NoContextConfigured);

    let mut t = term();
    let painted = paint(&mut t, &mut app);
    for expected in ["NAME", "CLUSTER", "USER", "canary-ctx", "canary-cluster", "canary-user"] {
        assert!(painted.contains(expected), "missing {expected} in:\n{painted}");
    }
}

#[test]
fn horizontal_scroll_never_lands_inside_a_column() {
    // A mid-column offset renders wrong: `screen_x` saturates at the left
    // edge, so the first column pins in place at FULL width while every
    // column after it shifts — they overlap, and nothing clips the hidden
    // left-hand prefix. Keeping offsets on column boundaries makes that
    // unrepresentable rather than something the cell renderer must handle.
    let mut app = App::new_for_test();
    app.conn.established();
    let store = app.nav.top().data_store().expect("root is a table");
    let wide = "0123456789abcdefghij"; // 20 cols each — wider than the frame
    store.apply(
        1,
        StorePayload::Baseline(TableBaseline {
            resource: crate::app::nav::rid(BuiltInKind::Pod),
            headers: vec!["NAME".into(), "B".into(), "C".into(), "D".into(), "E".into()],
            rows: vec![ResourceRow {
                name: "row".into(),
                namespace: Some("default".into()),
                cells: (0..5).map(|_| CellValue::Text(wide.to_string())).collect(),
                ..Default::default()
            }],
        }),
    );
    let mut t = term();
    paint(&mut t, &mut app);

    // Walk the column cursor right, repainting each time so the widget
    // re-runs its scroll adjustment, and check the offset after every step.
    for step in 0..5 {
        app.col_right();
        paint(&mut t, &mut app);
        let view = app.nav.top_mut().view(crate::app::ColumnLevel::Default, 200);
        let starts: Vec<u16> = view
            .col_widths
            .iter()
            .scan(0u16, |acc, w| {
                let s = *acc;
                *acc += w;
                Some(s)
            })
            .collect();
        let offset = app.nav.top().table_interaction().expect("table").col_offset;
        assert!(
            offset == 0 || starts.contains(&offset),
            "step {step}: col_offset {offset} is inside a column (starts: {starts:?})",
        );
    }
}
