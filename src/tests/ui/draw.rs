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

    app.ui.open(crate::app::Modal::Overlay(shell_overlay()));
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
    app.ui.open(crate::app::Modal::Overlay(Overlay::Help {
        viewport: crate::app::viewport::Viewport::default(),
    }));

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

#[test]
fn hostile_apiserver_text_cannot_reach_the_terminal() {
    // SEV-1 from the 2026-09-09 audit. The apiserver's `Status.message`
    // rides `StreamEvent::Error`/`Stale` into `Liveness`, which renders it
    // through a `Block` TITLE and a centred `Span` — and ratatui writes
    // title/Span symbols VERBATIM. Only `Buffer::set_string`/`set_line`
    // filter, which is why the flash path was never exposed and these two
    // were. A compromised apiserver, or an admission webhook whose denial
    // message an attacker controls, could smuggle OSC-52 (clipboard write)
    // or ESC[6n (cursor report → stdin injection while in raw mode).
    const HOSTILE: &str = "\u{1b}]52;c;aGF4\u{7}denied\u{1b}[6n";
    let mut app = App::new_for_test();
    app.conn.established();
    seed_row(&app, "canary-row-marker");
    let store = app.nav.top().data_store().expect("root is a table").clone();

    // The banner path: rows resident, so the message rides the title.
    store.apply(1, StorePayload::Stale(HOSTILE.to_string()));
    let mut t = term();
    let painted = paint(&mut t, &mut app);
    assert!(!painted.contains('\u{1b}'), "no ESC may reach the buffer");
    assert!(!painted.contains('\u{7}'), "no BEL may reach the buffer");
    assert!(painted.contains("denied"), "the human-readable part survives");

    // The status-line path: no rows, so the message is centred instead.
    store.clear();
    store.apply(2, StorePayload::Failed(HOSTILE.to_string()));
    let painted = paint(&mut t, &mut app);
    assert!(!painted.contains('\u{1b}'));
    assert!(!painted.contains('\u{7}'));
}

#[test]
fn cached_overview_counters_say_they_are_from_a_previous_visit() {
    // Found independently by three audit agents: `switch_context` marked the
    // restored core stores `Stale`, and NOTHING read that field — the
    // Overview's counters come from `i.rows` and gate on the LINK alone. So
    // A→B→A painted the previous visit's node/namespace counts exactly like
    // live ones. The unit test passed; the screen still lied.
    use crate::kube::protocol::ContextName;
    use crate::kube::resource_def::BuiltInKind;
    let mut app = App::new_for_test();
    app.conn.established();
    app.core.seed(BuiltInKind::Namespace, vec![ResourceRow {
        name: "kube-system".into(),
        ..Default::default()
    }]);

    // Leave for another context and come back.
    let a = ContextName::new("ctx-a").unwrap();
    let b = ContextName::new("ctx-b").unwrap();
    app.core.switch_context(Some(a.clone()), &b);
    app.core.switch_context(Some(b), &a);

    // The counters live on the Overview, which is where every switch lands.
    app.nav.reset(Element::Overview(crate::app::element::Overview));
    let mut t = term();
    let painted = paint(&mut t, &mut app);
    assert!(
        painted.contains("STALE"),
        "restored counters must say they're from a previous visit:\n{painted}"
    );
}

#[test]
fn the_light_palette_actually_reaches_the_screen() {
    // The palette is only worth having if it survives the whole render path.
    // The dialog fill is the sharpest probe: it used to be a hardcoded
    // near-black const applied over Clear, so on a light terminal every
    // dialog was a black hole regardless of theme.
    use ratatui::style::Color;
    let mut app = App::new_for_test();
    app.conn.established();
    app.ui.theme = crate::ui::theme::Theme::light();
    app.ui.open(crate::app::Modal::Overlay(Overlay::Help {
        viewport: crate::app::viewport::Viewport::default(),
    }));

    let mut t = term();
    paint(&mut t, &mut app);
    let fill = crate::ui::theme::Theme::light().dialog_fill;
    let painted_fill = t
        .backend()
        .buffer()
        .content
        .iter()
        .any(|c| c.style().bg == Some(fill));
    assert!(painted_fill, "the light dialog fill must reach the buffer");

    // And it is genuinely light — the old const was Rgb(25, 28, 38).
    let Color::Rgb(r, g, b) = fill else { panic!("dialog fill should be rgb") };
    let luma = 0.299 * r as f32 + 0.587 * g as f32 + 0.114 * b as f32;
    assert!(luma > 127.5, "a light theme's dialog must not be dark: {fill:?}");
}

// ---------------------------------------------------------------------------
// "Chrome that explains the content cannot be owned by the content." The log
// grep bar was painted inside `LogViewer`, which the view only instantiates
// when there are lines to draw — so it vanished in exactly the states that
// needed explaining, and covered the last row in the states that didn't.
// ---------------------------------------------------------------------------

/// A log element with `lines` already in its store, ready to push.
fn log_session_with(lines: &[&str]) -> Element {
    let session = LogSession::for_test(ContainerRef::new(
        "canarypod",
        "default",
        LogContainer::Named("app".to_string()),
    ));
    for content in lines {
        session
            .store()
            .push(0, LogLine { container: None, content: (*content).to_string() });
    }
    Element::LogSession(Box::new(session))
}

#[test]
fn a_grep_that_matches_nothing_still_shows_its_pattern() {
    // THE reported bug. Type a grep that matches nothing and the screen said
    // "No matching lines." with no sign of the pattern that emptied it —
    // nothing to read, nothing to correct, and no way to tell a typo from a
    // genuinely absent line.
    let mut app = App::new_for_test();
    app.conn.established();
    app.nav.push(log_session_with(&["hello world"]));
    app.nav.top_mut().log_view_mut().expect("log view").draft =
        Some("zzz-no-such-line".to_string());

    let mut t = term();
    let painted = paint(&mut t, &mut app);
    assert!(
        painted.contains("No matching lines."),
        "the grep really did empty the view:\n{painted}"
    );
    assert!(
        painted.contains("zzz-no-such-line"),
        "the pattern that emptied the view must stay on screen to be edited:\n{painted}"
    );
}

#[test]
fn a_committed_grep_that_matches_nothing_still_shows_its_chain() {
    // Same hole one level up: a COMMITTED grep (a `LogFilter` element) that
    // narrows to zero also lost its label, so the only way to learn what was
    // filtering the view was to pop it.
    let mut app = App::new_for_test();
    app.conn.established();
    let session = log_session_with(&["hello world"]);
    let filter = Element::derive_log_filter(
        &session,
        crate::app::nav::CompiledGrep::new("zzz-no-such-line"),
    )
    .expect("log session yields a line output");
    app.nav.push(session);
    app.nav.push(filter);

    let mut t = term();
    let painted = paint(&mut t, &mut app);
    assert!(
        painted.contains("No matching lines."),
        "the grep really did empty the view:\n{painted}"
    );
    assert!(
        painted.contains("zzz-no-such-line"),
        "a committed grep must keep naming itself when it matches nothing:\n{painted}"
    );
}

#[test]
fn the_grep_bar_does_not_cover_the_last_log_line() {
    // The other half of the same structural fault: the bar was painted over
    // the last content row while the Viewport still counted that row as
    // visible. With a grep active the tail line was drawn and then erased, and
    // no amount of scrolling could bring it back — `end()` was already at the
    // bottom. Now the bar claims its own layout row, so the content area is
    // genuinely one shorter and every measured row is a row you can see.
    let mut app = App::new_for_test();
    app.conn.established();
    let lines: Vec<String> = (0..200).map(|i| format!("line-{i} keep")).collect();
    let session = log_session_with(&lines.iter().map(String::as_str).collect::<Vec<_>>());
    let filter =
        Element::derive_log_filter(&session, crate::app::nav::CompiledGrep::new("keep"))
            .expect("log session yields a line output");
    app.nav.push(session);
    app.nav.push(filter);

    let mut t = term();
    paint(&mut t, &mut app); // first frame publishes the measured extent
    let painted = paint(&mut t, &mut app); // second tails to the true bottom
    assert!(
        painted.contains("line-199"),
        "the tail line must survive the grep bar that sits below it:\n{painted}"
    );
}
