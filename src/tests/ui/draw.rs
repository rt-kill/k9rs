use crate::app::element::{Element, LogSession};
use crate::app::store::StorePayload;
use crate::app::element::ViewExtent;
use crate::app::{App, ShellState, TakeoverOp};
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

/// The one painted ROW containing `needle`. `screen()` concatenates every
/// row into a single string, so asserting `contains` on it can be satisfied
/// by chrome somewhere else on the frame entirely — fine for a marker that
/// appears nowhere else, useless for a word like "n/a" that the header also
/// prints.
fn row_with(term: &Terminal<TestBackend>, needle: &str) -> String {
    let buf = term.backend().buffer();
    let width = buf.area.width as usize;
    buf.content
        .iter()
        .map(|c| c.symbol().to_string())
        .collect::<Vec<_>>()
        .chunks(width)
        .map(|row| row.concat())
        .find(|row| row.contains(needle))
        .unwrap_or_else(|| panic!("no painted row contains {needle:?}"))
}

fn paint(term: &mut Terminal<TestBackend>, app: &mut App) -> String {
    term.draw(|f| crate::ui::draw(f, app)).expect("draw");
    screen(term)
}

/// The `line-NNN` marker on the TOPMOST painted log row — i.e. which line the
/// window starts on. Position on screen is the whole question in the scroll
/// tests, so `contains` on the flattened frame (which `screen()` gives) would
/// pass for any scroll position at all.
fn top_log_marker(term: &Terminal<TestBackend>) -> String {
    let buf = term.backend().buffer();
    let width = buf.area.width as usize;
    buf.content
        .iter()
        .map(|c| c.symbol().to_string())
        .collect::<Vec<_>>()
        .chunks(width)
        .map(|row| row.concat())
        .find_map(|row| {
            let at = row.find("line-")?;
            Some(row[at..].split_whitespace().next()?.to_string())
        })
        .expect("no painted row carries a log marker")
}

/// Push the help element, exactly as `Action::Help` does.
fn push_help(app: &mut App) {
    app.conn.established();
    let class = crate::event::keymap::ViewClass::of(app.nav.top());
    let caps = Some(app.current_capabilities());
    let markable = app.nav.top().markable();
    let store = app.core.seed_keybindings(caps, app.config.keys, class, markable);
    let rid = crate::kube::local::LocalResourceKind::Keybinding.to_resource_id();
    app.nav.push(Element::ResourceList(crate::app::element::ResourceList::client(
        crate::app::element::QuerySpec {
            rid,
            namespace: crate::kube::protocol::Namespace::All,
            filter: None,
        },
        store,
        &app.kube.metrics,
        "keys".to_string(),
    )));
    assert!(app.nav.top().is_keybindings_view(), "that IS the help element");
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

fn shell_overlay() -> TakeoverOp {
    TakeoverOp::Shell(Box::new(ShellState {
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

    app.ui.open(crate::app::Modal::Takeover(shell_overlay()));
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
    app.ui.open(crate::app::Modal::Takeover(TakeoverOp::Edit {
            target: crate::kube::protocol::ObjectRef::new(
                crate::app::nav::rid(crate::kube::resource_def::BuiltInKind::Pod),
                "p".to_string(),
                crate::kube::protocol::Namespace::Named("default".to_string()),
            ),
            state: crate::app::EditState::AwaitingYaml,
        }));

    let painted = paint(&mut t, &mut app);
    assert!(
        painted.contains("canary-row-marker"),
        "a Dialog-extent overlay leaves the surrounding view alone"
    );
    // …and it says what it is doing, in the box its own extent asked for.
    // The label is the op's (`TakeoverOp::label`) because the extent has to
    // measure it; a renderer inventing its own would size one string twice.
    assert!(
        painted.contains("Fetching YAML for Pod/p"),
        "the takeover's own label is drawn:\n{painted}",
    );
}

#[test]
fn overlay_extents_are_declared_not_improvised() {
    // Extent drives the central erase; a new overlay kind must choose one.
    assert_eq!(shell_overlay().extent(term().get_frame().area()), ViewExtent::Full);
    assert_eq!(
        TakeoverOp::Edit {
            target: crate::kube::protocol::ObjectRef {
                resource: crate::app::nav::rid(BuiltInKind::Pod),
                namespace: crate::kube::protocol::Namespace::Named("default".into()),
                name: "p".into(),
            },
            state: crate::app::EditState::AwaitingYaml,
        }
        .extent(term().get_frame().area()),
        ViewExtent::Dialog {
            // Sized from its own label, in cells, exactly as the dialog
            // ELEMENTS are — which is the point of there being one extent
            // type: a takeover's box states its size, so it no longer has
            // to centre itself.
            width: "Fetching YAML for Pod/p…".chars().count() as u16 + 6,
            height: 3,
        }
    );
}

#[test]
fn a_dead_link_hides_the_frozen_log_tail() {
    // Logs had no link gate at all: on a daemon drop the lines stayed fully
    // painted — and the store can't self-report, since `live` stays true
    // across the gap. The view kept advertising a tail that had stopped.
    let mut app = App::new_for_test();
    app.conn.established();
    let session = LogSession::for_test(crate::kube::protocol::LogTarget::One("canarypod".to_string()), LogContainer::Named("app".to_string(),
    ));
    session.store().push(
        0,
        LogLine { source: None, timestamp: None, content: "canary-log-marker".to_string() },
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
    let session = LogSession::for_test(crate::kube::protocol::LogTarget::One("canarypod".to_string()), LogContainer::Named("app".to_string(),
    ));
    for i in 0..200 {
        session.store().push(0, LogLine { source: None, timestamp: None, content: format!("line {}", i) });
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
fn a_context_with_no_user_says_so_instead_of_showing_a_blank_cell() {
    // `ClusterIdentity`'s fields were `String`, and blank meant "the
    // kubeconfig didn't say". Six readers hand-wrote `if is_empty() { "n/a" }`;
    // this one — the contexts table — forgot, so a context whose entry names
    // no `user:` rendered an EMPTY cell while the header, describing the very
    // same field, said "n/a". `Option` + `CellValue::Placeholder` make the
    // omission unwritable.
    let mut app = App::new_for_test();
    crate::kube::session_events::apply_event(&mut app, crate::event::AppEvent::KubeconfigLoaded {
        contexts: vec![crate::app::KubeContext {
            name: crate::kube::protocol::ContextName::new("userless-ctx").unwrap(),
            // Exactly what kube-rs hands back for `user:`-less entry:
            // `Option<String>::None`, which the boundary unwraps to "".
            identity: crate::kube::protocol::ClusterIdentity::new(
                "canary-cluster".into(),
                String::new(),
            ),
            is_current: true,
        }],
        current_context: None,
        current_identity: crate::kube::protocol::ClusterIdentity::default(),
    });
    crate::kube::session_events::apply_event(&mut app, crate::event::AppEvent::NoContextConfigured);

    let mut t = term();
    let painted = paint(&mut t, &mut app);
    // The header prints "n/a" for its own three unknown fields, so this has
    // to be asserted on the CONTEXT ROW or it proves nothing.
    let row = row_with(&t, "userless-ctx");
    assert!(
        row.contains("canary-cluster"),
        "the known field still renders: {row:?}\n{painted}"
    );
    assert!(
        row.contains("n/a"),
        "an unknown user must SAY it is unknown, not render blank: {row:?}\n{painted}"
    );
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
    app.ui.open(crate::app::Modal::Takeover(TakeoverOp::Edit {
            target: crate::kube::protocol::ObjectRef::new(
                crate::app::nav::rid(crate::kube::resource_def::BuiltInKind::Pod),
                "p".to_string(),
                crate::kube::protocol::Namespace::Named("default".to_string()),
            ),
            state: crate::app::EditState::AwaitingYaml,
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
    let session = LogSession::for_test(crate::kube::protocol::LogTarget::One("canarypod".to_string()), LogContainer::Named("app".to_string(),
    ));
    for content in lines {
        session
            .store()
            .push(0, LogLine { source: None, timestamp: None, content: (*content).to_string() });
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
        Some(crate::app::text_input::TextInput::seeded("zzz-no-such-line"));

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

#[test]
fn help_documents_the_view_you_pressed_it_in() {
    // Help used to be one context-free dump for the whole app, so it listed
    // `s` (follow — only in logs) and `Ctrl-d` (half-page in a detail view,
    // DELETE in a table) with no indication of which view it meant. It was
    // therefore wrong in at least one view no matter where you stood. It now
    // projects `event::keymap` for the class it was opened FROM.
    //
    // Asserted against the seeded ROWS rather than the painted frame: help is
    // a table now, so the paint shows one scrolled window of it — which is
    // the point (you grep it), but makes "does this key appear" a question
    // about the data, not the pixels.
    fn help_rows(app: &mut App) -> Vec<String> {
        let class = crate::event::keymap::ViewClass::of(app.nav.top());
        let caps = Some(app.current_capabilities());
        let store = app.core.seed_keybindings(
            caps,
            app.config.keys,
            class,
            app.nav.top().markable(),
        );
        store.with_read(|i| {
            i.rows
                .iter()
                .map(|r| {
                    r.cells.iter().map(|c| c.to_string()).collect::<Vec<_>>().join(" | ")
                })
                .collect()
        })
    }



    let mut app = App::new_for_test();
    app.conn.established();

    // The root is a pod table.
    let table_help = help_rows(&mut app).join("\n");
    assert!(
        table_help.contains("Jump to owner"),
        "table help documents J, which used to appear nowhere:\n{table_help}"
    );
    assert!(
        !table_help.contains("Toggle follow"),
        "…and does NOT advertise the log view's `s`:\n{table_help}"
    );

    // Now stand in a log view.
    app.nav.push(Element::LogSession(Box::new(LogSession::for_test(crate::kube::protocol::LogTarget::One("canarypod".to_string()), LogContainer::Named("app".to_string(),
    )))));
    let log_help = help_rows(&mut app).join("\n");
    assert!(
        log_help.contains("Toggle follow"),
        "log help documents `s`:\n{log_help}"
    );
    assert!(
        !log_help.contains("Jump to owner"),
        "…and drops the table-only keys:\n{log_help}"
    );
}

#[test]
fn a_following_log_view_tracks_the_tail_as_lines_arrive() {
    // Nothing covered "does autoscroll actually autoscroll" end to end — the
    // Viewport was tested in isolation, and the render path that publishes the
    // wrap-expanded extent back into it was not. That publish is what makes
    // following work at all: the widget alone knows how many PHYSICAL rows the
    // lines occupy.
    let mut app = App::new_for_test();
    app.conn.established();
    let session = LogSession::for_test(crate::kube::protocol::LogTarget::One("canarypod".to_string()), LogContainer::Named("app".to_string(),
    ));
    for i in 0..200 {
        session.store().push(0, LogLine { source: None, timestamp: None, content: format!("line-{i}") });
    }
    app.nav.push(Element::LogSession(Box::new(session)));
    // The initial tail batch has settled — the event loop clears this once a
    // drain completes, and until it does autoscroll is deliberately suppressed
    // so the view doesn't race while the tail streams in.
    app.nav.top_mut().log_view_mut().expect("log view").initial_load = false;

    let mut t = term();
    // ONE paint. Following resolves inside the widget against the extent it
    // measures this frame, so there is no catch-up frame to wait for.
    let painted = paint(&mut t, &mut app);
    assert!(painted.contains("line-199"), "a following view sits on the tail:\n{painted}");

    // More lines arrive: the view must move with them, not be left behind.
    for i in 200..260 {
        app.nav
            .top()
            .log_store()
            .expect("log store")
            .push(0, LogLine { source: None, timestamp: None, content: format!("line-{i}") });
    }
    let painted = paint(&mut t, &mut app);
    assert!(painted.contains("line-259"), "…and follows the new tail:\n{painted}");
    assert!(!painted.contains("line-199"), "the old tail has scrolled off:\n{painted}");

    // Stop following: the view holds position while the log keeps growing.
    app.nav.top_mut().log_view_mut().expect("log view").viewport.toggle_follow();
    let held = paint(&mut t, &mut app);
    assert!(held.contains("line-259"), "still where we stopped");
    for i in 260..320 {
        app.nav
            .top()
            .log_store()
            .expect("log store")
            .push(0, LogLine { source: None, timestamp: None, content: format!("line-{i}") });
    }
    let after = paint(&mut t, &mut app);
    assert!(
        after.contains("line-259") && !after.contains("line-319"),
        "a stopped view stays put while the tail runs on ahead:\n{after}"
    );
}

/// P2, the half that could not be patched: the ring evicts from the front
/// while you are reading history, and a persisted ROW then names a different
/// line every time it does. The old cure shifted the offset up by the
/// eviction COUNT — a logical number applied to a physical row — so the view
/// crept toward the tail under wrap and jumped under a filter.
///
/// The position is a line now, so eviction above the window is arithmetic
/// that never happens.
#[test]
fn eviction_under_a_parked_view_does_not_move_it() {
    let mut app = App::new_for_test();
    app.conn.established();
    let session = LogSession::for_test(
        crate::kube::protocol::LogTarget::One("canarypod".to_string()),
        LogContainer::Named("app".to_string()),
    );
    // Fill the ring exactly, so the next arrival is the first eviction.
    let cap = session.store().max_lines();
    for i in 0..cap {
        session.store().push(0, LogLine { source: None, timestamp: None, content: format!("line-{i}") });
    }
    app.nav.push(Element::LogSession(Box::new(session)));
    app.nav.top_mut().log_view_mut().expect("log view").initial_load = false;

    let mut t = term();
    paint(&mut t, &mut app);
    let at_tail = top_log_marker(&t);
    app.nav.top_mut().log_view_mut().expect("log view").viewport.line_up(500);
    paint(&mut t, &mut app);
    let parked_on = top_log_marker(&t);
    assert_ne!(parked_on, at_tail, "sanity: the test scrolled back into history");

    // 300 more lines arrive; the ring is full, so 300 of the oldest go.
    let store = app.nav.top().log_store().expect("log store").clone();
    for i in cap..cap + 300 {
        store.push(0, LogLine { source: None, timestamp: None, content: format!("line-{i}") });
    }
    assert_eq!(store.with_read(|i| i.evicted), 300, "sanity: the test really did evict");

    paint(&mut t, &mut app);
    assert_eq!(
        top_log_marker(&t),
        parked_on,
        "the window must stay on the line it was reading, not drift with the ring",
    );
}

/// Toggling `w` re-flows every line to a different height, so a persisted row
/// number means something else the instant it is pressed: at 3 rows per line
/// and 50k lines you left line ~15,000 and arrived at line 30,000. The anchor
/// names the line, and the widget re-derives the row at whichever layout is
/// current — so `w` is a display toggle, not a navigation key.
#[test]
fn toggling_wrap_keeps_the_line_you_were_reading() {
    let mut app = App::new_for_test();
    app.conn.established();
    let session = LogSession::for_test(
        crate::kube::protocol::LogTarget::One("canarypod".to_string()),
        LogContainer::Named("app".to_string()),
    );
    // Three physical rows per line in a 100-column terminal, and the marker
    // repeats along the body so EVERY one of those rows names its line —
    // otherwise a continuation row reads as blank and the probe below silently
    // reports the next line down.
    for i in 0..200 {
        session.store().push(0, LogLine {
            source: None,
            timestamp: None,
            content: format!("line-{i:03} ").repeat(30),
        });
    }
    app.nav.push(Element::LogSession(Box::new(session)));
    {
        let view = app.nav.top_mut().log_view_mut().expect("log view");
        view.initial_load = false;
        view.wrap = true;
    }

    let mut t = term();
    paint(&mut t, &mut app);
    app.nav.top_mut().log_view_mut().expect("log view").viewport.line_up(300);
    paint(&mut t, &mut app);
    let marker = top_log_marker(&t);
    assert_ne!(marker, "line-199", "sanity: the test scrolled back into history");

    // `w` off — same line, unwrapped.
    app.nav.top_mut().log_view_mut().expect("log view").wrap = false;
    paint(&mut t, &mut app);
    assert_eq!(top_log_marker(&t), marker, "unwrapping moved off the line being read");

    // …and back on.
    app.nav.top_mut().log_view_mut().expect("log view").wrap = true;
    paint(&mut t, &mut app);
    assert_eq!(top_log_marker(&t), marker, "re-wrapping moved off the line being read");
}

#[test]
fn a_grep_that_momentarily_matches_nothing_keeps_your_scroll_position() {
    // Type one character of a grep that matches nothing, backspace it, and you
    // used to be at the TOP of a five-thousand-line log.
    //
    // A frame that draws a status screen renders no content, so it measures no
    // extent — and publishing that zero extent clamped the persisted offset to
    // `min(offset, 0)`. The connecting path was guarded against exactly this
    // by hand; "no matching lines" was not.
    let mut app = App::new_for_test();
    app.conn.established();
    let session = LogSession::for_test(crate::kube::protocol::LogTarget::One("canarypod".to_string()), LogContainer::Named("app".to_string(),
    ));
    for i in 0..500 {
        session.store().push(0, LogLine { source: None, timestamp: None, content: format!("line-{i}") });
    }
    app.nav.push(Element::LogSession(Box::new(session)));
    {
        let view = app.nav.top_mut().log_view_mut().expect("log view");
        view.initial_load = false;
    }

    let mut t = term();
    paint(&mut t, &mut app); // publish the real extent

    // Stop following and scroll back into history.
    {
        let vp = &mut app.nav.top_mut().log_view_mut().expect("log view").viewport;
        vp.line_up(200);
    }
    let parked = app.nav.top().log_view().expect("log view").viewport.offset();
    assert!(parked > 0, "we are somewhere in the middle: {parked}");

    // One keystroke of a grep that matches nothing → the "No matching lines."
    // status screen, which renders no content.
    app.nav.top_mut().log_view_mut().expect("log view").draft =
        Some(crate::app::text_input::TextInput::seeded("zzz-no-such-line"));
    let painted = paint(&mut t, &mut app);
    assert!(painted.contains("No matching lines."), "the status screen is up:\n{painted}");

    // Backspace it.
    app.nav.top_mut().log_view_mut().expect("log view").draft = None;
    paint(&mut t, &mut app);

    assert_eq!(
        app.nav.top().log_view().expect("log view").viewport.offset(),
        parked,
        "the scroll position survives a frame that measured nothing",
    );
}

/// A pattern that can't compile still searches — as plain text — and the bar
/// SAYS so. Before, `a\{2,3}` silently stopped being a regex and the only
/// thing on screen was a visible-line count of zero, which reads as "no such
/// lines" rather than "that isn't the pattern you wrote".
#[test]
fn a_degraded_grep_explains_itself_instead_of_showing_a_zero() {
    let mut app = App::new_for_test();
    app.conn.established();
    let session = log_session_with(&["aab", "aaab", "xyz"]);
    app.nav.push(session);

    // `\zs` compiles in Rust as the end-of-text anchor followed by `s`: a
    // valid regex that can never match. It is now refused by name.
    app.nav.top_mut().log_view_mut().unwrap().draft = Some(crate::app::text_input::TextInput::seeded(r"\zsaab"));
    let painted = paint(&mut term(), &mut app);
    assert!(painted.contains("literal"), "the bar must name the degradation:\n{painted}");
    assert!(painted.contains("zs"), "and say which atom caused it:\n{painted}");

    // A pattern that DOES compile keeps the ordinary count.
    app.nav.top_mut().log_view_mut().unwrap().draft = Some(crate::app::text_input::TextInput::seeded(r"aa\|xy"));
    let painted = paint(&mut term(), &mut app);
    assert!(!painted.contains("literal"), "a working regex says nothing:\n{painted}");
    assert!(painted.contains("visible"), "{painted}");
}

/// Smartcase is the quietest way a grep can under-match: one capital flips
/// the whole search, so `/Client` skips every `depth_client` and the only
/// thing on screen is a visible-line count of zero — which reads as "no such
/// line", not "no such line with that capitalisation".
///
/// The rule itself is wanted (it is vim's, and it is what makes `/INFO` mean
/// INFO). What was missing is any sign of it firing.
#[test]
fn a_case_sensitive_grep_says_so() {
    let mut app = App::new_for_test();
    app.conn.established();
    let session = LogSession::for_test(
        crate::kube::protocol::LogTarget::One("canarypod".to_string()),
        LogContainer::Named("app".to_string()),
    );
    for i in 0..20 {
        session.store().push(0, LogLine {
            source: None,
            timestamp: None,
            content: format!("depth_client={i}"),
        });
    }
    app.nav.push(Element::LogSession(Box::new(session)));
    app.nav.top_mut().log_view_mut().expect("log view").initial_load = false;
    let mut t = term();

    // All-lowercase: smartcase is off, the search is insensitive, no note.
    app.nav.top_mut().log_view_mut().unwrap().draft = Some(crate::app::text_input::TextInput::seeded("client"));
    let painted = paint(&mut t, &mut app);
    assert!(!painted.contains("case-sensitive"), "no capital, nothing to say:\n{painted}");

    // One capital flips it — and the bar now says why the view emptied.
    app.nav.top_mut().log_view_mut().unwrap().draft = Some(crate::app::text_input::TextInput::seeded("Client"));
    let painted = paint(&mut t, &mut app);
    assert!(
        painted.contains("case-sensitive"),
        "a capital turned smartcase off and the bar must say so:\n{painted}"
    );
    assert!(painted.contains("No matching lines."), "…which is why there are none");
}

/// A confirm dialog is a nav ELEMENT: it floats over the view that raised
/// it, that view stays on screen, and Esc is the ordinary pop.
///
/// It also owns EVERY key while it is up. `:` used to reach the global
/// handler and open the command prompt behind the box; the dialog's own
/// "any other key cancels" safety net — which exists so a spurious
/// keystroke dismisses it rather than being swallowed — was dead.
#[test]
fn a_confirm_dialog_floats_over_the_view_that_raised_it() {
    use crate::app::element::ViewExtent;
    let mut app = App::new_for_test();
    app.conn.established();
    seed_row(&app, "canary-row-marker");
    let mut t = term();
    let depth = app.nav.depth();

    app.nav.push(Element::Confirm(Box::new(crate::app::ConfirmDialog {
        message: "Delete pod/canary-row-marker?".to_string(),
        action_label: "Delete".to_string(),
        pending: crate::app::PendingAction::Single {
            op: crate::app::SingleOp::Delete,
            target: crate::kube::protocol::ObjectRef::new(
                crate::app::nav::rid(BuiltInKind::Pod),
                "canary-row-marker".to_string(),
                crate::kube::protocol::Namespace::Named("default".to_string()),
            ),
        },
        action_focused: false,
    })));

    // Content-sized, not a percentage of somebody's terminal.
    let ViewExtent::Dialog { height, .. } = app.nav.top().extent(t.get_frame().area()) else {
        panic!("a confirm must declare a Dialog extent");
    };
    assert_eq!(
        height, 11,
        "one message line plus its chrome — not 85% of the screen",
    );

    let painted = paint(&mut t, &mut app);
    assert!(painted.contains("Delete pod/canary-row-marker?"), "the question is drawn");
    assert!(
        painted.contains("canary-row-marker"),
        "…over the table that raised it, which stays visible:\n{painted}"
    );

    // Every key is the dialog's, globals included.
    use crossterm::event::KeyCode;
    for key in [KeyCode::Char(':'), KeyCode::Char('?'), KeyCode::Char('q')] {
        let action = crate::event::handler::handle_key_event(
            &app,
            crossterm::event::KeyEvent::new(key, crossterm::event::KeyModifiers::NONE),
        );
        assert_eq!(
            action,
            Some(crate::app::actions::Action::Cancel),
            "{key:?} must be answered by the dialog, not leak to the view under it",
        );
    }

    // …and Esc is the ordinary pop.
    assert_eq!(app.nav.depth(), depth + 1);
    app.nav.pop();
    assert_eq!(app.nav.depth(), depth, "no bespoke close path");
}

/// The container picker floats over the view that asked, and is a nav
/// element like every other dialog — one class, one modality rule, one pop.
#[test]
fn the_container_picker_is_a_floating_element() {
    use crate::app::element::ViewExtent;
    let mut app = App::new_for_test();
    app.conn.established();
    seed_row(&app, "canary-row-marker");
    let mut t = term();

    app.nav.push(Element::Picker(Box::new(crate::app::ContainerPicker {
        target: crate::kube::protocol::ObjectRef::new(
            crate::app::nav::rid(BuiltInKind::Pod),
            "canary-row-marker".to_string(),
            crate::kube::protocol::Namespace::Named("default".to_string()),
        ),
        choices: vec![
            crate::app::ContainerChoice {
                label: "web".into(),
                container: crate::kube::protocol::LogContainer::Named("web".into()),
            },
            crate::app::ContainerChoice {
                label: "sidecar".into(),
                container: crate::kube::protocol::LogContainer::Named("sidecar".into()),
            },
        ],
        selected: 0,
        action: crate::app::ContainerAction::Logs,
    })));

    // Sized from its own contents, like the confirm box — two choices plus
    // chrome, not a share of the terminal.
    assert!(matches!(
        app.nav.top().extent(t.get_frame().area()),
        ViewExtent::Dialog { height: 8, .. }
    ));

    let painted = paint(&mut t, &mut app);
    assert!(painted.contains("web") && painted.contains("sidecar"), "the choices are drawn");
    assert!(
        painted.contains("canary-row-marker"),
        "…over the table that asked, which stays visible:\n{painted}"
    );

    // Both dialogs share one class, so they share the modality rule.
    assert_eq!(app.nav.top().class(), crate::app::element::ElementClass::Dialog);
    assert!(app.nav.top().captures_all_input());
}

/// A form is a floating element like the other two dialogs — and this is the
/// test that would have caught `ui/mod.rs` losing its render arms, which is
/// how all three of them went missing at once.
#[test]
fn every_dialog_kind_paints_itself_over_its_parent() {
    let mut app = App::new_for_test();
    app.conn.established();
    seed_row(&app, "canary-row-marker");
    let target = crate::kube::protocol::ObjectRef::new(
        crate::app::nav::rid(BuiltInKind::Pod),
        "canary-row-marker".to_string(),
        crate::kube::protocol::Namespace::Named("default".to_string()),
    );

    let dialogs: Vec<(&str, Element, &str)> = vec![
        (
            "confirm",
            Element::Confirm(Box::new(crate::app::ConfirmDialog {
                message: "Delete pod/canary?".to_string(),
                action_label: "Delete".to_string(),
                pending: crate::app::PendingAction::Single {
                    op: crate::app::SingleOp::Delete,
                    target: target.clone(),
                },
                action_focused: false,
            })),
            "Delete pod/canary?",
        ),
        (
            "picker",
            Element::Picker(Box::new(crate::app::ContainerPicker {
                target: target.clone(),
                choices: vec![crate::app::ContainerChoice {
                    label: "sidecar".into(),
                    container: crate::kube::protocol::LogContainer::Named("sidecar".into()),
                }],
                selected: 0,
                action: crate::app::ContainerAction::Logs,
            })),
            "sidecar",
        ),
        (
            "form",
            Element::Form(Box::new(crate::app::FormDialog {
                submit: crate::app::FormSubmit::Scale,
                title: "Scale deploy/api".to_string(),
                subtitle: "namespace: default".to_string(),
                target: target.clone(),
                fields: vec![crate::app::FormFieldState {
                    name: "replicas".to_string(),
                    label: "Replicas".to_string(),
                    kind: crate::app::FormFieldKind::Number { min: 0, max: 100 },
                    value: crate::app::text_input::TextInput::seeded("3"),
                }],
                focused: 0,
            })),
            "Replicas",
        ),
    ];

    for (label, dialog, needle) in dialogs {
        let mut t = term();
        app.nav.push(dialog);
        assert!(
            app.nav.top().floats(),
            "{label}: must declare a Dialog extent",
        );
        assert_eq!(app.nav.top().class(), crate::app::element::ElementClass::Dialog);
        assert!(app.nav.top().captures_all_input(), "{label}: a dialog owns every key");

        let painted = paint(&mut t, &mut app);
        assert!(painted.contains(needle), "{label}: its own content is drawn:\n{painted}");
        assert!(
            painted.contains("canary-row-marker"),
            "{label}: …over the view that raised it, which stays visible:\n{painted}",
        );
        app.nav.pop();
    }
}

/// "The whole frame" means the TERMINAL, not whatever is on top of the stack.
///
/// `draw` had two rects and one name: the float path rebound `area` to the
/// dialog's box, so the `FullFrame` erase below it erased the BOX and the
/// shell's connect screen painted inside it, framed by the dialog it was
/// supposed to replace — the stale-background class the extent authority
/// exists to make impossible. Latent only because a dialog captures every
/// key, so today you cannot start a shell from under one; the next takeover
/// raised from a background event would have found it.
#[test]
fn a_full_frame_takeover_erases_the_frame_not_the_float_over_it() {
    let mut app = App::new_for_test();
    app.conn.established();
    seed_row(&app, "canary-row-marker");

    // A float on top, and a full-frame takeover raised while it is there.
    app.nav.push(Element::Confirm(Box::new(crate::app::ConfirmDialog {
        message: "Delete pod/canary?".to_string(),
        action_label: "Delete".to_string(),
        pending: crate::app::PendingAction::Single {
            op: crate::app::SingleOp::Delete,
            target: crate::kube::protocol::ObjectRef::new(
                crate::app::nav::rid(BuiltInKind::Pod),
                "canary-row-marker".to_string(),
                crate::kube::protocol::Namespace::Named("default".to_string()),
            ),
        },
        action_focused: false,
    })));
    app.ui.open(crate::app::Modal::Takeover(crate::app::TakeoverOp::Shell(Box::new(
        crate::app::ShellState {
            title: "shell-takeover-marker".to_string(),
            stream: None,
            connect_state: crate::app::ShellConnectState::Connecting,
            pending_output: Vec::new(),
        },
    ))));
    assert_eq!(
        app.ui.takeover().map(|op| op.extent(term().get_frame().area())),
        Some(ViewExtent::Full),
        "the shell connect screen claims the frame",
    );

    let mut t = term();
    let painted = paint(&mut t, &mut app);

    assert!(
        painted.contains("shell-takeover-marker"),
        "the takeover paints:\n{painted}",
    );
    assert!(
        !painted.contains("canary-row-marker"),
        "nothing of the view beneath survives a full-frame erase:\n{painted}",
    );
    assert!(
        !painted.contains("Delete pod/canary?"),
        "nor the dialog that happened to be on top when it was raised:\n{painted}",
    );
}

/// The column you sorted by stops repeating itself.
///
/// Help is three columns and thirteen of its rows said "Everywhere" down the
/// left edge — the value you are grouped BY, printed once per row. Collapsing
/// a repeat is a property of the table, not of help: it holds for a pod list
/// sorted by NAMESPACE too, and it is render-only, so copy and save still
/// carry the row's substance.
///
/// Three conditions are pinned here because each one is load-bearing: only
/// the SORTED column (equal values are contiguous only there), never the top
/// VISIBLE row (so the screen always carries the group it is in), and never a
/// cell the search matched (collapsing matches would read as "only one row
/// matched").
#[test]
fn a_sorted_column_stops_repeating_itself() {
    use crate::ui::widgets::{ResourceTable, ResourceTableState};
    use ratatui::buffer::Buffer;
    use ratatui::layout::Rect;
    use ratatui::widgets::StatefulWidget;

    let theme = crate::ui::theme::Theme::dark();
    let rows: Vec<Vec<String>> = [
        ["All", "?", "aaa"],
        ["All", ":", "bbb"],
        ["All", "R", "ccc"],
        ["View", "d", "ddd"],
    ]
    .iter()
    .map(|r| r.iter().map(|c| c.to_string()).collect())
    .collect();
    let headers = vec!["SEC", "KEY", "DESC"];
    // The widget takes measured widths (the memoized `PreparedView` carries
    // them in the real path). Deliberately generous — nothing here is a claim
    // about layout, the columns just have to be wide enough to read.
    let widths: Vec<u16> = vec![12, 12, 12];

    let paint = |sort: Option<usize>, offset: usize, pats: &[crate::util::SearchPattern]| -> Vec<String> {
        let area = Rect::new(0, 0, 60, 10);
        let mut buf = Buffer::empty(area);
        let mut st = ResourceTableState {
            selected: usize::MAX,
            offset,
            selected_col: 0,
            col_offset: 0,
            filtered_count: 0,
            visible_height: 0,
        };
        ResourceTable::new(headers.clone(), &rows, "help", &theme)
            .sort(sort, true)
            .col_widths(&widths)
            .search_patterns(pats)
            .render(area, &mut buf, &mut st);
        (0..area.height)
            .map(|y| (0..area.width).map(|x| buf[(x, y)].symbol()).collect::<String>())
            .collect()
    };

    // Sorted on SECTION: the run collapses after its first row.
    let shown = paint(Some(0), 0, &[]);
    let section_rows: Vec<&String> = shown
        .iter()
        .filter(|l| l.contains("aaa") || l.contains("bbb") || l.contains("ccc"))
        .collect();
    assert_eq!(section_rows.len(), 3, "all three rows drew:\n{shown:#?}");
    assert!(section_rows[0].contains("All"), "the first carries the group:\n{shown:#?}");
    assert!(
        !section_rows[1].contains("All") && !section_rows[2].contains("All"),
        "the rest do not repeat it:\n{shown:#?}",
    );
    // The group ENDS, and the next value still prints.
    assert!(
        shown.iter().any(|l| l.contains("View") && l.contains("ddd")),
        "a different value is never suppressed:\n{shown:#?}",
    );

    // NOT sorted on SECTION: equal neighbours are a coincidence, so nothing
    // is collapsed.
    let unsorted = paint(Some(1), 0, &[]);
    assert_eq!(
        unsorted.iter().filter(|l| l.contains("All")).count(),
        3,
        "sorted on KEY, the SECTION column prints in full:\n{unsorted:#?}",
    );

    // No sort column visible at all (`display_sort_col` is an Option because
    // the sorted column can be hidden): nothing is contiguous by assumption,
    // so nothing collapses.
    let no_sort = paint(None, 0, &[]);
    assert_eq!(
        no_sort.iter().filter(|l| l.contains("All")).count(),
        3,
        "with no sorted column there is no grouping to collapse:\n{no_sort:#?}",
    );

    // Scrolled: the top visible row carries its group even though the row
    // above it (off screen) has the same value.
    let scrolled = paint(Some(0), 1, &[]);
    assert!(
        scrolled.iter().any(|l| l.contains("All") && l.contains("bbb")),
        "the first visible row is never suppressed:\n{scrolled:#?}",
    );

    // Searched: every matching cell shows, or the highlight would claim one
    // row matched when three did.
    let searched = paint(Some(0), 0, &[crate::util::SearchPattern::new("All")]);
    assert_eq!(
        searched.iter().filter(|l| l.contains("All")).count(),
        3,
        "a matched cell always draws:\n{searched:#?}",
    );
}

/// The aliases view actually PAINTS.
///
/// Its rows, its element class and its routing all have tests; none of them
/// would notice if the renderer never drew it. That is exactly how five
/// render functions once went missing from `ui/mod.rs` with the suite still
/// green — nothing painted a dialog. So: paint it, and read the cells back
/// off the screen.
#[test]
fn the_aliases_view_paints_what_the_prompt_accepts() {
    let mut app = App::new_for_test();
    app.conn.established();

    let rid = crate::kube::local::LocalResourceKind::Alias.to_resource_id();
    app.nav.reset(
        app.core
            .client_root_element(&app.kube.metrics, &rid, crate::kube::protocol::Namespace::All)
            .expect("aliases is client-owned"),
    );

    let mut t = term();
    let painted = paint(&mut t, &mut app);

    assert!(painted.contains("ALIAS"), "the table header is drawn:\n{painted}");
    assert!(painted.contains("TARGET"), "…including the last column:\n{painted}");
    // A row from each section, so neither projection is silently empty.
    assert!(
        painted.contains("resource"),
        "the resource aliases are there:\n{painted}",
    );
    assert!(
        painted.contains("command"),
        "…and the command words:\n{painted}",
    );
    // And it is a NAVIGABLE table, not a document: `/` and sort belong to it
    // because it is an ordinary element.
    assert_eq!(app.nav.top().class(), crate::app::element::ElementClass::Table);
    assert!(!app.nav.top().markable(), "nothing to batch on a reference row");
}

/// The log view's hint bar reaches `back` on a real terminal.
///
/// The reported symptom: at ~97 columns the bar silently lost its tail, and
/// the tail ended in `<q> back`. Two fixes answered it — grouping the seven
/// `since` keys into `<0-6> range`, and giving the renderer the width — but
/// NOTHING paints the bar, so neither would notice if the view handed it the
/// wrong rect. It passes `bar_area.width` for the rect it renders into; this
/// is the test that says so.
#[test]
fn the_log_hint_bar_reaches_its_last_hint_on_screen() {
    use ratatui::backend::TestBackend;
    use ratatui::Terminal;

    for width in [97u16, 80, 100] {
        let mut app = App::new_for_test();
        app.conn.established();
        app.nav.push(Element::LogSession(Box::new(LogSession::for_test(
            crate::kube::protocol::LogTarget::One("canarypod".to_string()),
            LogContainer::Named("app".to_string()),
        ))));

        let mut t = Terminal::new(TestBackend::new(width, 24)).expect("test terminal");
        let painted = paint(&mut t, &mut app);

        assert!(painted.contains("follow"), "w={width}: the bar is drawn:\n{painted}");
        assert!(
            painted.contains("range"),
            "w={width}: the since keys are ONE hint, not seven:\n{painted}",
        );
        assert!(
            painted.contains("back"),
            "w={width}: the last hint is the way out — it must be on screen:\n{painted}",
        );
        assert!(
            !painted.contains('\u{2026}'),
            "w={width}: and nothing had to be dropped to get there:\n{painted}",
        );
    }
}

/// A dialog's box is the SMALLER of what it needs and what it allows itself.
///
/// Help was the one dialog sized by a flat percentage, and `ViewExtent`'s own
/// doc claimed boxes were "shrunk to whatever the content actually needs".
/// Nothing shrank anything. Its 105-column table was therefore clipped by 27
/// columns on a 100-wide terminal and floating in 53 columns of DEAD SPACE on
/// a 200-wide one — both halves of one unimplemented sentence, and the second
/// is the "unused column on the right edge" that went unreproduced for weeks
/// because 100×30 is on the clipped side of the crossover.
#[test]
fn a_dialog_fits_its_content_at_every_terminal_width() {
    use ratatui::backend::TestBackend;
    use ratatui::Terminal;

    let natural = {
        let mut app = App::new_for_test();
        push_help(&mut app);
        // Measured the way the renderer measures: the memo's own widths.
        let mut t = Terminal::new(TestBackend::new(400, 120)).expect("wide terminal");
        let _ = paint(&mut t, &mut app);
        let crate::app::element::ViewExtent::Dialog { width, .. } =
            app.nav.top().extent(ratatui::layout::Rect::new(0, 0, 400, 120))
        else {
            panic!("help floats")
        };
        width
    };
    assert!(
        (60..200).contains(&natural),
        "sanity: help's table is a few dozen columns wide, got {natural}",
    );

    for (term_w, term_h) in [(80u16, 24u16), (100, 30), (160, 50), (240, 80)] {
        let mut app = App::new_for_test();
        push_help(&mut app);
        let mut t = Terminal::new(TestBackend::new(term_w, term_h)).expect("terminal");
        let _ = paint(&mut t, &mut app);

        let frame = ratatui::layout::Rect::new(0, 0, term_w, term_h);
        let crate::app::element::ViewExtent::Dialog { width, .. } = app.nav.top().extent(frame)
        else {
            panic!("help floats")
        };
        let allowance = term_w * 80 / 100;

        // NEVER wider than the content needs — that is the dead space.
        assert!(
            width <= natural,
            "{term_w}x{term_h}: box {width} exceeds the content's {natural} — dead space",
        );
        // NEVER wider than it allows itself — it floats OVER something.
        assert!(
            width <= allowance,
            "{term_w}x{term_h}: box {width} exceeds its {allowance}-column allowance",
        );
        // And it takes everything it can, so a wide terminal is not wasted.
        assert_eq!(
            width,
            natural.min(allowance),
            "{term_w}x{term_h}: the box is exactly min(need, allowance)",
        );
    }
}

/// A table that cannot show all its columns says so.
///
/// The reported symptom was "AGE doesn't always appear". It always appeared —
/// a pod table with metrics is 200 columns wide, so at 97 you see seven of
/// fourteen and AGE, being LAST, is a hundred columns past the right edge.
/// Nothing said so, which is why it read as a missing field; and `Ctrl-R`
/// could never help, because it is a fact about the window, not the data.
///
/// The count goes in the TITLE, beside the row count, because the title sits
/// on the border and cannot be scrolled past.
#[test]
fn a_table_says_when_its_columns_do_not_fit() {
    use ratatui::backend::TestBackend;
    use ratatui::Terminal;

    let mut app = App::new_for_test();
    app.conn.established();
    // Three columns whose natural widths overflow a 60-column frame, the way
    // a real pod table does at 200: the NODE column alone is 41 of them.
    let store = app.nav.top().data_store().expect("root is a table");
    store.apply(
        1,
        StorePayload::Baseline(TableBaseline {
            resource: crate::app::nav::rid(BuiltInKind::Pod),
            headers: vec!["NAME".into(), "NODE".into(), "AGE".into()],
            rows: vec![ResourceRow {
                name: "canary-row-marker".into(),
                namespace: Some("default".into()),
                cells: vec![
                    CellValue::Text("canary-row-marker".to_string()),
                    CellValue::Text("ip-10-1-133-34.us-west-2.compute.internal".to_string()),
                    CellValue::Text("4m28s".to_string()),
                ],
                ..Default::default()
            }],
        }),
    );

    // Narrow: the columns cannot all fit, so the title reports the shortfall
    // and names a direction, because the column cursor is how you get there.
    let mut narrow = Terminal::new(TestBackend::new(60, 12)).expect("terminal");
    let painted = paint(&mut narrow, &mut app);
    assert!(
        painted.contains('\u{2192}'),
        "a clipped table must say so, with a direction:\n{painted}",
    );
    let digits: Vec<usize> = painted[painted.find("cols").map(|i| i.saturating_sub(6)).unwrap_or(0)..]
        .split(|c: char| !c.is_ascii_digit())
        .filter(|s| !s.is_empty())
        .map(|s| s.parse().unwrap())
        .take(2)
        .collect();
    assert_eq!(digits.len(), 2, "the count is two numbers:\n{painted}");
    let (shown, total) = (digits[0], digits[1]);
    assert_eq!(total, 3, "three columns in all");
    assert!(shown < total, "it reports {shown} of {total} — fewer than all");
    assert!(shown >= 1, "and at least one column is drawn");
    // AGE really is the one you cannot see, which is the reported symptom.
    assert!(
        !painted.contains("4m28s"),
        "the last column is off screen — that IS the complaint:\n{painted}",
    );

    // Wide enough for everything: silence. An indicator that is always on
    // is not an indicator.
    let mut wide = Terminal::new(TestBackend::new(300, 12)).expect("terminal");
    let painted = paint(&mut wide, &mut app);
    assert!(
        !painted.contains("cols"),
        "nothing is clipped, so nothing is claimed:\n{painted}",
    );
}

/// A bar or a message that does not fit says so, rather than losing its end.
///
/// Three surfaces made the same mistake in three different ways, and only
/// one of them had been fixed: the hint bar fitted itself, the log STATUS bar
/// did not (one `Line::from(spans)` with no width — 76 columns with every
/// field shown), and a flash wider than the line was CENTRED, which put it at
/// the left edge and clipped its tail in silence. The tail of an error is
/// usually the part that names the cause.
#[test]
fn a_one_line_surface_that_overflows_marks_the_cut() {
    use ratatui::backend::TestBackend;
    use ratatui::Terminal;

    // --- The log status bar, at a width its own indicators exceed. ---
    for width in [40u16, 60, 100] {
        let mut app = App::new_for_test();
        app.conn.established();
        app.nav.push(Element::LogSession(Box::new(LogSession::for_test(
            crate::kube::protocol::LogTarget::One("canarypod".to_string()),
            LogContainer::Named("app".to_string()),
        ))));
        let mut t = Terminal::new(TestBackend::new(width, 20)).expect("terminal");
        let painted = paint(&mut t, &mut app);
        // Never a half-written indicator: either a group is whole or absent.
        for label in ["AutoScroll:", "Wrap:", "Fields:", "Since:"] {
            if let Some(at) = painted.find(label) {
                let rest = &painted[at + label.len()..];
                assert!(
                    rest.starts_with(|c: char| c.is_alphanumeric()),
                    "w={width}: `{label}` is drawn without its value:\n{painted}",
                );
            }
        }
        assert!(
            painted.contains("AutoScroll:"),
            "w={width}: the first indicator always fits:\n{painted}",
        );
    }

    // --- A flash longer than the line. ---
    let mut app = App::new_for_test();
    app.conn.established();
    let long = "a".repeat(400);
    app.ui.flash = Some(crate::app::FlashMessage::error(long));
    let mut t = Terminal::new(TestBackend::new(60, 12)).expect("terminal");
    let painted = paint(&mut t, &mut app);
    assert!(
        painted.contains('\u{2026}'),
        "an over-long flash is visibly cut, not silently clipped:\n{painted}",
    );

    // …and one that fits is untouched, centred as before.
    let mut app = App::new_for_test();
    app.conn.established();
    app.ui.flash = Some(crate::app::FlashMessage::info("short and sweet".to_string()));
    let mut t = Terminal::new(TestBackend::new(60, 12)).expect("terminal");
    let painted = paint(&mut t, &mut app);
    assert!(painted.contains("short and sweet"), "a fitting flash is whole:\n{painted}");
    assert!(!painted.contains('\u{2026}'), "and unmarked:\n{painted}");
}

/// The title gives up its cheapest parts first, and never the warning.
///
/// It is the one line reporting what you cannot otherwise see, it lives in
/// the top border where ratatui truncates in silence, and I had just made it
/// LONGER by adding the column count. Dropping from the right would sacrifice
/// the liveness warning first — the single thing here you must not miss.
#[test]
fn a_crowded_title_keeps_the_warning_and_drops_the_hint() {
    use ratatui::backend::TestBackend;
    use ratatui::Terminal;

    let mut app = App::new_for_test();
    app.conn.established();
    // Enough columns to be clipped at any width, so the hint is on offer.
    let store = app.nav.top().data_store().expect("root is a table");
    store.apply(
        1,
        StorePayload::Baseline(TableBaseline {
            resource: crate::app::nav::rid(BuiltInKind::Pod),
            // Six node-name columns: ~270 wide, so clipped at every width a
            // terminal actually has — the pod table's real shape.
            headers: vec![
                "NAME".into(),
                "NODE1".into(),
                "NODE2".into(),
                "NODE3".into(),
                "NODE4".into(),
                "NODE5".into(),
                "AGE".into(),
            ],
            rows: vec![ResourceRow {
                name: "canary".into(),
                namespace: Some("default".into()),
                cells: vec![
                    CellValue::Text("canary".to_string()),
                    CellValue::Text("ip-10-1-133-34.us-west-2.compute.internal".to_string()),
                    CellValue::Text("ip-10-1-133-35.us-west-2.compute.internal".to_string()),
                    CellValue::Text("ip-10-1-133-36.us-west-2.compute.internal".to_string()),
                    CellValue::Text("ip-10-1-133-37.us-west-2.compute.internal".to_string()),
                    CellValue::Text("ip-10-1-133-38.us-west-2.compute.internal".to_string()),
                    CellValue::Text("4m".to_string()),
                ],
                ..Default::default()
            }],
        }),
    );
    // …and a staleness reason, which is the part that must survive.
    store.mark_stale("watch closed by apiserver");

    // Wide: everything is affordable, so everything is there.
    let mut wide = Terminal::new(TestBackend::new(200, 12)).expect("terminal");
    let painted = paint(&mut wide, &mut app);
    assert!(painted.contains("cols"), "the hint fits here:\n{painted}");
    assert!(painted.contains("watch closed"), "and so does the warning:\n{painted}");

    // Narrow: the hint is the first thing surrendered; the warning is not.
    let mut narrow = Terminal::new(TestBackend::new(46, 12)).expect("terminal");
    let painted = paint(&mut narrow, &mut app);
    assert!(
        painted.contains("watch closed"),
        "the warning is mandatory — stale rows must never read as live:\n{painted}",
    );
    assert!(
        !painted.contains("cols"),
        "…and the column hint is what pays for it, since the arrows still \
         reach those columns:\n{painted}",
    );
    // The view's name survives too: you have to know where you are.
    assert!(painted.contains("pods"), "the name is mandatory:\n{painted}");
}

/// A table with more rows than fit says WHICH rows, not just how many.
///
/// The log, describe and yaml views all have a scrollbar; the table had only
/// a total, so a 1200-row list gave no sense of position at all. It gets the
/// position in its TITLE rather than a scrollbar, because every one of those
/// other views reserves a column for the bar unconditionally and this is the
/// one view with no column to spare — a pod table with metrics is already 200
/// columns against a 97-column terminal.
#[test]
fn a_table_longer_than_its_window_says_where_you_are() {
    use ratatui::backend::TestBackend;
    use ratatui::Terminal;

    let mut app = App::new_for_test();
    app.conn.established();
    let store = app.nav.top().data_store().expect("root is a table");
    let rows: Vec<ResourceRow> = (0..200)
        .map(|i| ResourceRow {
            name: format!("pod-{i:03}"),
            namespace: Some("default".into()),
            cells: vec![
                CellValue::Text(format!("pod-{i:03}")),
                CellValue::Text("Running".to_string()),
            ],
            ..Default::default()
        })
        .collect();
    store.apply(
        1,
        StorePayload::Baseline(TableBaseline {
            resource: crate::app::nav::rid(BuiltInKind::Pod),
            headers: vec!["NAME".into(), "STATUS".into()],
            rows,
        }),
    );

    let mut t = Terminal::new(TestBackend::new(60, 12)).expect("terminal");
    let painted = paint(&mut t, &mut app);
    assert!(
        painted.contains("/200]"),
        "the total is still there, with a position:\n{painted}",
    );
    assert!(
        painted.contains("[1-"),
        "at the top, the window starts at row 1 (1-based, as a human reads):\n{painted}",
    );

    // Move the cursor past the window and the position follows it.
    for _ in 0..40 {
        app.nav.top_mut().select_next();
    }
    let painted = paint(&mut t, &mut app);
    assert!(
        !painted.contains("[1-"),
        "scrolled down, the window no longer starts at the first row:\n{painted}",
    );
    assert!(painted.contains("/200]"), "and the total does not change:\n{painted}");

    // A table that fits says only the count — a position would be noise.
    let mut app = App::new_for_test();
    app.conn.established();
    seed_row(&app, "only-row");
    let painted = paint(&mut t, &mut app);
    assert!(painted.contains("[1]"), "one row, one number:\n{painted}");
    assert!(!painted.contains("1-1/"), "no position when there is nowhere to be:\n{painted}");
}

/// A deep breadcrumb keeps its TAIL, and never hides under the help hint.
///
/// Every other bar on screen drops from the right; this one must not. The
/// last crumb is where you ARE — the first is a resource name you typed
/// yourself. And the ` ? help ` hint used to be stamped at a fixed right
/// position AFTER an unbounded breadcrumb line, so a long path lost its tail
/// underneath it with nothing to say so.
#[test]
fn a_deep_breadcrumb_keeps_its_tail_and_clears_the_hint() {
    use ratatui::backend::TestBackend;
    use ratatui::Terminal;

    let mut app = App::new_for_test();
    app.conn.established();
    seed_row(&app, "a-pod-with-quite-a-long-name");
    // Drill: each level lengthens the path from the left.
    for pat in ["a", "pod", "with", "quite", "long", "name"] {
        let predicate = crate::app::store::RowPredicate::Grep(
            crate::app::nav::CompiledGrep::new(pat),
        );
        if let Ok(el) = crate::app::element::Element::derive_filter(app.nav.top(), predicate) {
            app.nav.push(el);
        }
    }
    let path = app.nav.breadcrumb();
    assert!(path.len() > 40, "the fixture really is a long path: {path:?}");

    let mut t = Terminal::new(TestBackend::new(60, 12)).expect("terminal");
    let painted = paint(&mut t, &mut app);

    // The LAST crumb is on screen…
    let last = path.rsplit(" > ").next().expect("a last crumb");
    assert!(
        painted.contains(last),
        "the tail is where you are, so it must survive: {last:?}\n{painted}",
    );
    // …the cut is marked…
    assert!(painted.contains('\u{2026}'), "and the elision is visible:\n{painted}");
    // …and the hint is intact rather than overwritten, or overwriting.
    assert!(painted.contains("? help"), "the hint is whole:\n{painted}");

    // Wide enough: nothing elided, whole path shown.
    let mut wide = Terminal::new(TestBackend::new(200, 12)).expect("terminal");
    let painted = paint(&mut wide, &mut app);
    assert!(painted.contains(&path), "all of it fits here:\n{painted}");
}

/// What you are TYPING is always on screen, caret included.
///
/// Five render sites guarded the caret with `if cursor_x < right_edge`, so
/// the moment the text outgrew its bar the text clipped AND the caret stopped
/// being drawn: you were typing blind into your own `:` prompt or grep. The
/// guard looked defensive and was the bug.
#[test]
fn a_long_input_keeps_its_caret_and_its_tail_on_screen() {
    use ratatui::backend::TestBackend;
    use ratatui::Terminal;

    // The `:` prompt. The TAIL is what you just typed, so that is what has
    // to be visible — the head is a resource name you already know.
    let mut app = App::new_for_test();
    app.conn.established();
    seed_row(&app, "a-row");
    let typed = "clickhouseinstallations some-extremely-long-namespace-name-here";
    app.ui.open(crate::app::Modal::Command {
        input: crate::app::text_input::TextInput::seeded(typed),
        history_index: None,
    });

    let mut t = Terminal::new(TestBackend::new(40, 16)).expect("terminal");
    let painted = paint(&mut t, &mut app);
    let tail = &typed[typed.len() - 12..];
    assert!(
        painted.contains(tail),
        "the end of what you typed must be visible: {tail:?}\n{painted}",
    );
    // And the caret is actually placed, which is what used to be skipped.
    // Read off the backend, which records where the frame put it.
    let caret = t.get_cursor_position().expect("the backend tracks the caret");
    assert!(
        caret.x < 40,
        "the caret must land INSIDE the terminal, not past its right edge: {caret:?}",
    );

    // The log grep draft, behind a committed chain that pushes it right.
    let mut app = App::new_for_test();
    app.conn.established();
    app.nav.push(Element::LogSession(Box::new(LogSession::for_test(
        crate::kube::protocol::LogTarget::One("canarypod".to_string()),
        LogContainer::Named("app".to_string()),
    ))));
    for pat in ["alpha", "bravo", "charlie"] {
        if let Ok(el) = crate::app::element::Element::derive_log_filter(
            app.nav.top(),
            crate::app::nav::CompiledGrep::new(pat),
        ) {
            app.nav.push(el);
        }
    }
    let draft = "a-very-long-pattern-being-typed-right-now";
    // The log bar reads the element's own `draft`, not the table filter.
    app.nav.top_mut().log_view_mut().expect("a log element").draft =
        Some(crate::app::text_input::TextInput::seeded(draft));
    let mut t = Terminal::new(TestBackend::new(60, 16)).expect("terminal");
    let painted = paint(&mut t, &mut app);
    let tail = &draft[draft.len() - 10..];
    assert!(
        painted.contains(tail),
        "the draft's tail survives a committed chain pushing it right: {tail:?}\n{painted}",
    );
    assert!(
        painted.contains('\u{2588}'),
        "and its caret block is drawn:\n{painted}",
    );
}

/// The cluster header keeps the identifying END of each identifier.
///
/// `aws eks update-kubeconfig` without `--alias` writes a context of
/// `arn:aws:eks:us-west-2:123456789012:cluster/prod-usw2`. Every column
/// before `prod-usw2` is boilerplate shared with every other context you
/// have, so clipping on the right — which the `Paragraph` did — made the four
/// header lines read as four copies of the same ARN prefix.
#[test]
fn the_header_keeps_the_telling_end_of_an_arn() {
    use ratatui::backend::TestBackend;
    use ratatui::Terminal;

    let mut app = App::new_for_test();
    let arn = "arn:aws:eks:us-west-2:123456789012:cluster/prod-usw2";
    app.kube.context = crate::kube::protocol::ContextName::new(arn);
    app.conn.established();

    // Narrow enough that the ARN cannot fit beside the logo.
    let mut t = Terminal::new(TestBackend::new(80, 16)).expect("terminal");
    let painted = paint(&mut t, &mut app);
    assert!(
        painted.contains("prod-usw2"),
        "the cluster you are ON is the end of the ARN, so it must survive:\n{painted}",
    );
    assert!(painted.contains('\u{2026}'), "and the elision is marked:\n{painted}");

    // Wide: no elision, the whole thing.
    let mut wide = Terminal::new(TestBackend::new(200, 16)).expect("terminal");
    let painted = paint(&mut wide, &mut app);
    assert!(painted.contains(arn), "all of it fits here:\n{painted}");
}

/// A dialog's box and its chrome agree about the margin, because there is one.
///
/// `Element::extent` added a bare `+ 4` / `+ 2` in three match arms and
/// `ModalOverlay::render` subtracted `saturating_sub(4)` / `(2)` in another
/// file. The two cancelling was the only thing keeping the dialogs the right
/// size, and nothing said so — changing one would have silently mis-sized all
/// three. Pinned as the RELATIONSHIP, so the numbers cannot drift apart.
#[test]
fn a_dialogs_box_reserves_exactly_the_chrome_its_widget_spends() {
    let (mw, mh) = crate::ui::widgets::DIALOG_MARGIN;
    let mut app = App::new_for_test();
    app.conn.established();
    seed_row(&app, "canary-row-marker");
    let target = crate::kube::protocol::ObjectRef::new(
        crate::app::nav::rid(BuiltInKind::Pod),
        "canary-row-marker".to_string(),
        crate::kube::protocol::Namespace::Named("default".to_string()),
    );

    app.nav.push(Element::Confirm(Box::new(crate::app::ConfirmDialog {
        message: "Delete pod/canary-row-marker?".to_string(),
        action_label: "Delete".to_string(),
        pending: crate::app::PendingAction::Single {
            op: crate::app::SingleOp::Delete,
            target,
        },
        action_focused: false,
    })));

    let crate::app::element::ViewExtent::Dialog { width, height } =
        app.nav.top().extent(ratatui::layout::Rect::new(0, 0, 200, 60))
    else {
        panic!("a confirm floats")
    };
    let crate::app::element::Element::Confirm(d) = app.nav.top() else { panic!() };
    let (cw, ch) = crate::ui::widgets::confirm_dialog_size(d);
    assert_eq!(
        (width, height),
        (cw + mw, ch + mh),
        "the box is the widget's content plus exactly the chrome it spends",
    );

    // And it really draws: the box is big enough for the message after the
    // widget has taken its margin back out.
    let mut t = term();
    let painted = paint(&mut t, &mut app);
    assert!(
        painted.contains("Delete pod/canary-row-marker?"),
        "the message fits the box its own size asked for:\n{painted}",
    );
}
