use crate::app::element::{Element, LogSession};
use crate::app::store::StorePayload;
use crate::app::{App, Overlay, OverlayExtent, ShellState};
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
    app.ui.open(crate::app::Modal::Overlay(Overlay::ContainerSelect {
            target: crate::kube::protocol::ObjectRef::new(
                crate::app::nav::rid(BuiltInKind::Pod),
                "p".to_string(),
                crate::kube::protocol::Namespace::Named("default".to_string()),
            ),
            choices: vec![crate::app::ContainerChoice {
                label: "web".into(),
                container: crate::kube::protocol::LogContainer::Named("web".into()),
            }],
            selected: 0,
            action: crate::app::ContainerAction::Logs,
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
    app.ui.open(crate::app::Modal::Overlay(Overlay::ContainerSelect {
            target: crate::kube::protocol::ObjectRef::new(
                crate::app::nav::rid(BuiltInKind::Pod),
                "p".to_string(),
                crate::kube::protocol::Namespace::Named("default".to_string()),
            ),
            choices: vec![crate::app::ContainerChoice {
                label: "web".into(),
                container: crate::kube::protocol::LogContainer::Named("web".into()),
            }],
            selected: 0,
            action: crate::app::ContainerAction::Logs,
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
        let store = app.core.seed_keybindings(caps, app.config.keys, class);
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
        Some("zzz-no-such-line".to_string());
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
    app.nav.top_mut().log_view_mut().unwrap().draft = Some(r"\zsaab".to_string());
    let painted = paint(&mut term(), &mut app);
    assert!(painted.contains("literal"), "the bar must name the degradation:\n{painted}");
    assert!(painted.contains("zs"), "and say which atom caused it:\n{painted}");

    // A pattern that DOES compile keeps the ordinary count.
    app.nav.top_mut().log_view_mut().unwrap().draft = Some(r"aa\|xy".to_string());
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
    app.nav.top_mut().log_view_mut().unwrap().draft = Some("client".to_string());
    let painted = paint(&mut t, &mut app);
    assert!(!painted.contains("case-sensitive"), "no capital, nothing to say:\n{painted}");

    // One capital flips it — and the bar now says why the view emptied.
    app.nav.top_mut().log_view_mut().unwrap().draft = Some("Client".to_string());
    let painted = paint(&mut t, &mut app);
    assert!(
        painted.contains("case-sensitive"),
        "a capital turned smartcase off and the bar must say so:\n{painted}"
    );
    assert!(painted.contains("No matching lines."), "…which is why there are none");
}
