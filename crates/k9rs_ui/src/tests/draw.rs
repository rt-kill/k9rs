use k9rs_app::{Element, LogSession};
use k9rs_app::StorePayload;
use k9rs_app::ViewExtent;
use k9rs_app::{App, ShellState, TakeoverOp};
use k9rs_proto::protocol::{LogContainer, LogLine, TableBaseline};
use k9rs_proto::resource_def::BuiltInKind;
use k9rs_proto::row::{CellValue, ResourceRow};
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
    term.draw(|f| crate::draw(f, app)).expect("draw");
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
    let class = k9rs_app::ViewClass::of(app.nav.top());
    let caps = Some(app.current_capabilities());
    let markable = app.nav.top().markable();
    let store = app.core.seed_keybindings(caps, app.config.keys, class, markable);
    let rid = k9rs_proto::local_types::LocalResourceKind::Keybinding.to_resource_id();
    app.nav.push(Element::ResourceList(k9rs_app::ResourceList::client(
        k9rs_app::QuerySpec {
            rid,
            namespace: k9rs_proto::protocol::Namespace::All,
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
        connect_state: k9rs_app::ShellConnectState::Connecting,
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

    app.ui.open(k9rs_app::Modal::Takeover(shell_overlay()));
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
    app.ui.open(k9rs_app::Modal::Takeover(TakeoverOp::Edit {
            target: k9rs_proto::protocol::ObjectRef::new(
                k9rs_proto::resource_def::BuiltInKind::Pod.to_resource_id(),
                "p".to_string(),
                k9rs_proto::protocol::Namespace::Named("default".to_string()),
            ),
            state: k9rs_app::EditState::AwaitingYaml,
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
            target: k9rs_proto::protocol::ObjectRef {
                resource: BuiltInKind::Pod.to_resource_id(),
                namespace: k9rs_proto::protocol::Namespace::Named("default".into()),
                name: "p".into(),
            },
            state: k9rs_app::EditState::AwaitingYaml,
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
    let session = LogSession::for_test(k9rs_proto::protocol::LogTarget::one("canarypod".to_string()), LogContainer::Named("app".to_string(),
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
    let session = LogSession::for_test(k9rs_proto::protocol::LogTarget::one("canarypod".to_string()), LogContainer::Named("app".to_string(),
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
    k9rs_app::apply_event(&mut app, k9rs_app::AppEvent::KubeconfigLoaded {
        contexts: vec![
            k9rs_app::KubeContext {
                name: k9rs_proto::protocol::ContextName::new("canary-ctx").unwrap(),
                identity: k9rs_proto::protocol::ClusterIdentity::new(
                    "canary-cluster".into(), "canary-user".into(),
                ),
                is_current: true,
            },
        ],
        current_context: None,
        current_identity: k9rs_proto::protocol::ClusterIdentity::default(),
    });
    k9rs_app::apply_event(&mut app, k9rs_app::AppEvent::NoContextConfigured);

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
    k9rs_app::apply_event(&mut app, k9rs_app::AppEvent::KubeconfigLoaded {
        contexts: vec![k9rs_app::KubeContext {
            name: k9rs_proto::protocol::ContextName::new("userless-ctx").unwrap(),
            // Exactly what kube-rs hands back for `user:`-less entry:
            // `Option<String>::None`, which the boundary unwraps to "".
            identity: k9rs_proto::protocol::ClusterIdentity::new(
                "canary-cluster".into(),
                String::new(),
            ),
            is_current: true,
        }],
        current_context: None,
        current_identity: k9rs_proto::protocol::ClusterIdentity::default(),
    });
    k9rs_app::apply_event(&mut app, k9rs_app::AppEvent::NoContextConfigured);

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
        let view = app.nav.top_mut().view(k9rs_app::ColumnLevel::Default, 200);
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
    use k9rs_proto::protocol::ContextName;
    use k9rs_proto::resource_def::BuiltInKind;
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
    app.nav.reset(Element::Overview(k9rs_app::Overview));
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
    app.ui.theme = k9rs_theme::Theme::light();
    app.ui.open(k9rs_app::Modal::Takeover(TakeoverOp::Edit {
            target: k9rs_proto::protocol::ObjectRef::new(
                k9rs_proto::resource_def::BuiltInKind::Pod.to_resource_id(),
                "p".to_string(),
                k9rs_proto::protocol::Namespace::Named("default".to_string()),
            ),
            state: k9rs_app::EditState::AwaitingYaml,
        }));

    let mut t = term();
    paint(&mut t, &mut app);
    let fill = k9rs_theme::Theme::light().dialog_fill;
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
    let session = LogSession::for_test(k9rs_proto::protocol::LogTarget::one("canarypod".to_string()), LogContainer::Named("app".to_string(),
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
        Some(k9rs_app::TextInput::seeded("zzz-no-such-line"));

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
        k9rs_util::CompiledGrep::new("zzz-no-such-line"),
            k9rs_app::Sense::Keep,
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
        Element::derive_log_filter(&session, k9rs_util::CompiledGrep::new("keep"), k9rs_app::Sense::Keep)
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
    /// Every row of the sheet, both levels — reached the way Enter reaches
    /// them: walk the INDEX, and pull each section by the title its row
    /// carries. So this also drives the capture path end to end (an index
    /// row names a section; `keybinding_section` has it).
    fn help_rows(app: &mut App) -> Vec<String> {
        let class = k9rs_app::ViewClass::of(app.nav.top());
        let caps = Some(app.current_capabilities());
        let index = app.core.seed_keybindings(
            caps,
            app.config.keys,
            class,
            app.nav.top().markable(),
        );
        // `RowStore` is not in `k9rs_app`'s facade, so it cannot be NAMED
        // here — only used. Hence `with_read` inline rather than a helper
        // taking one.
        let index_rows: Vec<k9rs_proto::row::ResourceRow> =
            index.with_read(|i| i.rows.clone());
        let join = |rows: &[k9rs_proto::row::ResourceRow]| -> Vec<String> {
            rows.iter()
                .map(|r| r.cells.iter().map(|c| c.to_string()).collect::<Vec<_>>().join(" | "))
                .collect()
        };
        let mut out = join(&index_rows);
        for row in &index_rows {
            let title = match &row.cells[0] {
                k9rs_proto::row::CellValue::Text(t) => t.clone(),
                other => panic!("SECTION is text, got {other:?}"),
            };
            let section = app
                .core
                .keybinding_section(&title)
                .unwrap_or_else(|| panic!("the index named {title:?}, so it must be openable"));
            out.extend(join(&section.with_read(|i| i.rows.clone())));
        }
        out
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
    app.nav.push(Element::LogSession(Box::new(LogSession::for_test(k9rs_proto::protocol::LogTarget::one("canarypod".to_string()), LogContainer::Named("app".to_string(),
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
    let session = LogSession::for_test(k9rs_proto::protocol::LogTarget::one("canarypod".to_string()), LogContainer::Named("app".to_string(),
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
        k9rs_proto::protocol::LogTarget::one("canarypod".to_string()),
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
        k9rs_proto::protocol::LogTarget::one("canarypod".to_string()),
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
    let session = LogSession::for_test(k9rs_proto::protocol::LogTarget::one("canarypod".to_string()), LogContainer::Named("app".to_string(),
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
        Some(k9rs_app::TextInput::seeded("zzz-no-such-line"));
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
    app.nav.top_mut().log_view_mut().unwrap().draft = Some(k9rs_app::TextInput::seeded(r"\zsaab"));
    let painted = paint(&mut term(), &mut app);
    assert!(painted.contains("literal"), "the bar must name the degradation:\n{painted}");
    assert!(painted.contains("zs"), "and say which atom caused it:\n{painted}");

    // A pattern that DOES compile keeps the ordinary count.
    app.nav.top_mut().log_view_mut().unwrap().draft = Some(k9rs_app::TextInput::seeded(r"aa\|xy"));
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
        k9rs_proto::protocol::LogTarget::one("canarypod".to_string()),
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
    app.nav.top_mut().log_view_mut().unwrap().draft = Some(k9rs_app::TextInput::seeded("client"));
    let painted = paint(&mut t, &mut app);
    assert!(!painted.contains("case-sensitive"), "no capital, nothing to say:\n{painted}");

    // One capital flips it — and the bar now says why the view emptied.
    app.nav.top_mut().log_view_mut().unwrap().draft = Some(k9rs_app::TextInput::seeded("Client"));
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
    use k9rs_app::ViewExtent;
    let mut app = App::new_for_test();
    app.conn.established();
    seed_row(&app, "canary-row-marker");
    let mut t = term();
    let depth = app.nav.depth();

    app.nav.push(Element::Confirm(Box::new(k9rs_app::ConfirmDialog {
        message: "Delete pod/canary-row-marker?".to_string(),
        action_label: "Delete".to_string(),
        pending: k9rs_app::PendingAction::Single {
            op: k9rs_app::SingleOp::Delete,
            target: k9rs_proto::protocol::ObjectRef::new(
                BuiltInKind::Pod.to_resource_id(),
                "canary-row-marker".to_string(),
                k9rs_proto::protocol::Namespace::Named("default".to_string()),
            ),
        },
        focused: k9rs_app::ConfirmFocus::Cancel,
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
        let action = k9rs_app::handle_key_event(
            &app,
            crossterm::event::KeyEvent::new(key, crossterm::event::KeyModifiers::NONE),
        );
        assert_eq!(
            action,
            Some(k9rs_app::Action::Cancel),
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
    use k9rs_app::ViewExtent;
    let mut app = App::new_for_test();
    app.conn.established();
    seed_row(&app, "canary-row-marker");
    let mut t = term();

    app.nav.push(Element::Picker(Box::new(k9rs_app::ContainerPicker {
        target: k9rs_proto::protocol::ObjectRef::new(
            BuiltInKind::Pod.to_resource_id(),
            "canary-row-marker".to_string(),
            k9rs_proto::protocol::Namespace::Named("default".to_string()),
        ),
        choices: vec![
            k9rs_app::ContainerChoice {
                label: "web".into(),
                container: k9rs_proto::protocol::LogContainer::Named("web".into()),
            },
            k9rs_app::ContainerChoice {
                label: "sidecar".into(),
                container: k9rs_proto::protocol::LogContainer::Named("sidecar".into()),
            },
        ],
        selected: 0,
        action: k9rs_app::ContainerAction::Logs,
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
    assert_eq!(app.nav.top().class(), k9rs_app::ElementClass::Dialog);
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
    let target = k9rs_proto::protocol::ObjectRef::new(
        BuiltInKind::Pod.to_resource_id(),
        "canary-row-marker".to_string(),
        k9rs_proto::protocol::Namespace::Named("default".to_string()),
    );

    let dialogs: Vec<(&str, Element, &str)> = vec![
        (
            "confirm",
            Element::Confirm(Box::new(k9rs_app::ConfirmDialog {
                message: "Delete pod/canary?".to_string(),
                action_label: "Delete".to_string(),
                pending: k9rs_app::PendingAction::Single {
                    op: k9rs_app::SingleOp::Delete,
                    target: target.clone(),
                },
                focused: k9rs_app::ConfirmFocus::Cancel,
            })),
            "Delete pod/canary?",
        ),
        (
            "picker",
            Element::Picker(Box::new(k9rs_app::ContainerPicker {
                target: target.clone(),
                choices: vec![k9rs_app::ContainerChoice {
                    label: "sidecar".into(),
                    container: k9rs_proto::protocol::LogContainer::Named("sidecar".into()),
                }],
                selected: 0,
                action: k9rs_app::ContainerAction::Logs,
            })),
            "sidecar",
        ),
        (
            "form",
            Element::Form(Box::new(k9rs_app::FormDialog {
                submit: k9rs_app::FormSubmit::Scale,
                title: "Scale deploy/api".to_string(),
                subtitle: "namespace: default".to_string(),
                target: target.clone(),
                fields: vec![k9rs_app::FormFieldState {
                    name: "replicas".to_string(),
                    label: "Replicas".to_string(),
                    kind: k9rs_app::FormFieldKind::Number { min: 0, max: 100 },
                    value: k9rs_app::TextInput::seeded("3"),
                }],
                focused: k9rs_app::FormFocus::Field(0),
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
        assert_eq!(app.nav.top().class(), k9rs_app::ElementClass::Dialog);
        assert!(app.nav.top().captures_all_input(), "{label}: a dialog owns every key");

        let painted = paint(&mut t, &mut app);
        assert!(painted.contains(needle), "{label}: its own content is drawn:\n{painted}");
        if label == "form" {
            // A form had ONE button (OK), so submitting was the only way out
            // of one opened by mistake. Both must be on screen.
            assert!(painted.contains("OK"), "form: the OK button is drawn:\n{painted}");
            assert!(painted.contains("Cancel"), "form: the Cancel button is drawn:\n{painted}");
        }
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
    app.nav.push(Element::Confirm(Box::new(k9rs_app::ConfirmDialog {
        message: "Delete pod/canary?".to_string(),
        action_label: "Delete".to_string(),
        pending: k9rs_app::PendingAction::Single {
            op: k9rs_app::SingleOp::Delete,
            target: k9rs_proto::protocol::ObjectRef::new(
                BuiltInKind::Pod.to_resource_id(),
                "canary-row-marker".to_string(),
                k9rs_proto::protocol::Namespace::Named("default".to_string()),
            ),
        },
        focused: k9rs_app::ConfirmFocus::Cancel,
    })));
    app.ui.open(k9rs_app::Modal::Takeover(k9rs_app::TakeoverOp::Shell(Box::new(
        k9rs_app::ShellState {
            title: "shell-takeover-marker".to_string(),
            stream: None,
            connect_state: k9rs_app::ShellConnectState::Connecting,
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

/// Repeats collapse only in a column that DECLARES it groups.
///
/// The first version keyed on "is this the sorted column", which is true of
/// AGE, STATUS and every other per-row fact. Sorting a pod list by AGE put a
/// ReplicaSet's identically-aged pods adjacent and blanked eighteen of them —
/// reported, correctly, as "is the age not properly populated for every
/// single row?". Content present but not shown, reading as missing: the exact
/// class the rest of this file exists to prevent.
///
/// Help's `SECTION` is the one column that opts in, because it is the
/// category the rows were gathered under rather than a fact about each one.
#[test]
fn only_a_grouping_column_collapses_its_repeats() {
    use crate::widgets::{ResourceTable, ResourceTableState};
    use ratatui::buffer::Buffer;
    use ratatui::layout::Rect;
    use ratatui::widgets::StatefulWidget;

    let theme = k9rs_theme::Theme::dark();
    // Three rows sharing a value in column 0, as a rollout's pods share an age.
    let rows: Vec<Vec<String>> = [["All", "?", "aaa"], ["All", ":", "bbb"], ["View", "d", "ccc"]]
        .iter()
        .map(|r| r.iter().map(|c| c.to_string()).collect())
        .collect();
    let headers = vec!["SEC", "KEY", "DESC"];
    let widths: Vec<u16> = vec![12, 12, 12];

    let paint = |group: Option<usize>, offset: usize, pats: &[k9rs_util::SearchPattern]| -> Vec<String> {
        let area = Rect::new(0, 0, 60, 10);
        let mut buf = Buffer::empty(area);
        let mut st = ResourceTableState {
            selected: usize::MAX,
            offset,
            selected_col: 0,
            col_offset: 0,
            visible_height: 0,
        };
        ResourceTable::new(headers.clone(), &rows, "t", &theme)
            // SORTED on column 0 throughout: sorting is not the question.
            .sort(Some(0), true)
            .group_col(group)
            .col_widths(&widths)
            .search_patterns(pats)
            .render(area, &mut buf, &mut st);
        (0..area.height)
            .map(|y| (0..area.width).map(|x| buf[(x, y)].symbol()).collect::<String>())
            .collect()
    };

    // SORTED but NOT grouping — the AGE case. Every value prints.
    let plain = paint(None, 0, &[]);
    assert_eq!(
        plain.iter().filter(|l| l.contains("All")).count(),
        2,
        "a sorted column that does not group keeps every value:\n{plain:#?}",
    );

    // Grouping: the run collapses after its first row, and the next value
    // still prints.
    let grouped = paint(Some(0), 0, &[]);
    let rows_shown: Vec<&String> = grouped
        .iter()
        .filter(|l| l.contains("aaa") || l.contains("bbb"))
        .collect();
    assert_eq!(rows_shown.len(), 2, "both rows drew:\n{grouped:#?}");
    assert!(rows_shown[0].contains("All"), "the first carries the group");
    assert!(!rows_shown[1].contains("All"), "the second does not repeat it");
    assert!(
        grouped.iter().any(|l| l.contains("View") && l.contains("ccc")),
        "a different value is never suppressed:\n{grouped:#?}",
    );

    // Scrolled: the top visible row carries its group even though the row
    // above it (off screen) shares the value.
    let scrolled = paint(Some(0), 1, &[]);
    assert!(
        scrolled.iter().any(|l| l.contains("All") && l.contains("bbb")),
        "the first visible row is never suppressed:\n{scrolled:#?}",
    );

    // Searched: every matching cell shows, or the highlight would claim one
    // row matched when two did.
    let searched = paint(Some(0), 0, &[k9rs_util::SearchPattern::new("All")]);
    assert_eq!(
        searched.iter().filter(|l| l.contains("All")).count(),
        2,
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

    let rid = k9rs_proto::local_types::LocalResourceKind::Alias.to_resource_id();
    app.nav.reset(
        app.core
            .client_root_element(&app.kube.metrics, &rid, k9rs_proto::protocol::Namespace::All)
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
    assert_eq!(app.nav.top().class(), k9rs_app::ElementClass::Table);
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
            k9rs_proto::protocol::LogTarget::one("canarypod".to_string()),
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
        let k9rs_app::ViewExtent::Dialog { width, .. } =
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
        let k9rs_app::ViewExtent::Dialog { width, .. } = app.nav.top().extent(frame)
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
            k9rs_proto::protocol::LogTarget::one("canarypod".to_string()),
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
    app.ui.flash = Some(k9rs_app::FlashMessage::error(long));
    let mut t = Terminal::new(TestBackend::new(60, 12)).expect("terminal");
    let painted = paint(&mut t, &mut app);
    assert!(
        painted.contains('\u{2026}'),
        "an over-long flash is visibly cut, not silently clipped:\n{painted}",
    );

    // …and one that fits is untouched, centred as before.
    let mut app = App::new_for_test();
    app.conn.established();
    app.ui.flash = Some(k9rs_app::FlashMessage::info("short and sweet".to_string()));
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
        let predicate = k9rs_app::RowPredicate::Grep(
            k9rs_util::CompiledGrep::new(pat),
        );
        if let Ok(el) = k9rs_app::Element::derive_filter(app.nav.top(), predicate) {
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
    app.ui.open(k9rs_app::Modal::Command {
        input: k9rs_app::TextInput::seeded(typed),
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
        k9rs_proto::protocol::LogTarget::one("canarypod".to_string()),
        LogContainer::Named("app".to_string()),
    ))));
    for pat in ["alpha", "bravo", "charlie"] {
        if let Ok(el) = k9rs_app::Element::derive_log_filter(
            app.nav.top(),
            k9rs_util::CompiledGrep::new(pat),
            k9rs_app::Sense::Keep,
        ) {
            app.nav.push(el);
        }
    }
    let draft = "a-very-long-pattern-being-typed-right-now";
    // The log bar reads the element's own `draft`, not the table filter.
    app.nav.top_mut().log_view_mut().expect("a log element").draft =
        Some(k9rs_app::TextInput::seeded(draft));
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

/// The TABLE's filter bar keeps its caret and its tail on screen too.
///
/// A sibling of `a_long_input_keeps_its_caret_and_its_tail_on_screen`, which
/// covered the `:` prompt and the log grep and therefore passed while this
/// bar had no windowing at all: it appended the block cursor after the whole
/// pattern and let `set_line` clip, so `/` and `~` on a resource table
/// clipped at the right edge with the caret outside it — you typed blind —
/// and after `Left`/`Home` the block was drawn at the END while the
/// keystrokes landed mid-pattern. The handler wiring it even comments that
/// "the caret behaves here exactly as it does in the log grep and the
/// command prompt"; it did not.
///
/// Sixth member of a class that was supposed to be closed, and the tell was
/// `FilterInputState::caret_col` — doc: "Caret position for the renderer, in
/// columns" — having no callers anywhere in the workspace.
#[test]
fn the_table_filter_bar_keeps_its_caret_and_its_tail_on_screen() {
    let typed = "a-very-long-filter-pattern-being-typed-right-now";

    // Caret at the END: the tail is what you just typed.
    let mut app = App::new_for_test();
    app.conn.established();
    seed_row(&app, "a-row");
    {
        let fi = app.nav.top_mut().filter_input_mut().expect("a table filters");
        fi.start();
        *fi.input_mut() = k9rs_app::TextInput::seeded(typed);
    }
    let mut t = Terminal::new(TestBackend::new(40, 16)).expect("terminal");
    let painted = paint(&mut t, &mut app);
    let tail = &typed[typed.len() - 10..];
    assert!(
        painted.contains(tail),
        "the end of what you typed must be visible: {tail:?}\n{painted}",
    );
    assert!(painted.contains('\u{2588}'), "and its caret block is drawn:\n{painted}");
    // The counter is the tail that must never be pushed off: it is the only
    // thing saying whether the pattern matches anything.
    assert!(painted.contains("[0/1]"), "the match counter survives:\n{painted}");

    // Caret at the START, after Home: the block belongs at the beginning,
    // and the HEAD of the pattern is what is on screen.
    let mut app = App::new_for_test();
    app.conn.established();
    seed_row(&app, "a-row");
    {
        let fi = app.nav.top_mut().filter_input_mut().expect("a table filters");
        fi.start();
        *fi.input_mut() = k9rs_app::TextInput::seeded(typed);
        fi.input_mut().home();
    }
    let mut t = Terminal::new(TestBackend::new(40, 16)).expect("terminal");
    let painted = paint(&mut t, &mut app);
    let bar = row_with(&t, "a-very-long");
    let caret_at = bar.find('\u{2588}').expect("the caret block is drawn");
    let head_at = bar.find("a-very-long").expect("the head of the pattern shows");
    assert!(
        caret_at < head_at,
        "with the caret home, the block sits BEFORE the pattern, not after \
         it: {bar:?}",
    );
    assert!(
        !painted.contains(tail),
        "and the window followed the caret to the head, so the tail is off \
         screen:\n{painted}",
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
    app.kube.context = k9rs_proto::protocol::ContextName::new(arn);
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
    let (mw, mh) = k9rs_app::DIALOG_MARGIN;
    let mut app = App::new_for_test();
    app.conn.established();
    seed_row(&app, "canary-row-marker");
    let target = k9rs_proto::protocol::ObjectRef::new(
        BuiltInKind::Pod.to_resource_id(),
        "canary-row-marker".to_string(),
        k9rs_proto::protocol::Namespace::Named("default".to_string()),
    );

    app.nav.push(Element::Confirm(Box::new(k9rs_app::ConfirmDialog {
        message: "Delete pod/canary-row-marker?".to_string(),
        action_label: "Delete".to_string(),
        pending: k9rs_app::PendingAction::Single {
            op: k9rs_app::SingleOp::Delete,
            target,
        },
        focused: k9rs_app::ConfirmFocus::Cancel,
    })));

    let k9rs_app::ViewExtent::Dialog { width, height } =
        app.nav.top().extent(ratatui::layout::Rect::new(0, 0, 200, 60))
    else {
        panic!("a confirm floats")
    };
    let k9rs_app::Element::Confirm(d) = app.nav.top() else { panic!() };
    let (cw, ch) = k9rs_app::confirm_dialog_size(d);
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

/// The grep bar says which STEP emptied the chain, not just that it is empty.
///
/// It showed one aggregate — `[3 filters, 0 visible]` told you the chain was
/// dead but not which pattern killed it, and with a conjunction that is the
/// only question worth asking. Each step's own `retained` store already knew.
#[test]
fn the_grep_bar_counts_every_step_of_the_chain() {
    use k9rs_proto::protocol::{LogContainer, LogLine};
    use ratatui::backend::TestBackend;
    use ratatui::Terminal;

    let mut app = App::new_for_test();
    app.conn.established();
    let session = LogSession::for_test(
        k9rs_proto::protocol::LogTarget::one("canarypod".to_string()),
        LogContainer::Default,
    );
    for text in ["alpha one", "alpha two", "beta three"] {
        session.store().push(0, LogLine::untagged(text));
    }
    app.nav.push(Element::LogSession(Box::new(session)));

    // Two steps: the first keeps 2 of 3, the second keeps none of those.
    for (pat, sense) in [
        ("alpha", k9rs_app::Sense::Keep),
        ("zzz", k9rs_app::Sense::Keep),
    ] {
        let el = k9rs_app::Element::derive_log_filter(
            app.nav.top(),
            k9rs_util::CompiledGrep::new(pat),
            sense,
        )
        .expect("a log can be filtered");
        app.nav.push(el);
        let _ = app.nav.top_mut().log_visible();
    }

    let mut t = Terminal::new(TestBackend::new(70, 14)).expect("terminal");
    let painted = paint(&mut t, &mut app);
    assert!(
        painted.contains("alpha[2]"),
        "the first step kept two — say so:\n{painted}",
    );
    assert!(
        painted.contains("zzz[0]"),
        "and the second is the one that emptied it — THAT is the datum:\n{painted}",
    );
}

/// Sorting a pod list by AGE shows an age on EVERY row.
///
/// The reported regression, end to end: a ReplicaSet creates its pods within
/// the same second, so sorting by AGE put identical rendered values adjacent
/// and the repeat-collapse blanked all but the first of each run. Eighteen
/// rows with no age reads as broken data, not as grouping — and it was read
/// that way.
///
/// The widget-level rule is pinned by `only_a_grouping_column_collapses_its_
/// repeats`; this one pins the WIRING, because the bug was not in the rule
/// but in which column the view handed it.
#[test]
fn a_pod_list_sorted_by_age_shows_every_age() {
    use ratatui::backend::TestBackend;
    use ratatui::Terminal;

    let mut app = App::new_for_test();
    app.conn.established();
    let store = app.nav.top().data_store().expect("root is a table");
    // Four pods of one rollout: the SAME age, as a real ReplicaSet produces.
    let rows: Vec<ResourceRow> = (0..4)
        .map(|i| ResourceRow {
            name: format!("parser-web-{i}"),
            namespace: Some("default".into()),
            cells: vec![
                CellValue::Text(format!("parser-web-{i}")),
                CellValue::Text("3m43s".to_string()),
            ],
            ..Default::default()
        })
        .collect();
    store.apply(
        1,
        StorePayload::Baseline(TableBaseline {
            headers: vec!["NAME".into(), "AGE".into()],
            rows,
        }),
    );

    // Sort on AGE, which is where the blanks appeared.
    let age_col = app
        .nav
        .top()
        .headers_snapshot()
        .iter()
        .position(|h| h == "AGE")
        .expect("the fixture has an AGE column");
    let _ = app.nav.top_mut().sort_by(k9rs_app::SortTarget::Column(age_col));

    // Tall enough for all four data rows: the title's `[n-m/total]` makes a
    // short frame obvious, but this assertion is about the CELLS.
    let mut t = Terminal::new(TestBackend::new(70, 16)).expect("terminal");
    let painted = paint(&mut t, &mut app);
    let with_age = painted.matches("3m43s").count();
    assert_eq!(
        with_age, 4,
        "every row keeps its age — AGE is a fact about each pod, not a \
         heading they share:\n{painted}",
    );
}

/// No resource column collapses, whichever one you sort by.
///
/// The AGE regression was reported for AGE, but the broken rule applied to
/// EVERY sorted column — STATUS repeated "Running" and NODE repeated a node
/// name just as readily, and both would have blanked the same way. I had
/// written "worth re-checking on the next run"; this checks it instead.
#[test]
fn no_resource_column_collapses_whichever_one_is_sorted() {
    use ratatui::backend::TestBackend;
    use ratatui::Terminal;

    let mut app = App::new_for_test();
    app.conn.established();
    let store = app.nav.top().data_store().expect("root is a table");
    // Four pods that AGREE on every column — the shape a rollout produces,
    // and the shape that made the collapse visible.
    let node = "ip-10-1-177-41.us-west-2.compute.internal";
    let rows: Vec<ResourceRow> = (0..4)
        .map(|i| ResourceRow {
            name: format!("parser-web-{i}"),
            namespace: Some("default".into()),
            cells: vec![
                CellValue::Text(format!("parser-web-{i}")),
                CellValue::Text("Running".to_string()),
                CellValue::Text(node.to_string()),
                CellValue::Text("3m43s".to_string()),
            ],
            ..Default::default()
        })
        .collect();
    store.apply(
        1,
        StorePayload::Baseline(TableBaseline {
            headers: vec!["NAME".into(), "STATUS".into(), "NODE".into(), "AGE".into()],
            rows,
        }),
    );

    for (header, value) in [("STATUS", "Running"), ("NODE", node), ("AGE", "3m43s")] {
        let col = app
            .nav
            .top()
            .headers_snapshot()
            .iter()
            .position(|h| h == header)
            .unwrap_or_else(|| panic!("the fixture has {header}"));
        let _ = app.nav.top_mut().sort_by(k9rs_app::SortTarget::Column(col));

        let mut t = Terminal::new(TestBackend::new(120, 16)).expect("terminal");
        let painted = paint(&mut t, &mut app);
        assert_eq!(
            painted.matches(value).count(),
            4,
            "sorted by {header}: all four rows keep their {header} — it is a \
             fact about each pod, not a heading they share:\n{painted}",
        );
    }
}

/// A SECTION column still collapses its repeated values when sorted on.
///
/// This used to be asked of `?`, which was the feature's first customer: 61
/// rows with "Everywhere" repeated fourteen times down the left edge. `?` is
/// an INDEX now — one row per section — so it has no run left to collapse,
/// and asking it would assert nothing. The live case is the ALIAS view,
/// whose SECTION column repeats "resource" across every registry entry.
///
/// Kept rather than deleted: `groups()` is opt-in per column precisely
/// because the wrong column collapsing reads as MISSING DATA (it blanked
/// eighteen AGE cells once), so the behaviour needs a customer that proves
/// it still works.
#[test]
fn a_section_column_still_collapses_its_repeated_values() {
    use ratatui::backend::TestBackend;
    use ratatui::Terminal;

    let mut app = App::new_for_test();
    app.conn.established();
    let rid = k9rs_proto::local_types::LocalResourceKind::Alias.to_resource_id();
    app.nav.reset(
        app.core
            .client_root_element(&app.kube.metrics, &rid, k9rs_proto::protocol::Namespace::All)
            .expect("aliases is client-owned"),
    );

    // Tall enough for every alias row, so the run is on screen entire.
    let mut t = Terminal::new(TestBackend::new(200, 90)).expect("terminal");
    let painted = paint(&mut t, &mut app);

    let resource_rows = app.core.aliases.with_read(|i| {
        i.rows
            .iter()
            .filter(|r| {
                matches!(&r.cells[0], k9rs_proto::row::CellValue::Text(t) if t == "resource")
            })
            .count()
    });
    assert!(resource_rows > 3, "the fixture really has a run to collapse");

    let drawn = painted.matches("resource").count();
    assert!(
        drawn < resource_rows,
        "SECTION must still collapse — it is the category the rows were \
         gathered under, and this is the case the feature exists for \
         ({drawn} drawn vs {resource_rows} rows):\n{painted}",
    );
    assert!(drawn >= 1, "…but the group is still labelled once");
}

/// `G` on a just-opened describe reaches the bottom on the FIRST press.
///
/// I fixed this at the `Viewport` level and tested it there, then wrote
/// "worth re-checking on the next run" for the view. But the bug was never in
/// `Viewport` — it was the content path passing a literal `false` where the
/// follow flag belonged, so a unit test of `set_metrics` could not have
/// caught it. This drives the real describe view.
#[test]
fn g_on_a_freshly_opened_describe_reaches_the_bottom_at_once() {
    use ratatui::backend::TestBackend;
    use ratatui::Terminal;

    let mut app = App::new_for_test();
    app.conn.established();
    let mut state = k9rs_app::ContentViewState::default();
    // More lines than fit, each identifiable, so "which window is drawn" is
    // readable off the frame.
    state.set_content((0..200).map(|i| format!("line-{i:03}")).collect::<Vec<_>>().join("\n"));
    app.nav.push(Element::ContentView(k9rs_app::ContentView::new(
        k9rs_app::ContentSpec::Describe(k9rs_proto::protocol::ObjectRef::new(
            BuiltInKind::Pod.to_resource_id(),
            "canary".to_string(),
            k9rs_proto::protocol::Namespace::Named("default".to_string()),
        )),
        state,
        k9rs_app::ContentPhase::Ready,
    )));

    let mut t = Terminal::new(TestBackend::new(60, 14)).expect("terminal");

    // `G` BEFORE anything has rendered — the state the bug lived in. The
    // extent is still 0, so the intent is all the viewport has to go on.
    if let Element::ContentView(cv) = app.nav.top_mut() {
        cv.state.viewport.end();
    }
    let painted = paint(&mut t, &mut app);
    assert!(
        painted.contains("line-199"),
        "the FIRST `G` must land on the last line, not the top:\n{painted}",
    );
    assert!(
        !painted.contains("line-000"),
        "…and the top must be gone, not merely scrolled past:\n{painted}",
    );
}

/// A float paints inside its box, and a short terminal is not a crash.
///
/// `? help` was stamped with a direct `Buffer::set_string`, which clips at
/// the buffer edge and not at the area it was given. When the element floats,
/// the breadcrumb chunk is `Constraint::Length(0)` and ratatui puts a
/// trailing zero-length chunk's `y` at `area.bottom()` — one row PAST the
/// box. So:
///   - at a terminal height of 6 or less that row was outside the buffer
///     entirely, and `Buffer`'s index is a hard `panic!` rather than a
///     debug assert, so pressing `?` killed the TUI;
///   - at any taller height the hint was painted one row BELOW the help box,
///     over the view the float is supposed to leave intact.
///
/// Both are the same missing guard. Heights 1..=8 straddle the old panic
/// boundary (it was exactly 1..=6).
#[test]
fn a_float_paints_inside_its_box_at_every_terminal_height() {
    for h in 1u16..=8 {
        let mut app = App::new_for_test();
        seed_row(&app, "api-7d9f");
        push_help(&mut app);
        let mut t = Terminal::new(TestBackend::new(60, h)).expect("terminal");
        // The assertion IS that this returns.
        let _ = paint(&mut t, &mut app);
    }

    // Tall enough for the float to have a visible box with room under it.
    let mut app = App::new_for_test();
    seed_row(&app, "api-7d9f");
    push_help(&mut app);
    let mut t = Terminal::new(TestBackend::new(80, 20)).expect("terminal");
    let _ = paint(&mut t, &mut app);

    let buf = t.backend().buffer();
    let width = buf.area.width as usize;
    let rows: Vec<String> = buf
        .content
        .iter()
        .map(|c| c.symbol().to_string())
        .collect::<Vec<_>>()
        .chunks(width)
        .map(|r| r.concat())
        .collect();
    let carrying: Vec<usize> =
        rows.iter().enumerate().filter(|(_, r)| r.contains("? help")).map(|(i, _)| i).collect();
    assert_eq!(
        carrying,
        vec![rows.len() - 2],
        "exactly one `? help`, on the FRAME's breadcrumb row — a second one \
         means the float stamped its own over the view underneath:\n{}",
        rows.join("\n"),
    );
}

/// The breadcrumb's right-hand hint and its `Esc back` suffix cannot
/// overlap, because the path's budget is measured from the spans that are
/// actually emitted.
///
/// The budget was a hand-written `suffix_w = 9` sitting beside a comment
/// reading `" | Esc back"`. The spans emit `"| "` + `"Esc"` + `" back"` =
/// ten cells, so a path that filled its budget put the right-aligned hint
/// over the final `k` of `back`.
///
/// The collision needs the path to actually FILL its budget, which is why
/// the narrow widths are here: the breadcrumb below is ~15 cells, so from
/// about 40 columns up there is slack and the stray cell lands on a space.
/// Measured against the old arithmetic, width 26 is where it bit:
/// `" … > ctx | Esc bac ? help "`.
#[test]
fn a_drilled_breadcrumb_keeps_both_its_hint_and_its_back_key() {
    for w in [26u16, 28, 30, 32, 34, 36, 40, 60, 120] {
        let mut app = App::new_for_test();
        seed_row(&app, "api-7d9f");
        // `client_root_element` is the plain, non-floating resource list the
        // contexts view uses — a real second element, so the view paints its
        // `Esc back` suffix.
        let rid = k9rs_proto::local_types::LocalResourceKind::Context.to_resource_id();
        let drilled = app
            .core
            .client_root_element(&app.kube.metrics, &rid, k9rs_proto::protocol::Namespace::All)
            .expect("contexts is a client-owned resource");
        app.nav.push(drilled);
        assert!(app.nav.is_drilled(), "sanity: drilled, so `Esc back` is emitted");

        let mut t = Terminal::new(TestBackend::new(w, 20)).expect("terminal");
        let _ = paint(&mut t, &mut app);
        let row = row_with(&t, "? help");
        assert!(
            row.contains("Esc back"),
            "at width {w} the hint ate the back key: {row:?}",
        );
    }
}

/// A FORM's box reserves exactly the chrome its widget spends — including
/// its hint bar, which is the thing that was one cell short.
///
/// Sibling of `a_dialogs_box_reserves_exactly_the_chrome_its_widget_spends`,
/// which covers the confirm only and so passed throughout: `confirm_dialog_size`
/// was exported and the confirm widget consulted it, while `form_dialog_size`
/// was `pub(crate)` — unreachable from `k9rs_ui` — so the widget sized the box
/// from the hint line it built and the float sized it from a hardcoded
/// `if has_select { 52 } else { 38 }`. Measured, those lines are 41 and 55.
///
/// The fixture is the port-forward form, because that is where the three-column
/// error bit: a Select plus a 14-column label ("Container port") put the box
/// one cell under the hint's width and `<Esc> cancel` rendered `<Esc> cance`.
/// Losing the key that gets you out of a modal is the specific failure
/// `header::fit_bar` exists to prevent.
#[test]
fn a_forms_box_fits_the_hint_bar_that_names_its_exit() {
    let (mw, mh) = k9rs_app::DIALOG_MARGIN;
    let target = k9rs_proto::protocol::ObjectRef::new(
        BuiltInKind::Pod.to_resource_id(),
        "api-7d9f".to_string(),
        k9rs_proto::protocol::Namespace::Named("default".to_string()),
    );

    // The port-forward shape: a Select (so the arrow hints appear) beside the
    // longest label the form has.
    let port_forward = || {
        k9rs_app::FormDialog {
            submit: k9rs_app::FormSubmit::PortForward,
            title: "Port-forward pod/api-7d9f".to_string(),
            subtitle: "namespace: default".to_string(),
            target: target.clone(),
            fields: vec![
                k9rs_app::FormFieldState {
                    name: "container_port".to_string(),
                    label: "Container port".to_string(),
                    kind: k9rs_app::FormFieldKind::Select {
                        options: vec![
                            k9rs_app::SelectOption::new("80", "80"),
                            k9rs_app::SelectOption::new("8080", "8080"),
                        ],
                        selected: 0,
                    },
                    value: k9rs_app::TextInput::seeded("80"),
                },
                k9rs_app::FormFieldState {
                    name: "local_port".to_string(),
                    label: "Local port".to_string(),
                    kind: k9rs_app::FormFieldKind::Port,
                    value: k9rs_app::TextInput::seeded("8080"),
                },
            ],
            focused: k9rs_app::FormFocus::Field(0),
        }
    };

    // 1. The float reserves the widget's content plus exactly the chrome.
    let mut app = App::new_for_test();
    app.conn.established();
    seed_row(&app, "api-7d9f");
    app.nav.push(Element::Form(Box::new(port_forward())));
    let k9rs_app::ViewExtent::Dialog { width, height } =
        app.nav.top().extent(ratatui::layout::Rect::new(0, 0, 200, 60))
    else {
        panic!("a form floats")
    };
    let k9rs_app::Element::Form(d) = app.nav.top() else { panic!() };
    let (fw, fh) = k9rs_app::form_dialog_size(d);
    assert_eq!(
        (width, height),
        (fw + mw, fh + mh),
        "the box is the widget's content plus exactly the chrome it spends",
    );

    // 2. And the hint bar it declares actually fits: the box's inner width
    // has to hold it, which is the arithmetic that disagreed.
    let hints_w = k9rs_app::form_hints_width(d);
    assert!(
        hints_w + mw <= width,
        "the hint bar is {hints_w} columns and the box is {width} — the exit \
         key gets clipped",
    );

    // 3. Painted, end to end: `cancel` survives whole.
    let mut t = term();
    let painted = paint(&mut t, &mut app);
    assert!(
        painted.contains("Container port"),
        "the form draws its longest label:\n{painted}",
    );
    let bar = row_with(&t, "<Esc>");
    assert!(
        bar.contains("<Esc> cancel"),
        "the exit key is named in full, not clipped to `cance`: {bar:?}",
    );
    assert!(
        bar.contains("select"),
        "and the Select field's arrow hint is there too: {bar:?}",
    );
}

/// The YAML viewer paints inside its content area, matched or not.
///
/// This widget had no paint test at all, and three branches that disagreed
/// about where a line ends. The two search-highlight branches used
/// `buf.set_string`, which clips at the BUFFER edge rather than the area's,
/// so a long matched line painted across the right border and over the
/// scrollbar — while the identical line one keystroke earlier, unmatched,
/// went through `set_line(.., content_width)` and clipped correctly.
///
/// And `content_width` subtracted only the gutter and its separator while the
/// scrollbar painted at `inner.width - 1` AFTER the text, so the last column
/// of every long line was overwritten once the document outgrew one screen.
/// That is verbatim the bug `log_view` documents fixing — "the last character
/// of every line disappeared the moment a log grew past one screen" — in the
/// sibling that never got the fix.
#[test]
fn the_yaml_viewer_paints_inside_its_content_area() {
    // Lines wider than the viewport, and more of them than fit, so both the
    // horizontal clip and the scrollbar are in play.
    let long = "a".repeat(200);
    let body: String = (0..120)
        .map(|i| format!("key{i:03}: {long}"))
        .collect::<Vec<_>>()
        .join("\n");

    let paint_yaml = |search: Option<&str>| -> Terminal<TestBackend> {
        let mut app = App::new_for_test();
        app.conn.established();
        let mut state = k9rs_app::ContentViewState::default();
        state.set_content(body.clone());
        if let Some(pattern) = search {
            // `update_search` reads `search`, not `search_input` — the
            // committed term, not the draft.
            state.search = Some(pattern.to_string());
            state.update_search();
            assert!(!state.search_matches.is_empty(), "the fixture must MATCH");
        }
        app.nav.push(Element::ContentView(k9rs_app::ContentView::new(
            k9rs_app::ContentSpec::Yaml(k9rs_proto::protocol::ObjectRef::new(
                BuiltInKind::Pod.to_resource_id(),
                "canary".to_string(),
                k9rs_proto::protocol::Namespace::Named("default".to_string()),
            )),
            state,
            k9rs_app::ContentPhase::Ready,
        )));
        let mut t = Terminal::new(TestBackend::new(60, 16)).expect("terminal");
        let _ = paint(&mut t, &mut app);
        t
    };

    /// For each row that holds YAML content, the column its LAST `│` sits in
    /// — the block's right border, which nothing inside may paint over.
    /// Chrome rows (the search bar, the borders themselves) are excluded by
    /// requiring the content marker.
    fn content_row_borders(term: &Terminal<TestBackend>) -> Vec<usize> {
        let buf = term.backend().buffer();
        let w = buf.area.width as usize;
        buf.content
            .chunks(w)
            .filter_map(|row| {
                let text: String = row.iter().map(|c| c.symbol()).collect();
                if !text.contains("key0") {
                    return None;
                }
                row.iter()
                    .enumerate()
                    .filter(|(_, c)| c.symbol() == "│")
                    .map(|(i, _)| i)
                    .next_back()
            })
            .collect()
    }

    let plain = paint_yaml(None);
    let borders = content_row_borders(&plain);
    assert!(borders.len() > 4, "sanity: several content rows drew: {borders:?}");
    assert!(
        borders.iter().all(|&b| b == borders[0]),
        "the right border is a straight line — a long line painted over it: {borders:?}",
    );

    // The same document with a search active: EVERY line matches `key`, so
    // every content row takes the highlight branch. The border must not move.
    let matched = paint_yaml(Some("key"));
    assert_eq!(
        content_row_borders(&matched),
        borders,
        "a matched line clips exactly where an unmatched one does — the \
         highlight branches used to clip at the buffer edge instead",
    );
}

/// The OVERVIEW keeps the telling end of an ARN too.
///
/// Sibling of `the_header_keeps_the_telling_end_of_an_arn`, which passed
/// throughout: the two views already shared `app.attachment()` — unified
/// after the header named one context above another's cluster — but each
/// FITTED the values itself. The header elides from the left, because these
/// are identifiers whose distinguishing part is at the end; the overview
/// handed raw strings to a centred `Line`, which clips on the right. So the
/// landing page, which is the first thing you see, showed four copies of one
/// ARN prefix.
#[test]
fn the_overview_keeps_the_telling_end_of_an_arn() {
    let arn = "arn:aws:eks:us-west-2:123456789012:cluster/prod-usw2";
    let mut app = App::new_for_test();
    app.kube.context = k9rs_proto::protocol::ContextName::new(arn);
    app.conn.established();
    // The overview is the landing page; make it the top element.
    app.nav.reset(Element::Overview(Default::default()));
    // The HEADER renders the same ARN correctly, one row away. Without this
    // the assertion below passes on the header's row and says nothing about
    // the view under test — the trap `row_with`'s own doc warns about.
    app.ui.show_header = false;

    // Narrow enough that the ARN cannot fit.
    let mut t = Terminal::new(TestBackend::new(60, 20)).expect("terminal");
    let painted = paint(&mut t, &mut app);
    assert!(
        painted.contains("prod-usw2"),
        "the part that identifies the context must survive:\n{painted}",
    );
    let row = row_with(&t, "prod-usw2");
    assert!(
        row.contains('\u{2026}'),
        "and the cut is MARKED, not silent: {row:?}",
    );
}

/// The picker's box is the size its own geometry says, and it leaves the
/// same outer gap as the other two dialogs.
///
/// It drew into `area` directly while `ModalOverlay::render` — which the
/// confirm box and the form go through — is where `DIALOG_MARGIN` is SPENT.
/// `Element::extent` adds that margin for all three, so the picker's border
/// sat four columns and two rows wider than `container_picker_size`, making
/// its documented `clamp(30, 60)` a 34..64 box in practice.
#[test]
fn the_picker_spends_the_dialog_margin_like_its_siblings() {
    let mut app = App::new_for_test();
    app.conn.established();
    seed_row(&app, "canary-row-marker");
    let mut t = term();
    app.nav.push(Element::Picker(Box::new(k9rs_app::ContainerPicker {
        target: k9rs_proto::protocol::ObjectRef::new(
            BuiltInKind::Pod.to_resource_id(),
            "canary-row-marker".to_string(),
            k9rs_proto::protocol::Namespace::Named("default".to_string()),
        ),
        choices: vec![k9rs_app::ContainerChoice {
            label: "web".into(),
            container: k9rs_proto::protocol::LogContainer::Named("web".into()),
        }],
        selected: 0,
        action: k9rs_app::ContainerAction::Logs,
    })));

    let painted = paint(&mut t, &mut app);
    // The bottom border run is the box's width: `└` + n×`─` + `┘`.
    let drawn = painted
        .split('\u{2514}')
        .nth(1)
        .and_then(|rest| rest.split('\u{2518}').next())
        .map(|run| run.chars().count() + 2)
        .unwrap_or_else(|| panic!("the picker draws a box:\n{painted}"));
    let expected = k9rs_app::picker_hints_width() + k9rs_app::DIALOG_CHROME;
    assert_eq!(
        drawn as u16,
        expected.max(30),
        "the picker's box must be what its geometry says, not that plus the margin:\n{painted}",
    );
    assert!(painted.contains("web"), "and the choice is still drawn");
    // The hint fits ENTIRELY — the clipping this sizing exists to prevent.
    assert!(
        painted.contains("<Esc> back"),
        "the hint row must not be clipped:\n{painted}",
    );
}

/// The two background fillers CLIP rather than panic.
///
/// `Buffer`'s `Index` is a hard `panic!`, not a debug assert — the same
/// thing that killed `?` help at a seven-row terminal through
/// `set_string`. Both of these are `pub`, both indexed the buffer directly,
/// and "the caller must have got the rect from the frame" was stated
/// nowhere. Every current caller does; the contract did not say so.
#[test]
fn the_background_fillers_clip_outside_the_buffer() {
    use ratatui::layout::Rect;
    use ratatui::style::Style;
    let theme = k9rs_theme::Theme::dark();
    let mut buf = ratatui::buffer::Buffer::empty(Rect::new(0, 0, 10, 4));

    // Entirely outside, partly outside, and one row past the bottom — the
    // float case, where a `Constraint::Length(0)` chunk lands at
    // `area.bottom()`.
    for rect in [
        Rect::new(0, 0, 10, 4),
        Rect::new(5, 2, 40, 40),
        Rect::new(0, 4, 10, 1),
        Rect::new(100, 100, 5, 5),
        Rect::new(0, 0, 0, 0),
    ] {
        crate::fill_dialog_bg(&mut buf, rect, &theme);
    }

    // `fill_line_bg` takes a `Frame`, so it goes through a terminal.
    let mut t = ratatui::Terminal::new(ratatui::backend::TestBackend::new(10, 4))
        .expect("terminal");
    t.draw(|f| {
        for rect in [
            Rect::new(0, 0, 10, 1),
            Rect::new(0, 4, 10, 1),
            Rect::new(8, 1, 40, 1),
            Rect::new(100, 100, 5, 1),
        ] {
            crate::fill_line_bg(f, rect, Style::default());
        }
    })
    .expect("draw");
}

/// EVERY float paints at a terminal smaller than its own chrome.
///
/// The reported crash was `?` help at a seven-row terminal: the floating
/// view wrote past its box and panicked. Nothing drove a whole-frame paint
/// at a short terminal — `a_dialog_fits_its_content_at_every_terminal_width`
/// starts at 80×24, and the smallest terminal any other draw test used was
/// 46×12, both above every size that failed.
///
/// A panic is the assertion: `paint` unwraps `Terminal::draw`, so an index
/// past the buffer aborts this test. The cell count then says the frame was
/// written whole rather than abandoned early.
///
/// This is a SYMPTOM guard, not a discriminator for any one fix, and it is
/// worth saying what it does NOT reach. Measured twice: it still passes with
/// both background fillers unclipped, and still passes with the container
/// picker's `i >= content.height` guard removed — at these sizes the chrome
/// collapses to a `content` rect already inside the frame, so neither
/// unguarded write is reachable from here. Those two have their own direct
/// cover (`the_background_fillers_clip_outside_the_buffer`, and the picker's
/// own break). What this holds is the class: a float that asks for a box
/// bigger than the frame renders something and returns, for every float
/// kind — which is the thing the report said `?` did not do at seven rows.
#[test]
fn every_float_paints_at_a_terminal_smaller_than_its_chrome() {
    use ratatui::backend::TestBackend;
    use ratatui::Terminal;

    let target = k9rs_proto::protocol::ObjectRef::new(
        BuiltInKind::Pod.to_resource_id(),
        "canary".to_string(),
        k9rs_proto::protocol::Namespace::Named("default".to_string()),
    );
    // Built per paint, because `Element` is not `Clone` — which is the point
    // of it: an element owns a live stream handle.
    type Build = Box<dyn Fn(&k9rs_proto::protocol::ObjectRef) -> Option<Element>>;
    let floats: Vec<(&str, Build)> = vec![
        ("help", Box::new(|_| None)),
        (
            "confirm",
            Box::new(|target| Some(Element::Confirm(Box::new(k9rs_app::ConfirmDialog {
                message: "Delete pod/canary?".to_string(),
                action_label: "Delete".to_string(),
                pending: k9rs_app::PendingAction::Single {
                    op: k9rs_app::SingleOp::Delete,
                    target: target.clone(),
                },
                focused: k9rs_app::ConfirmFocus::Cancel,
            })))),
        ),
        (
            "picker",
            Box::new(|target| Some(Element::Picker(Box::new(k9rs_app::ContainerPicker {
                target: target.clone(),
                choices: vec![k9rs_app::ContainerChoice {
                    label: "sidecar".into(),
                    container: k9rs_proto::protocol::LogContainer::Named("sidecar".into()),
                }],
                selected: 0,
                action: k9rs_app::ContainerAction::Logs,
            })))),
        ),
        (
            "form",
            Box::new(|target| Some(Element::Form(Box::new(k9rs_app::FormDialog {
                submit: k9rs_app::FormSubmit::Scale,
                title: "Scale deploy/api".to_string(),
                subtitle: "namespace: default".to_string(),
                target: target.clone(),
                fields: vec![k9rs_app::FormFieldState {
                    name: "replicas".to_string(),
                    label: "Replicas".to_string(),
                    kind: k9rs_app::FormFieldKind::Number { min: 0, max: 100 },
                    value: k9rs_app::TextInput::seeded("3"),
                }],
                focused: k9rs_app::FormFocus::Field(0),
            })))),
        ),
    ];

    for (what, build) in floats {
    for (w, h) in [
        (80u16, 7u16), // the reported size
        (80, 6),
        (80, 5),
        (80, 4),
        (80, 3),
        (80, 2),
        (80, 1),
        (20, 4), // narrow AND short
        (4, 4),
        (1, 1),
    ] {
        let mut app = App::new_for_test();
        app.conn.established();
        seed_row(&app, "canary");
        match build(&target) {
            Some(el) => app.nav.push(el),
            None => push_help(&mut app),
        }
        let mut t = Terminal::new(TestBackend::new(w, h)).expect("terminal");
        let painted = paint(&mut t, &mut app);
        // `screen()` concatenates the rows, so the cell COUNT is the frame —
        // one symbol per cell. (The first version of this counted `lines()`,
        // which is a property of the helper, not of the paint: it is one
        // line, because nothing joins the rows with newlines.)
        assert_eq!(
            painted.chars().count(),
            usize::from(w) * usize::from(h),
            "{what} at {w}x{h}: every cell of the frame was written",
        );
    }
    }
}
