//! The agreement pin: every documented key must actually do what it says.
//!
//! Without this, `keymap.rs` is just a seventh hand-written copy that LOOKS
//! authoritative. With it, a key that gets rebound or removed breaks the build
//! instead of quietly lying in `?` help and in the bottom bar.

use super::*;
use crate::element::{Element, LogSession};
use crate::App;
use crate::handler::handle_key_event;
use k9rs_proto::protocol::LogContainer;

/// Put the app into a state whose top element has the given class.
fn app_in(class: ViewClass) -> App {
    let mut app = App::new_for_test();
    app.conn.established();
    match class {
        // `new_for_test` roots at a pod table.
        ViewClass::Table => {}
        ViewClass::Overview => {
            app.nav.reset(Element::Overview(crate::element::Overview));
        }
        ViewClass::Log => {
            app.nav.push(Element::LogSession(Box::new(LogSession::for_test(
                k9rs_proto::protocol::LogTarget::one("canarypod".to_string()), LogContainer::Default,
            ))));
        }
        ViewClass::Confirm => {
            app.nav.push(Element::Confirm(Box::new(crate::ConfirmDialog {
                message: "Delete pod/canary?".to_string(),
                action_label: "Delete".to_string(),
                pending: crate::PendingAction::Single {
                    op: crate::SingleOp::Delete,
                    target: k9rs_proto::protocol::ObjectRef::new(
                        k9rs_proto::resource_def::BuiltInKind::Pod.to_resource_id(),
                        "canary".to_string(),
                        k9rs_proto::protocol::Namespace::Named("default".to_string()),
                    ),
                },
                focused: crate::ConfirmFocus::Cancel,
            })));
        }
        // The two other dialog kinds, which this fixture used to skip — so
        // the keymap tests only ever saw a confirm box, and a test asserting
        // "a modal must ANSWER every global" and one asserting "a picker
        // DROPS `:`" could both pass while contradicting each other.
        ViewClass::Form => {
            app.nav.push(Element::Form(Box::new(crate::FormDialog {
                submit: crate::FormSubmit::Scale,
                title: "Scale deploy/api".to_string(),
                subtitle: "namespace: default".to_string(),
                target: k9rs_proto::protocol::ObjectRef::new(
                    k9rs_proto::resource_def::BuiltInKind::Deployment.to_resource_id(),
                    "api".to_string(),
                    k9rs_proto::protocol::Namespace::Named("default".to_string()),
                ),
                fields: vec![crate::FormFieldState {
                    name: "replicas".to_string(),
                    label: "Replicas".to_string(),
                    kind: crate::FormFieldKind::Number { min: 0, max: 100 },
                    value: crate::text_input::TextInput::seeded("3"),
                }],
                focused: crate::FormFocus::Field(0),
            })));
        }
        ViewClass::Picker => {
            app.nav.push(Element::Picker(Box::new(crate::ContainerPicker {
                target: k9rs_proto::protocol::ObjectRef::new(
                    k9rs_proto::resource_def::BuiltInKind::Pod.to_resource_id(),
                    "canary".to_string(),
                    k9rs_proto::protocol::Namespace::Named("default".to_string()),
                ),
                choices: vec![crate::ContainerChoice {
                    label: "web".into(),
                    container: LogContainer::Named("web".into()),
                }],
                selected: 0,
                action: crate::ContainerAction::Logs,
            })));
        }
        ViewClass::Detail => {
            app.nav.push(Element::ContentView(
                crate::element::ContentView::new(
                    crate::element::ContentSpec::Describe(k9rs_proto::protocol::ObjectRef::new(
                    k9rs_proto::resource_def::BuiltInKind::Pod.to_resource_id(),
                    "some-pod".to_string(),
                    k9rs_proto::protocol::Namespace::Named("default".to_string()),
                )),
                    crate::ContentViewState::default(),
                    crate::element::ContentPhase::Ready,
                ),
            ));
        }
    }
    assert_eq!(
        ViewClass::of(app.nav.top()),
        class,
        "the fixture must actually be in the class it claims",
    );
    app
}

#[test]
fn every_documented_view_key_dispatches_to_the_action_it_claims() {
    for &class in ViewClass::ALL {
        let app = app_in(class);
        for binding in for_class(class) {
            let Some((key, expected)) = binding.probe else { continue };
            let got = handle_key_event(&app, key);
            assert_eq!(
                got.as_ref(),
                Some(&expected),
                "{:?}: `{}` is documented as {:?} but dispatches to {:?}",
                class,
                binding.keys,
                expected,
                got,
            );
        }
    }
}

#[test]
fn every_documented_global_key_works_in_every_view() {
    // A "global" key that only works in some views is the drift this pins:
    // help shows one list for the whole app, so each entry has to hold
    // everywhere. `/` is the deliberate exception — detail and log views
    // intercept it as search — so it is checked per class below.
    for &class in ViewClass::ALL {
        let app = app_in(class);
        // A MODAL class is the one real exception, and it is a stronger
        // statement than "skip": every global must be CAPTURED there, not
        // leaked to the view underneath. A confirm box whose `:` opened the
        // command prompt behind it would be answering a question you could
        // no longer see.
        //
        // CAPTURED, not necessarily answered. This also asserted
        // `got.is_some()` — "and must answer it, not drop it" — which is
        // true of the confirm box (its safety net dismisses on any key) and
        // false of the other two: a form's characters belong to its typing
        // path and a picker simply ignores what it has no row for, both of
        // which reach the action layer as `None`. The fixture only ever
        // built a confirm box, so the over-strong half was never exercised
        // — while `tests/handler.rs` separately pinned the OPPOSITE for a
        // picker (`handle_key_event(&app, ':')` is `None`). Two universal
        // rules, contradicting, both green.
        if app.nav.top().captures_all_input() {
            for binding in global() {
                let Some((key, expected)) = binding.probe else { continue };
                assert_ne!(
                    handle_key_event(&app, key).as_ref(),
                    Some(&expected),
                    "{class:?}: a modal element must capture global `{}`, not pass it through",
                    binding.keys,
                );
            }
            continue;
        }
        for binding in global() {
            let Some((key, expected)) = binding.probe else { continue };
            if key.code == crossterm::event::KeyCode::Char('/') {
                continue;
            }
            // Overview has no marks and no table, but the global chords are
            // still emitted there — the ACTION layer decides whether they do
            // anything, which is the codebase's standing rule.
            let got = handle_key_event(&app, key);
            assert_eq!(
                got.as_ref(),
                Some(&expected),
                "{:?}: global `{}` is documented as {:?} but dispatches to {:?}",
                class,
                binding.keys,
                expected,
                got,
            );
        }
    }
}

#[test]
fn slash_means_filter_in_tables_and_search_in_content() {
    // The one documented global with a per-class meaning. Help says
    // "Filter (search, in detail and log views)"; this pins both halves.
    let table = app_in(ViewClass::Table);
    assert_eq!(
        handle_key_event(&table, crossterm::event::KeyEvent::from(
            crossterm::event::KeyCode::Char('/')
        )),
        Some(crate::actions::Action::Filter(String::new())),
    );
    for class in [ViewClass::Detail, ViewClass::Log] {
        let app = app_in(class);
        assert_eq!(
            handle_key_event(&app, crossterm::event::KeyEvent::from(
                crossterm::event::KeyCode::Char('/')
            )),
            Some(crate::actions::Action::SearchStart),
            "{class:?} intercepts `/` as search",
        );
    }
}

#[test]
fn the_keys_that_used_to_be_documented_nowhere_are_documented_now() {
    // `J`, `U` and `~` were bound in `handle_resource_view_keys` and appeared
    // in neither the help dump nor any hint bar. Surfaced by the 2026-07-20
    // help review; this is the pin that keeps them surfaced.
    let documented: Vec<&str> = for_class(ViewClass::Table).iter().map(|b| b.keys).collect();
    for key in ["J", "U", "~"] {
        assert!(
            documented.contains(&key),
            "`{key}` is bound but missing from the keymap: {documented:?}",
        );
    }
}

#[test]
fn a_hint_bar_is_a_subset_of_what_help_shows() {
    // The bars used to be four independent hand-written slices, free to
    // advertise a key help had never heard of. They project from the same
    // declaration now, so this holds by construction — pinned because the
    // construction is the point.
    //
    // A hint may now cover a RUN of keys (bindings sharing one bar label),
    // which is how seven `since` keys became `<0-6> range` instead of 44
    // columns of the bar. So the check is that each hint IS the derivation of
    // its run, which can only be true if both endpoints are real bindings.
    for &class in ViewClass::ALL {
        let bindings = for_class(class);
        let hints = bar_hints(class);
        let labels: Vec<&str> = bindings.iter().filter_map(|b| b.bar_label).collect();

        // One hint per RUN of equal labels, in order.
        let mut runs: Vec<&str> = Vec::new();
        for l in &labels {
            if runs.last() != Some(l) {
                runs.push(l);
            }
        }
        assert_eq!(
            hints.iter().map(|(_, l)| *l).collect::<Vec<_>>(),
            runs,
            "{class:?}: the bar is one hint per run of bar labels",
        );

        for (shown, label) in &hints {
            let members: Vec<&str> = bindings
                .iter()
                .filter(|b| b.bar_label == Some(*label))
                .map(|b| b.keys)
                .collect();
            let expect = match (members.first(), members.len()) {
                (Some(only), 1) => (*only).to_string(),
                (Some(first), _) => format!("{}-{}", first, members.last().unwrap()),
                (None, _) => unreachable!("every hint came from a binding"),
            };
            assert_eq!(
                shown, &expect,
                "{class:?}: hint `{label}` must name its own run's keys",
            );
            // A run is printed as `first-last`, which only reads as a range
            // for single-character keys. `Ctrl-d-Ctrl-k` would be nonsense,
            // so grouping is restricted to the keys it suits.
            if members.len() > 1 {
                for k in &members {
                    assert_eq!(
                        k.chars().count(),
                        1,
                        "{class:?}: `{k}` is grouped under `{label}` but is not a single key",
                    );
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// The REVERSE pin: every key the dispatcher answers is documented
// ---------------------------------------------------------------------------
//
// The agreement test above walks declared → dispatched. It structurally
// cannot see the other direction, and that is where keys go missing: Esc in
// a detail view was answered by `handle_detail_view_keys` and declared
// nowhere, while both sibling classes document theirs ("q / Esc" for Log,
// "q / Esc" for Table). `?` help and the hint bar showed `q` alone.
//
// So: sweep every chord a terminal can send, ask the dispatcher, and require
// anything it ANSWERS to appear in that class's declaration or in the
// globals. The comparison is on a normalized token, parsed out of the
// display strings — strictly, so a display format this does not understand
// fails the test rather than silently covering nothing.

/// The token a class declares when it answers anything — see
/// [`super::ANY_OTHER_KEY`]. Not a chord any key normalizes to, so it can
/// only come from a declaration.
const CATCH_ALL: &str = "<any>";

/// A chord as a comparable token: `"q"`, `"esc"`, `"ctrl-d"`, `"left"`.
fn token_of(key: &KeyEvent) -> String {
    use crossterm::event::KeyCode as C;
    let named = |n: &str| n.to_string();
    if key.modifiers.contains(KeyModifiers::CONTROL) {
        return match key.code {
            C::Char(' ') => "ctrl-space".to_string(),
            C::Char(c) => format!("ctrl-{}", c.to_ascii_lowercase()),
            other => format!("ctrl-{other:?}").to_lowercase(),
        };
    }
    match key.code {
        C::Char(' ') => named("space"),
        C::Char(c) => c.to_string(),
        C::Esc => named("esc"),
        C::Enter => named("enter"),
        C::Tab => named("tab"),
        C::BackTab => named("backtab"),
        C::Backspace => named("backspace"),
        C::Delete => named("delete"),
        C::Insert => named("insert"),
        C::Left => named("left"),
        C::Right => named("right"),
        C::Up => named("up"),
        C::Down => named("down"),
        C::Home => named("home"),
        C::End => named("end"),
        C::PageUp => named("pgup"),
        C::PageDown => named("pgdn"),
        other => format!("{other:?}").to_lowercase(),
    }
}

/// Parse one display string (`"g / Home"`, `"Ctrl-d / Ctrl-u"`) into tokens.
///
/// Returns `None` for a row that does not document a chord at all — the
/// command-word rows (`":ctx <name>"`). Panics on a token it does not
/// recognise, deliberately: an unparsed token would silently widen what this
/// test considers documented.
fn tokens_of(display: &str) -> Option<Vec<String>> {
    if display.contains('<') {
        return None;
    }
    let mut out = Vec::new();
    for raw in display.split(" / ") {
        let t = raw.trim();
        let token = match t {
            "Esc" => "esc".to_string(),
            "Enter" => "enter".to_string(),
            "Tab" => "tab".to_string(),
            "S-Tab" => "backtab".to_string(),
            "Space" => "space".to_string(),
            "PgDn" => "pgdn".to_string(),
            "PgUp" => "pgup".to_string(),
            "Home" => "home".to_string(),
            "End" => "end".to_string(),
            "\u{2190}" => "left".to_string(),
            "\u{2192}" => "right".to_string(),
            "\u{2191}" => "up".to_string(),
            "\u{2193}" => "down".to_string(),
            "Ctrl-Space" => "ctrl-space".to_string(),
            // A class that declares it answers EVERY remaining key. One
            // class does (the confirm box's safety net) and it says so.
            super::ANY_OTHER_KEY => CATCH_ALL.to_string(),
            _ => {
                if let Some(rest) = t.strip_prefix("Ctrl-") {
                    let mut cs = rest.chars();
                    let c = cs.next().expect("Ctrl- names a key");
                    assert!(cs.next().is_none(), "unparsed Ctrl chord in {display:?}");
                    format!("ctrl-{}", c.to_ascii_lowercase())
                } else if let Some(rest) = t.strip_prefix("Shift-") {
                    // A TRIPWIRE now, not a parser. No display string in the
                    // keymap spells a shifted letter this way any more:
                    // `KeyCombo::label` emits the bare capital because that
                    // is also the valid `keys:` spec (`Shift-f` is not one,
                    // and help used to advertise it), and the one
                    // hand-written `Shift-C` row was the odd one out.
                    panic!(
                        "{display:?} spells a shifted key as `Shift-{rest}` — write it as \
                         the capital `{}`, which is what `label()` emits and what a \
                         `keys:` entry takes",
                        rest.to_uppercase(),
                    );
                } else {
                    let mut cs = t.chars();
                    let c = cs.next().unwrap_or_else(|| panic!("empty token in {display:?}"));
                    assert!(
                        cs.next().is_none(),
                        "unrecognised key token {t:?} in {display:?} — teach `tokens_of` \
                         about it rather than leaving it uncovered",
                    );
                    c.to_string()
                }
            }
        };
        out.push(token);
    }
    Some(out)
}

/// Every chord worth asking about: the printable ASCII a terminal sends as
/// `Char`, the named keys, and Ctrl over the letters.
fn candidate_chords() -> Vec<KeyEvent> {
    use crossterm::event::KeyCode as C;
    let mut out = Vec::new();
    for c in 0x20u8..0x7f {
        out.push(KeyEvent::new(C::Char(c as char), KeyModifiers::NONE));
    }
    for c in b'a'..=b'z' {
        out.push(KeyEvent::new(C::Char(c as char), KeyModifiers::CONTROL));
    }
    for code in [
        C::Esc, C::Enter, C::Tab, C::BackTab, C::Backspace, C::Delete, C::Insert,
        C::Left, C::Right, C::Up, C::Down, C::Home, C::End, C::PageUp, C::PageDown,
    ] {
        out.push(KeyEvent::new(code, KeyModifiers::NONE));
    }
    out
}

#[test]
fn every_key_the_dispatcher_answers_is_documented() {
    let cfg = crate::KeysConfig::default();
    let mut undocumented: Vec<(ViewClass, String, String)> = Vec::new();
    for &class in ViewClass::ALL {
        let mut declared: std::collections::HashSet<String> = std::collections::HashSet::new();
        for binding in for_class(class).iter().chain(global().iter()) {
            match tokens_of(&binding.display_keys(&cfg)) {
                Some(tokens) => declared.extend(tokens),
                // A row documenting a command word, not a chord.
                None => assert!(
                    binding.probe.is_none(),
                    "{class:?}: {:?} has a probe but no parseable chord",
                    binding.keys,
                ),
            }
        }
        let app = app_in(class);
        // A table's OPERATION keys are documented per resource rather than
        // per class — `?` help derives them from the row's declared
        // capabilities, which is why they are not in `for_class`. Derived
        // here through the same two calls help uses, so this is not a third
        // copy of the rule.
        for op in &app.current_capabilities().operations {
            let chord = cfg
                .op_key(op)
                .or_else(|| crate::keymap::structural_chord(op));
            if let Some(chord) = chord {
                declared.insert(token_of(&chord.as_key_event()));
            }
        }
        for key in candidate_chords() {
            let Some(action) = handle_key_event(&app, key) else { continue };
            let token = token_of(&key);
            if !declared.contains(&token) && !declared.contains(CATCH_ALL) {
                undocumented.push((class, token, format!("{action:?}")));
            }
        }
    }
    assert!(
        undocumented.is_empty(),
        "keys answered but documented nowhere — add the row, or stop answering:\n{}",
        undocumented
            .iter()
            .map(|(c, k, a)| format!("  {c:?}: {k:?} -> {a}"))
            .collect::<Vec<_>>()
            .join("\n"),
    );
}

/// A HALF-bound column pair is named, not swallowed.
///
/// `col_move_label` was a two-armed match: both bound, or the bare arrows.
/// So `colRight: l` alone — which validates and works — left help showing
/// only `← / →` and the chord undocumented.
#[test]
fn a_half_bound_column_pair_still_names_its_chord() {
    use crate::KeyCombo;
    let row = |keys: &crate::KeysConfig| {
        for_class(ViewClass::Table)
            .into_iter()
            .find(|b| b.label == "Column cursor left / right")
            .expect("the column row is declared")
            .display_keys(keys)
    };

    let none = crate::KeysConfig::default();
    assert_eq!(row(&none), "\u{2190} / \u{2192}", "unbound: the arrows alone");

    let right_only = crate::KeysConfig { col_right: Some(KeyCombo::plain('l')), ..none };
    assert!(row(&right_only).contains('l'), "got {:?}", row(&right_only));

    let left_only = crate::KeysConfig { col_left: Some(KeyCombo::plain('h')), ..none };
    assert!(row(&left_only).contains('h'), "got {:?}", row(&left_only));

    let both = crate::KeysConfig {
        col_left: Some(KeyCombo::plain('h')),
        col_right: Some(KeyCombo::plain('l')),
        ..none
    };
    let shown = row(&both);
    assert!(shown.contains('h') && shown.contains('l'), "got {shown:?}");
}

/// The first/last-column jump appears in help whether or not it is bound —
/// it has no structural key in a table, so without a row the action was
/// invisible, and a user could not learn that `colFirst` existed from `?`.
///
/// Unbound, the row still has to SAY how, and it has to say it somewhere the
/// sheet will actually render. The key read `<bind colFirst / colLast>` —
/// 25 cells against help's 16-cell KEY cap, so the one row whose whole job
/// is to name what to bind was the one row clipped, to `<bind colFirst`.
/// The names live in the DESCRIPTION now, and this checks both halves: the
/// row names them, and the key fits.
#[test]
fn the_column_jump_is_documented_bound_or_not() {
    use crate::KeyCombo;
    let binding = |keys: &crate::KeysConfig| {
        let b = for_class(ViewClass::Table)
            .into_iter()
            .find(|b| b.label.starts_with("Jump to the first / last column"))
            .expect("the jump row is declared");
        (b.display_keys(keys), b.label)
    };
    let none = crate::KeysConfig::default();
    let (key, label) = binding(&none);
    assert!(
        format!("{key} {label}").contains("colFirst"),
        "unbound, the row must say how: {key:?} / {label:?}",
    );

    // …and the KEY has to fit the column help declares for it, or the
    // instruction is truncated away. Cap read from the declaration so the
    // two cannot drift.
    let cap = k9rs_proto::local_types::LocalResourceKind::Keybinding
        .column_defs()
        .iter()
        .find(|c| c.header == "KEY")
        .and_then(|c| c.max_width)
        .expect("help declares a KEY column with a ceiling");
    for keys in [
        none,
        crate::KeysConfig { col_first: Some(KeyCombo::plain('0')), ..none },
        crate::KeysConfig { col_last: Some(KeyCombo::plain('$')), ..none },
    ] {
        let (key, _) = binding(&keys);
        assert!(
            unicode_width::UnicodeWidthStr::width(key.as_str()) <= usize::from(cap),
            "the jump row's key {key:?} is wider than help's {cap}-cell KEY column,              so it renders clipped",
        );
    }

    let bound = crate::KeysConfig {
        col_first: Some(KeyCombo::plain('0')),
        col_last: Some(KeyCombo::plain('$')),
        ..none
    };
    assert_eq!(binding(&bound).0, "0 / $");
}
