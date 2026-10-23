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
                k9rs_proto::protocol::LogTarget::One("canarypod".to_string()), LogContainer::Default,
            ))));
        }
        ViewClass::Dialog => {
            app.nav.push(Element::Confirm(Box::new(crate::ConfirmDialog {
                message: "Delete pod/canary?".to_string(),
                action_label: "Delete".to_string(),
                pending: crate::PendingAction::Single {
                    op: crate::SingleOp::Delete,
                    target: k9rs_proto::protocol::ObjectRef::new(
                        crate::nav::rid(k9rs_proto::resource_def::BuiltInKind::Pod),
                        "canary".to_string(),
                        k9rs_proto::protocol::Namespace::Named("default".to_string()),
                    ),
                },
                action_focused: false,
            })));
        }
        ViewClass::Detail => {
            app.nav.push(Element::ContentView(
                crate::element::ContentView::new(
                    crate::element::ContentSpec::Describe(k9rs_proto::protocol::ObjectRef::new(
                    crate::nav::rid(k9rs_proto::resource_def::BuiltInKind::Pod),
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
        // no longer see — and its "any other key cancels" safety net, which
        // exists so a spurious keystroke dismisses the box, would be dead.
        if app.nav.top().captures_all_input() {
            for binding in global() {
                let Some((key, expected)) = binding.probe else { continue };
                let got = handle_key_event(&app, key);
                assert_ne!(
                    got.as_ref(),
                    Some(&expected),
                    "{class:?}: a modal element must capture global `{}`, not pass it through",
                    binding.keys,
                );
                assert!(got.is_some(), "{class:?}: …and must answer it, not drop it");
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
