//! The agreement pin: every documented key must actually do what it says.
//!
//! Without this, `keymap.rs` is just a seventh hand-written copy that LOOKS
//! authoritative. With it, a key that gets rebound or removed breaks the build
//! instead of quietly lying in `?` help and in the bottom bar.

use super::*;
use crate::app::element::{Element, LogSession};
use crate::app::App;
use crate::event::handler::handle_key_event;
use crate::kube::protocol::LogContainer;

/// Put the app into a state whose top element has the given class.
fn app_in(class: ViewClass) -> App {
    let mut app = App::new_for_test();
    app.conn.established();
    match class {
        // `new_for_test` roots at a pod table.
        ViewClass::Table => {}
        ViewClass::Overview => {
            app.nav.reset(Element::Overview(crate::app::element::Overview));
        }
        ViewClass::Log => {
            app.nav.push(Element::LogSession(Box::new(LogSession::for_test(
                crate::kube::protocol::LogTarget::One("canarypod".to_string()), LogContainer::Default,
            ))));
        }
        ViewClass::Detail => {
            app.nav.push(Element::ContentView(
                crate::app::element::ContentView::new(
                    crate::app::element::ContentSpec::Aliases,
                    crate::app::ContentViewState::default(),
                    crate::app::element::ContentPhase::Ready,
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

const CLASSES: [ViewClass; 4] =
    [ViewClass::Overview, ViewClass::Table, ViewClass::Detail, ViewClass::Log];

#[test]
fn every_documented_view_key_dispatches_to_the_action_it_claims() {
    for class in CLASSES {
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
    for class in CLASSES {
        let app = app_in(class);
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
        Some(crate::app::actions::Action::Filter(String::new())),
    );
    for class in [ViewClass::Detail, ViewClass::Log] {
        let app = app_in(class);
        assert_eq!(
            handle_key_event(&app, crossterm::event::KeyEvent::from(
                crossterm::event::KeyCode::Char('/')
            )),
            Some(crate::app::actions::Action::SearchStart),
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
    for class in CLASSES {
        let all: Vec<&str> = for_class(class).iter().map(|b| b.keys).collect();
        for (key, _) in bar_hints(class) {
            assert!(all.contains(&key), "{class:?}: bar key `{key}` is not in the keymap");
        }
    }
}
