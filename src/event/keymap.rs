//! The ONE description of what keys do — projected into `?` help and into
//! the bottom hint bars.
//!
//! # Why this exists
//!
//! Everything load-bearing in this codebase pins its parallel descriptions:
//! the select-gate's exhaustive match, the `gate_agrees_with_batch_support`
//! manifest test, the wire golden tags. The two user-facing key-doc surfaces
//! were the exception — `help.rs` held a 250-line hand-written dump, and four
//! view files each held their own hand-written hint slice. Six copies of the
//! keymap, none checked against the dispatcher, and they had drifted:
//!
//! - `J` (jump to owner), `U` (used-by) and `~` (column filter) were bound and
//!   documented NOWHERE.
//! - Help listed a context-free `s` and `Ctrl-d`, which mean different things
//!   in a table than in a log or a detail view — so the doc was wrong in at
//!   least one view no matter which view you were in.
//! - Help advertised `Ctrl-l → toggle full-fetch mode`, a feature that never
//!   existed (deleted in an earlier pass).
//!
//! # How it stays true
//!
//! Each [`Binding`] carries the [`Action`] its chord must produce. The
//! agreement test feeds every chord through the REAL dispatcher
//! ([`super::handler::handle_key_event`]) for its view class and asserts the
//! declared action comes back. A binding that stops working, or a chord that
//! gets rebound, fails the test rather than silently lying in the help.
//!
//! Bindings whose action depends on runtime state (`q` quits or pops
//! depending on drill depth; `S` sorts by whatever column the cursor is on)
//! carry `action: None` — they are documented but not mechanically probed,
//! and that is visible here rather than assumed.
//!
//! # What is NOT here
//!
//! Per-resource operations (`d`escribe, `y`aml, scale, restart, port-forward…)
//! already had a correct data-driven path: `caps.operations` × the effective
//! `KeysConfig` binding × `descriptor().label`. That path is untouched; help
//! renders it as its own section. This module covers the STRUCTURAL keys,
//! which are the ones that had no source of truth.

use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};

use crate::app::actions::Action;
use crate::app::element::Element;

/// The key-dispatch equivalence classes. These are not the eight `Element`
/// kinds and not the draw match's view kinds — they are exactly the arms of
/// the per-kind match in [`super::handler::handle_key_event`], so a view's
/// documented keys cannot describe a different keymap than the one that runs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ViewClass {
    /// The startup page.
    Overview,
    /// Every table: resource lists, row filters, derived rows, contexts.
    Table,
    /// YAML / describe / aliases.
    Detail,
    /// A log stream or a grep over one.
    Log,
}

impl ViewClass {
    /// The class of the element currently on top. THE mapping — the key
    /// dispatcher calls this too, so "which keymap is live" and "which keymap
    /// is documented" are one answer.
    pub fn of(top: &Element) -> Self {
        // DERIVED from `ElementClass`, not a second partition of the same
        // kinds. The two were independent, wildcard-free matches over the
        // identical 7 kinds — so both compiled, both were covered by tests,
        // and nothing checked they AGREED. A new kind could be
        // `ElementClass::Content` (satisfying the conformance test's
        // "not a table" assertions) and `ViewClass::Table` (so `?` documents
        // `/`, `S`, Space and the column keys at it, and every one of them
        // is a silent no-op through `table_interaction_mut()? → return`).
        // That is the contexts-view bug verbatim, one indirection above
        // where `ElementClass` was introduced to catch it.
        //
        // The names stay distinct because the questions are: `ElementClass`
        // is about data shape, `ViewClass` about which keymap runs.
        match top.class() {
            crate::app::element::ElementClass::Table => ViewClass::Table,
            crate::app::element::ElementClass::Log => ViewClass::Log,
            crate::app::element::ElementClass::Content => ViewClass::Detail,
            crate::app::element::ElementClass::Chrome => ViewClass::Overview,
        }
    }

    /// Section heading for this class in `?` help.
    pub fn title(self) -> &'static str {
        match self {
            ViewClass::Overview => "Overview",
            ViewClass::Table => "This view (table)",
            ViewClass::Detail => "This view (YAML / describe)",
            ViewClass::Log => "This view (logs)",
        }
    }
}

/// One documented key.
pub struct Binding {
    /// What to print, e.g. `"j / ↓"`. Several chords may share one row.
    pub keys: &'static str,
    pub label: &'static str,
    /// The chord the agreement test probes, and the action it must yield.
    /// `None` for keys whose action depends on runtime state — documented,
    /// deliberately not probed.
    pub probe: Option<(KeyEvent, Action)>,
    /// Short label for the one-line hint bar at the bottom of a view, or
    /// `None` to keep this key out of the bar. Separate from `label` because
    /// the two surfaces have opposite budgets: help has a column and wants a
    /// sentence, the bar has a shared line and wants a word.
    pub bar_label: Option<&'static str>,
    /// For chords the user can extend via `keys:` config: the EFFECTIVE label
    /// to print instead of `keys`. The probe still targets the structural
    /// chord (arrows always work), so the declaration stays checkable while
    /// the display stays truthful about the user's own bindings.
    pub effective_label: Option<fn(&crate::app::KeysConfig) -> String>,
}

impl Binding {
    /// What to print for this binding under the given config.
    pub fn display_keys(&self, keys: &crate::app::KeysConfig) -> String {
        match self.effective_label {
            Some(f) => f(keys),
            None => self.keys.to_string(),
        }
    }
}

/// Column-cursor movement: the arrows, plus the user's configured chords when
/// both are bound.
fn col_move_label(keys: &crate::app::KeysConfig) -> String {
    match (keys.col_left, keys.col_right) {
        (Some(l), Some(r)) => format!("{} / {} (or \u{2190}/\u{2192})", l.label(), r.label()),
        _ => "\u{2190} / \u{2192}".to_string(),
    }
}

fn plain(c: char) -> KeyEvent {
    KeyEvent::new(KeyCode::Char(c), KeyModifiers::NONE)
}

fn ctrl(c: char) -> KeyEvent {
    KeyEvent::new(KeyCode::Char(c), KeyModifiers::CONTROL)
}

fn code(c: KeyCode) -> KeyEvent {
    KeyEvent::new(c, KeyModifiers::NONE)
}

/// Shorthand for a probed, help-only binding.
fn b(keys: &'static str, label: &'static str, key: KeyEvent, action: Action) -> Binding {
    Binding { keys, label, probe: Some((key, action)), bar_label: None, effective_label: None }
}

/// Shorthand for a probed binding that also earns a slot in the hint bar,
/// under its own short label.
fn bar(
    keys: &'static str,
    label: &'static str,
    short: &'static str,
    key: KeyEvent,
    action: Action,
) -> Binding {
    Binding { keys, label, probe: Some((key, action)), bar_label: Some(short), effective_label: None }
}

/// Shorthand for a documented-but-unprobed binding (runtime-dependent action).
fn doc(keys: &'static str, label: &'static str) -> Binding {
    Binding { keys, label, probe: None, bar_label: None, effective_label: None }
}

/// Keys that work in every view, from `handle_global_keys`.
pub fn global() -> Vec<Binding> {
    vec![
        b(":", "Command mode", plain(':'), Action::CommandMode),
        b("/", "Filter (search in detail/log)", plain('/'), Action::Filter(String::new())),
        b("?", "Help", plain('?'), Action::Help),
        b("Ctrl-r", "Refresh", ctrl('r'), Action::Refresh),
        b("Ctrl-e", "Toggle header", ctrl('e'), Action::ToggleHeader),
        b("Ctrl-a", "Show aliases", ctrl('a'), Action::ShowAliases),
        b("Ctrl-w", "Toggle wide columns", ctrl('w'), Action::ToggleWide),
        b("Ctrl-z", "Toggle fault filter", ctrl('z'), Action::ToggleFaultFilter),
        b("Ctrl-Space", "Span-mark from nearest", ctrl(' '), Action::SpanMark),
        b("Ctrl-\\", "Clear all marks", ctrl('\\'), Action::ClearMarks),
        b("Ctrl-c", "Quit", ctrl('c'), Action::Quit),
        // Ctrl-s is SaveLogs in a log view and SaveTable everywhere else, so
        // the global row cannot name one action; the per-class rows below do.
        doc("Ctrl-s", "Save view to a file"),
        doc(":ctx <name>", "Switch context"),
        doc(":ns <name>", "Switch namespace"),
    ]
}

/// Keys for one view class, from that class's dispatch function.
pub fn for_class(class: ViewClass) -> Vec<Binding> {
    match class {
        ViewClass::Overview => vec![
            b("Tab / S-Tab", "Next / prev resource view", code(KeyCode::Tab), Action::NextTab),
            b("q", "Quit", plain('q'), Action::Quit),
        ],

        ViewClass::Table => vec![
            b("j / \u{2193}", "Move down", plain('j'), Action::NextItem),
            b("k / \u{2191}", "Move up", plain('k'), Action::PrevItem),
            b("PgDn / PgUp", "Page down / up", code(KeyCode::PageDown), Action::PageDown),
            b("g / Home", "Top", plain('g'), Action::Home),
            b("G / End", "Bottom", plain('G'), Action::End),
            Binding {
                effective_label: Some(col_move_label),
                ..b("\u{2190} / \u{2192}", "Column cursor left / right", code(KeyCode::Left), Action::ColLeft)
            },
            b("Enter", "Drill down", code(KeyCode::Enter), Action::Enter),
            doc("q / Esc", "Back / clear filter"),
            b("Tab / S-Tab", "Next / prev resource view", code(KeyCode::Tab), Action::NextTab),
            b("-", "Toggle last view", plain('-'), Action::ToggleLastView),
            b(
                "0",
                "All namespaces",
                plain('0'),
                Action::SwitchNamespace(crate::kube::protocol::Namespace::All),
            ),
            b("e", "Edit resource", plain('e'), Action::Edit),
            // These three were bound and documented NOWHERE before this file.
            b("J", "Jump to owner", plain('J'), Action::JumpToOwner),
            b("U", "Show what uses this", plain('U'), Action::UsedBy),
            b("E", "Events about this object", plain('E'), Action::ShowEvents),
            b("~", "Filter by hovered column", plain('~'), Action::ColumnFilter),
            b("f", "Show port-forwards", plain('f'), Action::ShowPortForwards),
            b("F", "Create a port-forward", plain('F'), Action::PortForward),
            b("c", "Copy", plain('c'), Action::Copy),
            b("Space", "Mark / unmark row", plain(' '), Action::ToggleMark),
            b("Ctrl-s", "Save table to a file", ctrl('s'), Action::SaveTable),
            b("O", "Sort / toggle direction", plain('O'), Action::ToggleSortDirection),
            b("N", "Sort by name", plain('N'), Action::Sort(crate::app::SortTarget::Named(crate::app::actions::COL_NAME))),
            b("A", "Sort by age", plain('A'), Action::Sort(crate::app::SortTarget::Named(crate::app::actions::COL_AGE))),
            doc("S", "Sort by cursor column"),
        ],

        ViewClass::Detail => vec![
            bar("j / k", "Scroll down / up", "scroll", plain('j'), Action::NextItem),
            b("PgDn / PgUp", "Page down / up", code(KeyCode::PageDown), Action::PageDown),
            b("Ctrl-d / Ctrl-u", "Half-page down / up", ctrl('d'), Action::PageDown),
            b("g / G", "Top / bottom", plain('g'), Action::Home),
            bar("/", "Search", "search", plain('/'), Action::SearchStart),
            bar("n / N", "Next / prev match", "next/prev", plain('n'), Action::SearchNext),
            bar("c", "Copy", "copy", plain('c'), Action::Copy),
            bar("q", "Back", "back", plain('q'), Action::Back),
        ],

        ViewClass::Log => vec![
            b("j / k", "Scroll down / up", plain('j'), Action::ScrollDown(1)),
            b("PgDn / PgUp", "Page down / up", code(KeyCode::PageDown), Action::PageDown),
            b("g / G", "Top / bottom", plain('g'), Action::Home),
            bar("s", "Toggle follow", "follow", plain('s'), Action::ToggleLogFollow),
            bar("w", "Toggle wrap", "wrap", plain('w'), Action::ToggleLogWrap),
            bar("t", "Cycle shown fields", "fields", plain('t'), Action::ToggleLogTimestamps),
            b("h / l", "Pan one column left / right", plain('h'), Action::ColLeft),
            b("H / L", "Pan a stride left / right", plain('H'), Action::PageLeft),
            b("^ / $", "Pan to the left margin / far edge", plain('^'), Action::ColFirst),
            b("Shift-C", "Clear logs", KeyEvent::new(KeyCode::Char('C'), KeyModifiers::SHIFT), Action::ClearLogs),
            b("/", "Grep (stacks)", plain('/'), Action::SearchStart),
            bar("c", "Copy", "copy", plain('c'), Action::Copy),
            b("Ctrl-s", "Save logs to a file", ctrl('s'), Action::SaveLogs),
            bar("0", "Tail", "tail", plain('0'), Action::LogSince(None)),
            bar("1", "1m", "1m", plain('1'), Action::LogSince(Some("1m".into()))),
            bar("2", "5m", "5m", plain('2'), Action::LogSince(Some("5m".into()))),
            bar("3", "15m", "15m", plain('3'), Action::LogSince(Some("15m".into()))),
            bar("4", "30m", "30m", plain('4'), Action::LogSince(Some("30m".into()))),
            bar("5", "1h", "1h", plain('5'), Action::LogSince(Some("1h".into()))),
            bar("6", "24h", "24h", plain('6'), Action::LogSince(Some("24h".into()))),
            bar("q", "Back", "back", plain('q'), Action::Back),
        ],
    }
}

/// The `(key, label)` pairs for a view's bottom hint bar — the same
/// declaration help reads, filtered to what fits on one line.
///
/// Only the detail and log views HAVE a bar; a table's key doc is `?` help,
/// which is why no `Table` binding carries a `bar_label`. Declaring one there
/// would be a label nothing renders.
pub fn bar_hints(class: ViewClass) -> Vec<(&'static str, &'static str)> {
    for_class(class)
        .into_iter()
        .filter_map(|b| b.bar_label.map(|short| (b.keys, short)))
        .collect()
}

#[cfg(test)]
#[path = "../tests/event/keymap.rs"]
mod tests;
