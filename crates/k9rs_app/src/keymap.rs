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

use crate::actions::Action;
use crate::KeyCombo;
use k9rs_proto::protocol::OperationKind;
use crate::element::Element;

/// Declares [`ViewClass`] once: the variants, their `?` help headings and
/// [`ViewClass::ALL`] all come out of the same list.
///
/// `ALL` used to be a hand-written `[ViewClass; 5]` in a test mirror, and
/// nothing forced it to stay exhaustive. A sixth class would break [`of`]
/// and [`title`] — both exhaustive matches — but drop silently out of every
/// keymap agreement test, which is `BuiltInKind::ALL`'s failure verbatim one
/// layer up: a list that claims totality because someone wrote it that way
/// once.
///
/// [`of`]: ViewClass::of
/// [`title`]: ViewClass::title
macro_rules! view_classes {
    ($(#[$m:meta])* pub enum $name:ident {
        $($(#[$vm:meta])* $variant:ident => $title:literal),* $(,)?
    }) => {
        $(#[$m])*
        #[derive(Debug, Clone, Copy, PartialEq, Eq)]
        pub enum $name { $($(#[$vm])* $variant),* }

        impl $name {
            /// Every class, generated from the enum's own declaration.
            pub const ALL: &'static [$name] = &[$($name::$variant),*];

            /// Section heading for this class in `?` help.
            pub fn title(self) -> &'static str {
                match self { $($name::$variant => $title),* }
            }
        }
    };
}

view_classes! {
    /// The key-dispatch equivalence classes. These are not the eight `Element`
    /// kinds and not the draw match's view kinds — they are exactly the arms of
    /// the per-kind match in [`super::handler::handle_key_event`], so a view's
    /// documented keys cannot describe a different keymap than the one that runs.
    pub enum ViewClass {
        /// The startup page.
        Overview => "Overview",
        /// Every table: resource lists, row filters, derived rows, contexts.
        Table => "This view (table)",
        /// YAML / describe / aliases.
        Detail => "This view (YAML / describe)",
        /// A log stream or a grep over one.
        Log => "This view (logs)",
        /// A yes/no dialog floating over the view that raised it.
        Dialog => "This dialog",
    }
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
            crate::element::ElementClass::Table => ViewClass::Table,
            crate::element::ElementClass::Log => ViewClass::Log,
            crate::element::ElementClass::Content => ViewClass::Detail,
            crate::element::ElementClass::Chrome => ViewClass::Overview,
            crate::element::ElementClass::Dialog => ViewClass::Dialog,
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
    pub effective_label: Option<fn(&crate::KeysConfig) -> String>,
}

impl Binding {
    /// What to print for this binding under the given config.
    pub fn display_keys(&self, keys: &crate::KeysConfig) -> String {
        match self.effective_label {
            Some(f) => f(keys),
            None => self.keys.to_string(),
        }
    }
}

/// Column-cursor movement: the arrows, plus the user's configured chords when
/// both are bound.
fn col_move_label(keys: &crate::KeysConfig) -> String {
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
                Action::SwitchNamespace(k9rs_proto::protocol::Namespace::All),
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
            b("N", "Sort by name", plain('N'), Action::Sort(crate::SortTarget::Named(crate::actions::COL_NAME))),
            b("A", "Sort by age", plain('A'), Action::Sort(crate::SortTarget::Named(crate::actions::COL_AGE))),
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

        ViewClass::Dialog => vec![
            bar("y", "Confirm", "yes", plain('y'), Action::Confirm),
            bar("n / Esc", "Cancel", "no", plain('n'), Action::Cancel),
            b("\u{2190} / \u{2192} / Tab", "Switch button", code(KeyCode::Tab), Action::ToggleDialogButton),
            b("Enter", "Activate the focused button", code(KeyCode::Enter), Action::Cancel),
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
            b(
                "/",
                "Grep — stacks; `!pat` EXCLUDES, `\\!` for a literal `!`",
                plain('/'),
                Action::SearchStart,
            ),
            bar("c", "Copy", "copy", plain('c'), Action::Copy),
            b("Ctrl-s", "Save logs to a file", ctrl('s'), Action::SaveLogs),
            bar("0", "Tail", "range", plain('0'), Action::LogRange(crate::actions::LogRange::Tail)),
            bar("1", "1m", "range", plain('1'), Action::LogRange(crate::actions::LogRange::Since("1m".into()))),
            bar("2", "5m", "range", plain('2'), Action::LogRange(crate::actions::LogRange::Since("5m".into()))),
            bar("3", "15m", "range", plain('3'), Action::LogRange(crate::actions::LogRange::Since("15m".into()))),
            bar("4", "30m", "range", plain('4'), Action::LogRange(crate::actions::LogRange::Since("30m".into()))),
            bar("5", "1h", "range", plain('5'), Action::LogRange(crate::actions::LogRange::Since("1h".into()))),
            bar("6", "24h", "range", plain('6'), Action::LogRange(crate::actions::LogRange::Since("24h".into()))),
            // The end of the ladder. Documented as its own row because "from
            // the beginning" is not a duration — which is exactly why the
            // old `Option<String>` payload could not express it, and why the
            // oldest logs were unreachable past 24h.
            bar(
                "7",
                "Everything (oldest first, stops when the buffer is full)",
                "range",
                plain('7'),
                Action::LogRange(crate::actions::LogRange::Head),
            ),
            // Esc documented alongside `q` because in a LOG view `Back` peels
            // one grep off the chain (a committed grep IS an element), and
            // the chain was the one thing here with no stated way to undo it.
            // The bar still shows just `<q> back`: the label is the display
            // string, `bar_label` is the bar's.
            bar("q / Esc", "Back — pops one grep off the chain", "back", plain('q'), Action::Back),
        ],
    }
}

/// The `(key, label)` pairs for a view's bottom hint bar — the same
/// declaration help reads, filtered to what fits on one line.
///
/// Only the detail and log views HAVE a bar; a table's key doc is `?` help,
/// which is why no `Table` binding carries a `bar_label`. Declaring one there
/// would be a label nothing renders.
/// The hint-bar projection of a class's keymap.
///
/// Consecutive bindings that share a bar label are ONE hint, printed as the
/// range of their keys: the seven `since` bindings are `0`…`6` under "range",
/// so the bar says `<0-6> range` where it used to spend 44 of its columns
/// listing `tail`, `1m`, `5m`, `15m`, `30m`, `1h`, `24h` one at a time. Help
/// still documents each key with its own duration, because help has a column
/// per row and the bar has a shared line — the same split `bar_label` already
/// exists for, applied to a group instead of a key.
///
/// Grouping is DECLARED (by repeating the label), not inferred, and the keys
/// are printed first-to-last, so a run must be contiguous and in order. That
/// is a property of the list above, where it is visible.
pub fn bar_hints(class: ViewClass) -> Vec<(String, &'static str)> {
    let mut out: Vec<(String, &'static str)> = Vec::new();
    for b in for_class(class) {
        let Some(short) = b.bar_label else { continue };
        match out.last_mut() {
            Some((keys, label)) if *label == short => {
                // Extend the run: keep the first key, re-point the last.
                let first = keys.split('-').next().unwrap_or(keys).to_string();
                *keys = format!("{}-{}", first, b.keys);
            }
            _ => out.push((b.keys.to_string(), short)),
        }
    }
    out
}

#[cfg(test)]
#[path = "tests/keymap.rs"]
mod tests;

// ---------------------------------------------------------------------------
// Structural chords
// ---------------------------------------------------------------------------

/// The fixed chord that reaches an operation when it has no descriptor
/// default key — `None` for everything that does.
///
/// Three operations are structural: `Delete`, `ForceKill` and `PortForward`
/// are destructive or modal enough that they are NOT on a bare letter. Their
/// chords were written as the strings `"Ctrl-d"`, `"Ctrl-k"` and `"Shift-F"`
/// in two places in `help.rs` AND matched separately in `super::handler` —
/// three copies of one fact, none of them the authority, and `Shift-F` was
/// the one the handler did not gate on the resource declaring the operation
/// at all.
///
/// Returns a [`KeyCombo`] rather than a string so the handler can compare
/// against a key event and help can print it, from the same declaration.
///
/// It lived as `OperationKind::structural_chord` in `kube::protocol` — the
/// WIRE module — which made that module depend on `app::KeyCombo` and was the
/// last upward reference out of `kube`. It is here rather than `KeyCombo`
/// being pushed down because every caller was already in `ui` or `event`, and
/// a chord is a keybinding fact, not a wire fact. This file is where keys are
/// declared, which is what its own doc already claimed.
///
/// The match stays EXHAUSTIVE over `OperationKind`, and exhaustiveness is
/// checked wherever the match is written — so a new operation still has to
/// say which kind of key reaches it, exactly as before the move.
pub fn structural_chord(op: &OperationKind) -> Option<KeyCombo> {
    match op {
        OperationKind::Delete => Some(KeyCombo { ctrl: true, ch: 'd' }),
        OperationKind::ForceKill => Some(KeyCombo { ctrl: true, ch: 'k' }),
        OperationKind::PortForward => Some(KeyCombo::plain('F')),
        // Everything else is reached by its descriptor's default key, or by a
        // chord the user configured.
        OperationKind::Describe
        | OperationKind::Yaml
        | OperationKind::Scale
        | OperationKind::StreamLogs
        | OperationKind::PreviousLogs
        | OperationKind::Shell
        | OperationKind::Restart
        | OperationKind::ShowNode
        | OperationKind::NodeShell
        | OperationKind::DecodeSecret
        | OperationKind::TriggerCronJob
        | OperationKind::ToggleSuspendCronJob
        | OperationKind::Apply
        | OperationKind::Custom(_) => None,
    }
}
