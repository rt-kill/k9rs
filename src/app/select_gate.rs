//! Select mode — the vim-like mode the app is in while rows are marked.
//!
//! The mode is DERIVED, never stored: the app is in select mode iff the
//! top element has marks (`App::select_mode`). A stored flag could
//! desync from the marked set (flag on, zero marks) at any of the five
//! mark-mutating write paths; deriving makes that state unrepresentable.
//!
//! Enforcement is ONE choke point at the Action layer, applied after
//! key→Action mapping and before dispatch. Gating actions (not keys)
//! closes every mapping route at once — global keys, per-view keys,
//! configurable operation bindings, and the overlay-capability
//! fall-through all funnel through it — where a per-keymap replacement
//! would have to own several mapping layers and every future binding.
//!
//! In select mode the three batch-capable operations transform into
//! their DISTINCT batch actions (`Delete` → `BatchDelete`, …) — the
//! keymap itself never produces a batch action, and the single-target
//! handlers keep no "marked set non-empty?" fallback, so single and
//! batch semantics cannot silently substitute for one another. Every
//! single-target action is dead with a teaching flash. Navigation,
//! marking, view operations, and scope-leaving resets stay live —
//! leaving the scope (Tab, `:cmd`, namespace switch) drops the store
//! and its marks by ownership, which is the sanctioned way select mode
//! ends implicitly.

use crate::app::actions::Action;
use crate::app::App;

/// Outcome of gating one action through select mode.
#[derive(Debug, PartialEq, Eq)]
pub enum Gated {
    Pass(Action),
    /// Swallowed; the message is flashed to teach the exits.
    Blocked(&'static str),
}

const DEAD_KEY: &str =
    "Single-row action — in select mode (Space toggles, Ctrl-\\ clears marks)";
const STALE_SELECT: &str =
    "Selection changed — the marked rows are gone (nothing was done)";

impl App {
    /// Derived, never stored: select mode IS "the top element has marks".
    /// `has_marks()` is already scoped to the markable kinds (store-backed
    /// lists/filters), whose marks feed batch dispatch and are pruned
    /// atomically with row removals.
    pub fn select_mode(&self) -> bool {
        self.nav.top().has_marks()
    }

    /// What the last PAINTED frame showed (recorded by the renderer) —
    /// the mode the user believes they are in.
    fn rendered_select_mode(&self) -> bool {
        self.nav
            .top()
            .table_interaction()
            .map(|i| i.rendered_select_mode)
            .unwrap_or(false)
    }
}

/// Gate an action through select mode. Called once, between key→Action
/// mapping and dispatch.
///
/// The match is EXHAUSTIVE with no wildcard: adding an `Action` variant
/// forces a select-mode decision here at compile time.
pub fn gate_action(app: &App, action: Action) -> Gated {
    if !app.select_mode() {
        // Race guard: marks can empty ASYNCHRONOUSLY (a delta pruning the
        // last marked row) and repaints coalesce, so a keypress aimed at
        // a batch can arrive after the mode silently flipped to normal.
        // If the user last SAW select mode, a batch-capable key must not
        // fall through to single-target semantics on the hovered row —
        // that is the exact conflation this mode exists to kill. (Marks
        // never appear asynchronously — only user keys add them, and a
        // paint runs between two keys — so the stale direction is only
        // select→normal.)
        if app.rendered_select_mode()
            && matches!(action, Action::Delete | Action::Restart | Action::ForceKill)
        {
            return Gated::Blocked(STALE_SELECT);
        }
        return Gated::Pass(action);
    }

    match action {
        // Batch-capable operations (`OperationKind::batch_support()` ==
        // PerItem) transform into their distinct batch actions. The
        // manifest⇄gate agreement is pinned by test.
        Action::Delete => Gated::Pass(Action::BatchDelete),
        Action::Restart => Gated::Pass(Action::BatchRestart),
        Action::ForceKill => Gated::Pass(Action::BatchForceKill),

        // Single-target actions: dead in select mode.
        Action::Enter
        | Action::Describe
        | Action::Yaml
        | Action::Logs
        | Action::PreviousLogs
        | Action::Shell
        | Action::Edit
        | Action::Scale
        | Action::PortForward
        | Action::ShowPortForwards
        | Action::ShowNode
        | Action::NodeShell
        | Action::DecodeSecret
        | Action::TriggerCronJob
        | Action::SuspendCronJob
        | Action::JumpToOwner
        | Action::UsedBy
        | Action::OverlayCapability(_) => Gated::Blocked(DEAD_KEY),

        // Everything else stays live: navigation, marking, view ops,
        // dialogs, and the scope-leaving resets (which end select mode
        // by dropping the store — ownership, not policy).
        a @ (Action::Quit
        | Action::Back
        | Action::Help
        | Action::NextTab
        | Action::PrevTab
        | Action::NextItem
        | Action::PrevItem
        | Action::PageUp
        | Action::PageDown
        | Action::Home
        | Action::End
        | Action::Filter(_)
        | Action::ClearFilter
        | Action::ToggleLogFollow
        | Action::ToggleLogWrap
        | Action::ToggleLogTimestamps
        | Action::ClearLogs
        | Action::ScrollUp(_)
        | Action::ScrollDown(_)
        | Action::SwitchNamespace(_)
        | Action::SwitchContext(_)
        | Action::ToggleHeader
        | Action::Refresh
        | Action::Copy
        | Action::Confirm
        | Action::Cancel
        | Action::CommandMode
        | Action::ToggleDialogButton
        | Action::Sort(_)
        | Action::ToggleSortDirection
        | Action::SearchStart
        | Action::SearchExec(_)
        | Action::SearchNext
        | Action::SearchPrev
        | Action::SearchClear
        | Action::ToggleLastView
        | Action::ToggleMark
        | Action::SpanMark
        | Action::ClearMarks
        | Action::SaveTable
        | Action::SaveLogs
        | Action::ShowAliases
        | Action::LogSince(_)
        | Action::ColLeft
        | Action::ColRight
        | Action::ColFirst
        | Action::ColLast
        | Action::ColumnFilter
        | Action::ToggleWide
        | Action::ToggleFaultFilter
        | Action::FlashInfo(_)
        | Action::BatchDelete
        | Action::BatchRestart
        | Action::BatchForceKill) => Gated::Pass(a),
    }
}

#[cfg(test)]
#[path = "../tests/app/select_gate.rs"]
mod tests;
