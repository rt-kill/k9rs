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
mod tests {
    use super::*;
    use crate::app::store::StorePayload;
    use crate::kube::protocol::{
        BatchSupport, ObjectKey, OperationKind, RowChange, TableBaseline, TableDelta,
    };
    use crate::kube::resources::row::{CellValue, ResourceRow};

    fn row(name: &str) -> ResourceRow {
        ResourceRow {
            cells: vec![CellValue::Text(name.into()), CellValue::Text("Running".into())],
            name: name.into(),
            namespace: Some("ns".into()),
            ..Default::default()
        }
    }

    fn seeded_app(marked: &[&str]) -> App {
        let app = App::new_for_test();
        let store = std::sync::Arc::clone(app.nav.top().data_store().expect("test root is a list"));
        store.apply(
            1,
            StorePayload::Baseline(TableBaseline {
                resource: crate::app::nav::rid(crate::kube::resource_def::BuiltInKind::Pod),
                headers: vec!["NAME".into(), "STATUS".into()],
                rows: vec![row("a"), row("b"), row("c")],
            }),
        );
        for name in marked {
            assert_eq!(
                store.toggle_mark(&ObjectKey::new("ns".to_string(), name.to_string())),
                Some(true),
            );
        }
        app
    }

    #[test]
    fn select_mode_is_derived_from_marks() {
        assert!(!seeded_app(&[]).select_mode());
        assert!(seeded_app(&["a"]).select_mode());
    }

    #[test]
    fn gate_transforms_batch_capable_ops_in_select_mode() {
        let app = seeded_app(&["a"]);
        assert_eq!(gate_action(&app, Action::Delete), Gated::Pass(Action::BatchDelete));
        assert_eq!(gate_action(&app, Action::Restart), Gated::Pass(Action::BatchRestart));
        assert_eq!(gate_action(&app, Action::ForceKill), Gated::Pass(Action::BatchForceKill));
    }

    #[test]
    fn gate_kills_single_target_actions_in_select_mode() {
        let app = seeded_app(&["a"]);
        for action in [
            Action::Enter,
            Action::Describe,
            Action::Yaml,
            Action::Logs,
            Action::Shell,
            Action::Edit,
            Action::Scale,
            Action::PreviousLogs,
            Action::PortForward,
            Action::ShowPortForwards,
            Action::ShowNode,
            Action::NodeShell,
            Action::DecodeSecret,
            Action::TriggerCronJob,
            Action::SuspendCronJob,
            Action::JumpToOwner,
            Action::UsedBy,
            Action::OverlayCapability("x".into()),
        ] {
            assert!(
                matches!(gate_action(&app, action.clone()), Gated::Blocked(_)),
                "{action:?} must be dead in select mode",
            );
        }
    }

    #[test]
    fn gate_passes_navigation_marking_and_view_ops_in_select_mode() {
        let app = seeded_app(&["a"]);
        for action in [
            Action::NextItem,
            Action::PrevItem,
            Action::PageUp,
            Action::PageDown,
            Action::ToggleMark,
            Action::SpanMark,
            Action::ClearMarks,
            Action::Filter(String::new()),
            Action::ClearFilter,
            Action::Refresh,
            Action::Copy,
            Action::SaveTable,
            Action::Sort(crate::app::SortTarget::Last),
            Action::NextTab,
            Action::CommandMode,
            Action::Quit,
            Action::Help,
        ] {
            assert_eq!(gate_action(&app, action.clone()), Gated::Pass(action));
        }
    }

    #[test]
    fn gate_is_inert_in_normal_mode() {
        let app = seeded_app(&[]);
        assert_eq!(gate_action(&app, Action::Delete), Gated::Pass(Action::Delete));
        assert_eq!(gate_action(&app, Action::Describe), Gated::Pass(Action::Describe));
    }

    /// The stale-frame guard list is derived by hand (`Delete | Restart |
    /// ForceKill` in `gate_action`) — this pins it to the manifest: every
    /// `PerItem` operation's action must be swallowed when the last
    /// painted frame showed select mode but the marks are gone. A future
    /// PerItem op that transforms in select mode but lacks the stale
    /// guard would re-open the exact race the guard exists to kill.
    #[test]
    fn stale_guard_covers_every_per_item_operation() {
        use crate::kube::protocol::{BatchSupport, OperationKind as Op};
        let mut app = seeded_app(&[]);
        app.nav.top_mut().table_interaction_mut().unwrap().rendered_select_mode = true;
        let all = [
            Op::Describe, Op::Yaml, Op::Delete, Op::Restart, Op::Scale,
            Op::StreamLogs, Op::PreviousLogs, Op::PortForward, Op::Shell,
            Op::ShowNode, Op::ForceKill, Op::NodeShell, Op::DecodeSecret,
            Op::TriggerCronJob, Op::ToggleSuspendCronJob,
            Op::Custom("x".to_string()),
        ];
        for op in all {
            if op.batch_support() == BatchSupport::PerItem {
                assert!(
                    matches!(gate_action(&app, op.to_action()), Gated::Blocked(_)),
                    "{op:?} is PerItem but its action survives a stale select frame",
                );
            }
        }
    }

    /// The mode-flip race guard: the frame the user last saw said select
    /// mode, but an async delta has since pruned the marks. A
    /// batch-capable key must be swallowed — falling through to the
    /// single-target op on the hovered row is the exact conflation this
    /// mode exists to kill.
    #[test]
    fn stale_rendered_select_mode_swallows_batch_keys() {
        let mut app = seeded_app(&["a"]);
        app.nav.top_mut().table_interaction_mut().unwrap().rendered_select_mode = true;
        let store = std::sync::Arc::clone(app.nav.top().data_store().unwrap());
        store.apply(
            1,
            StorePayload::Delta(TableDelta {
                changes: vec![RowChange::Remove(ObjectKey::new(
                    "ns".to_string(),
                    "a".to_string(),
                ))],
            }),
        );
        assert!(!app.select_mode(), "prune ended select mode");
        assert!(matches!(gate_action(&app, Action::Delete), Gated::Blocked(_)));
        // Non-batch keys are unaffected by the stale bit.
        assert_eq!(gate_action(&app, Action::NextItem), Gated::Pass(Action::NextItem));
        // Once a paint records normal mode, the guard releases.
        app.nav.top_mut().table_interaction_mut().unwrap().rendered_select_mode = false;
        assert_eq!(gate_action(&app, Action::Delete), Gated::Pass(Action::Delete));
    }

    /// Manifest⇄gate agreement, pinned: every operation's
    /// `batch_support()` stance is exactly what the gate does with its
    /// action in select mode — PerItem transforms into a Batch* action,
    /// SingleOnly blocks.
    #[test]
    fn gate_agrees_with_batch_support_manifest() {
        use OperationKind as Op;
        let app = seeded_app(&["a"]);
        let all = [
            Op::Describe,
            Op::Yaml,
            Op::Delete,
            Op::Restart,
            Op::Scale,
            Op::StreamLogs,
            Op::PreviousLogs,
            Op::PortForward,
            Op::Shell,
            Op::ShowNode,
            Op::ForceKill,
            Op::NodeShell,
            Op::DecodeSecret,
            Op::TriggerCronJob,
            Op::ToggleSuspendCronJob,
            Op::Custom("x".to_string()),
        ];
        for op in all {
            let gated = gate_action(&app, op.to_action());
            match op.batch_support() {
                BatchSupport::PerItem => assert!(
                    matches!(
                        gated,
                        Gated::Pass(
                            Action::BatchDelete | Action::BatchRestart | Action::BatchForceKill
                        )
                    ),
                    "{op:?} is PerItem but the gate didn't transform it",
                ),
                BatchSupport::SingleOnly => assert!(
                    matches!(gated, Gated::Blocked(_)),
                    "{op:?} is SingleOnly but the gate let it through",
                ),
            }
        }
    }
}
