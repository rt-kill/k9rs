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

/// Esc is the mode's exit, not the scope's, while marks exist. Vim-like:
/// the same key that leaves visual mode. Only the FIRST press is shadowed —
/// clearing the marks ends select mode, so the next Esc pops as always.
#[test]
fn esc_clears_the_selection_before_it_pops_the_stack() {
    let app = seeded_app(&["a"]);
    assert_eq!(
        gate_action(&app, Action::ClearFilter),
        Gated::Pass(Action::ClearMarks),
        "in select mode Esc leaves the MODE",
    );
    let app = seeded_app(&[]);
    assert_eq!(
        gate_action(&app, Action::ClearFilter),
        Gated::Pass(Action::ClearFilter),
        "with nothing marked Esc is the ordinary nav pop",
    );
}

/// The FULL chain, key→action→gate — the half that was actually broken.
/// The gate transform was only ever reachable if the key handler emitted
/// something for Esc, and at a root list it emitted `None`.
#[test]
fn esc_reaches_the_gate_even_at_an_undrilled_root() {
    let app = seeded_app(&["a"]);
    assert!(!app.nav.is_drilled(), "the case that used to swallow the key");
    let action = crate::event::handler::handle_key_event(
        &app,
        crossterm::event::KeyEvent::new(
            crossterm::event::KeyCode::Esc,
            crossterm::event::KeyModifiers::NONE,
        ),
    )
    .expect("Esc must produce an action for the gate to shadow");
    assert_eq!(gate_action(&app, action), Gated::Pass(Action::ClearMarks));
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
                matches!(gate_action(&app, crate::app::actions::Action::from(&op)), Gated::Blocked(_)),
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
        let gated = gate_action(&app, crate::app::actions::Action::from(&op));
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
