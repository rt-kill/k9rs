use super::*;
use crate::store::StorePayload;
use k9rs_proto::protocol::{
    BatchSupport, ObjectKey, OperationKind, RowChange, TableBaseline, TableDelta,
};
use k9rs_proto::row::{CellValue, ResourceRow};

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
        // (`Logs` is NOT here — it is the one Aggregate action and
        // retargets to `BatchLogs`; see `logs_aggregate_and_previous_logs_does_not`.)
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
        Action::SaveTable,
        Action::Sort(crate::SortTarget::Named(crate::actions::COL_AGE)),
        Action::NextTab,
        Action::CommandMode,
        Action::Quit,
        Action::Help,
    ] {
        assert_eq!(gate_action(&app, action.clone()), Gated::Pass(action));
    }
}

/// `c` copies the SELECTION, not the table.
///
/// It used to pass through unchanged, so with rows marked `Copy` still dumped
/// every visible row — mark three pods, press `c`, get all forty-seven. That
/// is the single-vs-batch conflation the whole mode exists to kill, and it
/// retargets here for the same reason `Delete` does: the gate is the one place
/// that knows the mode. Unlike the other batch actions it mutates nothing, so
/// it stays available under `--read-only`.
#[test]
fn copy_takes_the_selection_in_select_mode() {
    let app = seeded_app(&["a"]);
    assert_eq!(gate_action(&app, Action::Copy), Gated::Pass(Action::BatchCopy));

    // With nothing marked there is no mode and no retarget: `c` is the
    // ordinary whole-table copy.
    let app = seeded_app(&[]);
    assert_eq!(gate_action(&app, Action::Copy), Gated::Pass(Action::Copy));

    assert!(
        !Action::BatchCopy.is_mutating(),
        "copying is not a mutation — read-only sessions must keep it",
    );
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
    let action = crate::handler::handle_key_event(
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
/// ForceKill | Logs` in `gate_action`) — this pins it to the manifest:
/// every BATCH-CAPABLE operation's action must be swallowed when the last
/// painted frame showed select mode but the marks are gone. A future
/// batch op that transforms in select mode but lacks the stale guard
/// would re-open the exact race the guard exists to kill: the key lands
/// on the hovered row instead of the set the user was looking at.
#[test]
fn stale_guard_covers_every_batch_capable_operation() {
    use k9rs_proto::protocol::{BatchSupport, OperationKind as Op};
    let mut app = seeded_app(&[]);
    app.nav.top_mut().table_interaction_mut().unwrap().rendered_select_mode = true;
    // ONE list, `OperationKind::BUILT_IN`, so the manifest and the tests
    // that check it cannot disagree about which operations exist.
    for op in Op::BUILT_IN {
        if op.batch_support() != BatchSupport::SingleOnly {
            assert!(
                matches!(gate_action(&app, crate::actions::Action::from(op)), Gated::Blocked(_)),
                "{op:?} is batch-capable but its action survives a stale select frame",
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
    // ONE list, `OperationKind::BUILT_IN`, so the manifest and the tests
    // that check it cannot disagree about which operations exist.
    // `Custom` is not in BUILT_IN (it is overlay-declared and unbounded), so
    // it is checked explicitly — it is the variant most likely to drift.
    let custom = Op::Custom("x".to_string());
    for op in Op::BUILT_IN.iter().chain(std::iter::once(&custom)) {
        let gated = gate_action(&app, crate::actions::Action::from(op));
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
            BatchSupport::Aggregate => assert!(
                matches!(gated, Gated::Pass(Action::BatchLogs)),
                "{op:?} is Aggregate but the gate didn't transform it",
            ),
            BatchSupport::SingleOnly => assert!(
                matches!(gated, Gated::Blocked(_)),
                "{op:?} is SingleOnly but the gate let it through",
            ),
        }
    }
}

/// Aggregate is a DISTINCT stance, not a second name for PerItem: it must
/// collapse to ONE action, never fan out. `StreamLogs` is the whole reason
/// the variant exists (its sibling `PreviousLogs` stays single-only — the
/// crashed-container question is about one container).
#[test]
fn logs_aggregate_and_previous_logs_does_not() {
    use k9rs_proto::protocol::{BatchSupport, OperationKind as Op};
    assert_eq!(Op::StreamLogs.batch_support(), BatchSupport::Aggregate);
    assert_eq!(Op::PreviousLogs.batch_support(), BatchSupport::SingleOnly);

    let app = seeded_app(&["a"]);
    assert_eq!(gate_action(&app, Action::Logs), Gated::Pass(Action::BatchLogs));
    assert!(matches!(gate_action(&app, Action::PreviousLogs), Gated::Blocked(_)));
    // And the keymap can't produce the batch action itself — only this gate.
    assert_eq!(
        gate_action(&app, Action::BatchLogs),
        Gated::Pass(Action::BatchLogs),
        "BatchLogs must survive the gate unchanged (no double transform)",
    );
}
