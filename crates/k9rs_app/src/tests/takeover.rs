//! One modal slot, so two-modals-at-once is unrepresentable.


// ---------------------------------------------------------------------------
// Modal — one slot, so two-modals-at-once is unrepresentable
// ---------------------------------------------------------------------------

mod modal {
    use crate::{App, Modal, TakeoverOp};

    #[test]
    fn opening_a_modal_displaces_whatever_was_open() {
        // These were four independent fields cleared by hand at 53 sites;
        // any path that forgot one left a modal buried under another,
        // invisible until the top one closed. Now it is an assignment.
        let mut app = App::new_for_test();
        app.ui.open(Modal::Command { input: crate::text_input::TextInput::seeded(":pods"), history_index: None });
        assert!(app.ui.command_input().is_some());

        app.ui.open(Modal::Takeover(TakeoverOp::Edit {
            target: k9rs_proto::protocol::ObjectRef::new(
                k9rs_proto::resource_def::BuiltInKind::Pod.to_resource_id(),
                "p".to_string(),
                k9rs_proto::protocol::Namespace::Named("default".to_string()),
            ),
            state: crate::EditState::AwaitingYaml,
        }));
        assert!(app.ui.takeover().is_some());
        assert!(
            app.ui.command_input().is_none(),
            "the command prompt cannot still be open behind the overlay",
        );

        app.ui.close_modal();
        assert!(!app.ui.is_modal());
        assert!(app.ui.takeover().is_none());
    }

    #[test]
    fn exactly_one_accessor_answers_at_a_time() {
        // The accessors partition the slot: whatever is open, every OTHER
        // accessor must say no. This is the property the four-field version
        // could not state, let alone hold.
        let mut app = App::new_for_test();
        let opens: Vec<(&str, Modal)> = vec![
            ("command", Modal::Command { input: crate::text_input::TextInput::new(), history_index: None }),
            ("overlay", Modal::Takeover(TakeoverOp::Edit {
            target: k9rs_proto::protocol::ObjectRef::new(
                k9rs_proto::resource_def::BuiltInKind::Pod.to_resource_id(),
                "p".to_string(),
                k9rs_proto::protocol::Namespace::Named("default".to_string()),
            ),
            state: crate::EditState::AwaitingYaml,
        })),
        ];
        for (label, modal) in opens {
            app.ui.open(modal);
            let answers = [
                app.ui.takeover().is_some(),
                app.ui.command_input().is_some(),
            ];
            assert_eq!(
                answers.iter().filter(|a| **a).count(),
                1,
                "{label}: exactly one modal accessor may answer",
            );
        }
    }
}

// ---------------------------------------------------------------------------
// The confirm box's focus
// ---------------------------------------------------------------------------

fn confirm() -> crate::ConfirmDialog {
    crate::ConfirmDialog {
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
        focused: crate::ConfirmFocus::default(),
    }
}

/// A confirm box opens on Cancel, by NAME. It was a `bool` whose safe
/// default was `false`, with the polarity in prose and re-read at three
/// sites — the key handler, the action layer and the widget.
#[test]
fn a_confirm_box_opens_on_cancel() {
    let d = confirm();
    assert_eq!(d.focused, crate::ConfirmFocus::Cancel);
    assert!(!d.action_focused(), "…so Enter cancels until you move focus");
}

/// The cycle reaches both buttons and comes back — ONE function owns the
/// order, so neither can be reachable in one direction only.
#[test]
fn the_focus_cycle_reaches_both_buttons_and_returns() {
    let mut d = confirm();
    d.focus_next();
    assert_eq!(d.focused, crate::ConfirmFocus::Action);
    assert!(d.action_focused(), "…and now Enter confirms");
    d.focus_next();
    assert_eq!(d.focused, crate::ConfirmFocus::Cancel, "and round again");
}
