//! The dialog box sizes — the arithmetic, without a frame.
//!
//! This file had no mirror at all, which is the half of its own story it
//! could not tell: `form_dialog_size` once modelled the hint bar's width as
//! `if has_select { 52 } else { 38 }` against a line that measures 41 and
//! 55, and the port-forward form came out one cell short so `<Esc> cancel`
//! rendered as `<Esc> cance`. One computation is the fix; a test is what
//! says it is still one.

use super::*;

fn confirm(message: &str, action_label: &str) -> crate::ConfirmDialog {
    crate::ConfirmDialog {
        message: message.to_string(),
        action_label: action_label.to_string(),
        pending: crate::PendingAction::BatchDelete(Vec::new()),
        focused: crate::ConfirmFocus::Cancel,
    }
}

fn field(label: &str, kind: FormFieldKind) -> crate::FormFieldState {
    crate::FormFieldState {
        name: label.to_lowercase(),
        label: label.to_string(),
        kind,
        value: crate::text_input::TextInput::seeded(""),
    }
}

fn form(fields: Vec<crate::FormFieldState>) -> crate::FormDialog {
    crate::FormDialog {
        submit: crate::FormSubmit::PortForward,
        title: "Port-forward pod/api".to_string(),
        subtitle: "namespace: prod".to_string(),
        target: k9rs_proto::protocol::ObjectRef::new(
            k9rs_proto::resource_def::BuiltInKind::Pod.to_resource_id(),
            "api".to_string(),
            k9rs_proto::protocol::Namespace::Named("prod".to_string()),
        ),
        fields,
        focused: crate::FormFocus::Field(0),
    }
}

/// THE invariant the hint bar's clipping came from: the box's INNER width —
/// what is left after the border and the widget's own padding — is always
/// enough for the hints it will draw.
///
/// Inner, not the box: a 60-column box draws into 54 columns (a 1-cell
/// border each side and `Padding::new(2, 2, ..)` inside it), and the
/// port-forward form's hints are 55. That is how `<Esc> cancel` came to
/// render as `<Esc> cance` while the box looked wide enough.
///
/// Checked for several shapes, because the hint line is conditional — the
/// arrow row appears only with a Select field, which is where the old
/// hardcoded model was furthest off.
#[test]
fn a_form_box_always_fits_its_own_hint_bar() {
    // Border (1 each side) + the block's horizontal padding (2 each side).
    const CHROME: u16 = 2 + 4;
    let shapes = [
        vec![field("Replicas", FormFieldKind::Number { min: 0, max: 100 })],
        vec![
            field("Container port", FormFieldKind::Select { options: vec![crate::SelectOption::new("80", "80")], selected: 0 }),
            field("Local port", FormFieldKind::Number { min: 1, max: 65535 }),
        ],
        // A label far wider than any real one, to show the width is driven
        // by the content and not by a constant.
        vec![field(
            "An extremely long field label indeed",
            FormFieldKind::Number { min: 0, max: 1 },
        )],
    ];
    for fields in shapes {
        let dialog = form(fields);
        let (w, _) = form_dialog_size(&dialog);
        let inner = w.saturating_sub(CHROME);
        assert!(
            inner >= form_hints_width(&dialog),
            "a {w}-column box draws into {inner} and cannot fit {}-column hints: {:?}",
            form_hints_width(&dialog),
            form_hints(&dialog),
        );
    }
}

/// A Select field widens the hint bar, and the box with it — the conditional
/// the old hardcoded pair got wrong in both directions.
#[test]
fn a_select_field_widens_the_hints_and_the_box() {
    let plain = form(vec![field("Replicas", FormFieldKind::Number { min: 0, max: 9 })]);
    let with_select = form(vec![field(
        "Replicas",
        FormFieldKind::Select {
            options: vec![crate::SelectOption::new("1", "1"), crate::SelectOption::new("2", "2")],
            selected: 0,
        },
    )]);
    assert!(
        form_hints_width(&with_select) > form_hints_width(&plain),
        "the arrow row is what makes the width conditional",
    );
    assert_eq!(form_hints(&plain).len() + 1, form_hints(&with_select).len());
    // And the `<Esc> cancel` hint — the one that gets you out — is last in
    // both, which is why losing the tail was losing the exit.
    for d in [&plain, &with_select] {
        assert_eq!(form_hints(d).last().map(|(k, _)| *k), Some("<Esc>"));
    }
}

/// The box grows one row per field, and every size has room for a border,
/// a title and the hint row. A box that cannot hold its own chrome centres
/// its content outside itself.
#[test]
fn a_form_box_grows_with_its_fields() {
    let one = form_dialog_size(&form(vec![field("A", FormFieldKind::Number { min: 0, max: 1 })]));
    let three = form_dialog_size(&form(vec![
        field("A", FormFieldKind::Number { min: 0, max: 1 }),
        field("B", FormFieldKind::Number { min: 0, max: 1 }),
        field("C", FormFieldKind::Number { min: 0, max: 1 }),
    ]));
    assert_eq!(three.1, one.1 + 2, "one row per extra field");
    assert!(one.1 > DIALOG_MARGIN.1 + 2, "and room for the chrome");
}

/// A confirm box fits its message AND its buttons, and stays inside the
/// clamp — a long message must not grow the box past 72 columns, and a short
/// one must not shrink it under the buttons.
#[test]
fn a_confirm_box_fits_its_message_and_its_buttons() {
    let (w, h) = confirm_dialog_size(&confirm("Delete pod/canary?", "Delete"));
    assert!((36..=72).contains(&w), "clamped: {w}");
    assert_eq!(h, 9, "one message line");

    let long = "x".repeat(200);
    let (wide, _) = confirm_dialog_size(&confirm(&long, "Delete"));
    assert_eq!(wide, 72, "a long message stops at the clamp");

    // A tiny message still leaves room for `[ Cancel ]  [ Force Kill ]`.
    let (narrow, _) = confirm_dialog_size(&confirm("?", "Force Kill"));
    let buttons = " Cancel ".len() + " Force Kill ".len();
    assert!(
        narrow as usize >= buttons,
        "a {narrow}-column box cannot hold {buttons} columns of buttons",
    );
}

/// Each message line costs a row, so a multi-line confirmation is not drawn
/// over its own border.
#[test]
fn a_confirm_box_grows_with_its_message_lines() {
    let one = confirm_dialog_size(&confirm("Delete 3 pods?", "Delete")).1;
    let three = confirm_dialog_size(&confirm("Delete 3 pods?\na\nb", "Delete")).1;
    assert_eq!(three, one + 2);
}

/// The picker is clamped too, and holds one row per choice plus its chrome.
#[test]
fn a_picker_fits_its_choices_within_the_clamp() {
    let picker = |labels: &[&str]| crate::ContainerPicker {
        target: k9rs_proto::protocol::ObjectRef::new(
            k9rs_proto::resource_def::BuiltInKind::Pod.to_resource_id(),
            "api".to_string(),
            k9rs_proto::protocol::Namespace::Named("prod".to_string()),
        ),
        choices: labels
            .iter()
            .map(|l| crate::ContainerChoice {
                label: (*l).to_string(),
                container: k9rs_proto::protocol::LogContainer::Named((*l).to_string()),
            })
            .collect(),
        selected: 0,
        action: crate::ContainerAction::Logs,
    };
    let (w, h) = container_picker_size(&picker(&["web", "sidecar"]));
    assert!((30..=60).contains(&w), "clamped: {w}");
    assert_eq!(h, 2 + 4, "one row per choice plus the chrome");

    let (wide, _) = container_picker_size(&picker(&["a".repeat(100).as_str()]));
    assert_eq!(wide, 60, "a long label stops at the clamp");
}
