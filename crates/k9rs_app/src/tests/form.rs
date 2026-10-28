//! Unit tests for `form` (mirrored out of the source file).

// ---------------------------------------------------------------------------
// form_submit_tests (mirrored out of form.rs)
// ---------------------------------------------------------------------------

use super::*;
use k9rs_proto::protocol::{Namespace, ObjectRef, ResourceId};
use k9rs_proto::resource_def::BuiltInKind;

fn pod_target() -> ObjectRef {
    ObjectRef::new(
        ResourceId::BuiltIn(BuiltInKind::Pod),
        "test-pod",
        Namespace::from_user_command("default"),
    )
}

#[test]
fn scale_build_command_valid() {
    let target = pod_target();
    let fields = vec![FormFieldState {
        name: "replicas".into(),
        label: "Replicas".into(),
        kind: FormFieldKind::Number { min: 0, max: 100 },
        value: "3".into(),
    }];
    let cmd = FormSubmit::Scale.build_command(&target, &fields).unwrap();
    match cmd {
        k9rs_proto::protocol::SessionCommand::Scale { replicas, .. } => {
            assert_eq!(replicas, 3);
        }
        _ => panic!("expected Scale command"),
    }
}

#[test]
fn scale_build_command_invalid() {
    let target = pod_target();
    let fields = vec![FormFieldState {
        name: "replicas".into(),
        label: "Replicas".into(),
        kind: FormFieldKind::Number { min: 0, max: 100 },
        value: "abc".into(),
    }];
    assert!(FormSubmit::Scale.build_command(&target, &fields).is_err());
}

#[test]
fn port_forward_build_command_valid() {
    let target = pod_target();
    let fields = vec![
        FormFieldState {
            name: "container_port".into(),
            label: "".into(),
            kind: FormFieldKind::Port,
            value: "8080".into(),
        },
        FormFieldState {
            name: "local_port".into(),
            label: "".into(),
            kind: FormFieldKind::Port,
            value: "9090".into(),
        },
    ];
    let cmd = FormSubmit::PortForward.build_command(&target, &fields).unwrap();
    match cmd {
        k9rs_proto::protocol::SessionCommand::PortForward { local_port, container_port, .. } => {
            assert_eq!(container_port, 8080);
            assert_eq!(local_port, 9090);
        }
        _ => panic!("expected PortForward command"),
    }
}

#[test]
fn port_forward_build_command_select_uses_typed_index() {
    let target = pod_target();
    // The pf_ports path builds a Select container_port. The chosen option is
    // the typed `selected` index — no digit-string round-trip — so selected=1
    // resolves to the second option ("8443").
    let fields = vec![
        FormFieldState {
            name: "container_port".into(),
            label: "".into(),
            kind: FormFieldKind::Select {
                options: vec![
                    SelectOption::new("8080", "8080"),
                    SelectOption::new("8443", "8443"),
                ],
                selected: 1,
            },
            value: crate::text_input::TextInput::new(),
        },
        FormFieldState {
            name: "local_port".into(),
            label: "".into(),
            kind: FormFieldKind::Port,
            value: "9090".into(),
        },
    ];
    let cmd = FormSubmit::PortForward.build_command(&target, &fields).unwrap();
    match cmd {
        k9rs_proto::protocol::SessionCommand::PortForward { container_port, local_port, .. } => {
            assert_eq!(container_port, 8443);
            assert_eq!(local_port, 9090);
        }
        _ => panic!("expected PortForward command"),
    }
}

// ---------------------------------------------------------------------------
// Focus cycle — the Tab order, and the Cancel button that was never there
// ---------------------------------------------------------------------------

/// A form with `n` text fields, caret on the first one.
fn form_with(n: usize) -> FormDialog {
    FormDialog {
        submit: FormSubmit::Scale,
        title: "Scale".to_string(),
        subtitle: String::new(),
        target: pod_target(),
        fields: (0..n)
            .map(|i| FormFieldState {
                name: format!("f{i}"),
                label: format!("field{i}"),
                kind: FormFieldKind::Text { max_len: None },
                value: crate::text_input::TextInput::new(),
            })
            .collect(),
        focused: FormFocus::Field(0),
    }
}

/// THE reported gap: there was exactly one button, so submitting was the
/// only way out of a form opened by mistake. Tab must reach Cancel.
#[test]
fn tab_reaches_cancel_after_ok() {
    let mut f = form_with(2);
    assert_eq!(f.focused, FormFocus::Field(0));
    f.focus_next();
    assert_eq!(f.focused, FormFocus::Field(1));
    f.focus_next();
    assert_eq!(f.focused, FormFocus::Ok);
    f.focus_next();
    assert_eq!(f.focused, FormFocus::Cancel, "Cancel must be in the Tab cycle");
    f.focus_next();
    assert_eq!(f.focused, FormFocus::Field(0), "and the cycle wraps");
}

/// Reachable in BOTH directions — one function owns the cycle precisely so
/// a button cannot be reachable forwards and not backwards.
#[test]
fn shift_tab_walks_the_same_cycle_backwards() {
    let mut f = form_with(2);
    f.focus_prev();
    assert_eq!(f.focused, FormFocus::Cancel);
    f.focus_prev();
    assert_eq!(f.focused, FormFocus::Ok);
    f.focus_prev();
    assert_eq!(f.focused, FormFocus::Field(1));
    f.focus_prev();
    assert_eq!(f.focused, FormFocus::Field(0));
}

/// A fieldless form is just the two buttons, and must not strand the caret.
#[test]
fn a_form_with_no_fields_cycles_its_two_buttons() {
    let mut f = form_with(0);
    f.focused = FormFocus::Ok;
    f.focus_next();
    assert_eq!(f.focused, FormFocus::Cancel);
    f.focus_next();
    assert_eq!(f.focused, FormFocus::Ok, "wraps to OK, not to a field that isn't there");
    f.focus_prev();
    assert_eq!(f.focused, FormFocus::Cancel);
}

/// A button is not a text sink. The cursor-style gate and the editing keys
/// both ask this, and a bare index answered "field 2" for the OK button of
/// a two-field form only because `get()` happened to return `None`.
#[test]
fn buttons_are_not_fields() {
    let mut f = form_with(2);
    assert!(f.focused_field().is_some());
    assert!(f.field_focused(0));
    assert!(!f.ok_focused() && !f.cancel_focused());

    f.focused = FormFocus::Ok;
    assert!(f.focused_field().is_none());
    assert!(f.ok_focused() && !f.cancel_focused());
    assert!(!f.field_focused(0) && !f.field_focused(2));
    assert!(f.current_field_mut().is_none(), "typing must not land on a button");

    f.focused = FormFocus::Cancel;
    assert!(f.focused_field().is_none());
    assert!(f.cancel_focused() && !f.ok_focused());
}

/// Every field a schema declares is a field the submit dispatcher looks up.
///
/// The schemas and `build_command` are supposed to share one set of names,
/// and both docs said so — but the schemas held bare literals
/// (`name: "container_port"`), so the two agreed only by eye. Renaming a
/// schema field compiled and surfaced as `Err("Missing field: …")` at the
/// moment the user pressed OK, which is the outcome the doc called
/// impossible. They spell the names with `form_field_name` now; this drives
/// the pairing from the SCHEMA so a literal creeping back is caught here as
/// well as by the compiler.
#[test]
fn every_schema_field_is_one_the_dispatcher_looks_up() {
    use k9rs_proto::protocol::{FormFieldSchemaKind, OperationKind};

    let mut checked = 0usize;
    for op in [OperationKind::Scale, OperationKind::PortForward] {
        let Some(schema) = op.form_schema() else {
            panic!("{op:?} is a form operation and must have a schema")
        };
        let submit = FormSubmit::from_operation(op.clone())
            .unwrap_or_else(|| panic!("{op:?} has a schema, so it has a submit"));

        // Build the field states straight off the schema, with a value each
        // kind will accept. The NAMES are the schema's.
        let fields: Vec<FormFieldState> = schema
            .fields
            .iter()
            .map(|f| FormFieldState {
                name: f.name.to_string(),
                label: f.label.to_string(),
                kind: match f.kind {
                    FormFieldSchemaKind::Number { min, max, .. } => {
                        FormFieldKind::Number { min, max }
                    }
                    FormFieldSchemaKind::Port
                    | FormFieldSchemaKind::DynamicSelect { .. } => FormFieldKind::Port,
                },
                value: "2".to_string().into(),
            })
            .collect();
        assert!(!fields.is_empty(), "{op:?}'s schema declares fields");

        let built = submit.build_command(&pod_target(), &fields);
        assert!(
            built.is_ok(),
            "{op:?}: the dispatcher could not find a field the schema declares \
             — the two name sets have drifted: {:?}",
            built.err(),
        );
        checked += 1;
    }
    assert_eq!(checked, 2, "both form operations exercised");
}
