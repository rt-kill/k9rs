//! Capability builder: turns a `ResourceTypeMeta` into the typed
//! `ResourceCapabilities` manifest the server sends to the TUI after a
//! successful subscribe. Lives in its own module so the test surface in
//! `event/handler_tests.rs` can call into the same code path the real
//! server uses — no duplication, no drift.
//!
//! Local resources don't go through here; they declare their own
//! `LocalResourceSource::capabilities()` directly.

use crate::kube::protocol::{
    FieldKind, FormField, InputSchema, OperationDescriptor, OperationKind,
    ResourceCapabilities,
};
use crate::kube::resource_types;

/// Build the full set of `OperationDescriptor`s for a built-in K8s resource
/// type identified by its plural name. Unknown plurals (CRDs) get just the
/// always-on operations: describe, yaml, delete.
pub fn for_k8s(plural: &str) -> ResourceCapabilities {
    let mut ops = base_operations();

    // Per-meta operations: logs / shell / scale / restart / port-forward.
    if let Some(meta) = resource_types::find_by_plural(plural) {
        if meta.supports_logs {
            ops.push(op_no_input(OperationKind::StreamLogs, "Logs"));
            ops.push(op_no_input(OperationKind::PreviousLogs, "Previous logs"));
        }
        if meta.supports_port_forward {
            ops.push(op_no_input(OperationKind::PortForward, "Port forward"));
        }
        if meta.supports_shell {
            // Pod-only: exec, drill to node, force kill.
            ops.push(op_no_input(OperationKind::Shell, "Shell"));
            ops.push(op_no_input(OperationKind::ShowNode, "Show node"));
            ops.push(OperationDescriptor {
                kind: OperationKind::ForceKill,
                label: "Force kill".into(),
                input: InputSchema::None,
                requires_confirm: true,
            });
        }
        if meta.supports_scale {
            ops.push(OperationDescriptor {
                kind: OperationKind::Scale,
                label: "Scale".into(),
                input: InputSchema::Form(vec![FormField {
                    name: "replicas".into(),
                    label: "Replicas".into(),
                    kind: FieldKind::Number { min: 0, max: 1_000_000 },
                }]),
                requires_confirm: false,
            });
        }
        if meta.supports_restart {
            ops.push(OperationDescriptor {
                kind: OperationKind::Restart,
                label: "Restart".into(),
                input: InputSchema::None,
                requires_confirm: true,
            });
        }
    }

    // Plural-specific operations not derivable from `ResourceTypeMeta`.
    match plural {
        "secrets" => {
            ops.push(op_no_input(OperationKind::DecodeSecret, "Decode"));
        }
        "cronjobs" => {
            ops.push(op_no_input(OperationKind::TriggerCronJob, "Trigger now"));
            ops.push(op_no_input(OperationKind::ToggleSuspendCronJob, "Toggle suspend"));
        }
        _ => {}
    }

    ResourceCapabilities { operations: ops }
}

/// Operations every K8s resource gets: describe, yaml, delete (with confirm).
fn base_operations() -> Vec<OperationDescriptor> {
    vec![
        op_no_input(OperationKind::Describe, "Describe"),
        op_no_input(OperationKind::Yaml, "YAML"),
        OperationDescriptor {
            kind: OperationKind::Delete,
            label: "Delete".into(),
            input: InputSchema::None,
            requires_confirm: true,
        },
    ]
}

/// Tiny constructor for the common case: an operation with no input and no
/// confirm dialog.
fn op_no_input(kind: OperationKind, label: &str) -> OperationDescriptor {
    OperationDescriptor {
        kind,
        label: label.into(),
        input: InputSchema::None,
        requires_confirm: false,
    }
}
