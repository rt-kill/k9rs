//! Unit tests for `actions` (mirrored out of the source file).

/// A `Custom` operation maps to `Action::OverlayCapability` carrying its name.
///
/// This lived in `k9rs-proto`'s own test mirror, next to the descriptor
/// assertion it was bundled with — a client contract asserted from the wire
/// crate, which made `cargo test -p k9rs-proto` depend on the whole TUI. The
/// wire half stayed there; this half belongs here, with the `From` impl.
#[test]
fn a_custom_operation_becomes_an_overlay_capability() {
    use crate::kube::protocol::OperationKind;
    let op = OperationKind::Custom("my-op".into());
    let action = crate::app::actions::Action::from(&op);
    assert!(matches!(action, crate::app::actions::Action::OverlayCapability(ref s) if s == "my-op"));
}
