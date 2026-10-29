//! What `variant_list!` generates, tested on a TOY enum rather than on
//! either real user.
//!
//! The property is the MACRO's: a new unit variant joins the list without
//! anyone touching the list. On `OperationKind` or `LocalResourceKind` that
//! cannot be asserted — adding a variant to measure it is a source edit,
//! and the exhaustive matches across four crates refuse to compile until it
//! is handled, which is the other half of the guarantee.
//!
//! This block lived in `tests/protocol.rs` — the mirror of the module the
//! macro used to be declared in. The macro moved out to serve a second
//! enum; its test did not follow, so `variant_list.rs` read as untested
//! while a toy enum sat in another module's mirror. Block-move residue, the
//! class that has bitten this tree repeatedly.

// No `use super::*`: the macro reaches here through `#[macro_use]` at the
// crate root, and the toy enum this declares is all the module needs.

// ---------------------------------------------------------------------------
// `BUILT_IN` is generated, and this is what generated means
// ---------------------------------------------------------------------------

variant_list! {
    list = BUILT_IN;
    // The macro emits `pub`, which inside a test module is an unreachable
    // `pub` — the lint is armed workspace-wide and it is right. Allowed
    // here rather than parameterising the macro's visibility for one toy.
    #[allow(unreachable_pub)]
    /// A stand-in for the two real enums `variant_list!` declares, with the
    /// shape they share: unit variants interleaved with a payload-carrying
    /// one.
    ///
    /// Testing the MACRO rather than either real use, because the property
    /// is about the macro: a new unit variant joins the list without anyone
    /// touching the list. On `OperationKind` or `LocalResourceKind` that
    /// cannot be asserted — adding a variant to measure it is a source
    /// edit, and the exhaustive matches across four crates refuse to
    /// compile until it is handled, which is the other half of the
    /// guarantee.
    pub enum ToyOperation {
        First,
        Second,
        /// Carries a payload, so it is not in the list.
        Open(String),
        /// Declared AFTER the payload variant, the way `Apply` is — the
        /// order is the wire's and the macro must not reorder it.
        Last,
    }
}

/// Every unit variant is in the list, in declaration order, and the
/// payload-carrying one is not.
#[test]
fn the_macro_lists_every_unit_variant_in_order() {
    assert_eq!(
        ToyOperation::BUILT_IN,
        &[ToyOperation::First, ToyOperation::Second, ToyOperation::Last],
        "unit variants only, and `Last` keeps its place after the payload one",
    );
}

/// The payload variant keeps its declared TAG, which is why the macro
/// munches in order instead of emitting two groups: `Custom` sits between
/// `ToggleSuspendCronJob` and `Apply` on the real enum, and moving it would
/// be a protocol break.
#[test]
fn the_macro_preserves_wire_tag_order() {
    let tag = |v: &ToyOperation| {
        u32::from_le_bytes(
            bincode::serialize(v).expect("serialize")[..4]
                .try_into()
                .expect("four bytes"),
        )
    };
    assert_eq!(tag(&ToyOperation::First), 0);
    assert_eq!(tag(&ToyOperation::Second), 1);
    assert_eq!(tag(&ToyOperation::Open(String::new())), 2);
    assert_eq!(tag(&ToyOperation::Last), 3, "declared fourth, tagged fourth");
}

