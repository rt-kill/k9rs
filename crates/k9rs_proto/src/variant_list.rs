//! `variant_list!` — declare a wire enum and generate its own list of
//! variants from the same declaration.
//!
//! Its own module, declared before every other in [`crate`], because
//! `macro_rules!` is textually scoped: this serves two enums in two modules
//! and the first of them (`local_types`) is declared before `protocol`, so
//! the macro could not have stayed where its first user was.

/// Declare an enum and generate a `&'static [Self]` of its unit variants.
///
/// The list used to be a second hand-written array beside the enum. Nothing
/// asserted it was complete — and nothing could, because iterating a list
/// can only check the entries it has. A variant missing from it still
/// compiled every exhaustive match, so it looked thoroughly handled while
/// escaping every check that derives its universe from the list.
///
/// Two enums use it, and the failure mode was live in both:
///
/// - [`OperationKind`](crate::protocol::OperationKind)'s `BUILT_IN` feeds
///   `KeysConfig::validate`'s reserved- and duplicate-chord checks (whose
///   stated purpose is "a startup error, not a mystery"), the stale guard,
///   and the batch-support manifest.
/// - [`LocalResourceKind`](crate::local_types::LocalResourceKind)'s `ALL`
///   feeds `find_by_alias` and the client's alias view — so a kind missing
///   from it is a resource that exists and **cannot be reached or seen**.
///   That list was hand-written, and `LocalResourceKind`'s module doc
///   claimed "no parallel static table" while being one. Its wire-tag test
///   asserts `ALL.len() + 1`, which cannot catch the omission either:
///   leaving a new variant out leaves the length unchanged.
///
/// Why a token muncher rather than two groups: the declaration ORDER is the
/// wire's tag order, and each enum's payload-carrying variant sits in the
/// middle of it (`OperationKind::Custom` between `ToggleSuspendCronJob` and
/// `Apply`; `LocalResourceKind::Custom` between `PortForward` and
/// `Context`). Emitting the unit variants and then the payload one would
/// swap tags, which is a protocol break for a cosmetic gain. So the entries
/// are consumed one at a time, in order, and only the unit ones are
/// collected.
///
/// Same intent as `built_in_kinds!` and `view_classes!` — the lists Rust
/// cannot enumerate for itself.
macro_rules! variant_list {
    (
        list = $list:ident;
        $(#[$m:meta])* pub enum $name:ident { $($entries:tt)* }
    ) => {
        variant_list!(@collect $name $list [$(#[$m])*] [] [] $($entries)*);
    };

    // A variant WITH a payload: declared, but not a member of the list — it
    // is declared elsewhere and unbounded, so there is no list to put it in.
    // Matched before the unit arm because it is the more specific shape.
    (@collect $name:ident $list:ident [$($m:tt)*] [$($decl:tt)*] [$($items:tt)*]
        $(#[$vm:meta])* $v:ident ( $t:ty ) , $($rest:tt)*
    ) => {
        variant_list!(@collect $name $list [$($m)*]
            [$($decl)* $(#[$vm])* $v($t),]
            [$($items)*]
            $($rest)*);
    };

    // A unit variant: declared AND listed.
    (@collect $name:ident $list:ident [$($m:tt)*] [$($decl:tt)*] [$($items:tt)*]
        $(#[$vm:meta])* $v:ident , $($rest:tt)*
    ) => {
        variant_list!(@collect $name $list [$($m)*]
            [$($decl)* $(#[$vm])* $v,]
            [$($items)* $name::$v,]
            $($rest)*);
    };

    (@collect $name:ident $list:ident [$($m:tt)*] [$($decl:tt)*] [$($items:tt)*]) => {
        $($m)*
        #[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
        pub enum $name { $($decl)* }

        // The enum takes its allow from the invocation's attributes; this
        // block cannot, so the allow lives here. It is inert for the two
        // real enums (`pub` in `pub` modules with cross-crate callers) and
        // load-bearing for the toy enum the macro's own test declares
        // inside a test module.
        #[allow(unreachable_pub)]
        impl $name {
            /// Every unit variant, in wire-tag order, generated from the
            /// enum's own declaration — so it cannot be missing one. A
            /// payload-carrying variant is absent; see the macro's doc.
            pub const $list: &'static [$name] = &[$($items)*];
        }
    };
}

#[cfg(test)]
#[path = "tests/variant_list.rs"]
mod tests;
