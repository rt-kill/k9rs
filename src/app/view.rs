//! Derived (client-side projected) view types.
//!
//! A `DerivedViewKind` is a view built by reshaping a row the client already
//! has, rather than by subscribing to anything: the containers of a pod, and
//! nothing else so far. The kind owns its behaviour — label, columns,
//! operations, projection — so adding one never touches `DrillTarget`.
//!
//! (This module used to also hold `ViewId`, a parallel identity for "what a
//! nav step is looking at". The 2026-07 redesign made `Element` the nav-stack
//! node and `ViewId` had no callers outside this file afterwards — it was a
//! SECOND answer to questions `Element` already answers, sitting in a file
//! the next person would reach for. Its doc still referenced the `NavStep`
//! that redesign deleted.)


// ---------------------------------------------------------------------------
// DerivedViewKind — closed enum of client-side derived views
// ---------------------------------------------------------------------------

/// A client-side derived view type. The DATA enum is defined in
/// [`crate::kube::resources::row`] (it rides the wire inside
/// `DrillTarget::Derived`, WIRE-FROZEN there); the BEHAVIOR lives here via
/// exhaustive-match accessors — same pattern as [`crate::kube::resource_def::BuiltInKind`]
/// and [`crate::kube::local::LocalResourceKind`]. Adding a variant
/// forces filling in all metadata arms.
pub use crate::kube::resources::row::DerivedViewKind;


