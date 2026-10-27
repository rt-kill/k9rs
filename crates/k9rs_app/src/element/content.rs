//! Content elements: a YAML or describe body, or a decoded secret.
//!
//! `ContentPhase` is why these are not just a `String`: it replaced an
//! `awaiting_response: bool`, so "fetching forever with no request behind it"
//! stopped being a representable state. `DecodedSecret` is its own
//! `ContentSpec` kind precisely so decoded bytes can never be cached as the
//! target's describe text.


use k9rs_proto::protocol::ObjectRef;

// ---------------------------------------------------------------------------
// ContentView / Overview elements
// ---------------------------------------------------------------------------

/// What a content element shows. Targeted kinds carry their target — the
/// response-delivery identity and the refresh spec in one.
/// `DecodedSecret` is its own kind, NOT a `Describe`: its content arrives
/// on the same wire event but must never be cached as the target's
/// describe text, and Ctrl-R must re-DECODE, not re-describe — labeling it
/// `Describe` (as it once was) made both mistakes representable.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ContentSpec {
    Yaml(ObjectRef),
    Describe(ObjectRef),
    DecodedSecret(ObjectRef),
}

/// Fetch lifecycle of a one-shot content view. Replaces an
/// `awaiting_response: bool` that had no failure edge: a fetch orphaned by
/// a session rebuild left the flag `true` forever — an eternally animated
/// "Loading..." with no request in flight behind it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ContentPhase {
    /// A fetch for this element is in flight on the CURRENT session.
    Fetching,
    /// Content present (delivered, cache-served, or locally built).
    Ready,
    /// The fetch can no longer complete (the session it was issued on is
    /// gone). Renders the reason; Ctrl-R re-issues.
    Failed(String),
}

/// A one-shot fetched text view (yaml / describe / decoded secret).
#[derive(Debug)]
pub struct ContentView {
    pub kind: ContentSpec,
    /// See [`ContentPhase`]. Also gates the delivery-time cache write:
    /// only a view with the fetch in flight caches what arrives.
    pub phase: ContentPhase,
    pub state: crate::ContentViewState,
    pub(super) label: String,
}

impl ContentView {
    pub fn new(kind: ContentSpec, state: crate::ContentViewState, phase: ContentPhase) -> Self {
        let label = match &kind {
            ContentSpec::Yaml(t) => format!("yaml({})", t.name),
            ContentSpec::Describe(t) => format!("describe({})", t.name),
            ContentSpec::DecodedSecret(t) => format!("decoded({})", t.name),
        };
        Self { kind, phase, state, label }
    }

}
