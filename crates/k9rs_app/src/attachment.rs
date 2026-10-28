//! What the session is attached to, or aiming at.
//!
//! This exists because the question had three answers. `ui::header` asked it
//! twice — `context_label` over
//! `(context_switch.target(), connecting, context)` and `display_identity`
//! over `(connecting, identity)` — and `session` asked a third time over
//! `(context_switch.target(), context)`. Two wildcard-free matches over
//! overlapping state with different arity: both compiled, both were tested,
//! and nothing checked they agreed. They did not. Between
//! `begin_context_switch` and the new session's `KubeconfigLoaded` the header
//! read `staging (connecting…)` above prod's cluster and prod's user, which
//! is the window a user actually sits watching.
//!
//! The fix is not a third careful match. It is that the name and the identity
//! travel as ONE value, which only [`App::attachment`] can build — the pair
//! is [`Attached`], whose fields are private to this module, so no other
//! code can put a name beside an identity at all. Same shape as the
//! `ElementClass` → `ViewClass` derivation in `keymap`, for the same reason.
//!
//! What that does and does not buy, stated precisely because the first
//! version of this paragraph overclaimed: it makes ONE PRODUCER structural
//! rather than conventional. It does not make a mismatched pair
//! unrepresentable, and nothing can — [`ClusterIdentity`] does not record
//! which context it describes, so `(prod, staging's identity)` is a pair no
//! type here can reject. The correspondence rests on the single producer,
//! which is why there is exactly one and why it is the thing under test.
//!
//! [`App::attachment`]: crate::App::attachment

use k9rs_proto::protocol::{ClusterIdentity, ContextName};

/// A context name and what is known about THAT context, as one value.
///
/// Private fields and no public constructor, so the only code that can pair
/// a name with an identity is this module — in practice
/// [`Attachment::live`] and [`Attachment::connecting`], which
/// [`crate::App::attachment`] is the only caller of. That is the enforceable
/// half of the invariant; see the module doc for the half that is not.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Attached {
    context: ContextName,
    identity: ClusterIdentity,
}

impl Attached {
    /// The context being named.
    pub fn context(&self) -> &ContextName {
        &self.context
    }

    /// What is known about [`Attached::context`]. All-unknown renders as
    /// `n/a` in every field, which is already how this codebase spells "the
    /// kubeconfig did not say".
    pub fn identity(&self) -> &ClusterIdentity {
        &self.identity
    }
}

/// The context the chrome is describing, together with what is known about
/// it.
///
/// **Invariant: `identity` describes `context`, or it is all-unknown.** It is
/// never some other context's. An all-unknown [`ClusterIdentity`] renders as
/// `n/a` in every field, which is already how this codebase spells "the
/// kubeconfig did not say" — so "we are aiming at staging and do not yet know
/// its cluster" displays honestly instead of borrowing prod's. The invariant
/// is kept by there being one producer, and the one producer is kept by
/// [`Attached`]'s private fields; it is not checkable from the values.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Attachment {
    /// No `current-context`: there is no session and never will be until the
    /// user picks one. The picker is the whole UI; there is nothing to name.
    NoContext,
    /// Confirmed by the daemon's handshake. The identity is the daemon's,
    /// which is authoritative — it carries the server version too.
    Live(Attached),
    /// Aiming at a named context, not attached yet.
    Connecting(Attached),
    /// Attached to nothing and aiming at nothing nameable yet — the initial
    /// handshake, before the kubeconfig read has reported.
    Opening,
}

impl Attachment {
    /// A confirmed attachment: the daemon's handshake answered.
    pub fn live(context: ContextName, identity: ClusterIdentity) -> Self {
        Attachment::Live(Attached { context, identity })
    }

    /// Aiming at a context, not attached yet.
    pub fn connecting(context: ContextName, identity: ClusterIdentity) -> Self {
        Attachment::Connecting(Attached { context, identity })
    }

    /// The context being named, if there is one.
    pub fn context(&self) -> Option<&ContextName> {
        match self {
            Attachment::Live(a) | Attachment::Connecting(a) => Some(a.context()),
            Attachment::NoContext | Attachment::Opening => None,
        }
    }

    /// What is known about [`Attachment::context`] — guaranteed to describe
    /// that context and no other. All-unknown when nothing is attached.
    pub fn identity(&self) -> ClusterIdentity {
        match self {
            Attachment::Live(a) | Attachment::Connecting(a) => a.identity().clone(),
            Attachment::NoContext | Attachment::Opening => ClusterIdentity::default(),
        }
    }
}

#[cfg(test)]
#[path = "tests/attachment.rs"]
mod tests;
