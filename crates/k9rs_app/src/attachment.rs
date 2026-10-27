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
//! are ONE value, produced by one arm, so "names one context, describes
//! another" is not a state this type can hold. Same shape as the
//! `ElementClass` → `ViewClass` derivation in `keymap`, for the same
//! reason.

use k9rs_proto::protocol::{ClusterIdentity, ContextName};

/// The context the chrome is describing, together with what is known about
/// it.
///
/// **Invariant: `identity` describes `context`, or it is all-unknown.** It is
/// never some other context's. An all-unknown [`ClusterIdentity`] renders as
/// `n/a` in every field, which is already how this codebase spells "the
/// kubeconfig did not say" — so "we are aiming at staging and do not yet know
/// its cluster" displays honestly instead of borrowing prod's.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Attachment {
    /// No `current-context`: there is no session and never will be until the
    /// user picks one. The picker is the whole UI; there is nothing to name.
    NoContext,
    /// Confirmed by the daemon's handshake. The identity is the daemon's,
    /// which is authoritative — it carries the server version too.
    Live { context: ContextName, identity: ClusterIdentity },
    /// Aiming at a named context, not attached yet.
    Connecting { context: ContextName, identity: ClusterIdentity },
    /// Attached to nothing and aiming at nothing nameable yet — the initial
    /// handshake, before the kubeconfig read has reported.
    Opening,
}

impl Attachment {
    /// The context being named, if there is one.
    pub fn context(&self) -> Option<&ContextName> {
        match self {
            Attachment::Live { context, .. } | Attachment::Connecting { context, .. } => {
                Some(context)
            }
            Attachment::NoContext | Attachment::Opening => None,
        }
    }

    /// What is known about [`Attachment::context`] — guaranteed to describe
    /// that context and no other. All-unknown when nothing is attached.
    pub fn identity(&self) -> ClusterIdentity {
        match self {
            Attachment::Live { identity, .. } | Attachment::Connecting { identity, .. } => {
                identity.clone()
            }
            Attachment::NoContext | Attachment::Opening => ClusterIdentity::default(),
        }
    }

    /// Whether the daemon has confirmed this attachment. `false` means the
    /// chrome should mark it as in-flight.
    pub fn is_confirmed(&self) -> bool {
        matches!(self, Attachment::Live { .. })
    }
}

#[cfg(test)]
#[path = "tests/attachment.rs"]
mod tests;
