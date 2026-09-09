// ---------------------------------------------------------------------------
// Liveness — the ONE authority on whether what a view is about to paint is
// still true
// ---------------------------------------------------------------------------

//! A view that paints data is making a claim: *this is what the cluster
//! looks like*. Two independent hops have to be up for that claim to hold —
//! client↔daemon ([`crate::app::Connection`]) and daemon↔cluster (the
//! watcher, reported per-store as [`TableDataState`]) — and either can be
//! down while the rows sit on screen looking perfectly current.
//!
//! Before this type, every data view answered that question for itself. The
//! resource table asked `conn.is_connecting() || total_rows == 0 ||
//! is_initializing`; the overview asked only `conn.is_connecting()`; the log
//! view asked nothing at all; and no one asked about the cluster hop, which
//! had no answer to give. So each round of "the connecting screen still
//! shows the old background" fixed one surface and left the class alive —
//! the user reported it three times. The gates weren't wrong individually;
//! there were just several of them, and a view is exactly the wrong place to
//! keep re-deriving a fact about the whole world.
//!
//! So: one value, computed once per frame from both hops, consulted by every
//! view that paints LIVE data — tables, logs, the overview's counters, the
//! header. (One-shot snapshots — yaml, describe — deliberately don't ask:
//! they are a fetch, not a stream, and carry their own `ContentPhase`.) A new
//! streaming surface gets liveness by asking, and a new failure mode is added
//! here, where every surface picks it up at once.

use crate::app::table::TableDataState;
use crate::app::types::Connection;

/// How much a view may claim about the data it is about to paint.
///
/// Ordered by authority, and that order is the whole design: the link being
/// down outranks anything a store believes, because a store's state is only
/// as fresh as the connection that delivers it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Liveness {
    /// Both hops are up. Paint normally.
    Live,
    /// The client↔daemon link is down (reconnect, context switch, daemon
    /// restart). Nothing on screen is live and nothing can be known — not
    /// even whether the cluster is fine — so the view owns its whole area
    /// with a connecting screen rather than framing rows it can't vouch for.
    Connecting,
    /// No context has been chosen, so there is no session and never will be
    /// until the user picks one. Distinct from [`Connecting`](Self::Connecting)
    /// because that one promises the screen fills itself in; this one is
    /// waiting on a person.
    NoContext,
    /// Linked, but this stream hasn't delivered its first baseline yet.
    /// There is nothing to show and nothing to hide.
    Loading,
    /// Linked, data arrived, but the daemon's watch stopped feeding it: the
    /// rows are a frozen snapshot of a moment that has passed.
    Stale(String),
    /// The subscription failed terminally.
    Failed(String),
}

impl Liveness {
    /// Liveness of the client↔daemon link alone — for views whose data
    /// source has no cluster-side stream state to consult (logs, exec, the
    /// header chrome, the overview's aggregate counters).
    pub fn of_link(conn: &Connection) -> Self {
        match conn.link() {
            crate::app::types::LinkState::Live => Liveness::Live,
            crate::app::types::LinkState::Connecting => Liveness::Connecting,
            crate::app::types::LinkState::NoContext => Liveness::NoContext,
        }
    }

    /// Full liveness for a table-backed view: the link first, then — only
    /// if the link is up, since otherwise the store's state is itself
    /// stale information — what the store has to say.
    pub fn of(conn: &Connection, state: TableDataState) -> Self {
        match Self::of_link(conn) {
            // Both mean the store's state is not worth consulting: with no
            // link its "Ready" is just the last thing that arrived over a
            // connection that has since died, and with no context there was
            // never a stream to produce one.
            link @ (Liveness::Connecting | Liveness::NoContext) => link,
            _ => Self::of_store(state),
        }
    }

    /// Liveness from the STORE alone, for rows that do not come over the
    /// connection at all — a client-owned query (see
    /// [`crate::app::element::LiveQuery::client`]). Asking `of` here would
    /// let a dead daemon hide the kubeconfig's own contexts, which is
    /// precisely the screen you need when the daemon can't be reached.
    pub fn of_store(state: TableDataState) -> Self {
        match state {
            TableDataState::Ready => Liveness::Live,
            TableDataState::Initializing => Liveness::Loading,
            TableDataState::Stale(reason) => Liveness::Stale(reason),
            TableDataState::Failed(err) => Liveness::Failed(err),
        }
    }

    /// Whether the view may paint its rows at all.
    ///
    /// The split is not "is the data perfect" but **can the user still act on
    /// it**. Under `Stale` and `Failed` the link is up, so every operation
    /// (delete, edit, logs, shell) still reaches the apiserver and still
    /// does exactly what it says — the rows are merely a few seconds old, and
    /// blanking them would throw away the cursor, the marks and the scroll
    /// position over a cluster hiccup that heals itself. Under `Connecting`
    /// nothing works at all, so there is nothing to preserve and stale rows
    /// are pure lie. Whatever this returns, [`Self::warning`] makes sure the
    /// screen never *silently* shows data it can't vouch for.
    pub fn shows_data(&self) -> bool {
        match self {
            Liveness::Live | Liveness::Stale(_) | Liveness::Failed(_) => true,
            Liveness::Connecting | Liveness::Loading | Liveness::NoContext => false,
        }
    }

    /// The banner for a view that IS painting data it can't fully vouch for.
    /// `Some` exactly when [`Self::shows_data`] is true but the data isn't
    /// live — the pairing that keeps "we showed stale rows" from ever being
    /// silent.
    /// The reason text is CLUSTER-CONTROLLED (an apiserver `Status.message`
    /// reaches here via `StreamEvent::Error`/`Stale`), and this string is
    /// rendered into a `Block` title — a path that writes symbols verbatim,
    /// unlike `Buffer::set_string`. Sanitising here, where the display string
    /// is BUILT, means no render site has to know that; the receive boundary
    /// cleans it too, but this is the last line and the one that travels with
    /// the string.
    pub fn warning(&self) -> Option<String> {
        use crate::util::sanitize_terminal as clean;
        match self {
            Liveness::Stale(reason) => Some(format!("⚠ STALE — {}", clean(reason))),
            Liveness::Failed(err) => Some(format!("✗ ERROR — {}", clean(err))),
            Liveness::Live | Liveness::Connecting | Liveness::Loading | Liveness::NoContext => None,
        }
    }

    /// The centered line for a view with no rows to paint. `empty_label`
    /// supplies the genuinely-empty wording ("No pods found."), which only
    /// `Live` can reach; `anim` drives the two waiting states so the spinner
    /// comes from the one animation authority rather than a local clock.
    pub fn status_text(
        &self,
        anim: &crate::app::anim::Anim,
        empty_label: impl FnOnce() -> String,
    ) -> String {
        match self {
            Liveness::NoContext => "No context selected — pick one to connect.".to_string(),
            Liveness::Connecting => anim.bar("Connecting..."),
            Liveness::Loading => anim.bar("Loading..."),
            // Cluster-controlled; see `warning` above.
            Liveness::Failed(err) => format!("Error: {}", crate::util::sanitize_terminal(err)),
            Liveness::Stale(reason) => format!("Stale: {}", crate::util::sanitize_terminal(reason)),
            Liveness::Live => empty_label(),
        }
    }
}

#[cfg(test)]
#[path = "../tests/app/liveness.rs"]
mod mirror_tests;
