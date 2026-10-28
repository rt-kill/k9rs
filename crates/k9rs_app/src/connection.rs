//! The daemon link's lifecycle, and the context switch that rides on it.
//!
//! One concept across four types: [`LinkState`] is where the link IS,
//! `ReconnectPlan` is what happens next, [`Connection`] fuses them so no
//! caller can set one without the other, and [`ContextSwitchState`] is the
//! three-state cycle layered on top. This was five independent `App` fields
//! whose missed edges shipped two real bugs — a context switch that locked
//! out every later `:ctx`, and stale rows painted behind a "connecting"
//! overlay.
//!
//! It lived in `types.rs` among five unrelated concerns. Its test mirror was
//! already three separate `mod` blocks (`connection`, `no_context`,
//! `context_switch_tests`), which is what a file wanting to be split looks
//! like.

use std::time::Duration;
/// The three-state lifecycle of a context switch. Replaces the prior pair
/// of `context_switch_pending: bool` + `pending_context_switch: Option<String>`
/// with a single source of truth:
///
/// - [`Self::Stable`] — no switch in flight, new switches allowed.
/// - [`Self::Requested`] — user asked to switch; the session main loop will
///   pick it up at the top of its next iteration and drop the current
///   `ClientSession`.
/// - [`Self::InFlight`] — main loop has taken the request and is bringing up
///   the new connection; it carries the target so the state fully
///   describes the switch in progress. Blocks further switches until the
///   connection *resolves*, either way.
///
/// Transitions form a single cycle: Stable → Requested → InFlight → Stable.
/// The closing edge fires on EITHER outcome of the connection attempt —
/// `ConnectionEstablished` (success) or `ConnectionFailed` (the target was
/// unreachable) — both via [`settle`](Self::settle). There is deliberately
/// no lingering `Failed` state: a resolved switch is `Stable` or it is still
/// in flight, nothing in between. Attempting a new switch from Requested or
/// InFlight is rejected at `begin_context_switch`.
///
/// `InFlight` is not bounded by the TYPE, and this doc used to say it was
/// ("stuck InFlight is unrepresentable" — it names no construct, because
/// there is none). `InFlight(name)` is constructible and nothing in the enum
/// forces an exit; what closes the cycle is that [`settle`](Self::settle) is
/// called on both outcomes, from two sites in two different crates. There is
/// no timeout, so a connection attempt that reports NEITHER outcome leaves
/// this in flight indefinitely — the same residual `App::pending_batch`
/// documents for itself, and a wire correlation id plus a deadline would be
/// the full fix for both.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ContextSwitchState {
    Stable,
    Requested(k9rs_proto::protocol::ContextName),
    InFlight(k9rs_proto::protocol::ContextName),
}

impl ContextSwitchState {
    /// True if a new switch can be initiated. The only "accepting" state.
    pub fn is_stable(&self) -> bool {
        matches!(self, Self::Stable)
    }

    /// The context this switch is aiming at. `Some` for both [`Self::Requested`]
    /// and [`Self::InFlight`], `None` for [`Self::Stable`] — the RECONNECT path uses it
    /// ("keep aiming at the switch target across a mid-switch daemon blip"),
    /// which genuinely wants both states.
    ///
    /// CAUTION for connection-OUTCOME edges (`ConnectionEstablished` /
    /// `ConnectionFailed`): those must match [`Self::InFlight`] explicitly, never
    /// this method. `Requested` means the switch's session hasn't been built
    /// yet, so an outcome arriving in that state belongs to a PRIOR attempt
    /// — reading it as the switch's outcome tears down the wrong world (or
    /// settles away a switch the user just queued).
    pub fn target(&self) -> Option<&k9rs_proto::protocol::ContextName> {
        match self {
            Self::Stable => None,
            Self::Requested(name) | Self::InFlight(name) => Some(name),
        }
    }

    /// If the state is [`Self::Requested`], take the target name and
    /// transition to [`Self::InFlight`], which keeps the target for the duration
    /// of the connection attempt. Returns `None` in any other state and
    /// leaves the state alone. Used by the main loop to atomically consume
    /// a pending request without racing a second call.
    pub fn take_requested(&mut self) -> Option<k9rs_proto::protocol::ContextName> {
        // Discriminant check first so we don't swap out state we can't
        // recover from. If it's not Requested, bail untouched.
        let Self::Requested(name) = self else { return None; };
        let name = name.clone();
        // Safe: we just checked the discriminant on the line above, and
        // `&mut self` means nothing else can mutate in between.
        *self = Self::InFlight(name.clone());
        Some(name)
    }

    /// Resolve an in-flight switch back to [`Self::Stable`], so the next switch is
    /// accepted. This is the closing edge of the cycle and fires on EITHER
    /// outcome: `ConnectionEstablished` (the new session is up) or
    /// `ConnectionFailed` (the target was unreachable). Idempotent — a no-op
    /// from `Stable`, so a steady-state reconnect success can call it freely.
    pub fn settle(&mut self) {
        *self = Self::Stable;
    }
}

/// Whether the daemon connection is LIVE right now — handshake complete and
/// not since dropped. The single authority the render layer consults to show
/// a "Connecting…" screen instead of stale, no-longer-live rows during a
/// reconnect or context switch.
///
/// One axis of [`Connection`]; see its docs for how the axes divide. It is
/// a STORED flag, deliberately NOT derived from the reconnect plan — the
/// plan is consumed at the *start* of the rebuild (i.e. mid-gap), so a
/// derived predicate would flicker the screen back to stale rows before
/// the new connection is actually up.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LinkState {
    /// No session is wanted: no context has been chosen. Nothing is being
    /// attempted and nothing will be until the user picks one. Distinct from
    /// [`Connecting`](Self::Connecting), which promises the screen is about
    /// to fill in on its own — here it never will, and saying "connecting…"
    /// forever would be the same lie in a new place.
    NoContext,
    Connecting,
    Live,
}

/// When the main loop should next attempt a session rebuild. Replaces the
/// `reconnect_requested: bool` + `reconnect_at: Option<Instant>` pair,
/// whose meaning lived in the cross-product ("`at` is only read under
/// `requested`") — the exact two-fields-one-truth shape
/// [`ContextSwitchState`] was created to kill for switches.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ReconnectPlan {
    /// No rebuild wanted.
    Idle,
    /// Rebuild on the next loop turn.
    Now,
    /// Rebuild once the backoff deadline passes (a previous attempt
    /// failed; don't hammer a down daemon).
    At(std::time::Instant),
}

/// The client↔daemon connection lifecycle, owned as ONE value with total
/// edges — previously five loose `App` fields (`link`, `has_connected`,
/// `reconnect_requested`, `reconnect_at`, `reconnect_backoff`) that every
/// event arm had to co-update in the right combination; the 2026-07
/// context-lock bug and the 2026-08 audit's top findings were both missed
/// edges of that implicit machine.
///
/// Three independent axes, each with one owner:
/// - **link**: is the connection live THIS FRAME (render gate)?
/// - **has_connected**: did any session EVER connect (initial-failure-is-
///   fatal rule)? Monotonic.
/// - **plan** + **backoff**: when should the loop rebuild next?
///
/// [`ContextSwitchState`] stays deliberately separate: it answers WHICH
/// context is being brought up; this type doesn't care why the link is
/// down, only that it is and when to try again.
#[derive(Debug)]
pub struct Connection {
    link: LinkState,
    has_connected: bool,
    plan: ReconnectPlan,
    backoff: Duration,
}

impl Connection {
    const INITIAL_BACKOFF: Duration = Duration::from_millis(500);
    const MAX_BACKOFF: Duration = Duration::from_secs(5);

    /// Startup state: the initial handshake hasn't landed yet.
    pub fn new() -> Self {
        Self {
            link: LinkState::Connecting,
            has_connected: false,
            plan: ReconnectPlan::Idle,
            backoff: Self::INITIAL_BACKOFF,
        }
    }

    /// The link's own state — read by [`crate::Liveness::of_link`],
    /// which is the one place the render layer asks.
    pub fn link(&self) -> LinkState {
        self.link
    }

    /// There is no context to connect to. Terminal until a switch is
    /// requested: no plan, so the main loop never rebuilds — a retry with
    /// nothing to retry against would just be a loop.
    pub fn no_context(&mut self) {
        self.link = LinkState::NoContext;
        self.plan = ReconnectPlan::Idle;
    }

    /// Whether any session EVER connected — distinguishes a fatal initial
    /// failure from a retryable reconnect failure.
    pub fn has_ever_connected(&self) -> bool {
        self.has_connected
    }

    /// Is a rebuild due at `now`? Pure read — the choke point consumes the
    /// plan via [`Self::begin_rebuild`] once it commits.
    pub fn rebuild_due(&self, now: std::time::Instant) -> bool {
        match self.plan {
            ReconnectPlan::Idle => false,
            ReconnectPlan::Now => true,
            ReconnectPlan::At(t) => now >= t,
        }
    }

    /// The choke point committed to a session rebuild (reconnect OR context
    /// switch): consume the plan and show the connecting screen for the
    /// whole gap. Cleared to Live only by [`Self::established`].
    pub fn begin_rebuild(&mut self) {
        self.plan = ReconnectPlan::Idle;
        self.link = LinkState::Connecting;
    }

    /// A context switch was REQUESTED (session not rebuilt yet — that
    /// happens at the top of the next loop turn): show the connecting
    /// screen immediately rather than one stale frame later.
    pub fn switch_requested(&mut self) {
        self.link = LinkState::Connecting;
    }

    /// `ConnectionEstablished`: the link is live, future failures are
    /// reconnects, the backoff resets. A still-pending plan (arrived
    /// between a failure and its deadline) collapses to Now — its backoff
    /// deadline was aimed at a connection that no longer exists.
    pub fn established(&mut self) {
        self.link = LinkState::Live;
        self.has_connected = true;
        self.backoff = Self::INITIAL_BACKOFF;
        if self.plan != ReconnectPlan::Idle {
            self.plan = ReconnectPlan::Now;
        }
    }

    /// `DaemonDisconnected`: the live session died — rebuild on the next
    /// turn and show the connecting screen NOW (this frame).
    pub fn disconnected(&mut self) {
        self.link = LinkState::Connecting;
        if self.plan == ReconnectPlan::Idle {
            self.plan = ReconnectPlan::Now;
        }
    }

    /// A context SWITCH failed and we're falling back to the previous
    /// context: reconnect immediately (the daemon is up — the TARGET was
    /// unreachable — so no backoff).
    pub fn switch_failed_fallback(&mut self) {
        self.plan = ReconnectPlan::Now;
    }

    /// A steady-state RECONNECT attempt failed: schedule the next one a
    /// backoff out, and double the backoff (clamped).
    pub fn reconnect_failed_backoff(&mut self, now: std::time::Instant) {
        self.plan = ReconnectPlan::At(now + self.backoff);
        self.backoff = (self.backoff * 2).min(Self::MAX_BACKOFF);
    }
}

impl Default for Connection {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
#[path = "tests/connection.rs"]
mod tests;
