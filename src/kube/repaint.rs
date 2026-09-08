//! Redraw-timing policy for the client event loop.
//!
//! The loop declares *intent* — `on_input` for interaction, `on_data` for
//! streamed updates and animation ticks — and this type decides *when* the
//! next frame paints. Input paints immediately; data paints coalesce to a
//! frame budget so a burst of watch events (or a busy cluster's steady
//! trickle) collapses into one frame instead of one-paint-per-event. Over a
//! high-latency link (SSH) that bounds bytes-on-wire without adding any input
//! latency.
//!
//! Encapsulating the policy here replaces a bare `needs_redraw: bool`, which
//! could not represent "coalesce this" and so forced the loop to hand-juggle
//! the flag against its timers. The three states below make the
//! instant-vs-coalesce distinction explicit and unrepresentable as a bool.

use std::time::{Duration, Instant};

/// When the event loop should next paint.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Repaint {
    /// Nothing pending. `wake_at()` is `None`, so the select loop blocks on
    /// real events rather than spinning.
    Idle,
    /// Input demands an instant frame — bypasses the coalescing budget.
    Now,
    /// Data changed; paint no later than this deadline. Repeated `on_data`
    /// calls before the deadline collapse into this one frame.
    Coalescing(Instant),
}

impl Repaint {
    /// Interaction (key, resize, view re-entry) → paint on the next loop turn,
    /// beating any pending coalesced frame.
    pub fn on_input(&mut self) {
        *self = Repaint::Now;
    }

    /// An animation frame is due (the loop's animation clock fired) → paint on
    /// the next turn, at the spinner's own cadence rather than the data budget.
    pub fn on_animation(&mut self) {
        *self = Repaint::Now;
    }

    /// Streamed data or an animation tick changed the display → coalesce.
    /// Arms a deadline `budget` out when idle; keeps the earliest already-armed
    /// deadline (so a burst paints within `budget` of its *first* update, never
    /// drifting later); never downgrades a pending `Now`.
    pub fn on_data(&mut self, now: Instant, budget: Duration) {
        if *self == Repaint::Idle {
            *self = Repaint::Coalescing(now + budget);
        }
    }

    /// Should the loop paint at `now`? Evaluated at the top of every loop turn,
    /// so it stays correct even during an event flood that starves the timer
    /// arm in `select!`.
    pub fn due(&self, now: Instant) -> bool {
        match self {
            Repaint::Idle => false,
            Repaint::Now => true,
            Repaint::Coalescing(deadline) => now >= *deadline,
        }
    }

    /// The instant to wake the select loop so a not-yet-due coalesced frame
    /// still fires when events go quiet. `None` when idle (block on events) or
    /// `Now` (`due()` is already true, so the top-of-loop gate paints).
    pub fn wake_at(&self) -> Option<Instant> {
        match self {
            Repaint::Coalescing(deadline) => Some(*deadline),
            _ => None,
        }
    }

    /// Record that a frame was painted → back to idle.
    pub fn painted(&mut self) {
        *self = Repaint::Idle;
    }
}

#[cfg(test)]
#[path = "../tests/kube/repaint.rs"]
mod tests;
