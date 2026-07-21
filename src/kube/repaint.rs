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
mod tests {
    use super::*;

    const BUDGET: Duration = Duration::from_millis(100);

    #[test]
    fn idle_is_not_due_and_never_wakes() {
        let r = Repaint::Idle;
        assert!(!r.due(Instant::now()));
        assert_eq!(r.wake_at(), None);
    }

    #[test]
    fn input_paints_immediately() {
        let mut r = Repaint::Idle;
        r.on_input();
        assert!(r.due(Instant::now()));
        // `Now` needs no timer wake — the top-of-loop gate paints it.
        assert_eq!(r.wake_at(), None);
    }

    #[test]
    fn data_coalesces_to_deadline() {
        let t0 = Instant::now();
        let mut r = Repaint::Idle;
        r.on_data(t0, BUDGET);
        assert!(!r.due(t0)); // not yet
        assert!(!r.due(t0 + BUDGET / 2)); // still within budget
        assert!(r.due(t0 + BUDGET)); // deadline reached
        assert_eq!(r.wake_at(), Some(t0 + BUDGET));
    }

    #[test]
    fn earliest_deadline_wins() {
        let t0 = Instant::now();
        let mut r = Repaint::Idle;
        r.on_data(t0, BUDGET);
        r.on_data(t0 + BUDGET / 2, BUDGET); // a later update…
        // …does not push the deadline out: still first update + budget.
        assert_eq!(r.wake_at(), Some(t0 + BUDGET));
    }

    #[test]
    fn input_beats_pending_coalesce() {
        let t0 = Instant::now();
        let mut r = Repaint::Idle;
        r.on_data(t0, BUDGET);
        r.on_input();
        assert_eq!(r, Repaint::Now);
        assert!(r.due(t0)); // instant, no waiting for the deadline
    }

    #[test]
    fn on_data_never_downgrades_now() {
        let t0 = Instant::now();
        let mut r = Repaint::Idle;
        r.on_input();
        r.on_data(t0, BUDGET);
        assert_eq!(r, Repaint::Now); // still instant
    }

    #[test]
    fn painted_returns_to_idle() {
        let t0 = Instant::now();
        let mut r = Repaint::Idle;
        r.on_data(t0, BUDGET);
        r.painted();
        assert_eq!(r, Repaint::Idle);
        assert!(!r.due(t0 + BUDGET));
        assert_eq!(r.wake_at(), None);
    }

    #[test]
    fn wake_at_matches_deadline() {
        let t0 = Instant::now();
        let mut r = Repaint::Idle;
        r.on_data(t0, BUDGET);
        assert_eq!(r.wake_at(), Some(t0 + BUDGET));
    }
}
