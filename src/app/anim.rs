//! Self-contained animation clock for loading / spinner indicators.
//!
//! Why this is its own component: a spinner's smoothness must not depend on
//! the *data*-repaint cadence, and "keep repainting while something is
//! loading" must not be a hand-maintained list of loading states sitting far
//! from the code that actually draws spinners. Those two couplings are why the
//! loading bar went jumpy again every time the event loop or the data path was
//! retuned:
//!   * the phase was read from wall-clock time while the frame was painted off
//!     a *relative* coalescing deadline — two unsynchronized clocks beating
//!     against each other; and
//!   * the animation frame rate WAS the data coalescing budget (one `tick_rate`
//!     knob doing both jobs), so tuning data batching moved the spinner.
//!
//! Here the animation owns all three things it used to borrow from the loop:
//!   * PHASE — a frame counter advanced once per animation frame ([`Anim::advance`]),
//!     so motion is exactly one step per painted frame: smooth by construction,
//!     with no wall-clock read to beat against the paint schedule.
//!   * LIVENESS — declared by the act of drawing. [`Anim::bar`] is the *only*
//!     spinner primitive, and it marks the frame, so an on-screen spinner keeps
//!     itself animating; there is no separate predicate to keep in sync.
//!   * CADENCE — the event loop wakes it every [`FRAME_INTERVAL`], independent
//!     of the data coalescing budget, so retuning data batching can never make
//!     the spinner jumpy again.

use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

/// The spinner's own frame interval (~15 fps), decoupled from the data-repaint
/// budget. Short enough to read as smooth motion, long enough to stay cheap.
pub const FRAME_INTERVAL: Duration = Duration::from_millis(66);

/// Animation clock for loading indicators. One per UI; see the module docs.
#[derive(Debug, Default)]
pub struct Anim {
    /// Current phase. Advanced once per animation frame by the event loop.
    frame: u64,
    /// Set by [`Anim::mark`] whenever a spinner draws in the current frame; the
    /// loop reads it after painting to decide whether to schedule the next
    /// frame, then clears it with [`Anim::begin_frame`]. Atomic so the
    /// immutable-`&App` draw paths (yaml / describe) can declare liveness
    /// without needing a `&mut`.
    animating: AtomicBool,
}

impl Anim {
    /// Render the loading snake for `label` at the current phase, AND declare
    /// that a spinner is on screen this frame (keeping the animation alive).
    ///
    /// This is the SOLE spinner primitive — every loading indicator renders
    /// through it — so liveness can never drift from what is actually drawn.
    pub fn bar(&self, label: &str) -> String {
        self.mark();
        const BAR_WIDTH: usize = 8;
        const SNAKE_LEN: usize = 4;
        let pos = (self.frame as usize) % (BAR_WIDTH + SNAKE_LEN);
        let bar: String = (0..BAR_WIDTH)
            .map(|i| if i >= pos.saturating_sub(SNAKE_LEN) && i < pos { '=' } else { ' ' })
            .collect();
        format!("[{}] {}", bar, label)
    }

    /// Declare that a spinner painted this frame. Called only by [`Anim::bar`].
    fn mark(&self) {
        self.animating.store(true, Ordering::Relaxed);
    }

    /// Did a spinner draw in the frame just painted? Read by the event loop
    /// after each paint to decide whether to schedule the next animation frame.
    pub fn is_animating(&self) -> bool {
        self.animating.load(Ordering::Relaxed)
    }

    /// Clear the per-frame liveness flag. The loop calls this before painting a
    /// new frame, so `is_animating()` reflects only THIS frame's spinners.
    pub fn begin_frame(&self) {
        self.animating.store(false, Ordering::Relaxed);
    }

    /// Advance to the next animation phase. The loop calls this once per
    /// animation frame (see [`FRAME_INTERVAL`]).
    pub fn advance(&mut self) {
        self.frame = self.frame.wrapping_add(1);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn drawing_the_bar_declares_liveness() {
        let a = Anim::default();
        assert!(!a.is_animating(), "idle until a spinner draws");
        let _ = a.bar("Loading...");
        assert!(a.is_animating(), "rendering the bar keeps the animation alive");
    }

    #[test]
    fn a_frame_with_no_spinner_stops_the_animation() {
        let a = Anim::default();
        let _ = a.bar("x");
        a.begin_frame(); // next frame drew no spinner
        assert!(!a.is_animating(), "liveness is per-frame, not sticky");
    }

    #[test]
    fn phase_steps_one_column_per_frame() {
        // Motion is a pure function of the frame counter — no wall clock — so
        // consecutive frames are distinct and it can never alias against the
        // paint schedule.
        let mut a = Anim::default();
        let f0 = a.bar("x");
        a.advance();
        let f1 = a.bar("x");
        a.advance();
        let f2 = a.bar("x");
        assert_ne!(f0, f1, "each animation frame is a distinct phase");
        assert_ne!(f1, f2);
    }

    #[test]
    fn phase_wraps_without_panicking() {
        let mut a = Anim::default();
        for _ in 0..1000 {
            a.advance();
        }
        assert!(a.bar("x").contains("]"), "still renders after wrapping");
    }
}
