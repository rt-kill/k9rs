//! What the header renders.

use super::*;
use k9rs_app::{bar_hints, ViewClass};
use k9rs_theme::Theme;

/// A bar that does not fit says so, and never silently loses its tail.
///
/// The log bar was 115 columns against a 97-column terminal, so `1h`, `24h`
/// and `back` vanished — `back` being the key that gets you out. Grouping the
/// `since` keys bought 31 of those columns back; this is the guard that holds
/// for every class at every width, including the ones nobody has measured.
#[test]
fn a_hint_bar_fits_or_admits_it() {
    let theme = Theme::dark();
    for &class in ViewClass::ALL {
        let hints = bar_hints(class);
        if hints.is_empty() {
            continue;
        }
        // The natural width, which every class must now fit in 80 columns —
        // the narrowest terminal worth designing for.
        let full = render_keybinding_bar(&hints, u16::MAX, &theme);
        assert!(
            full.width() <= 80,
            "{class:?}: the bar wants {} columns; group or shorten its hints",
            full.width(),
        );
        assert!(
            !full.spans.iter().any(|s| s.content.contains('\u{2026}')),
            "{class:?}: nothing is dropped when there is room",
        );

        // Narrower than the first hint: nothing fits, and it still says so
        // rather than drawing a half-written one.
        for w in [1u16, 4, 12, 40, 60] {
            let line = render_keybinding_bar(&hints, w, &theme);
            assert!(
                line.width() <= w as usize,
                "{class:?} at width {w}: drew {} columns",
                line.width(),
            );
            let dropped = line.spans.iter().any(|s| s.content.contains('\u{2026}'));
            assert_eq!(
                dropped,
                full.width() > w as usize,
                "{class:?} at width {w}: the ellipsis must appear exactly when something is cut",
            );
        }
    }
}
