//! The table title's budget.
//!
//! At the widget, not through a frame: `build_title_spans` is where the
//! decision lives, and the whole-frame test for it (`a_crowded_title_keeps_
//! the_warning_and_drops_the_hint`) uses the title `pods` — four cells —
//! so it cannot see what a long one does.

use super::*;
use k9rs_theme::Theme;

fn spans_text(line: &ratatui::text::Line<'_>) -> String {
    line.spans.iter().map(|s| s.content.as_ref()).collect()
}

/// A LONG view name gives way; the warning does not.
///
/// This function's doc calls the name and the warning mandatory "whatever it
/// costs", and the budget subtracted both — but nothing bounded the name,
/// and the title is emitted name-first into a space ratatui truncates from
/// the RIGHT. A real CRD plural (38 cells with its leading space) plus a
/// 26-cell apiserver stale reason needs 65 columns of title before anything
/// optional is admitted, so under that the WARNING was the part cut, in
/// silence. The breadcrumb row also names the view; the warning has nowhere
/// else to go.
///
/// Swept across the widths where the two compete, because the one that
/// bites depends on both lengths and picking a single number is how the
/// sibling whole-frame test came to use a four-cell title and see nothing.
#[test]
fn a_long_view_name_gives_way_before_the_warning() {
    let theme = Theme::dark();
    let rows: Vec<Vec<String>> = vec![vec!["alerts".to_string()]];
    let warning = "watch closed by apiserver";
    for width in [30u16, 40, 50, 60, 64] {
        let table = ResourceTable::new(
            vec!["NAME"],
            &rows,
            "prometheusrules.monitoring.coreos.com",
            &theme,
        )
        .warning(Some(warning));

        let line = table.build_title_spans(1, 0..1, None, width);
        let text = spans_text(&line);
        assert!(
            text.contains(warning),
            "at {width} columns the warning must survive in full: {text:?}",
        );
        assert!(
            line.width() <= usize::from(width),
            "…and the whole title must fit, so the renderer cuts nothing: \
             {} cells in {width}: {text:?}",
            line.width(),
        );
        assert!(
            text.contains('\u{2026}'),
            "at {width} columns the NAME is what gave way, visibly: {text:?}",
        );
    }

    // Wide enough for both: nothing is elided.
    let table = ResourceTable::new(
        vec!["NAME"],
        &rows,
        "prometheusrules.monitoring.coreos.com",
        &theme,
    )
    .warning(Some(warning));
    let text = spans_text(&table.build_title_spans(1, 0..1, None, 120));
    assert!(text.contains("prometheusrules.monitoring.coreos.com"), "{text:?}");
    assert!(text.contains(warning), "{text:?}");
}

/// A short name is untouched — the clamp must not elide what fits.
#[test]
fn a_short_view_name_is_left_alone() {
    let theme = Theme::dark();
    let rows: Vec<Vec<String>> = vec![vec!["canary".to_string()]];
    let table = ResourceTable::new(vec!["NAME"], &rows, "pods", &theme)
        .warning(Some("watch closed by apiserver"));
    let text = spans_text(&table.build_title_spans(1, 0..1, None, 80));
    assert!(text.starts_with(" pods"), "{text:?}");
    assert!(!text.contains('\u{2026}'), "nothing to elide: {text:?}");
    assert!(text.contains("watch closed by apiserver"));
}

/// With NO warning the name gets the whole width — the clamp is budgeted
/// against the warning, not a fixed reserve.
#[test]
fn without_a_warning_the_name_keeps_the_width() {
    let theme = Theme::dark();
    let rows: Vec<Vec<String>> = vec![vec!["alerts".to_string()]];
    let long = "prometheusrules.monitoring.coreos.com";
    let table = ResourceTable::new(vec!["NAME"], &rows, long, &theme);
    let text = spans_text(&table.build_title_spans(1, 0..1, None, 80));
    assert!(text.contains(long), "it fits, so it is whole: {text:?}");
}

/// Even at an absurdly narrow width the title says *something* and does not
/// panic — `truncate_left` has to cope with a budget of nothing.
#[test]
fn a_title_at_a_hostile_width_does_not_panic() {
    let theme = Theme::dark();
    let rows: Vec<Vec<String>> = vec![vec!["alerts".to_string()]];
    for width in [0u16, 1, 2, 3, 8, 20] {
        let table = ResourceTable::new(
            vec!["NAME"],
            &rows,
            "prometheusrules.monitoring.coreos.com",
            &theme,
        )
        .warning(Some("watch closed by apiserver"));
        let _ = spans_text(&table.build_title_spans(1, 0..1, None, width));
    }
}
