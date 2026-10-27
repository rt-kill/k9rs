//! Cutting text to a column budget.

use super::*;

#[test]
fn test_truncate_short() {
    assert_eq!(truncate("hello", 10), "hello");
}

#[test]
fn test_truncate_exact() {
    assert_eq!(truncate("hello", 5), "hello");
}

#[test]
fn test_truncate_long() {
    let result = truncate("hello world", 8);
    assert_eq!(result, "hello w\u{2026}");
}

#[test]
fn test_truncate_zero() {
    assert_eq!(truncate("hello", 0), "");
}

// -- vim magic mode tests -------------------------------------------------

/// The mirror of `truncate`, for the surfaces where the TAIL is the answer.
#[test]
fn truncate_left_keeps_the_end_and_marks_the_cut() {
    use crate::width::truncate_left;

    // Fits: untouched, and borrowed.
    assert_eq!(truncate_left("pods", 10), std::borrow::Cow::Borrowed("pods"));

    // Does not fit: the END survives, with a leading ellipsis.
    // 12 columns in ALL: eleven of tail plus the mark, not twelve plus.
    assert_eq!(truncate_left("pods > /api > describe(web)", 12), "\u{2026}scribe(web)");
    assert_eq!(truncate_left("abcdef", 3), "\u{2026}ef");

    // Degenerate budgets are answers, not panics.
    assert_eq!(truncate_left("abcdef", 1), "\u{2026}");
    assert_eq!(truncate_left("abcdef", 0), "");

    // Never wider than asked, for anything — including wide glyphs and
    // clusters a byte slice would split down the middle.
    for s in [
        "pods > /api > describe(web-7f9c)",
        "日本語のリソース > ポッド",
        "a\u{1F469}\u{200D}\u{1F467}b",
        "\u{26a0}\u{FE0F} warning",
    ] {
        for max in 0..24usize {
            let out = truncate_left(s, max);
            assert!(
                unicode_width::UnicodeWidthStr::width(out.as_ref()) <= max,
                "truncate_left({s:?}, {max}) = {out:?} is wider than {max}",
            );
        }
    }
}
