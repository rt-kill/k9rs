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

/// `pad_to_cells` pads in COLUMNS, which `format!("{:<width$}")` does not.
///
/// The table's cell painter cut its value by display columns and then padded
/// it with `{:<width$}`, a CHAR count — two different numbers for any value
/// holding wide or zero-width glyphs, so one cell was measured two ways.
/// These are the cases where they diverge, with the char count noted to show
/// by how much.
#[test]
fn pad_to_cells_counts_columns_not_chars() {
    use unicode_width::UnicodeWidthStr;
    let cases = [
        ("abcdef", 6usize, 10usize), // ASCII: the two agree
        ("日本語", 3, 10),            // 6 columns, 3 chars
        ("🎉🎉", 2, 10),             // 4 columns, 2 chars
        ("e\u{301}e\u{301}", 4, 10), // 2 columns, 4 chars
        ("", 0, 4),
    ];
    for (s, chars, cells) in cases {
        assert_eq!(s.chars().count(), chars, "fixture {s:?}");
        let padded = pad_to_cells(s, cells);
        assert_eq!(
            UnicodeWidthStr::width(padded.as_str()),
            cells,
            "{s:?} ({chars} chars, {} columns) padded to {:?}, which is {} columns",
            UnicodeWidthStr::width(s),
            padded,
            UnicodeWidthStr::width(padded.as_str()),
        );
        assert!(padded.starts_with(s), "the value itself is kept: {padded:?}");
    }
    // Already wider than the budget: pads nothing, cuts nothing. `truncate`
    // is the one that cuts.
    assert_eq!(pad_to_cells("日本語", 2), "日本語");
}

/// `truncate` budgets in COLUMNS, and says so with `…`.
///
/// It counted `s.chars()`, so a CJK or emoji string measured up to half
/// under its true width and came back UNCHANGED — after which `set_line`
/// clipped it with no ellipsis to say a cut had happened. Flash messages
/// carry daemon and apiserver strings, and losing their tail silently is the
/// exact failure the flash widget's own comment claims to have fixed.
#[test]
fn truncate_budgets_columns_not_chars() {
    use unicode_width::UnicodeWidthStr;
    // Six chars, twelve columns, into an eight-column line.
    let cut = truncate("日本語テスト", 8);
    assert!(cut.ends_with('\u{2026}'), "a cut is marked: {cut:?}");
    assert!(
        UnicodeWidthStr::width(cut.as_str()) <= 8,
        "{cut:?} is {} columns, over the 8 asked for",
        UnicodeWidthStr::width(cut.as_str()),
    );
    // It used to come back whole, because 6 chars <= 8.
    assert_ne!(cut, "日本語テスト", "the old char count let this through untouched");

    // Within budget in columns: untouched, no ellipsis.
    assert_eq!(truncate("日本語", 6), "日本語");
    assert_eq!(truncate("日本語", 12), "日本語");

    // A wide glyph that would straddle the budget is dropped whole rather
    // than halved — there is no half-glyph to draw.
    let tight = truncate("日本語", 4);
    assert_eq!(tight, "日\u{2026}", "got {tight:?}");

    // Combining marks ride with their base character: a byte slice between
    // the two panics.
    let accented = "e\u{301}".repeat(10);
    let cut = truncate(&accented, 5);
    assert_eq!(UnicodeWidthStr::width(cut.as_str()), 5, "got {cut:?}");
    assert!(cut.ends_with('\u{2026}'));
}
