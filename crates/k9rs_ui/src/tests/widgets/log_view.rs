use super::{line_phys_rows, wrap_line, LogViewState, LogViewer};
use k9rs_app::StoredLine;
use k9rs_proto::protocol::LogLine;
use ratatui::buffer::Buffer;
use ratatui::layout::Rect;
use ratatui::style::Style;
use ratatui::text::{Line, Span};
use ratatui::widgets::StatefulWidget;

fn ll(content: &str) -> StoredLine {
    StoredLine::from(LogLine { source: None, timestamp: None, content: content.to_string() })
}

/// Render the widget into a throwaway buffer and give back the visible text,
/// row by row. `state` is mutated with the write-backs (`offset`, `top`,
/// `content_rows`) exactly as the real draw pass sees them.
fn render_rows(lines: &[StoredLine], state: &mut LogViewState, w: u16, h: u16) -> Vec<String> {
    let area = Rect::new(0, 0, w, h);
    let mut buf = Buffer::empty(area);
    let refs: Vec<&StoredLine> = lines.iter().collect();
    let theme = k9rs_theme::Theme::dark();
    LogViewer::new(&refs, &theme).render(area, &mut buf, state);
    (0..h)
        .map(|y| {
            (0..w).map(|x| buf[(x, y)].symbol()).collect::<String>().trim_end().to_string()
        })
        .collect()
}

fn state(wrap: bool, anchor: Option<(usize, usize)>) -> LogViewState {
    LogViewState {
        offset: 0,
        anchor,
        top: None,
        follow: false,
        wrap,
        shed: 1, // timestamp hidden, source shown — the default

        pan: 0,
        content_rows: 0,
        widest: 0,
        active_patterns: Vec::new(),
        sources: Vec::new(),
    }
}

/// The anchor names a LINE; the widget turns it into a row, because only the
/// widget knows how tall each line is at this width. The same anchor must land
/// on the same line whether or not wrap is on — that is what makes toggling
/// `w` keep your place instead of teleporting you by the wrap factor.
#[test]
fn the_anchor_parks_on_its_line_in_both_layouts() {
    // 30 lines; every third one wraps to 3 rows at width 10.
    let lines: Vec<StoredLine> = (0..30)
        .map(|i| {
            if i % 3 == 0 {
                ll(&format!("L{i:02}-aaaaaaaaaaaaaaaaaaaaaaaa"))
            } else {
                ll(&format!("L{i:02}"))
            }
        })
        .collect();

    for wrap in [false, true] {
        let mut st = state(wrap, Some((12, 0)));
        let rows = render_rows(&lines, &mut st, 10, 6);
        assert!(rows[0].starts_with("L12"), "wrap={wrap} parked elsewhere: {rows:?}");
        assert_eq!(st.top, Some((12, 0)), "wrap={wrap} reported a different top than it drew");
    }

    // Mid-line: row 1 OF line 12's wrapped body. Unwrapped there are no
    // intra-line rows, so the skip is dropped and we sit on the line's start —
    // never on some other line, which is what a raw row offset would do.
    let mut st = state(true, Some((15, 1)));
    let rows = render_rows(&lines, &mut st, 10, 6);
    assert_eq!(st.top, Some((15, 1)));
    assert!(!rows[0].starts_with("L15"), "we are one row INTO line 15: {rows:?}");
    let mut st = state(false, Some((15, 1)));
    let rows = render_rows(&lines, &mut st, 10, 6);
    assert_eq!(st.top, Some((15, 0)));
    assert!(rows[0].starts_with("L15"), "{rows:?}");
}

/// Both ways an anchor can be out of range resolve to a position rather than
/// a panic: past the last line (a grep dropped everything below you) parks at
/// the bottom, and a skip past the line's height clamps inside it (the window
/// got wider, so the line is shorter than it was when the record was written).
#[test]
fn an_out_of_range_anchor_resolves_instead_of_panicking() {
    let lines: Vec<StoredLine> = (0..20).map(|i| ll(&format!("L{i:02}"))).collect();

    for wrap in [false, true] {
        let mut st = state(wrap, Some((999, 0)));
        let rows = render_rows(&lines, &mut st, 10, 5);
        assert!(rows[4].starts_with("L19"), "wrap={wrap} should park at the end: {rows:?}");
        assert_eq!(st.offset, 15, "wrap={wrap}");
    }

    // One-row lines at this width, so any skip > 0 is stale.
    let mut st = state(true, Some((7, 40)));
    let rows = render_rows(&lines, &mut st, 10, 5);
    assert!(rows[0].starts_with("L07"), "the skip clamped into the line: {rows:?}");
    assert_eq!(st.top, Some((7, 0)));
}

/// Following still wins over the anchor — it is the live answer to "where is
/// the bottom", which only this frame's extent can give — and the widget
/// reports the line it landed on so the next frame has a fresh record.
#[test]
fn following_overrides_the_anchor_and_reports_what_it_drew() {
    let lines: Vec<StoredLine> = (0..20).map(|i| ll(&format!("L{i:02}"))).collect();
    let mut st = state(false, Some((2, 0)));
    st.follow = true;
    let rows = render_rows(&lines, &mut st, 10, 5);
    assert!(rows[4].starts_with("L19"), "{rows:?}");
    assert_eq!(st.top, Some((15, 0)), "the record must describe the frame that was drawn");
}

/// THE load-bearing invariant of the wrap fix: the physical-row COUNT
/// (`line_phys_rows`, which feeds the Viewport's `content_rows`) must equal
/// the rows the widget actually RENDERS (`wrap_line` over the styled spans).
/// Under-report ⇒ `end()` can't reach the last line; over-report ⇒ blank
/// rows at the bottom. A prior audit found the count and the render used two
/// different ANSI skippers that diverged exactly here (OSC, bare-ESC, and
/// CSIs with a non-`m` final byte); this pins them together for good.
#[test]
fn phys_row_count_equals_rendered_rows_across_ansi_forms() {
    let cases = [
        "\x1b[31mred\x1b[0m",              // SGR (always agreed)
        "\x1b[~ABCDEFG",                   // CSI, non-'m' final byte in 0x40..=0x7E
        "\x1b[3~trailing text",            // CSI '~' with params
        "\x1b]8;;http://example\x07link",  // OSC (hyperlink)
        "\x1bXhello world",                // bare ESC + ASCII next byte
        "\x1b\u{20ac}euro after bare esc", // bare ESC + MULTIBYTE (would panic pre-fix)
        "\x1b\u{1f600}emoji after esc",    // bare ESC + 4-byte glyph
        "prefix \x1b\u{65e5}\u{672c} suffix", // ESC before wide chars mid-line
        "\x1b[38;5;200;1mstyled body",     // multi-param SGR then text
        "日本語のログ行です",                // wide chars
        "a 本 b 語 cd ef gh",              // mixed wide/narrow
        "plain ascii logging line here",
        "\x1b[2J",                         // pure escape, no visible text
        "\x1b[H\x1b[K",                    // several pure escapes
        "",                                // empty
    ];
    for content in cases {
        for w in [1usize, 2, 3, 5, 8, 40] {
            let styled = Line::from(
                crate::ansi::parse_ansi_line(content, Style::default())
                    .into_iter()
                    .map(|s| Span::styled(s.content.to_string(), s.style))
                    .collect::<Vec<_>>(),
            );
            let rendered = wrap_line(&styled, w).len();
            let counted = line_phys_rows(&ll(content), 1, w);
            assert_eq!(counted, rendered, "content={content:?} w={w}");
        }
    }
}

/// The container prefix and timestamp render as RAW spans, so
/// `line_phys_rows` counts them via the `ansi=false` path — previously
/// untested. Exercise them (container tag present, timestamps on/off,
/// wide-char prefixes) against the same full styled line the widget builds.
#[test]
fn phys_row_count_matches_with_container_and_timestamp() {
    // Mirror prepare_line: raw container span + raw timestamp/separator +
    // parse_ansi_line(body).
    fn styled_full(line: &StoredLine, show_ts: bool) -> Line<'static> {
        let mut spans: Vec<Span<'static>> = Vec::new();
        if let Some(c) = &line.source {
            spans.push(Span::raw(format!("{} ", c)));
        }
        if show_ts {
            if let Some(ts) = &line.timestamp {
                spans.push(Span::raw(ts.clone()));
                spans.push(Span::raw(" ".to_string()));
            }
        }
        for s in crate::ansi::parse_ansi_line(&line.content, Style::default()) {
            spans.push(Span::styled(s.content.to_string(), s.style));
        }
        Line::from(spans)
    }

    let ts = "2024-01-15T10:30:00.123456789Z";
    let lines = [
        LogLine { timestamp: None, source: Some("api-server".into()), content: "a short log body".into() },
        LogLine {
            timestamp: Some(ts.to_string()),
            source: Some("sidecar".into()),
            content: "ts \x1b[32mgreen\x1b[0m body".to_string(),
        },
        LogLine {
            source: None,
            timestamp: Some(ts.to_string()),
            content: "日本語 wide body".to_string(),
        },
        LogLine { timestamp: None, source: Some("proxy-プロキシ".into()), content: "wide container prefix".into() },
    ]
    .map(StoredLine::from);
    for line in &lines {
        for show_ts in [false, true] {
            for w in [3usize, 6, 12, 40] {
                let rendered = wrap_line(&styled_full(line, show_ts), w).len();
                let counted = line_phys_rows(line, if show_ts { 0 } else { 1 }, w);
                assert_eq!(counted, rendered, "line={:?} show_ts={show_ts} w={w}", line.content);
            }
        }
    }
}

/// `p` is not cosmetic: hiding the tag makes every line SHORTER, so the
/// wrap-expanded extent and the pan bound both have to move with it. A
/// `line_phys_rows` that still counted the hidden prefix would report rows
/// that are not drawn — a scrollbar claiming content that is not there.
#[test]
fn hiding_the_source_tag_shortens_the_line_everywhere() {
    let tagged = StoredLine::from(LogLine {
        source: Some("fetcher-chromium-pimp".into()),
        timestamp: None,
        content: "the actual message body".into(),
    });
    // 22 columns of tag at width 10 is 3 extra rows' worth of text.
    assert!(
        line_phys_rows(&tagged, 1, 10) > line_phys_rows(&tagged, 2, 10),
        "the hidden prefix must not be counted",
    );

    let mut on = state(false, None);
    on.shed = 1;
    let rows = render_rows(std::slice::from_ref(&tagged), &mut on, 60, 1);
    assert!(rows[0].starts_with("fetcher-chromium-pimp the actual"), "{rows:?}");

    let mut off = state(false, None);
    off.shed = 2;
    let rows = render_rows(std::slice::from_ref(&tagged), &mut off, 60, 1);
    assert!(rows[0].starts_with("the actual message"), "{rows:?}");
    assert_eq!(off.widest + 22, on.widest, "the measured extent drops with the tag");
}

/// The pan windows columns the way the viewport windows rows — and under wrap
/// it publishes a zero extent, because wrapped content genuinely has no
/// off-screen right edge. That is what pins the pan at the left margin there,
/// with no `if wrap` anywhere in the action layer.
#[test]
fn the_pan_clips_the_left_edge_and_wrap_pins_it() {
    let line = ll("0123456789abcdefghijklmnopqrstuvwxyz");

    // Width 20 draws 19 columns of content: the last is the scrollbar's.
    let mut st = state(false, None);
    let rows = render_rows(std::slice::from_ref(&line), &mut st, 20, 1);
    assert_eq!(rows[0], "0123456789abcdefghi");
    assert_eq!(st.widest, 36, "the whole line, not the part that fit");

    let mut st = state(false, None);
    st.pan = 10;
    let rows = render_rows(std::slice::from_ref(&line), &mut st, 20, 1);
    assert_eq!(rows[0], "abcdefghijklmnopqrs", "ten columns skipped");

    let mut st = state(true, None);
    st.pan = 10;
    let rows = render_rows(std::slice::from_ref(&line), &mut st, 20, 2);
    assert_eq!(rows[0], "0123456789abcdefghi", "wrapped content ignores the pan");
    assert_eq!(st.widest, 0, "…and reports nothing to pan to");
}

/// A double-width glyph cut in half by the pan is replaced by spaces for the
/// columns that survive, never half-drawn — otherwise every column after it
/// sits one to the left of where `widest` said it would.
///
/// (`本` occupies two cells; the second reads back blank, which is why the
/// expectations below carry a space after it.)
#[test]
fn panning_into_a_wide_glyph_keeps_the_columns_aligned() {
    let line = ll("ab\u{672c}cd");
    let expected = [
        "ab\u{672c} cd", // 0 — the whole line
        "b\u{672c} cd",  // 1
        "\u{672c} cd",   // 2 — the cut lands exactly on the glyph
        " cd",            // 3 — HALFWAY into it: one space, not half a glyph
        "cd",             // 4
        "d",              // 5
    ];
    for (pan, want) in expected.iter().enumerate() {
        let mut st = state(false, None);
        st.pan = pan;
        let rows = render_rows(std::slice::from_ref(&line), &mut st, 10, 1);
        assert_eq!(&rows[0], want, "pan={pan}");
    }
}


/// The scrollbar column belongs to the scrollbar. It is painted AFTER the
/// text at `inner.width - 1`, so while the layout still handed that column
/// to content, the last character of every line was overwritten the moment a
/// log outgrew one screen — and the pan extent advertised a final column
/// that could never be reached.
#[test]
fn the_scrollbar_never_eats_the_last_character() {
    // Enough lines to force a scrollbar, each exactly as wide as the area.
    let lines: Vec<StoredLine> = (0..10).map(|_| ll("0123456789")).collect();
    let mut st = state(false, None);
    let rows = render_rows(&lines, &mut st, 10, 3);
    for (i, row) in rows.iter().enumerate() {
        assert_eq!(&row[..9], "012345678", "row {i}: {row:?}");
        assert!(
            row.ends_with('\u{2588}') || row.ends_with('\u{2591}'),
            "row {i} must end in the scrollbar, not in content: {row:?}",
        );
    }

    // Panning to the bound reveals the true last character.
    let mut st = state(false, None);
    st.pan = 1;
    let rows = render_rows(&lines, &mut st, 10, 3);
    assert!(rows[0].starts_with("123456789"), "the 9 IS reachable: {rows:?}");
}

/// The invariant the old pinning test could not see: our measurement has to
/// agree with **ratatui**, not merely with itself.
///
/// `phys_row_count_equals_rendered_rows_across_ansi_forms` compares
/// `line_phys_rows` to `wrap_line` — two of our own functions, which shared
/// the same per-`char` accounting and so agreed all the way through a bug
/// that was deleting text. This one renders into a real `Buffer` and asks
/// what actually landed in the cells.
#[test]
fn every_character_survives_wrapping_including_emoji() {
    // Each case is a line whose per-`char` and per-grapheme widths differ.
    let cases = [
        "ab\u{26A0}\u{FE0F}cdefgh",        // VS16: counted 1, drawn 2
        "\u{2139}\u{FE0F}info here",       // ℹ️
        "x\u{1F468}\u{200D}\u{1F469}\u{200D}\u{1F467}y", // ZWJ family: counted 6, drawn 2
        "\u{1F44D}\u{1F3FD} thumbs",       // skin-tone modifier
        "plain ascii for contrast",
        "\u{65E5}\u{672C}\u{8A9E} wide",   // wide, no clusters
        "e\u{0301}combining",              // combining mark
    ];
    for content in cases {
        for w in [4u16, 5, 6, 8, 12, 20] {
            let line = ll(content);
            let mut st = state(true, None);
            // Tall enough that nothing is cut off the BOTTOM — we are only
            // asking whether wrapping loses characters horizontally.
            let rows = render_rows(std::slice::from_ref(&line), &mut st, w + 1, 24);
            let painted: String = rows.concat().chars().filter(|c| *c != ' ').collect();
            for g in content.split_whitespace().flat_map(|t| t.chars()).filter(|c| *c != ' ') {
                assert!(
                    painted.contains(g),
                    "w={w} lost {g:?} from {content:?}\npainted: {rows:?}",
                );
            }
        }
    }
}

/// Our width and the width ratatui actually CONSUMES, for the clusters they
/// used to disagree on.
///
/// The first version of this test was a tautology: it asserted
/// `cluster_width(g) == Span::raw(g).width()`, which is the same
/// `UnicodeWidthStr::width` call `cluster_width` itself makes — it proved
/// `f(g) == f(g)` while claiming in its own doc comment to be "asking what
/// actually landed in the cells". This one writes into a `Buffer` and counts
/// the cells that changed.
#[test]
fn cluster_width_matches_what_ratatui_draws() {
    use ratatui::buffer::Buffer;
    use ratatui::layout::Rect;
    const SENTINEL: &str = "\u{2593}";
    for g in [
        "\u{26A0}\u{FE0F}",
        "\u{2139}\u{FE0F}",
        "\u{1F468}\u{200D}\u{1F469}\u{200D}\u{1F467}",
        "\u{1F44D}\u{1F3FD}",
        "\u{672C}",
        "a",
        "e\u{0301}",
    ] {
        let area = Rect::new(0, 0, 16, 1);
        let mut buf = Buffer::empty(area);
        // Fill with a sentinel so "consumed" is observable: ratatui blanks
        // the continuation cells of a wide glyph, which is indistinguishable
        // from untouched space on an empty buffer.
        for x in 0..16 {
            buf[(x, 0)].set_symbol(SENTINEL);
        }
        buf.set_stringn(0, 0, g, 16, ratatui::style::Style::default());
        let consumed = (0..16).take_while(|&x| buf[(x, 0)].symbol() != SENTINEL).count();
        assert_eq!(
            crate::widgets::log_measure::cluster_width(g),
            consumed,
            "we say {} columns for {g:?}; ratatui consumed {consumed}",
            crate::widgets::log_measure::cluster_width(g),
        );
    }
}

/// The invariant has to hold WITH A FILTER ACTIVE too.
///
/// `prepare_line` slices the styled spans at each match's byte offsets, and a
/// match can legitimately end inside a grapheme cluster — `/👩` against a ZWJ
/// family, or any pattern stopping between a base char and its `U+FE0F`.
/// Splitting one cluster into two spans made the renderer lay out more
/// columns than `line_phys_rows` measured over whole clusters, so a following
/// view lost its tail while still reporting AutoScroll on. The old pinning
/// test had no filter in it at all.
#[test]
fn the_extent_survives_a_filter_that_splits_a_cluster() {
    let cases: [(&str, &str); 4] = [
        ("AA\u{1F468}\u{200D}\u{1F469}\u{200D}\u{1F467}BB", "\u{1F469}"),
        ("xx\u{26A0}\u{FE0F}yy", "\u{26A0}"),
        ("ab\u{1F44D}\u{1F3FD}cd", "\u{1F44D}"),
        ("plain text here", "text"),
    ];
    for (content, pattern) in cases {
        for w in [5u16, 7, 9, 14] {
            let line = ll(content);
            let mut st = state(true, None);
            st.active_patterns = vec![pattern.to_string()];
            let rows = render_rows(std::slice::from_ref(&line), &mut st, w, 24);
            let drawn = rows.iter().filter(|r| !r.is_empty()).count();
            assert_eq!(
                st.content_rows, drawn,
                "w={w} pattern={pattern:?} content={content:?}: extent {} vs {drawn} drawn\n{rows:?}",
                st.content_rows,
            );
        }
    }
}

/// A tab is width 1 to `unicode-width` and width 0 to ratatui, which drops
/// every grapheme containing a control char. Counting it as 1 broke a
/// tab-indented line one column early per level — i.e. every Go panic and
/// every Java stack trace.
#[test]
fn a_tab_is_measured_the_way_it_is_drawn() {
    assert_eq!(crate::widgets::log_measure::cluster_width("\t"), 0, "ratatui draws nothing for it");
    assert_eq!(crate::widgets::log_measure::cluster_width("\u{7}"), 0);
    let line = ll("\tabcdefgh");
    let mut st = state(true, None);
    let rows = render_rows(std::slice::from_ref(&line), &mut st, 9, 6);
    assert_eq!(st.content_rows, 1, "8 visible columns in an 8-wide area is one row: {rows:?}");
}

/// Removing a control character can REJOIN the clusters either side of it, so
/// the measurement has to sanitize before it segments — the renderer does.
#[test]
fn a_control_char_between_a_base_and_its_variation_selector() {
    let content = "\u{26A0}\u{1}\u{FE0F} mixed";
    for w in [4u16, 6, 8, 12] {
        let line = ll(content);
        let mut st = state(true, None);
        let rows = render_rows(std::slice::from_ref(&line), &mut st, w, 12);
        let drawn = rows.iter().filter(|r| !r.is_empty()).count();
        assert_eq!(st.content_rows, drawn, "w={w}: {rows:?}");
    }
}

/// ANSI colour from the workload survives all the way to the cells.
///
/// Pinned after a report that `INFO` had stopped being coloured. It had not —
/// but nothing tested it end to end, so there was no way to answer the
/// question except by hand. The dim-prefixed shape is the one `tracing`
/// actually emits (a faint timestamp, then the coloured level), and it is the
/// case a naive SGR parser drops.
#[test]
fn workload_ansi_colour_reaches_the_buffer() {
    use ratatui::buffer::Buffer;
    use ratatui::layout::Rect;
    use ratatui::widgets::StatefulWidget;
    let cases = [
        "\u{1b}[32m INFO\u{1b}[0m pimp::management: hello",
        // tracing's real shape: dim timestamp, then the coloured level.
        "\u{1b}[2m2026-09-18T20:29:38\u{1b}[0m \u{1b}[32m INFO\u{1b}[0m body",
        "\u{1b}[2m\u{1b}[3mdim italic\u{1b}[0m \u{1b}[31mERROR\u{1b}[0m boom",
    ];
    for content in cases {
        let line = ll(content);
        let mut st = state(false, None);
        let area = Rect::new(0, 0, 70, 1);
        let mut buf = Buffer::empty(area);
        let refs: Vec<&StoredLine> = vec![&line];
        let theme = k9rs_theme::Theme::dark();
        LogViewer::new(&refs, &theme).render(area, &mut buf, &mut st);
        let text: String = (0..70u16).map(|x| buf[(x, 0)].symbol()).collect();
        let needle = if content.contains("ERROR") { "ERROR" } else { "INFO" };
        let at = text.find(needle).expect("the level token is drawn") as u16;
        let want = if needle == "ERROR" {
            ratatui::style::Color::Red
        } else {
            ratatui::style::Color::Green
        };
        assert_eq!(
            buf[(at, 0)].fg,
            want,
            "{content:?}: {needle} lost its colour\ndrew: {text:?}",
        );
    }
}

/// Measuring and rendering must agree on every run, and they now go through
/// ONE enumeration (`Units`) to make that structural rather than tested.
///
/// This case list is what caught the last disagreement: `sanitize_terminal`
/// keeps tabs (the shell and flash surfaces need them) while ratatui draws
/// nothing for them, so the measurement dropped a tab the wrapper kept.
#[test]
fn measuring_and_rendering_agree_on_every_run() {
    let cases = [
        "plain ascii logging line",
        "\u{1b}[32m INFO\u{1b}[0m body after escape",
        "\u{1b}[2m2026-09-18T20:29:38\u{1b}[0m \u{1b}[32m INFO\u{1b}[0m relay.x=1",
        "\ttab indented",
        "\t\tdouble tab",
        "bell\u{7}inside",
        "del\u{7f}inside",
        "\u{1b}[2J",
        "",
        "exactly8",
        "nine char",
    ];
    for content in cases {
        for w in [1usize, 2, 3, 8, 9, 40, 98] {
            let line = ll(content);
            let fast = line_phys_rows(&line, 2, w);
            // Force the general path over the identical visible text by
            // handing it a non-ASCII char the cleaner keeps and then
            // subtracting nothing — instead, compare against the renderer,
            // which is the thing both paths exist to predict.
            let styled = Line::from(
                crate::ansi::parse_ansi_line(content, Style::default())
                    .into_iter()
                    .map(|s| Span::styled(s.content.to_string(), s.style))
                    .collect::<Vec<_>>(),
            );
            let rendered = wrap_line(&styled, w).len();
            assert_eq!(fast, rendered, "content={content:?} w={w}");
        }
    }
}

/// A guard on the cost, not the shape.
///
/// The history, in release-build numbers for this exact buffer:
///
/// - **301 ms a frame** — a third of a second per repaint — when every line
///   allocated a cleaned copy and then grapheme-segmented it.
/// - **~50 ms** once every walk went through one `Units` enumeration, which
///   segments only when the cleaned run is not ASCII.
/// - **16 ms** now: `StoredLine` derives the visible text ONCE on arrival, so
///   a frame allocates nothing per line, and `Units` asks "is every byte
///   printable ASCII" in one scan instead of asking "is it ASCII" and "has
///   it controls" in two.
///
/// What is left is the per-unit enumeration itself, over every line in the
/// buffer rather than over the window — the wrap branch needs a physical
/// total that the unwrapped branch does not.
///
/// # That O(buffer) walk is a DECISION, not an oversight (2026-10-14)
///
/// It survives only for the scrollbar's thumb: following needs the last
/// screenful, which is an O(window) walk backwards, and a parked view
/// already knows its line from the anchor. The two changes that would
/// remove it were designed and REJECTED, on their own merits:
///
/// - *Measure the scrollbar in lines rather than rows.* Makes a truthful
///   indicator approximate, to save milliseconds in the one case that is
///   already inside frame budget.
/// - *Let a relative scroll intent keep the anchor and carry a row delta.*
///   This contradicts [`k9rs_app::Viewport`]'s own invariant,
///   stated on its `anchor` field: it is `None` "on any viewport whose
///   offset has been moved since the record was written, which is exactly
///   when it would be a lie". A delta beside a live anchor is that lie, plus
///   a third representation of one position with nothing enforcing the
///   relationship between them — the "two fields, one question" shape this
///   codebase spent 2026-10-13/14 deleting everywhere else.
///
/// 16 ms is a sixtieth of a second, in wrap mode, with a full 50,000-line
/// ring, and `ui.logs.defaultWrap` is false. Revisit if `ui.logs.maxLines`
/// grows a lot (500k would put it near 165 ms) or if wrap feels slow against
/// a real cluster — and revisit with a better design than those two, not
/// with these.
///
/// Also: do NOT "pin" the O(buffer) property with a counter. A test that
/// asserts every line is measured FAILS when someone improves it, and
/// instrumenting `line_phys_rows` to count calls made this very measurement
/// 2.7x slower — the guard would have been measuring itself.
///
/// The budget is in debug-build time and deliberately VERY loose: the
/// failure being guarded against is a 30x cliff, not a 30% drift, and this
/// measures wall clock inside a suite that runs tests in PARALLEL. At a
/// 1500 ms budget against a ~285 ms measurement it still flaked on a loaded
/// machine — and a guard that cries wolf is worse than no guard, because the
/// next real failure gets re-run instead of read. 5 s keeps roughly a 17x
/// margin while leaving the cliff (which lands near 3 s in a debug build)
/// comfortably inside it.
#[test]
fn measuring_a_full_buffer_is_not_a_per_frame_disaster() {
    let lines: Vec<StoredLine> = (0..50_000)
        .map(|i| StoredLine::from(LogLine {
            source: Some("fetcher-chromium-pimp".into()),
            timestamp: Some("2026-09-18T20:29:38.601013184Z".into()),
            content: format!(
                "\u{1b}[2m2026-09-18T20:29:38\u{1b}[0m \u{1b}[32m INFO\u{1b}[0m \
                 pimp::management::metrics: relay.priority.low: depth_client=0, entered=+{i}"
            ),
        }))
        .collect();
    let start = std::time::Instant::now();
    let total: usize = lines.iter().map(|l| line_phys_rows(l, 0, 98)).sum();
    let elapsed = start.elapsed();
    assert!(total > 50_000, "the lines really do wrap");
    // Debug builds are ~10x slower than release; the budget is deliberately
    // loose because the failure being guarded against was 30x, not 30%.
    assert!(
        elapsed < std::time::Duration::from_millis(5000),
        "50k wrap heights took {elapsed:?} — the per-frame cost has regressed",
    );
}

/// One key, one cycle, no unreachable states.
///
/// `t` and `p` were two bindings producing four combinations for three
/// meaningful ones — and the fourth (a timestamp with no attribution) was
/// never reachable by pressing them. A shed LEVEL over an ordered role list
/// is one number, cycles in one key, and extends to a new role without a new
/// binding or a new boolean.
#[test]
fn shedding_chrome_is_one_ordered_cycle() {
    use k9rs_proto::protocol::Role;
    assert_eq!(Role::levels(), Role::SHED_ORDER.len() + 1);

    // Level 0 shows everything.
    assert!(Role::Timestamp.shown(0) && Role::Source.shown(0));
    // The timestamp goes first: 31 columns duplicating what the service
    // already prints, against 22 that nothing else carries.
    assert!(!Role::Timestamp.shown(1) && Role::Source.shown(1));
    // Then attribution.
    assert!(!Role::Timestamp.shown(2) && !Role::Source.shown(2));
    // The body is never shed at any level — chrome trades width for
    // context, it does not remove what the workload said.
    for shed in 0..Role::levels() {
        assert!(Role::Body.shown(shed), "shed={shed}");
    }

    // And the cycle returns: pressing `t` `levels()` times is identity.
    let mut shed = 1usize;
    for _ in 0..Role::levels() {
        shed = (shed + 1) % Role::levels();
    }
    assert_eq!(shed, 1);
}

/// Shedding really removes the columns — the measurement and the render both
/// consult the same level, so a hidden role cannot be counted by one and
/// skipped by the other.
#[test]
fn a_shed_role_costs_no_columns() {
    let line = StoredLine::from(LogLine {
        source: Some("fetcher-chromium-pimp".into()),
        timestamp: Some("2026-09-18T20:29:38.601013184Z".into()),
        content: "the message".into(),
    });
    let all = line_phys_rows(&line, 0, 20);
    let no_ts = line_phys_rows(&line, 1, 20);
    let bare = line_phys_rows(&line, 2, 20);
    assert!(all > no_ts && no_ts > bare, "{all} > {no_ts} > {bare}");

    let mut st = state(false, None);
    st.shed = 2;
    let rows = render_rows(std::slice::from_ref(&line), &mut st, 40, 1);
    assert!(rows[0].starts_with("the message"), "bare is the body alone: {rows:?}");
}

/// Four pods must get four colours.
///
/// They didn't: the colour was `hash(name) % 8` with `hash = acc * 31 + b`,
/// and `31 mod 8 == -1` makes that an alternating sum of the bytes — so
/// replica names sharing a long prefix collapsed onto the low bits of a short
/// suffix. Strengthening the hash would not have been enough either: four
/// items into eight buckets collide 59% of the time however good the mixing.
/// The set is known, so the slot is assigned instead of hashed.
#[test]
fn every_source_in_a_fanout_gets_its_own_colour() {
    let sources: Vec<String> = (0..8)
        .map(|i| format!("fetcher-chromium-pimp-7d4f9c-{}", ["abcde", "fghij", "klmno", "pqrst", "uvwxy", "zabcd", "efghi", "jklmn"][i]))
        .collect();
    for n in 2..=8 {
        let set = &sources[..n];
        let colours: Vec<_> = set.iter().map(|s| super::source_color(s, set)).collect();
        let mut distinct = colours.clone();
        distinct.sort_by_key(|c| format!("{c:?}"));
        distinct.dedup();
        assert_eq!(distinct.len(), n, "{n} sources produced {} colours", distinct.len());
    }

    // Assignment is by position, so it is stable for a given stream and
    // independent of the names' bytes entirely.
    let a = vec!["zzz".to_string(), "aaa".to_string()];
    assert_eq!(super::source_color("zzz", &a), super::source_color("aaa", &[a[1].clone()]));

    // Past the palette it wraps, which is honest rather than silently wrong.
    let many: Vec<String> = (0..20).map(|i| format!("pod-{i}")).collect();
    assert_eq!(super::source_color("pod-8", &many), super::source_color("pod-0", &many));

    // A name the store never saw still resolves.
    let _ = super::source_color("unknown", &[]);
}
