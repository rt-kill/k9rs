//! Unit tests for `ui::ansi` (mirrored out of the source file).

use super::*;

#[test]
fn parse_ansi_line_drops_non_tab_controls_from_text() {
    // SGR coloring is preserved, but a raw CR/BEL in the text run is
    // dropped so it can't reach the wrap-mode Paragraph render path.
    let base = ratatui::style::Style::default();
    let spans = parse_ansi_line("hi\rthere\x07", base);
    let joined: String = spans.iter().map(|s| s.content.as_ref()).collect();
    assert_eq!(joined, "hithere");
    // A real SGR sequence still produces multiple styled spans.
    let colored = parse_ansi_line("\x1b[31mred\x1b[0m", base);
    let text: String = colored.iter().map(|s| s.content.as_ref()).collect();
    assert_eq!(text, "red");
}

/// `visible_text` must equal the concatenation of the renderer's spans — BY
/// CONSTRUCTION, not by coincidence. It is the offset space the filter and
/// the highlighter both index, so a divergence means a pattern matching text
/// that is not on screen, or failing to match text that is.
///
/// This is the invariant the deleted `strip_ansi` could never satisfy: its
/// CSI rules were its own (`is_ascii_alphabetic() || '~' || '@'`), where
/// `parse_ansi_line` goes through `skip_ansi_escape`. Indexing one with the
/// other's offsets has already produced a mid-character slice panic here.
#[test]
fn visible_text_matches_the_spans() {
    let cases = [
        "plain text",
        "\u{1b}[32m INFO\u{1b}[0m body",
        // tracing's real shape, and the one that broke `/workers=`
        "\u{1b}[2m2026-09-18T20:29:38\u{1b}[0m \u{1b}[32m INFO\u{1b}[0m workers=0",
        "\u{1b}[2mworkers\u{1b}[0m=0",
        "\u{1b}[~ABCDEFG",                     // CSI, non-'m' final byte
        "\u{1b}[3~trailing text",
        "\u{1b}]8;;http://example\u{7}link",   // OSC hyperlink
        "\u{1b}Xhello",                        // bare ESC + ASCII
        "\u{1b}\u{20ac}euro",                  // bare ESC + multibyte
        "\u{1b}[38;5;200;1mstyled",
        "bell\u{7}inside",
        "del\u{7f}inside",
        "\ttab indented",
        "\u{65e5}\u{672c}\u{8a9e}",
        "\u{1b}[2J",
        "",
    ];
    for raw in cases {
        let spans: String = parse_ansi_line(raw, ratatui::style::Style::default())
            .iter()
            .map(|s| s.content.to_string())
            .collect();
        assert_eq!(
            crate::util::visible_text(raw).as_ref(),
            spans,
            "visible_text disagrees with the rendered spans for {raw:?}",
        );
    }
}
