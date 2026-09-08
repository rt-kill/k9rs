use super::{line_phys_rows, wrap_line, LogTimestamp, LogViewer};
use crate::kube::protocol::LogLine;
use ratatui::style::Style;
use ratatui::text::{Line, Span};

fn ll(content: &str) -> LogLine {
    LogLine { container: None, content: content.to_string() }
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
                crate::util::parse_ansi_line(content, Style::default())
                    .into_iter()
                    .map(|s| Span::styled(s.content.to_string(), s.style))
                    .collect::<Vec<_>>(),
            );
            let rendered = wrap_line(&styled, w).len();
            let counted = line_phys_rows(&ll(content), false, w);
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
    fn styled_full(line: &LogLine, show_ts: bool) -> Line<'static> {
        let mut spans: Vec<Span<'static>> = Vec::new();
        if let Some(c) = &line.container {
            spans.push(Span::raw(format!("{} ", c)));
        }
        let body = match (show_ts, LogViewer::parse_timestamp(&line.content)) {
            (true, Some(LogTimestamp { timestamp, content })) => {
                spans.push(Span::raw(timestamp.to_string()));
                spans.push(Span::raw(" ".to_string()));
                content
            }
            (false, Some(LogTimestamp { content, .. })) => content,
            _ => &line.content,
        };
        for s in crate::util::parse_ansi_line(body, Style::default()) {
            spans.push(Span::styled(s.content.to_string(), s.style));
        }
        Line::from(spans)
    }

    let ts = "2024-01-15T10:30:00.123456789Z";
    let lines = [
        LogLine { container: Some("api-server".into()), content: "a short log body".into() },
        LogLine { container: Some("sidecar".into()), content: format!("{ts} ts \x1b[32mgreen\x1b[0m body") },
        LogLine { container: None, content: format!("{ts} 日本語 wide body") },
        LogLine { container: Some("proxy-プロキシ".into()), content: "wide container prefix".into() },
    ];
    for line in &lines {
        for show_ts in [false, true] {
            for w in [3usize, 6, 12, 40] {
                let rendered = wrap_line(&styled_full(line, show_ts), w).len();
                let counted = line_phys_rows(line, show_ts, w);
                assert_eq!(counted, rendered, "line={:?} show_ts={show_ts} w={w}", line.content);
            }
        }
    }
}
