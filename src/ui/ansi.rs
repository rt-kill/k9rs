//! ANSI SGR → ratatui Spans.
//!
//! These three lived in `util`, which made the BOTTOM of the tree depend on
//! ratatui — and `proto` depends on `util`, so the daemon would have linked
//! the TUI's rendering library for code it never calls.
//!
//! The seam is real rather than convenient: `skip_ansi_escape` is the one
//! escape tokenizer and `visible_text` is built on it, so both stay in
//! `util` where every text consumer can reach them. What moves is only the
//! part whose output type is a `Span` — a rendering fact. The tokenizer is
//! still shared, which is what the agreement test in `util`'s mirror checks,
//! and that test keeps working because it reaches DOWN to the tokenizer from
//! here rather than up.

use crate::util::{sanitize_terminal, skip_ansi_escape};

/// Push a text run as a Span, dropping non-tab control characters. Zero-
/// copy (borrowed slice) in the common no-control case; allocates a
/// cleaned `String` only when a control char is present.
fn push_ansi_text<'a>(
    spans: &mut Vec<ratatui::text::Span<'a>>,
    text: &'a str,
    style: ratatui::style::Style,
) {
    use ratatui::text::Span;
    if text.is_empty() {
        return;
    }
    if text.chars().any(|c| c.is_control() && c != '\t') {
        spans.push(Span::styled(sanitize_terminal(text), style));
    } else {
        spans.push(Span::styled(text, style));
    }
}

/// Parse ANSI SGR escape sequences in a log line into ratatui Spans.
/// Non-SGR escape sequences are stripped (not displayed). Text between
/// escapes becomes a Span styled with the accumulated SGR state. Non-tab
/// control characters in the text runs are dropped (a hostile log line
/// can't leak a raw `\r`/BEL or a stray control into the wrap-mode
/// Paragraph render path).
///
/// The application is the authority on its own output coloring — we
/// preserve it faithfully instead of stripping and re-guessing.
pub fn parse_ansi_line<'a>(line: &'a str, base: ratatui::style::Style) -> Vec<ratatui::text::Span<'a>> {
    let mut spans = Vec::new();
    let mut style = base;
    let bytes = line.as_bytes();
    let len = bytes.len();
    let mut i = 0;
    let mut text_start = 0;

    while i < len {
        if let Some(next) = skip_ansi_escape(bytes, i) {
            // Flush the visible run before this escape.
            if i > text_start {
                push_ansi_text(&mut spans, &line[text_start..i], style);
            }
            // An SGR (`CSI … m`) updates the style; every other escape just
            // vanishes. `next - 1` is the final byte (guaranteed ≥ i+2 for a CSI).
            if bytes.get(i + 1) == Some(&b'[') && next > i + 2 && bytes[next - 1] == b'm' {
                style = apply_sgr(&line[i + 2..next - 1], base, style);
            }
            i = next;
            text_start = i;
        } else {
            i += 1;
        }
    }
    // Flush remaining text.
    if text_start < len {
        push_ansi_text(&mut spans, &line[text_start..], style);
    }
    // A line that is ENTIRELY escapes has no visible content — return no spans
    // (renders blank). Do NOT fall back to pushing the raw line here: that
    // would render the escape bytes as text AND make the wrap-height count
    // (which skips escapes via `skip_ansi_escape`) disagree with the render,
    // reintroducing the "wrap mode can't reach the last line" bug.
    spans
}

/// Apply SGR (Select Graphic Rendition) parameters to the current style.
/// Parameters are semicolon-separated numbers like "1;31" (bold red).
fn apply_sgr(
    params: &str,
    base: ratatui::style::Style,
    mut style: ratatui::style::Style,
) -> ratatui::style::Style {
    use ratatui::style::{Color, Modifier};

    let mut nums = params.split(';').filter_map(|s| s.parse::<u8>().ok()).peekable();
    // \x1b[m (empty params) is equivalent to \x1b[0m (reset).
    if nums.peek().is_none() {
        return base;
    }
    while let Some(n) = nums.next() {
        match n {
            0 => style = base,
            1 => style = style.add_modifier(Modifier::BOLD),
            2 => style = style.add_modifier(Modifier::DIM),
            3 => style = style.add_modifier(Modifier::ITALIC),
            4 => style = style.add_modifier(Modifier::UNDERLINED),
            7 => style = style.add_modifier(Modifier::REVERSED),
            22 => style = style.remove_modifier(Modifier::BOLD | Modifier::DIM),
            23 => style = style.remove_modifier(Modifier::ITALIC),
            24 => style = style.remove_modifier(Modifier::UNDERLINED),
            27 => style = style.remove_modifier(Modifier::REVERSED),
            // Standard foreground colors.
            30 => style = style.fg(Color::Black),
            31 => style = style.fg(Color::Red),
            32 => style = style.fg(Color::Green),
            33 => style = style.fg(Color::Yellow),
            34 => style = style.fg(Color::Blue),
            35 => style = style.fg(Color::Magenta),
            36 => style = style.fg(Color::Cyan),
            37 => style = style.fg(Color::Gray),
            38 => {
                // Extended foreground: 38;5;N (256-color) or 38;2;R;G;B (RGB).
                match nums.next() {
                    Some(5) => {
                        if let Some(idx) = nums.next() {
                            style = style.fg(Color::Indexed(idx));
                        }
                    }
                    Some(2) => {
                        let r = nums.next().unwrap_or(0);
                        let g = nums.next().unwrap_or(0);
                        let b = nums.next().unwrap_or(0);
                        style = style.fg(Color::Rgb(r, g, b));
                    }
                    _ => {}
                }
            }
            39 => style = style.fg(Color::Reset),
            // Standard background colors.
            40..=47 => {
                let colors = [Color::Black, Color::Red, Color::Green, Color::Yellow,
                              Color::Blue, Color::Magenta, Color::Cyan, Color::Gray];
                style = style.bg(colors[(n - 40) as usize]);
            }
            48 => {
                // Extended background: 48;5;N or 48;2;R;G;B.
                match nums.next() {
                    Some(5) => {
                        if let Some(idx) = nums.next() {
                            style = style.bg(Color::Indexed(idx));
                        }
                    }
                    Some(2) => {
                        let r = nums.next().unwrap_or(0);
                        let g = nums.next().unwrap_or(0);
                        let b = nums.next().unwrap_or(0);
                        style = style.bg(Color::Rgb(r, g, b));
                    }
                    _ => {}
                }
            }
            49 => style = style.bg(Color::Reset),
            // Bright foreground colors.
            90 => style = style.fg(Color::DarkGray),
            91 => style = style.fg(Color::LightRed),
            92 => style = style.fg(Color::LightGreen),
            93 => style = style.fg(Color::LightYellow),
            94 => style = style.fg(Color::LightBlue),
            95 => style = style.fg(Color::LightMagenta),
            96 => style = style.fg(Color::LightCyan),
            97 => style = style.fg(Color::White),
            // Bright background colors.
            100..=107 => {
                let colors = [Color::DarkGray, Color::LightRed, Color::LightGreen, Color::LightYellow,
                              Color::LightBlue, Color::LightMagenta, Color::LightCyan, Color::White];
                style = style.bg(colors[(n - 100) as usize]);
            }
            _ => {} // Unknown SGR code — ignore.
        }
    }
    style
}

#[cfg(test)]
#[path = "../tests/ui/ansi.rs"]
mod tests;
