use ratatui::{
    layout::{Constraint, Layout, Rect},
    text::{Line, Span},
    widgets::{Block, Paragraph, Scrollbar, ScrollbarOrientation, ScrollbarState},
    Frame,
};

use unicode_width::UnicodeWidthStr;

use crate::app::{App, Route};

/// Draw the describe view showing kubectl describe output.
///
/// Layout:
/// - Scrollable text content with search highlighting
/// - Bottom bar with keybindings (or search input)
pub fn draw_describe(f: &mut Frame, app: &App, area: Rect) {
    let theme = &app.ui.theme;

    let chunks = Layout::vertical([
        Constraint::Fill(1),   // describe content
        Constraint::Length(1), // keybinding bar
    ])
    .split(area);

    let content_area = chunks[0];
    let bar_area = chunks[1];

    // Extract state and resource type/name from route
    let (describe, resource_type, resource_name) = match &app.route {
        Route::ContentView { kind: crate::app::ContentViewKind::Aliases, ref state, .. } => (state, "aliases", ""),
        Route::ContentView { target: Some(ref target), ref state, .. } => (state, target.resource.display_label(), target.name.as_str()),
        _ => return, // Not a describe view — nothing to draw
    };

    if !describe.content.is_empty() {
        // Render from the producer's typed lines when present (a structured
        // describe); fall back to text inference for genuinely-opaque content
        // (YAML, aliases, kubectl-fallback text) where no structure is known.
        let typed = !describe.describe_lines.is_empty();
        let total_lines = if typed {
            describe.describe_lines.len()
        } else {
            describe.content.lines().count()
        };

        // Build title with optional search info
        let title = if let Some(ref search) = describe.search {
            if describe.search_matches.is_empty() {
                format!(
                    " Describe: {}/{} [/{} - no matches] ",
                    resource_type, resource_name, search
                )
            } else {
                format!(
                    " Describe: {}/{} [/{} - {}/{}] ",
                    resource_type, resource_name, search,
                    describe.current_match + 1,
                    describe.search_matches.len()
                )
            }
        } else {
            format!(
                " Describe: {}/{} [{}/{}] ",
                resource_type,
                resource_name,
                describe.scroll + 1,
                total_lines
            )
        };

        let block = Block::bordered()
            .title(title)
            .title_style(theme.title)
            .border_style(theme.border);

        let inner = block.inner(content_area);
        f.render_widget(block, content_area);

        if inner.height > 0 && inner.width > 0 {
            let visible_height = inner.height as usize;

            let max_scroll = crate::util::content_max_scroll(total_lines, visible_height);
            let start = describe.scroll.min(max_scroll);
            let end = (start + visible_height).min(total_lines);

            // A search match (current or other) overrides the per-line role
            // styling below; returns the highlight style for an absolute index.
            let highlight = |abs_idx: usize| -> Option<ratatui::style::Style> {
                if !describe.search_matches.is_empty()
                    && describe.current_match < describe.search_matches.len()
                    && describe.search_matches[describe.current_match] == abs_idx
                {
                    Some(theme.search_match)
                } else if describe.search_matches.binary_search(&abs_idx).is_ok() {
                    Some(theme.filter)
                } else {
                    None
                }
            };

            let visible_lines: Vec<Line> = if typed {
                describe.describe_lines[start..end]
                    .iter()
                    .enumerate()
                    .map(|(vi, dl)| match highlight(start + vi) {
                        Some(style) => Line::from(Span::styled(dl.text.as_str(), style)),
                        None => match &dl.kind {
                            crate::kube::protocol::DescribeLineKind::Section => {
                                Line::from(Span::styled(dl.text.as_str(), theme.header))
                            }
                            crate::kube::protocol::DescribeLineKind::Field { key_end } => {
                                field_spans(&dl.text, *key_end, theme)
                            }
                            crate::kube::protocol::DescribeLineKind::Plain => {
                                Line::from(Span::styled(dl.text.as_str(), theme.row_normal))
                            }
                        },
                    })
                    .collect()
            } else {
                describe
                    .content
                    .lines()
                    .skip(start)
                    .take(end.saturating_sub(start))
                    .enumerate()
                    .map(|(vi, line)| match highlight(start + vi) {
                        Some(style) => Line::from(Span::styled(line, style)),
                        None => infer_line(line, theme),
                    })
                    .collect()
            };

            // Render with scroll=(0,0) since we already windowed the content
            let paragraph = Paragraph::new(visible_lines);
            f.render_widget(paragraph, inner);
        }

        let inner = Block::bordered().inner(content_area);
        if total_lines > inner.height as usize {
            let max_scroll = crate::util::content_max_scroll(total_lines, inner.height as usize);
            let mut scrollbar_state = ScrollbarState::new(max_scroll)
                .position(describe.scroll.min(max_scroll));
            let scrollbar = Scrollbar::new(ScrollbarOrientation::VerticalRight);
            f.render_stateful_widget(scrollbar, content_area, &mut scrollbar_state);
        }
    } else {
        let block = Block::bordered()
            .title(format!(" Describe: {}/{} ", resource_type, resource_name))
            .title_style(theme.title)
            .border_style(theme.border);
        let inner = block.inner(content_area);
        f.render_widget(block, content_area);
        crate::ui::draw_centered_loading(f, inner, "Loading...", theme.status_pending);
    }

    // Bottom bar: search input or keybinding hints
    crate::ui::fill_line_bg(f, bar_area, theme.status_bar);

    if describe.search_input_active {
        // Show search input prompt
        let prompt = format!(" /{}", describe.search_input);
        let line = Line::from(Span::styled(prompt, theme.filter));
        f.render_widget(line, bar_area);
        // Place cursor after the search input text
        let cursor_x = bar_area.x + 2 + describe.search_input.width() as u16; // +2 for " /"
        let cursor_y = bar_area.y;
        if cursor_x < bar_area.x + bar_area.width {
            f.set_cursor_position((cursor_x, cursor_y));
        }
    } else {
        // Keybinding bar
        let hints = vec![
            ("j/k", "scroll"),
            ("g/G", "top/bottom"),
            ("Ctrl-d/u", "page"),
            ("/", "search"),
            ("n/N", "next/prev"),
            ("Esc", "back"),
        ];

        let line = crate::ui::header::render_keybinding_bar(&hints, theme);
        f.render_widget(line, bar_area);
    }
}

/// Style a tagged `Field` line: indent + key + value (colon onward), matching
/// the long-standing visual but driven by the producer's `key_end` rather than
/// the renderer re-finding the colon.
fn field_spans<'a>(text: &'a str, key_end: usize, theme: &crate::ui::theme::Theme) -> Line<'a> {
    let indent_len = text.len() - text.trim_start().len();
    if indent_len <= key_end && key_end <= text.len() && text.is_char_boundary(key_end) {
        Line::from(vec![
            Span::styled(&text[..indent_len], theme.row_normal),
            Span::styled(&text[indent_len..key_end], theme.yaml_key),
            Span::styled(&text[key_end..], theme.row_normal),
        ])
    } else {
        // Defensive: a malformed tag renders plain rather than panic-slicing.
        Line::from(Span::styled(text, theme.row_normal))
    }
}

/// Best-effort styling for genuinely-opaque text (YAML, aliases, kubectl
/// fallback): the former render-time inference, retained only where there is no
/// producer structure to trust.
fn infer_line<'a>(line: &'a str, theme: &crate::ui::theme::Theme) -> Line<'a> {
    let trimmed = line.trim();
    if trimmed.is_empty() {
        Line::from(Span::styled(line, theme.row_normal))
    } else if trimmed.ends_with(':') && trimmed.len() > 1 {
        Line::from(Span::styled(line, theme.header))
    } else if let Some(colon_pos) = trimmed.find(':') {
        let indent_len = line.len() - line.trim_start().len();
        Line::from(vec![
            Span::styled(&line[..indent_len], theme.row_normal),
            Span::styled(&trimmed[..colon_pos], theme.yaml_key),
            Span::styled(&trimmed[colon_pos..], theme.row_normal),
        ])
    } else {
        Line::from(Span::styled(line, theme.row_normal))
    }
}
