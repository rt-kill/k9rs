use ratatui::{
    layout::{Constraint, Layout, Rect},
    text::{Line, Span},
    widgets::{Block, Paragraph, Scrollbar, ScrollbarOrientation, ScrollbarState},
    Frame,
};

use crate::app::{App, Route};

/// Draw the describe view showing kubectl describe output.
///
/// Layout:
/// - Scrollable text content with search highlighting
/// - Bottom bar with keybindings (or search input)
pub fn draw_describe(f: &mut Frame, app: &App, area: Rect) {
    let theme = &app.theme;

    let chunks = Layout::vertical([
        Constraint::Fill(1),   // describe content
        Constraint::Length(1), // keybinding bar
    ])
    .split(area);

    let content_area = chunks[0];
    let bar_area = chunks[1];

    let describe = &app.describe;

    // Extract resource type and name from route if available
    let (resource_type, resource_name) = match &app.route {
        Route::Describe { resource, name, .. } => (resource.as_str(), name.as_str()),
        _ => ("unknown", "unknown"),
    };

    if !describe.content.is_empty() {
        let all_lines: Vec<&str> = describe.content.lines().collect();
        let total_lines = all_lines.len();

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

            // Only build Line objects for the visible window -- avoids the u16
            // scroll truncation at 65535 and is much faster for large content.
            let start = describe.scroll.min(total_lines.saturating_sub(visible_height.max(1)));
            let end = (start + visible_height).min(total_lines);

            let visible_lines: Vec<Line> = all_lines[start..end]
                .iter()
                .enumerate()
                .map(|(vi, &line)| {
                    let abs_line_idx = start + vi;

                    // Check if this line is a search match
                    let is_current_match = !describe.search_matches.is_empty()
                        && describe.current_match < describe.search_matches.len()
                        && describe.search_matches[describe.current_match] == abs_line_idx;
                    let is_match = describe.search_matches.binary_search(&abs_line_idx).is_ok();

                    if is_current_match {
                        Line::from(Span::styled(line, theme.search_match))
                    } else if is_match {
                        Line::from(Span::styled(line, theme.filter))
                    } else {
                        // Color-code describe output at any indentation level.
                        let trimmed = line.trim();
                        if trimmed.is_empty() {
                            Line::from(Span::styled(line, theme.row_normal))
                        } else if trimmed.ends_with(':') && trimmed.len() > 1 {
                            // Section header at any level (e.g. "Containers:", "  Limits:")
                            Line::from(Span::styled(line, theme.header))
                        } else if let Some(colon_pos) = trimmed.find(':') {
                            // Key: value pair at any indentation level.
                            // Preserve the leading whitespace.
                            let indent_len = line.len() - line.trim_start().len();
                            let indent = &line[..indent_len];
                            let key = &trimmed[..colon_pos];
                            let value = &trimmed[colon_pos..];
                            Line::from(vec![
                                Span::styled(indent.to_string(), theme.row_normal),
                                Span::styled(key.to_string(), theme.yaml_key),
                                Span::styled(value.to_string(), theme.row_normal),
                            ])
                        } else {
                            Line::from(Span::styled(line, theme.row_normal))
                        }
                    }
                })
                .collect();

            // Render with scroll=(0,0) since we already windowed the content
            let paragraph = Paragraph::new(visible_lines);
            f.render_widget(paragraph, inner);
        }

        // Scrollbar
        let inner = Block::bordered().inner(content_area);
        if total_lines > inner.height as usize {
            let mut scrollbar_state = ScrollbarState::new(total_lines)
                .position(describe.scroll);
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
        if inner.height > 0 && inner.width > 0 {
            let loading_text = crate::util::loading_bar("Loading...");
            let text_len = loading_text.len() as u16;
            let loading = Line::from(Span::styled(
                loading_text,
                theme.status_pending,
            ));
            let center_y = inner.y + inner.height / 2;
            let center_x = inner.x + inner.width.saturating_sub(text_len) / 2;
            f.render_widget(
                loading,
                Rect::new(center_x, center_y, inner.width, 1),
            );
        }
    }

    // Bottom bar: search input or keybinding hints
    let bg = " ".repeat(bar_area.width as usize);
    let bg_line = Line::styled(bg, theme.status_bar);
    f.render_widget(bg_line, bar_area);

    if describe.search_input_active {
        // Show search input prompt
        let prompt = format!(" /{}", describe.search_input);
        let line = Line::from(Span::styled(prompt, theme.filter));
        f.render_widget(line, bar_area);
        // Place cursor after the search input text
        let cursor_x = bar_area.x + 2 + describe.search_input.len() as u16; // +2 for " /"
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

        let mut spans = Vec::new();
        spans.push(Span::styled(" ", theme.status_bar));
        for (i, (key, desc)) in hints.iter().enumerate() {
            spans.push(Span::styled(format!("<{}>", key), theme.status_bar_key));
            spans.push(Span::styled(format!(" {} ", desc), theme.status_bar));
            if i < hints.len() - 1 {
                spans.push(Span::styled("\u{2502}", theme.status_bar));
            }
        }

        let line = Line::from(spans);
        f.render_widget(line, bar_area);
    }
}
