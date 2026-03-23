use ratatui::{
    buffer::Buffer,
    layout::Rect,
    style::Modifier,
    text::{Line, Span},
    widgets::{Block, StatefulWidget, Widget},
};

use crate::ui::theme::Theme;

/// State for the YAML viewer widget.
pub struct YamlViewState {
    pub scroll: usize,
    pub search: Option<String>,
    pub search_matches: Vec<usize>,
    pub current_match: usize,
}

impl YamlViewState {
    pub fn new() -> Self {
        Self {
            scroll: 0,
            search: None,
            search_matches: Vec::new(),
            current_match: 0,
        }
    }

    pub fn scroll_up(&mut self, amount: usize) {
        self.scroll = self.scroll.saturating_sub(amount);
    }

    pub fn scroll_down(&mut self, amount: usize, total_lines: usize, visible: usize) {
        let max_scroll = total_lines.saturating_sub(visible);
        self.scroll = (self.scroll + amount).min(max_scroll);
    }

    pub fn scroll_to_top(&mut self) {
        self.scroll = 0;
    }

    pub fn scroll_to_bottom(&mut self, total_lines: usize, visible: usize) {
        self.scroll = total_lines.saturating_sub(visible);
    }

    /// Find all lines containing the search term.
    pub fn update_search(&mut self, content: &str) {
        self.search_matches.clear();
        self.current_match = 0;

        if let Some(ref term) = self.search {
            if term.is_empty() {
                return;
            }
            let lower_term = term.to_lowercase();
            for (i, line) in content.lines().enumerate() {
                if line.to_lowercase().contains(&lower_term) {
                    self.search_matches.push(i);
                }
            }
        }
    }

    /// Jump to the next search match.
    pub fn next_match(&mut self, visible: usize) {
        if self.search_matches.is_empty() {
            return;
        }
        self.current_match = (self.current_match + 1) % self.search_matches.len();
        let target_line = self.search_matches[self.current_match];
        // Center the match in the viewport
        self.scroll = target_line.saturating_sub(visible / 2);
    }

    /// Jump to the previous search match.
    pub fn prev_match(&mut self, visible: usize) {
        if self.search_matches.is_empty() {
            return;
        }
        self.current_match = if self.current_match == 0 {
            self.search_matches.len() - 1
        } else {
            self.current_match - 1
        };
        let target_line = self.search_matches[self.current_match];
        self.scroll = target_line.saturating_sub(visible / 2);
    }
}

impl Default for YamlViewState {
    fn default() -> Self {
        Self::new()
    }
}

/// YAML viewer with syntax highlighting.
///
/// Supports scrolling, line numbers, and search-within-YAML highlighting.
/// Uses simple regex-based highlighting for YAML keys, strings, and numbers
/// rather than requiring syntect at render time.
pub struct YamlViewer<'a> {
    content: &'a str,
    title: &'a str,
    theme: &'a Theme,
}

impl<'a> YamlViewer<'a> {
    pub fn new(content: &'a str, title: &'a str, theme: &'a Theme) -> Self {
        Self {
            content,
            title,
            theme,
        }
    }

    /// Classify and style a single line of YAML.
    fn style_yaml_line<'b>(line: &'b str, theme: &'b Theme) -> Vec<Span<'b>> {
        let trimmed = line.trim();

        // Comment lines
        if trimmed.starts_with('#') {
            return vec![Span::styled(line, theme.row_normal.add_modifier(Modifier::DIM))];
        }

        // Lines with key: value
        if let Some(colon_pos) = find_yaml_colon(line) {
            let (key_part, rest) = line.split_at(colon_pos);
            let mut spans = vec![Span::styled(key_part.to_string(), theme.yaml_key)];

            // The colon
            if rest.len() > 1 {
                spans.push(Span::styled(":".to_string(), theme.yaml_key));
                let value = &rest[1..]; // skip ':'
                let value_trimmed = value.trim();

                if value_trimmed.is_empty() {
                    spans.push(Span::styled(value.to_string(), theme.row_normal));
                } else if value_trimmed.starts_with('"')
                    || value_trimmed.starts_with('\'')
                    || value_trimmed.starts_with('|')
                    || value_trimmed.starts_with('>')
                {
                    spans.push(Span::styled(value.to_string(), theme.yaml_string));
                } else if value_trimmed == "true"
                    || value_trimmed == "false"
                    || value_trimmed == "null"
                    || value_trimmed == "~"
                {
                    spans.push(Span::styled(value.to_string(), theme.yaml_number));
                } else if value_trimmed.parse::<f64>().is_ok() {
                    spans.push(Span::styled(value.to_string(), theme.yaml_number));
                } else {
                    spans.push(Span::styled(value.to_string(), theme.yaml_string));
                }
            } else {
                spans.push(Span::styled(":".to_string(), theme.yaml_key));
            }

            return spans;
        }

        // List items
        if trimmed.starts_with("- ") {
            let indent_len = line.len() - line.trim_start().len();
            let indent = &line[..indent_len];
            let rest = &line[indent_len..];
            return vec![
                Span::styled(indent.to_string(), theme.row_normal),
                Span::styled(rest.to_string(), theme.yaml_string),
            ];
        }

        // --- document separator
        if trimmed == "---" || trimmed == "..." {
            return vec![Span::styled(line, theme.border)];
        }

        vec![Span::styled(line.to_string(), theme.row_normal)]
    }
}

/// Find the position of the YAML key colon (not inside quotes).
fn find_yaml_colon(line: &str) -> Option<usize> {
    let trimmed = line.trim();
    if trimmed.starts_with("- ") || trimmed.starts_with('#') {
        return None;
    }

    let mut in_quote = false;
    let mut quote_char = ' ';
    for (i, ch) in line.char_indices() {
        if in_quote {
            if ch == quote_char {
                in_quote = false;
            }
            continue;
        }
        if ch == '"' || ch == '\'' {
            in_quote = true;
            quote_char = ch;
            continue;
        }
        if ch == ':' && (i + 1 >= line.len() || line.as_bytes().get(i + 1) == Some(&b' ') || i + 1 == line.len()) {
            return Some(i);
        }
    }
    None
}

impl StatefulWidget for YamlViewer<'_> {
    type State = YamlViewState;

    fn render(self, area: Rect, buf: &mut Buffer, state: &mut Self::State) {
        let lines: Vec<&str> = self.content.lines().collect();
        let total_lines = lines.len();

        // Build title
        let title_text = if let Some(ref search) = state.search {
            if state.search_matches.is_empty() {
                format!(" {} [/{} - no matches] ", self.title, search)
            } else {
                format!(
                    " {} [/{} - {}/{}] ",
                    self.title,
                    search,
                    state.current_match + 1,
                    state.search_matches.len()
                )
            }
        } else {
            format!(
                " {} [{}/{}] ",
                self.title,
                state.scroll + 1,
                total_lines
            )
        };

        let block = Block::bordered()
            .title(title_text)
            .title_style(self.theme.title)
            .border_style(self.theme.border);

        let inner = block.inner(area);
        block.render(area, buf);

        if inner.height == 0 || inner.width == 0 {
            return;
        }

        let visible_height = inner.height as usize;

        // Clamp scroll
        let max_scroll = total_lines.saturating_sub(visible_height);
        if state.scroll > max_scroll {
            state.scroll = max_scroll;
        }

        // Line number gutter width
        let line_num_width = format!("{}", total_lines).len() as u16 + 1; // +1 for padding

        let content_x = inner.x + line_num_width + 1; // +1 for separator
        let content_width = inner.width.saturating_sub(line_num_width + 1);

        // Render visible lines
        let end = (state.scroll + visible_height).min(total_lines);
        for (vi, line_idx) in (state.scroll..end).enumerate() {
            let y = inner.y + vi as u16;

            // Line number
            let line_num = format!("{:>width$}", line_idx + 1, width = line_num_width as usize);
            buf.set_string(inner.x, y, &line_num, self.theme.line_number);

            // Separator
            buf.set_string(
                inner.x + line_num_width,
                y,
                "\u{2502}",
                self.theme.border,
            );

            let line = lines[line_idx];

            // Check if this line is a search match
            let is_match = state.search_matches.binary_search(&line_idx).is_ok();
            let is_current_match = !state.search_matches.is_empty()
                && state.current_match < state.search_matches.len()
                && state.search_matches[state.current_match] == line_idx;

            if is_current_match {
                // Highlight the entire line for the current match
                buf.set_string(content_x, y, line, self.theme.search_match);
            } else if is_match {
                // Dim highlight for other matches
                let match_style = self.theme.filter;
                buf.set_string(content_x, y, line, match_style);
            } else {
                // Syntax-highlighted YAML
                let spans = Self::style_yaml_line(line, self.theme);
                let styled_line = Line::from(spans);
                buf.set_line(content_x, y, &styled_line, content_width);
            }
        }

        // Scrollbar indicator (right-edge marks)
        if total_lines > visible_height {
            let scrollbar_height = visible_height;
            let thumb_size = ((visible_height as f64 / total_lines as f64)
                * scrollbar_height as f64)
                .max(1.0) as usize;
            let max_scroll_val = total_lines.saturating_sub(visible_height);
            let scroll_pos = state.scroll.min(max_scroll_val);
            let thumb_pos = if max_scroll_val > 0 {
                ((scroll_pos as f64 / max_scroll_val as f64)
                    * (scrollbar_height - thumb_size) as f64) as usize
            } else {
                0
            };

            let scrollbar_x = inner.x + inner.width - 1;
            for i in 0..scrollbar_height {
                let y = inner.y + i as u16;
                if i >= thumb_pos && i < thumb_pos + thumb_size {
                    buf.set_string(scrollbar_x, y, "\u{2588}", self.theme.border);
                } else {
                    buf.set_string(scrollbar_x, y, "\u{2591}", self.theme.border);
                }
            }
        }
    }
}
