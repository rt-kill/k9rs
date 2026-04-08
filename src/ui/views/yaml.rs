use ratatui::{
    layout::{Constraint, Layout, Rect},
    text::{Line, Span},
    Frame,
};

use unicode_width::UnicodeWidthStr;

use crate::app::{App, Route};
use crate::ui::widgets::{YamlViewer, YamlViewState};

/// Draw the YAML view with syntax highlighting.
///
/// Layout:
/// - YAML content with line numbers and syntax highlighting
/// - Bottom bar with keybindings (or search input)
pub fn draw_yaml(f: &mut Frame, app: &App, area: Rect) {
    let theme = &app.theme;

    let chunks = Layout::vertical([
        Constraint::Fill(1),   // YAML content
        Constraint::Length(1), // keybinding bar
    ])
    .split(area);

    let content_area = chunks[0];
    let bar_area = chunks[1];

    // Extract resource type and name from route
    let (resource_type, resource_name) = match &app.route {
        Route::Yaml { ref target, .. } => (target.resource.display_label(), target.name.as_str()),
        _ => ("unknown", "unknown"),
    };
    let yaml_title = format!("YAML: {}/{}", resource_type, resource_name);

    let yaml = match &app.route {
        Route::Yaml { ref state, .. } => state,
        _ => return, // Not a yaml view — nothing to draw
    };

    if !yaml.content.is_empty() {
        let viewer = YamlViewer::new(
            &yaml.content,
            &yaml_title,
            theme,
        );

        let mut view_state = YamlViewState {
            scroll: yaml.scroll,
            search: yaml.search.clone(),
            search_matches: yaml.search_matches.clone(),
            current_match: yaml.current_match,
        };

        f.render_stateful_widget(viewer, content_area, &mut view_state);
    } else {
        let block = ratatui::widgets::Block::bordered()
            .title(format!(" {} ", yaml_title))
            .title_style(theme.title)
            .border_style(theme.border);
        let inner = block.inner(content_area);
        f.render_widget(block, content_area);
        if inner.height > 0 && inner.width > 0 {
            let loading_text = crate::util::loading_bar("Loading...");
            let text_len = loading_text.width() as u16;
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

    if yaml.search_input_active {
        // Show search input prompt
        let prompt = format!(" /{}", yaml.search_input);
        let line = Line::from(Span::styled(prompt, theme.filter));
        f.render_widget(line, bar_area);
        // Place cursor after the search input text
        let cursor_x = bar_area.x + 2 + yaml.search_input.width() as u16; // +2 for " /"
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
