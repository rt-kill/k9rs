use ratatui::{
    layout::{Constraint, Layout, Rect},
    text::{Line, Span},
    Frame,
};

use unicode_width::UnicodeWidthStr;

use crate::app::App;
use crate::ui::widgets::{YamlViewer, YamlViewState};

/// Draw the YAML view with syntax highlighting.
///
/// Layout:
/// - YAML content with line numbers and syntax highlighting
/// - Bottom bar with keybindings (or search input)
pub fn draw_yaml(f: &mut Frame, app: &mut App, area: Rect) {
    let theme = &app.ui.theme;

    let chunks = Layout::vertical([
        Constraint::Fill(1),   // YAML content
        Constraint::Length(1), // keybinding bar
    ])
    .split(area);

    let content_area = chunks[0];
    let bar_area = chunks[1];

    // Publish the (non-wrapping) extent to the viewport BEFORE reading the
    // offset — the render owns geometry, the action layer never guesses it.
    let inner_rows = content_area.height.saturating_sub(2) as usize;
    if let crate::app::element::Element::ContentView(cv) = app.nav.top_mut() {
        let lc = cv.state.line_count();
        cv.state.viewport.set_metrics(lc, inner_rows, false);
    }

    // The top element IS the view.
    use crate::app::element::{ContentSpec, Element};
    let Element::ContentView(cv) = app.nav.top() else { return };
    let (resource_type, resource_name) = match &cv.kind {
        ContentSpec::Yaml(target)
        | ContentSpec::Describe(target)
        | ContentSpec::DecodedSecret(target) => {
            (target.resource.display_label(), target.name.as_str())
        }
        ContentSpec::Aliases => ("unknown", "unknown"),
    };
    let yaml_title = format!("YAML: {}/{}", resource_type, resource_name);
    let yaml = &cv.state;

    // The PHASE decides the chrome, not content emptiness: a spinner is
    // only ever shown while a fetch is genuinely in flight, a failed fetch
    // says so (static, with the retry hint), and Ready renders whatever
    // arrived.
    match &cv.phase {
        crate::app::element::ContentPhase::Fetching => {
            let block = ratatui::widgets::Block::bordered()
                .title(format!(" {} ", yaml_title))
                .title_style(theme.title)
                .border_style(theme.border);
            let inner = block.inner(content_area);
            f.render_widget(block, content_area);
            crate::ui::draw_centered_loading(f, inner, "Loading...", theme.status_pending, &app.ui.anim);
        }
        crate::app::element::ContentPhase::Failed(reason) => {
            let block = ratatui::widgets::Block::bordered()
                .title(format!(" {} ", yaml_title))
                .title_style(theme.title)
                .border_style(theme.border);
            let inner = block.inner(content_area);
            f.render_widget(block, content_area);
            crate::ui::draw_centered_message(
                f,
                inner,
                &format!("✗ {} — Ctrl-R retries", reason),
                theme.status_failed,
            );
        }
        crate::app::element::ContentPhase::Ready => {
            let viewer = YamlViewer::new(
                &yaml.content,
                &yaml_title,
                theme,
            );

            let mut view_state = YamlViewState {
                scroll: yaml.viewport.offset(),
                search: yaml.search.clone(),
                search_matches: yaml.search_matches.clone(),
                current_match: yaml.current_match,
            };

            f.render_stateful_widget(viewer, content_area, &mut view_state);
        }
    }

    // Bottom bar: search input or keybinding hints
    crate::ui::fill_line_bg(f, bar_area, theme.status_bar);

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
