use ratatui::{
    layout::{Constraint, Layout, Rect},
    text::{Line, Span},
    Frame,
};


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
        // `follow` is honoured here, not hardcoded `false`. `G` on a
        // freshly-loaded view set `follow` and moved to `max_scroll()` — but
        // the extent was still 0 because nothing had rendered yet, so it went
        // to the TOP and the flag was then ignored forever: the first `G`
        // after a load silently did nothing and only a second one worked.
        // Resolving follow against the extent measured THIS frame is the same
        // rule the log widget follows, and `line_up`/`home`/`scroll_to` all
        // clear the flag, so this cannot strand the view at the bottom.
        let following = cv.state.viewport.following();
        cv.state.viewport.set_metrics(lc, inner_rows, following);
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
                yaml.content(),
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
        // The input is WINDOWED to what the bar can hold, with the caret as
        // the anchor — see `TextInput::window`. The caret used to be dropped
        // entirely once the pattern outgrew the bar (`if cursor_x < right`),
        // so you typed blind into your own grep.
        let avail = (bar_area.width as usize).saturating_sub(2);
        let (shown, caret_col) = yaml.search_input.window(avail);
        let line = Line::from(Span::styled(format!(" /{}", shown), theme.filter));
        f.render_widget(line, bar_area);
        f.set_cursor_position((bar_area.x + 2 + caret_col as u16, bar_area.y));
    } else {
        // Keybinding bar
        // Projected from the ONE keymap declaration, so this bar cannot
        // advertise a key the dispatcher doesn't have (or miss one it does).
        let hints = crate::event::keymap::bar_hints(
            crate::event::keymap::ViewClass::Detail,
        );
        let line = crate::ui::header::render_keybinding_bar(&hints, bar_area.width, theme);
        f.render_widget(line, bar_area);
    }
}
