pub mod header;
pub mod theme;
pub mod views;
pub mod widgets;

/// Header panel height in rows (context/cluster/user + logo).
pub const HEADER_HEIGHT: u16 = 5;

/// Guaranteed-visible dark background for dialog interiors. Applied after
/// Clear + Block to prevent skin-overridable `dialog_bg: default` from
/// making dialogs invisible against the terminal background.
pub const DIALOG_BG: ratatui::style::Color = ratatui::style::Color::Rgb(25, 28, 38);

/// Fill a rectangular area with the dialog background color.
pub fn fill_dialog_bg(buf: &mut ratatui::buffer::Buffer, area: ratatui::layout::Rect) {
    let style = ratatui::style::Style::default().bg(DIALOG_BG);
    for row in area.y..area.y + area.height {
        for col in area.x..area.x + area.width {
            buf[(col, row)].set_style(style);
        }
    }
}

/// Center a rectangle of given dimensions within an outer area.
pub fn centered_rect(area: ratatui::layout::Rect, width: u16, height: u16) -> ratatui::layout::Rect {
    use ratatui::layout::{Constraint, Layout};
    let vert = Layout::vertical([
        Constraint::Fill(1), Constraint::Length(height), Constraint::Fill(1),
    ]).split(area);
    let horiz = Layout::horizontal([
        Constraint::Fill(1), Constraint::Length(width), Constraint::Fill(1),
    ]).split(vert[1]);
    horiz[1]
}

/// Render a centered loading spinner+text inside an area. Used by yaml,
/// describe, resource, and log views when awaiting initial data.
pub fn draw_centered_loading(
    f: &mut ratatui::Frame,
    area: ratatui::layout::Rect,
    message: &str,
    style: ratatui::style::Style,
) {
    if area.height == 0 || area.width == 0 {
        return;
    }
    let text = crate::util::loading_bar(message);
    let text_len = UnicodeWidthStr::width(text.as_str()) as u16;
    let line = ratatui::text::Line::from(ratatui::text::Span::styled(text, style));
    let center_y = area.y + area.height / 2;
    let center_x = area.x + area.width.saturating_sub(text_len) / 2;
    f.render_widget(line, ratatui::layout::Rect::new(center_x, center_y, area.width, 1));
}

/// Fill a single-row area with a background style without allocating a string.
pub fn fill_line_bg(f: &mut ratatui::Frame, area: ratatui::layout::Rect, style: ratatui::style::Style) {
    let buf = f.buffer_mut();
    for x in area.x..area.x + area.width {
        buf[(x, area.y)].set_style(style);
    }
}

use unicode_width::UnicodeWidthStr;

use ratatui::layout::Rect;
use ratatui::style::Modifier;
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Clear, Widget};
use ratatui::Frame;

use crate::app::{App, Route};
use crate::ui::widgets::{ConfirmDialogWidget, FormDialogWidget, FilterBar, FlashWidget, HelpOverlay};

/// Main UI draw function.
///
/// Routes to the correct view based on `app.route`, then overlays flash messages,
/// help screen, and confirmation dialogs as needed.
pub fn draw(f: &mut Frame, app: &mut App) {
    let area = f.area();

    match &app.route {
        Route::Overview => {
            views::overview::draw_overview(f, app, area);
        }
        Route::Resources => {
            views::resource::draw_resources(f, app, area);
        }
        Route::ContentView { kind: crate::app::ContentViewKind::Yaml, .. } => {
            views::yaml::draw_yaml(f, app, area);
        }
        Route::ContentView { .. } => {
            // Describe and Aliases both use the describe view
            views::describe::draw_describe(f, app, area);
        }
        Route::Logs { .. } => {
            views::log::draw_logs(f, app, area);
        }
        Route::Shell(ref shell) => {
            views::shell::draw_shell(f, shell, area, &app.ui.theme);
        }
        Route::Help => {
            // Draw resource view underneath the help overlay
            views::resource::draw_resources(f, app, area);
            draw_help_overlay(f, app);
        }
        Route::Contexts => {
            views::context::draw_contexts(f, app, area);
        }
        Route::ContainerSelect { ref target, selected, .. } => {
            let target = target.clone();
            let sel = *selected;
            views::resource::draw_resources(f, app, area);
            draw_container_select(f, app, &target, sel);
        }
        Route::EditingResource { .. } => {
            // Snapshot the label first (immutable borrow on `app.route`),
            // then run the resource view (mutable borrow on `app`), then
            // draw the overlay (immutable borrow on `app.ui.theme`). The
            // borrows are disjoint in time so the borrow checker accepts
            // them in this order.
            let label = if let Route::EditingResource { ref target, ref state } = app.route {
                let kind = target.resource.display_label().to_string();
                let name = target.name.clone();
                match state {
                    crate::app::EditState::AwaitingYaml => format!("Fetching YAML for {}/{}…", kind, name),
                    crate::app::EditState::EditorReady { .. } => format!("Launching editor for {}/{}…", kind, name),
                    crate::app::EditState::Applying { .. } => format!("Applying {}/{}…", kind, name),
                }
            } else { String::new() };
            views::resource::draw_resources(f, app, area);
            draw_centered_overlay(f, &label, &app.ui.theme);
        }
    }

    // Draw command prompt overlay on top of any view when command mode is active.
    // In resource/context views this is handled inline, but for sub-views
    // (logs, describe, yaml) we need to draw it as an overlay so the user can
    // see what they're typing.
    if !matches!(app.route, Route::Resources | Route::Contexts | Route::Overview) {
        if app.ui.input_mode.is_active() {
            draw_command_overlay(f, app);
        }
        if app.nav.filter_input().active {
            draw_filter_overlay(f, app);
        }
    }

    // Draw flash messages on top of everything
    draw_flash(f, app);

    // Draw confirmation dialog if present
    draw_confirm_dialog(f, app);

    // Draw the generic form dialog if present (Scale, PortForward, …).
    if let Some(ref dialog) = app.ui.form_dialog {
        let widget = FormDialogWidget::new(dialog, &app.ui.theme);
        f.render_widget(widget, f.area());
    }
}

/// Draw the help overlay on top of the current view.
fn draw_help_overlay(f: &mut Frame, app: &App) {
    let theme = &app.ui.theme;
    let help = HelpOverlay::new(theme, app.ui.help_scroll, Some(app.current_capabilities()));
    f.render_widget(help, f.area());
}

/// Draw a single status line centered horizontally in the visible area.
/// Used by the modal stages of the unified edit flow ("Fetching YAML…",
/// "Launching editor…", "Applying…") to give the user feedback while the
/// async pipeline runs.
fn draw_centered_overlay(f: &mut Frame, label: &str, theme: &crate::ui::theme::Theme) {
    use ratatui::widgets::Clear;
    let area = f.area();
    if area.height == 0 || area.width == 0 {
        return;
    }
    let len = label.chars().count() as u16;
    let pad = 2u16;
    let box_w = (len + pad * 2 + 2).min(area.width);
    let box_h = 3u16.min(area.height);
    let x = area.x + (area.width.saturating_sub(box_w)) / 2;
    let y = area.y + (area.height.saturating_sub(box_h)) / 2;
    let rect = ratatui::layout::Rect::new(x, y, box_w, box_h);
    f.render_widget(Clear, rect);
    let block = ratatui::widgets::Block::bordered()
        .border_style(theme.dialog_border)
        .style(theme.dialog_bg);
    let inner = block.inner(rect);
    f.render_widget(block, rect);
    if inner.height > 0 && inner.width > 0 {
        let line = Line::from(Span::styled(label, theme.info_value));
        let lx = inner.x + inner.width.saturating_sub(len) / 2;
        f.render_widget(line, ratatui::layout::Rect::new(lx, inner.y, inner.width, 1));
    }
}

/// Draw flash messages at the bottom of the screen.
fn draw_flash(f: &mut Frame, app: &App) {
    if let Some(ref flash) = app.ui.flash {
        if flash.is_expired() {
            return;
        }

        let theme = &app.ui.theme;
        let area = f.area();

        // Position flash at the very bottom line
        if area.height < 1 {
            return;
        }
        let flash_area = ratatui::layout::Rect::new(
            area.x,
            area.y + area.height.saturating_sub(1),
            area.width,
            1,
        );

        let flash_widget = FlashWidget::new(flash, theme);
        f.render_widget(flash_widget, flash_area);
    }
}

/// Draw the container selection overlay for multi-container pods.
fn draw_container_select(
    f: &mut Frame,
    app: &App,
    target: &crate::kube::protocol::ObjectRef,
    selected_idx: usize,
) {
    use ratatui::layout::{Constraint, Layout, Rect};
    use ratatui::text::{Line, Span};
    use ratatui::widgets::{Block, Clear};

    let theme = &app.ui.theme;
    let area = f.area();

    // Find the pod's containers (typed field on ResourceRow). Init
    // containers get an `init:` display prefix so the user can tell
    // them apart from regular containers — derived from the typed
    // `kind` discriminant rather than carried as a string prefix in
    // `name` like the older shape.
    use crate::kube::resources::row::ContainerKind;
    let containers: Vec<String> = app.table_for(&target.resource)
        .and_then(|t| t.items.iter().find(|p| {
            p.name == target.name && p.namespace.as_deref() == target.namespace.as_option()
        }))
        .map(|p| p.containers.iter().map(|ci| match ci.kind {
            ContainerKind::Init => format!("init:{}", ci.name),
            ContainerKind::Regular => ci.name.clone(),
        }).collect())
        .unwrap_or_default();

    if containers.is_empty() {
        return;
    }

    // Size the dialog
    let max_name_len = containers.iter().map(|c| c.len()).max().unwrap_or(10);
    let dialog_width = (max_name_len as u16 + 6).clamp(30, 60).min(area.width.saturating_sub(4));
    let dialog_height = (containers.len() as u16 + 4).min(area.height.saturating_sub(4));

    // Center the dialog
    let vert = Layout::vertical([
        Constraint::Fill(1),
        Constraint::Length(dialog_height),
        Constraint::Fill(1),
    ]).split(area);
    let horiz = Layout::horizontal([
        Constraint::Fill(1),
        Constraint::Length(dialog_width),
        Constraint::Fill(1),
    ]).split(vert[1]);
    let dialog_area = horiz[1];

    Clear.render(dialog_area, f.buffer_mut());

    let block = Block::bordered()
        .title(Span::styled(
            format!(" Select Container ({}) ", target.name),
            theme.title,
        ))
        .border_style(theme.dialog_border)
        .style(theme.dialog_bg);

    let inner = block.inner(dialog_area);
    f.render_widget(block, dialog_area);

    if inner.height == 0 || inner.width == 0 {
        return;
    }

    fill_dialog_bg(f.buffer_mut(), inner);

    // Render container list
    for (i, container) in containers.iter().enumerate() {
        if i as u16 >= inner.height.saturating_sub(1) {
            break;
        }
        let y = inner.y + i as u16;
        let is_selected = i == selected_idx;
        let style = if is_selected { theme.selected } else { theme.row_normal };

        // Fill background for selected item
        if is_selected {
            for dx in 0..inner.width {
                f.buffer_mut().set_string(inner.x + dx, y, " ", theme.selected);
            }
        }

        let line = Line::from(Span::styled(format!("  {}", container), style));
        f.render_widget(line, Rect::new(inner.x, y, inner.width, 1));
    }

    // Hint at bottom
    let hint_y = inner.y + inner.height.saturating_sub(1);
    let hint = Line::from(vec![
        Span::styled(" <Enter>", theme.status_bar_key),
        Span::styled(" select  ", theme.help_desc),
        Span::styled("<Esc>", theme.status_bar_key),
        Span::styled(" back", theme.help_desc),
    ]);
    f.render_widget(hint, Rect::new(inner.x, hint_y, inner.width, 1));
}

/// Draw the confirmation dialog overlay.
fn draw_confirm_dialog(f: &mut Frame, app: &App) {
    if let Some(ref dialog) = app.ui.confirm_dialog {
        let theme = &app.ui.theme;
        let dialog_widget = ConfirmDialogWidget::new(dialog, theme);
        f.render_widget(dialog_widget, f.area());
    }
}

/// Draw the command prompt as an overlay at the bottom of the screen.
/// Used for sub-views (logs, describe, yaml) where the resource view's
/// inline command prompt is not available.
fn draw_command_overlay(f: &mut Frame, app: &App) {
    let area = f.area();
    if area.height < 5 {
        return;
    }
    let theme = &app.ui.theme;

    // 3-line box at the bottom, above the last 2 lines (indicator/flash)
    let overlay_y = area.y + area.height.saturating_sub(5);
    let overlay_area = Rect::new(area.x, overlay_y, area.width, 3);

    // Clear the area underneath
    Clear.render(overlay_area, f.buffer_mut());

    let block = Block::bordered()
        .border_style(theme.border_focused);
    let inner = block.inner(overlay_area);
    f.render_widget(block, overlay_area);

    if inner.height == 0 || inner.width == 0 {
        return;
    }

    let input = app.ui.input_mode.input().unwrap_or("");
    let ghost: String = app.best_completion()
        .and_then(|c| c.strip_prefix(input).map(str::to_string))
        .unwrap_or_default();

    let prefix = app.ui.input_mode.prompt();
    let prefix_len: u16 = prefix.width() as u16;
    let typed_len = input.width() as u16;

    let mut spans = vec![
        Span::styled(prefix, theme.command.add_modifier(Modifier::BOLD)),
        Span::styled(input, theme.command),
    ];

    if !ghost.is_empty() {
        spans.push(Span::styled(
            ghost,
            theme.command_suggestion.add_modifier(Modifier::DIM | Modifier::ITALIC),
        ));
    }

    let line = Line::from(spans);
    f.render_widget(line, inner);

    // Place terminal cursor
    let cursor_x = inner.x + prefix_len + typed_len;
    let cursor_y = inner.y;
    if cursor_x < inner.x + inner.width {
        f.set_cursor_position((cursor_x, cursor_y));
    }
}

/// Draw the filter bar as an overlay at the bottom of the screen.
/// Used for sub-views where the resource view's inline filter bar is not available.
fn draw_filter_overlay(f: &mut Frame, app: &App) {
    let area = f.area();
    if area.height < 5 {
        return;
    }
    let theme = &app.ui.theme;

    let overlay_y = area.y + area.height.saturating_sub(5);
    let overlay_area = Rect::new(area.x, overlay_y, area.width, 3);

    Clear.render(overlay_area, f.buffer_mut());

    let counts = app.active_table_items_count();
    let match_count = if app.nav.filter_input().text.is_empty() { counts.total } else { counts.filtered };
    let filter_bar = FilterBar::new(
        app.nav.filter_input().active,
        &app.nav.filter_input().text,
        match_count,
        counts.total,
        theme,
    );
    f.render_widget(filter_bar, overlay_area);
}
