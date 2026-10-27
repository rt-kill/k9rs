pub mod header;
pub(crate) mod views;
pub mod ansi;
pub(crate) mod widgets;

/// Header panel height in rows (context/cluster/user + logo).
pub const HEADER_HEIGHT: u16 = 5;


/// Fill a rectangular area with the theme's dialog background
/// ([`k9rs_theme::Theme::dialog_fill`]). Applied after Clear + Block: the fill is
/// opaque and deliberately NOT skin-overridable, so a skin setting
/// `dialog_bg: default` can't render dialogs invisible against the terminal.
/// It follows the palette rather than being a const, so a light theme gets a
/// light dialog instead of a near-black hole.
pub fn fill_dialog_bg(
    buf: &mut ratatui::buffer::Buffer,
    area: ratatui::layout::Rect,
    theme: &k9rs_theme::Theme,
) {
    let style = ratatui::style::Style::default().bg(theme.dialog_fill);
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
    anim: &k9rs_app::Anim,
) {
    if area.height == 0 || area.width == 0 {
        return;
    }
    let text = anim.bar(message);
    let text_len = UnicodeWidthStr::width(text.as_str()) as u16;
    let line = ratatui::text::Line::from(ratatui::text::Span::styled(text, style));
    let center_y = area.y + area.height / 2;
    let center_x = area.x + area.width.saturating_sub(text_len) / 2;
    f.render_widget(line, ratatui::layout::Rect::new(center_x, center_y, area.width, 1));
}

/// Static counterpart of [`draw_centered_loading`]: one centered message,
/// NO spinner and no animation liveness — for terminal states (a failed
/// fetch, honest emptiness) where animated chrome would falsely promise
/// that something is still in flight.
pub fn draw_centered_message(
    f: &mut ratatui::Frame,
    area: ratatui::layout::Rect,
    message: &str,
    style: ratatui::style::Style,
) {
    if area.height == 0 || area.width == 0 {
        return;
    }
    let text_len = UnicodeWidthStr::width(message) as u16;
    let line = ratatui::text::Line::from(ratatui::text::Span::styled(message.to_string(), style));
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

use k9rs_app::Element;
use k9rs_app::{App, TakeoverOp};
use crate::widgets::{ConfirmDialogWidget, FormDialogWidget, FilterBar, FlashWidget};

/// Main UI draw function.
///
/// Blindly renders the TOP nav element (exhaustive over element kinds),
/// then layers the overlay slot and the input/flash/dialog chrome.
pub fn draw(f: &mut Frame, app: &mut App) {
    // TWO rects, deliberately named apart: `frame` is the terminal, `top_area`
    // is whatever the top element draws into. They differ exactly when the top
    // element floats — and when a `FullFrame` takeover went out over a float,
    // the one name shadowing the other meant "erase the frame" erased the
    // DIALOG BOX and the shell painted inside it. A takeover claiming the
    // whole frame is a statement about the terminal, never about whatever
    // happens to be on top of the stack.
    let frame = f.area();

    // A FLOATING element is drawn over the view beneath it, so that view is
    // painted first — and its box is `Clear`ed before the float lands, so a
    // view that paints only chrome can't leave the parent inside its border.
    // Driven by the element's OWN declared extent, the same rule the overlay
    // slot follows, so neither the erase nor the parent paint can be
    // forgotten by an individual view.
    // Warm the top element's view memo BEFORE asking its extent: a table
    // dialog sizes its box from the same `col_widths` the renderer lays out
    // with, and a cold memo would have it answer "no opinion" on the one
    // frame that opens it. Cheap — `view()` returns the memo when warm, and
    // the action path warms it too.
    app.nav
        .top_mut()
        .ensure_view_cached(app.ui.column_level, app.config.ui.max_column_width);
    let top_area = match app.nav.top().extent(frame) {
        k9rs_app::ViewExtent::Full => frame,
        k9rs_app::ViewExtent::Dialog { width, height } => {
            if let Some(float) = app.nav.lift_top_for_render() {
                draw_top_element(f, app, frame);
                app.nav.replace_lifted(float);
            }
            dialog_box(f, frame, width, height, &app.ui.theme)
        }
    };

    draw_top_element(f, app, top_area);

    // The single modal slot, over whatever view is showing.
    //
    // Its area comes from the SAME `ViewExtent` a floating element declares,
    // placed by the same `dialog_box`. The view above still painted (it
    // publishes the render geometry and the select-mode bit the action layer
    // reads back), but a `Full` takeover gets an erased frame, so nothing of
    // it may show through — a renderer that paints only chrome cannot leave
    // the old view inside its border. Erasing centrally is what stops that
    // being re-remembered per renderer.
    let takeover_area = match app.ui.takeover().map(|op| op.extent(frame)) {
        None => frame,
        Some(k9rs_app::ViewExtent::Full) => {
            f.render_widget(Clear, frame);
            frame
        }
        Some(k9rs_app::ViewExtent::Dialog { width, height }) => {
            dialog_box(f, frame, width, height, &app.ui.theme)
        }
    };
    match &app.ui.takeover() {
        Some(op @ TakeoverOp::Edit { .. }) => {
            draw_takeover_label(f, &op.label(), takeover_area, &app.ui.theme);
        }
        Some(TakeoverOp::Shell(shell)) => {
            views::shell::draw_shell(f, shell, takeover_area, &app.ui.theme, &app.ui.anim);
        }
        None => {}
    }

    // Draw command prompt overlay on top of any view when command mode is
    // active. In table/context views this is handled inline, but sub-views
    // (logs, describe, yaml) need it as an overlay so the user can see
    // what they're typing.
    let top_is_inline = app.nav.top().renders_command_inline();
    if !top_is_inline {
        if app.ui.command_input().is_some() {
            draw_command_overlay(f, app);
        }
        if app.nav.top().filter_input().active() {
            draw_filter_overlay(f, app);
        }
    }

    // WHO DRAWS THE MODAL SLOT — exhaustive, so a new variant breaks the
    // build here instead of silently painting nothing.
    //
    // Each arm names the call that renders it; every one of those calls
    // re-asks its own accessor further down, which is why this match can be
    // empty. That indirection is exactly the problem it guards: `TakeoverOp`
    // has a wildcard-free dispatch in four places, `Modal` itself had one in
    // none, so a fifth variant would have been invisible on screen AND
    // unable to capture a key (see `try_handle_input_mode`'s fall-through).
    match app.ui.modal() {
        // draw_command_overlay below, or inline for the table kinds.
        k9rs_app::Modal::Command { .. } => {}
        // The overlay match above.
        k9rs_app::Modal::Takeover(_) => {}
        k9rs_app::Modal::None => {}
    }

    // Draw flash messages on top of everything
    draw_flash(f, app);

    // Draw confirmation dialog if present


}


/// Size, erase and back a centered dialog box, returning the rect its content
/// draws into.
///
/// ONE authority for "where a floating thing goes and what is underneath it".
/// `Clear` alone resets to the TERMINAL default, which is why a float with no
/// background of its own read as a hole punched in the view rather than a
/// sheet laid over it.
pub(crate) fn dialog_box(
    f: &mut Frame,
    frame: ratatui::layout::Rect,
    width: u16,
    height: u16,
    theme: &k9rs_theme::Theme,
) -> ratatui::layout::Rect {
    let box_area = centered_rect(frame, width, height);
    f.render_widget(Clear, box_area);
    fill_dialog_bg(f.buffer_mut(), box_area, theme);
    box_area
}

/// The per-kind view dispatch, EXHAUSTIVE over element kinds. Extracted so a
/// floating element can invoke it twice — once for the view it floats over,
/// once for itself — without either call learning that it is the one doing it.
fn draw_top_element(f: &mut Frame, app: &mut App, area: ratatui::layout::Rect) {
    match app.nav.top() {
        Element::Overview(_) => {
            views::overview::draw_overview(f, app, area);
        }
        Element::ResourceList(_) | Element::RowFilter(_) | Element::DerivedRows(_) => {
            views::resource::draw_resources(f, app, area);
        }
        Element::ContentView(cv) => match cv.kind {
            k9rs_app::ContentSpec::Yaml(_) => views::yaml::draw_yaml(f, app, area),
            _ => views::describe::draw_describe(f, app, area),
        },
        Element::LogSession(_) | Element::LogFilter(_) => {
            views::log::draw_logs(f, app, area);
        }
        // The dialogs draw into the box the float path already sized,
        // cleared and filled for them. The frame owns geometry; a widget
        // owns content. Two centrings would be two authorities for one rect,
        // which is how both of these were wrong the first time.
        Element::Confirm(dialog) => {
            f.render_widget(ConfirmDialogWidget::new(dialog, &app.ui.theme), area);
        }
        Element::Form(dialog) => {
            f.render_widget(FormDialogWidget::new(dialog, &app.ui.theme), area);
        }
        Element::Picker(p) => {
            let names: Vec<String> = p.choices.iter().map(|c| c.label.clone()).collect();
            let (target, sel) = (p.target.clone(), p.selected);
            draw_container_select(f, app, &target, &names, sel, area);
        }
    }
}

/// Draw a takeover's one-line status into the box the extent already sized,
/// cleared and backed for it — the edit flow's "Fetching YAML…", "Launching
/// editor…", "Applying…".
///
/// It used to size and centre ITSELF from `f.area()`, which made it a second
/// geometry authority beside the float path: the same mistake, in the same
/// file, that `draw_top_element`'s dialog arms carry a comment about.
fn draw_takeover_label(
    f: &mut Frame,
    label: &str,
    rect: ratatui::layout::Rect,
    theme: &k9rs_theme::Theme,
) {
    if rect.height == 0 || rect.width == 0 {
        return;
    }
    let block = ratatui::widgets::Block::bordered()
        .border_style(theme.dialog_border)
        .style(theme.dialog_bg);
    let inner = block.inner(rect);
    f.render_widget(block, rect);
    if inner.height > 0 && inner.width > 0 {
        let len = label.chars().count() as u16;
        let line = Line::from(Span::styled(label.to_string(), theme.info_value));
        let lx = inner.x + inner.width.saturating_sub(len) / 2;
        f.render_widget(line, ratatui::layout::Rect::new(lx, inner.y, inner.width, 1));
    }
}

/// Draw flash messages at the bottom of the screen.
fn draw_flash(f: &mut Frame, app: &App) {
    if let Some(ref flash) = app.ui.flash {
        if flash.is_expired(&app.config.ui.flash) {
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

/// Draw the container selection overlay for multi-container pods. The
/// container list was captured on the route at construction — the dialog
/// is self-contained.
/// Draw the picker into the box the float path already sized for it.
///
/// `area` IS the dialog — it no longer centres itself, because the frame
/// owns geometry (`Element::extent`) and the widget owns content.
fn draw_container_select(
    f: &mut Frame,
    app: &App,
    target: &k9rs_proto::protocol::ObjectRef,
    containers: &[String],
    selected_idx: usize,
    area: ratatui::layout::Rect,
) {
    use ratatui::layout::Rect;
    use ratatui::text::{Line, Span};
    use ratatui::widgets::{Block, Clear};

    let theme = &app.ui.theme;
    let dialog_area = area;

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

    fill_dialog_bg(f.buffer_mut(), inner, theme);

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

    let input = app.ui.command_input().map_or("", |(i, _)| i);
    let ghost: String = app.best_completion()
        .and_then(|c| c.strip_prefix(input).map(str::to_string))
        .unwrap_or_default();

    let prefix = app.ui.command_prompt();
    let prefix_len: u16 = prefix.width() as u16;

    // WINDOWED around the caret: a command longer than the box used to clip
    // AND lose its caret (`if cursor_x < inner.width`), so you typed blind.
    let avail = (inner.width as usize).saturating_sub(prefix_len as usize);
    let (shown, caret_col) = match app.ui.command_text() {
        Some(t) => t.window(avail),
        None => (input, input.width()),
    };

    let mut spans = vec![
        Span::styled(prefix, theme.command.add_modifier(Modifier::BOLD)),
        Span::styled(shown.to_string(), theme.command),
    ];

    // The ghost is a HINT, so it takes only what the input left over — it
    // must never push the caret off, nor get clipped without being bounded.
    let left = avail.saturating_sub(shown.width());
    if !ghost.is_empty() && left > 0 {
        spans.push(Span::styled(
            k9rs_util::width::truncate(&ghost, left),
            theme.command_suggestion.add_modifier(Modifier::DIM | Modifier::ITALIC),
        ));
    }

    let line = Line::from(spans);
    f.render_widget(line, inner);

    // The caret is INSIDE the window by construction, so there is nothing
    // left to guard against.
    f.set_cursor_position((inner.x + prefix_len + caret_col as u16, inner.y));
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
    let match_count = if app.nav.top().filter_input().text().is_empty() { counts.total } else { counts.filtered };
    let fallback = app.nav.top().grep_fallback();
    let filter_bar = FilterBar::new(
        app.nav.top().filter_input().active(),
        app.nav.top().filter_input().text(),
        match_count,
        counts.total,
        theme,
    )
    .fallback(fallback.as_deref());
    f.render_widget(filter_bar, overlay_area);
}

#[cfg(test)]
#[path = "tests/draw.rs"]
mod mirror_tests;
