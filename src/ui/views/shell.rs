use ratatui::prelude::*;
use ratatui::widgets::Block;

use crate::app::ShellState;
use crate::ui::theme::Theme;

/// Render a shell session inside a bordered block. Shows a loading
/// indicator while connecting, then renders the vt100 screen buffer.
pub fn draw_shell(
    f: &mut Frame,
    shell: &ShellState,
    area: Rect,
    theme: &Theme,
) {
    let block = Block::bordered()
        .title(Span::styled(
            format!(" shell: {} ", shell.title),
            theme.title,
        ))
        .border_style(theme.border);
    let inner = block.inner(area);
    f.render_widget(block, area);

    if inner.width == 0 || inner.height == 0 {
        return;
    }

    // Show loading indicator until first data arrives from the daemon.
    if !shell.connected {
        let loading = crate::util::loading_bar("Connecting...");
        let line = Line::from(Span::styled(loading, theme.info_value));
        let center_y = inner.y + inner.height / 2;
        let center_x = inner.x + inner.width.saturating_sub(line.width() as u16) / 2;
        f.render_widget(line, Rect::new(center_x, center_y, inner.width, 1));
        return;
    }

    let screen = shell.parser.screen();
    let buf = f.buffer_mut();
    // vt100::Screen::size() returns (rows, cols).
    let (scr_rows, scr_cols) = screen.size();
    for row in 0..inner.height.min(scr_rows) {
        for col in 0..inner.width.min(scr_cols) {
            let Some(cell) = screen.cell(row, col) else { continue };
            if cell.is_wide_continuation() { continue; }

            let x = inner.x + col;
            let y = inner.y + row;

            let ch = cell.contents();
            let fg = vt100_color_to_ratatui(cell.fgcolor());
            let bg = vt100_color_to_ratatui(cell.bgcolor());

            let mut style = Style::default();
            if let Some(c) = fg { style = style.fg(c); }
            if let Some(c) = bg { style = style.bg(c); }
            if cell.bold() { style = style.add_modifier(Modifier::BOLD); }
            if cell.italic() { style = style.add_modifier(Modifier::ITALIC); }
            if cell.underline() { style = style.add_modifier(Modifier::UNDERLINED); }
            if cell.inverse() { style = style.add_modifier(Modifier::REVERSED); }

            let display = if ch.is_empty() { " " } else { &ch };
            buf.set_string(x, y, display, style);
        }
    }

    // Draw cursor.
    let (cursor_row, cursor_col) = screen.cursor_position();
    let cx = inner.x + cursor_col;
    let cy = inner.y + cursor_row;
    if cx < inner.x + inner.width && cy < inner.y + inner.height {
        f.set_cursor_position((cx, cy));
    }
}

fn vt100_color_to_ratatui(color: vt100::Color) -> Option<Color> {
    match color {
        vt100::Color::Default => None,
        vt100::Color::Idx(i) => Some(Color::Indexed(i)),
        vt100::Color::Rgb(r, g, b) => Some(Color::Rgb(r, g, b)),
    }
}
