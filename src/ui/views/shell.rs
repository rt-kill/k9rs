use ratatui::prelude::*;
use ratatui::widgets::Block;

use crate::app::ShellState;
use crate::ui::theme::Theme;

/// Render the shell overlay. Only visible during the Connecting phase —
/// once the daemon confirms the connection, the session loop suspends
/// the TUI and enters raw bridge mode (this code is never reached).
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

    let loading = crate::util::loading_bar("Connecting...");
    let line = Line::from(Span::styled(loading, theme.info_value));
    let center_y = inner.y + inner.height / 2;
    let center_x = inner.x + inner.width.saturating_sub(line.width() as u16) / 2;
    f.render_widget(line, Rect::new(center_x, center_y, inner.width, 1));
}
