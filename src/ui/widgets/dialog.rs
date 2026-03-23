use ratatui::{
    buffer::Buffer,
    layout::{Constraint, Layout, Rect},
    text::{Line, Span},
    widgets::{Block, Clear, Padding, Widget},
};

use crate::app::ConfirmDialog;
use crate::ui::theme::Theme;

/// Confirmation dialog widget — a centered modal overlay with Yes/No buttons.
/// Used for destructive operations like delete or scale.
pub struct ConfirmDialogWidget<'a> {
    dialog: &'a ConfirmDialog,
    theme: &'a Theme,
}

impl<'a> ConfirmDialogWidget<'a> {
    pub fn new(dialog: &'a ConfirmDialog, theme: &'a Theme) -> Self {
        Self { dialog, theme }
    }

    /// Calculate the centered dialog area within the given area.
    fn centered_rect(area: Rect, width: u16, height: u16) -> Rect {
        let vert = Layout::vertical([
            Constraint::Fill(1),
            Constraint::Length(height),
            Constraint::Fill(1),
        ])
        .split(area);

        let horiz = Layout::horizontal([
            Constraint::Fill(1),
            Constraint::Length(width),
            Constraint::Fill(1),
        ])
        .split(vert[1]);

        horiz[1]
    }
}

impl Widget for ConfirmDialogWidget<'_> {
    fn render(self, area: Rect, buf: &mut Buffer) {
        // Size the dialog to fit the message with some padding
        let msg_len = self.dialog.message.len() as u16 + 6; // padding
        let dialog_width = msg_len.max(30).min(60).min(area.width.saturating_sub(4));
        let dialog_height = 7u16.min(area.height.saturating_sub(2));

        let dialog_area = Self::centered_rect(area, dialog_width, dialog_height);

        // Clear the area behind the dialog
        Clear.render(dialog_area, buf);

        // Draw the dialog box
        let block = Block::bordered()
            .title(" Confirm ")
            .title_style(self.theme.flash_error)
            .border_style(self.theme.dialog_border)
            .style(self.theme.dialog_bg)
            .padding(Padding::new(1, 1, 0, 0));

        let inner = block.inner(dialog_area);
        block.render(dialog_area, buf);

        if inner.height == 0 || inner.width == 0 {
            return;
        }

        // Render message centered on the first content line
        let msg = &self.dialog.message;
        let msg_x = inner.x + inner.width.saturating_sub(msg.len() as u16) / 2;
        let msg_line = Line::from(Span::styled(msg.as_str(), self.theme.help_desc));
        buf.set_line(msg_x, inner.y, &msg_line, inner.width);

        // Render buttons on the last line of the inner area
        let button_y = inner.y + inner.height.saturating_sub(1);

        let yes_text = " Yes ";
        let no_text = " No ";
        let gap = 4u16;
        let total_button_width = yes_text.len() as u16 + no_text.len() as u16 + gap;
        let button_start = inner.x + inner.width.saturating_sub(total_button_width) / 2;

        // Determine which button is active based on yes_selected
        let (yes_style, no_style) = if self.dialog.yes_selected {
            (self.theme.dialog_button_active, self.theme.dialog_button_inactive)
        } else {
            (self.theme.dialog_button_inactive, self.theme.dialog_button_active)
        };

        buf.set_string(button_start, button_y, yes_text, yes_style);
        let no_start = button_start + yes_text.len() as u16 + gap;
        buf.set_string(no_start, button_y, no_text, no_style);

        // Hint line below buttons (if space allows)
        if inner.height >= 4 {
            let hint_y = button_y.saturating_sub(1);
            if hint_y > inner.y {
                let hint = Line::from(vec![
                    Span::styled("<\u{2190}/\u{2192}>", self.theme.status_bar_key),
                    Span::styled(" toggle  ", self.theme.help_desc),
                    Span::styled("<Enter>", self.theme.status_bar_key),
                    Span::styled(" confirm  ", self.theme.help_desc),
                    Span::styled("<Esc>", self.theme.status_bar_key),
                    Span::styled(" cancel", self.theme.help_desc),
                ]);
                let hint_x = inner.x + inner.width.saturating_sub(42) / 2;
                buf.set_line(hint_x, hint_y, &hint, inner.width);
            }
        }
    }
}
