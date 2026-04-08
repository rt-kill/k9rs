use ratatui::{
    buffer::Buffer,
    layout::Rect,
    text::{Line, Span},
    widgets::Widget,
};
use unicode_width::UnicodeWidthStr;

use crate::app::{FlashLevel, FlashMessage};
use crate::ui::theme::Theme;

/// Flash message widget.
/// info=lawngreen bold, warn=darkorange bold, error=orangered bold.
pub struct FlashWidget<'a> {
    message: &'a FlashMessage,
    theme: &'a Theme,
}

impl<'a> FlashWidget<'a> {
    pub fn new(message: &'a FlashMessage, theme: &'a Theme) -> Self {
        Self { message, theme }
    }
}

impl Widget for FlashWidget<'_> {
    fn render(self, area: Rect, buf: &mut Buffer) {
        if area.height == 0 || area.width == 0 {
            return;
        }

        let (prefix, style) = match self.message.level {
            FlashLevel::Info => ("\u{2139} ", self.theme.flash_info),
            FlashLevel::Warn => ("\u{26a0} ", self.theme.flash_warn),
            FlashLevel::Error => ("\u{2717} ", self.theme.flash_error),
        };

        // Fill the entire line background first so the message doesn't float
        for dx in 0..area.width {
            buf.set_string(area.x + dx, area.y, " ", style);
        }

        // Center-align flash text using display width (not byte length)
        let content = format!("{}{}", prefix, self.message.message);
        let content_width = content.width() as u16;
        let start_x = area.x + area.width.saturating_sub(content_width) / 2;

        let line = Line::from(Span::styled(content, style));
        buf.set_line(start_x, area.y, &line, area.width.saturating_sub(start_x - area.x));
    }
}
