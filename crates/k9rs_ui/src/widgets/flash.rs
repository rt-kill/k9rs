use ratatui::{
    buffer::Buffer,
    layout::Rect,
    text::{Line, Span},
    widgets::Widget,
};
use unicode_width::UnicodeWidthStr;

use k9rs_app::{FlashLevel, FlashMessage};
use k9rs_theme::Theme;

/// Flash message widget.
/// info=lawngreen bold, warn=darkorange bold, error=orangered bold.
pub(crate) struct FlashWidget<'a> {
    message: &'a FlashMessage,
    theme: &'a Theme,
}

impl<'a> FlashWidget<'a> {
    pub(crate) fn new(message: &'a FlashMessage, theme: &'a Theme) -> Self {
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

        // Centred when it fits; LEFT-ALIGNED and visibly cut when it does
        // not. Centring a message wider than the line put `start_x` at the
        // left edge and let `set_line` clip the right, so a long error lost
        // its TAIL in silence — and the tail of an error is usually the part
        // that names the cause. Truncating here marks the cut with `…`, and
        // the whole message is still in the daemon log.
        let content = format!("{}{}", prefix, self.message.message);
        let width = area.width as usize;
        let (content, start_x) = if content.width() <= width {
            let pad = area.width.saturating_sub(content.width() as u16) / 2;
            (content, area.x + pad)
        } else {
            (k9rs_util::width::truncate(&content, width), area.x)
        };

        let line = Line::from(Span::styled(content, style));
        buf.set_line(start_x, area.y, &line, area.width.saturating_sub(start_x - area.x));
    }
}
