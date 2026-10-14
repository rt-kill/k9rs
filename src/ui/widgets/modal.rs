use ratatui::{
    buffer::Buffer,
    layout::Rect,
    text::{Line, Span},
    widgets::{Block, Clear, Padding, Widget},
};

use crate::ui::theme::Theme;

/// The columns and rows a `ModalOverlay` spends on its own OUTER margin,
/// before the border — the gap that makes a dialog read as a sheet laid over
/// the view rather than a panel welded to it.
///
/// A free constant rather than an associated one so it can be named without
/// a turbofish: `ModalOverlay` is generic over its content closure.
///
/// The element that sizes a dialog's box has to add exactly this much, and it
/// used to add it as a bare `+ 4` / `+ 2` in three separate match arms,
/// paired with the `saturating_sub(4)` / `saturating_sub(2)` in this file.
/// The two numbers cancelling was the only thing keeping the dialogs the
/// right size, and nothing said so — changing one would have silently
/// mis-sized all three, in a different crate module.
pub const DIALOG_MARGIN: (u16, u16) = (4, 2);

/// Requested dialog dimensions (clamped to the available area).
pub struct DialogSize {
    pub width: u16,
    pub height: u16,
}

/// A centered modal overlay that handles all dialog chrome: centering,
/// background clear, bordered block with title, guaranteed-visible
/// background fill, and an optional bottom hint bar.
///
/// The caller supplies a content-rendering closure that receives the
/// inner rect (after border + padding + hint reservation) and draws
/// only the unique parts (message, buttons, fields, list items, etc.).
///
/// Adding a new dialog type is ~20 lines of content rendering — zero
/// centering/clear/block/fill/hint boilerplate.
pub struct ModalOverlay<'a, F>
where
    F: FnOnce(Rect, &mut Buffer),
{
    title: &'a str,
    size: DialogSize,
    hints: Option<Line<'a>>,
    theme: &'a Theme,
    render_content: F,
}

impl<'a, F> ModalOverlay<'a, F>
where
    F: FnOnce(Rect, &mut Buffer),
{
    pub fn new(theme: &'a Theme, size: DialogSize, render_content: F) -> Self {
        Self {
            title: "",
            size,
            hints: None,
            theme,
            render_content,
        }
    }

    pub fn title(mut self, title: &'a str) -> Self {
        self.title = title;
        self
    }

    pub fn hints(mut self, hints: Line<'a>) -> Self {
        self.hints = Some(hints);
        self
    }
}

impl<F> Widget for ModalOverlay<'_, F>
where
    F: FnOnce(Rect, &mut Buffer),
{
    fn render(self, area: Rect, buf: &mut Buffer) {
        let (mw, mh) = DIALOG_MARGIN;
        let w = self.size.width.min(area.width.saturating_sub(mw));
        let h = self.size.height.min(area.height.saturating_sub(mh));

        let dialog_area = crate::ui::centered_rect(area, w, h);
        // Fill the entire dialog area (border + interior) with the
        // guaranteed-visible bg FIRST, then render the border on top.
        // This ensures the border cells also have the dialog bg, not
        // the terminal default left by Clear.
        Clear.render(dialog_area, buf);
        crate::ui::fill_dialog_bg(buf, dialog_area, self.theme);

        let title_str = if self.title.is_empty() {
            String::new()
        } else {
            format!(" {} ", self.title)
        };
        let block = Block::bordered()
            .title(Span::styled(&title_str, self.theme.dialog_border))
            .border_style(self.theme.dialog_border)
            .padding(Padding::new(2, 2, 1, 0));

        let inner = block.inner(dialog_area);
        block.render(dialog_area, buf);

        if inner.height == 0 || inner.width == 0 {
            return;
        }

        // Reserve the bottom row for hints if present.
        let content_area = if self.hints.is_some() && inner.height > 1 {
            Rect { height: inner.height.saturating_sub(1), ..inner }
        } else {
            inner
        };

        (self.render_content)(content_area, buf);

        if let Some(hint) = self.hints {
            let hint_y = inner.y + inner.height.saturating_sub(1);
            let hint_width = hint.width() as u16;
            let hx = inner.x + inner.width.saturating_sub(hint_width) / 2;
            buf.set_line(hx, hint_y, &hint, inner.width);
        }
    }
}
