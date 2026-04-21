use ratatui::{
    buffer::Buffer,
    layout::Rect,
    text::{Line, Span},
    widgets::Widget,
};

use unicode_width::UnicodeWidthStr;

use crate::app::{ConfirmDialog, FormDialog, FormFieldKind, FormFieldState};
use crate::ui::theme::Theme;
use crate::ui::widgets::modal::{ModalOverlay, DialogSize};

/// Confirmation dialog — centered modal with Cancel / Action buttons.
pub struct ConfirmDialogWidget<'a> {
    dialog: &'a ConfirmDialog,
    theme: &'a Theme,
}

impl<'a> ConfirmDialogWidget<'a> {
    pub fn new(dialog: &'a ConfirmDialog, theme: &'a Theme) -> Self {
        Self { dialog, theme }
    }
}

impl Widget for ConfirmDialogWidget<'_> {
    fn render(self, area: Rect, buf: &mut Buffer) {
        let msg_len = self.dialog.message.width() as u16 + 8;
        let cancel_text = format!(" Cancel ");
        let action_text = format!(" {} ", self.dialog.action_label);
        let button_width = cancel_text.len() as u16 + action_text.len() as u16 + 4;
        let w = msg_len.max(button_width + 8).clamp(36, 64);

        let hints = Line::from(vec![
            Span::styled("<\u{2190}/\u{2192}>", self.theme.status_bar_key),
            Span::styled(" toggle  ", self.theme.help_desc),
            Span::styled("<Enter>", self.theme.status_bar_key),
            Span::styled(" confirm  ", self.theme.help_desc),
            Span::styled("<Esc>", self.theme.status_bar_key),
            Span::styled(" cancel", self.theme.help_desc),
        ]);

        let theme = self.theme;
        let dialog = self.dialog;

        ModalOverlay::new(theme, DialogSize { width: w, height: 9 }, |content, buf| {
            // Centered message.
            let msg = &dialog.message;
            let mx = content.x + content.width.saturating_sub(msg.width() as u16) / 2;
            buf.set_line(mx, content.y, &Line::from(
                Span::styled(msg.as_str(), theme.help_desc),
            ), content.width);

            // Buttons: [ Cancel ]  [ Action ]
            let by = content.y + content.height.saturating_sub(2);
            let gap = 4u16;
            let total = cancel_text.len() as u16 + action_text.len() as u16 + gap;
            let bx = content.x + content.width.saturating_sub(total) / 2;
            let (cs, as_) = if dialog.action_focused {
                (theme.dialog_button_inactive, theme.dialog_button_active)
            } else {
                (theme.dialog_button_active, theme.dialog_button_inactive)
            };
            buf.set_string(bx, by, &cancel_text, cs);
            buf.set_string(bx + cancel_text.len() as u16 + gap, by, &action_text, as_);
        })
        .title("Confirm")
        .hints(hints)
        .render(area, buf);
    }
}

// ---------------------------------------------------------------------------
// Form dialog
// ---------------------------------------------------------------------------
//
// Generic schema-driven dialog widget. Renders any `FormDialog` regardless
// of which `OperationKind` it's gathering input for. Each field shows up as
// a labeled input control whose shape comes from `FormFieldKind`:
//   - Text  → free-form line edit with a block cursor
//   - Number → digit-only line edit
//   - Port   → digit-only line edit (1..=65535 enforced on submit)
//   - Select → cycling picker with `< value >` and a hint about Left/Right
//
// Below the fields the widget draws an OK button (focusable as the last
// position) and a hint bar with Tab/Enter/Esc bindings. There is no Cancel
// button — Esc cancels.

pub struct FormDialogWidget<'a> {
    dialog: &'a FormDialog,
    theme: &'a Theme,
}

impl<'a> FormDialogWidget<'a> {
    pub fn new(dialog: &'a FormDialog, theme: &'a Theme) -> Self {
        Self { dialog, theme }
    }
}

impl Widget for FormDialogWidget<'_> {
    fn render(self, area: Rect, buf: &mut Buffer) {
        let label_w = self.dialog.fields.iter()
            .map(|f| f.label.width() as u16).max().unwrap_or(0);
        let w = (label_w + 30).max(50);
        let n = self.dialog.fields.len() as u16;
        let h = n + 9; // subtitle + blank + fields + blank + button + blank + hint + borders + pad

        let hints = Line::from(vec![
            Span::styled("<Tab>", self.theme.status_bar_key),
            Span::styled(" next  ", self.theme.help_desc),
            Span::styled("<\u{2190}/\u{2192}>", self.theme.status_bar_key),
            Span::styled(" select  ", self.theme.help_desc),
            Span::styled("<Enter>", self.theme.status_bar_key),
            Span::styled(" submit  ", self.theme.help_desc),
            Span::styled("<Esc>", self.theme.status_bar_key),
            Span::styled(" cancel", self.theme.help_desc),
        ]);

        let theme = self.theme;
        let dialog = self.dialog;
        let label_col = label_w + 2;

        ModalOverlay::new(theme, DialogSize { width: w, height: h }, |content, buf| {
            let mut y = content.y;

            // Subtitle.
            if !dialog.subtitle.is_empty() && y < content.y + content.height {
                buf.set_line(content.x, y, &Line::from(
                    Span::styled(dialog.subtitle.as_str(), theme.info_label),
                ), content.width);
                y += 2;
            }

            // Fields.
            for (idx, field) in dialog.fields.iter().enumerate() {
                if y >= content.y + content.height { break; }
                let focused = dialog.focused == idx;
                render_form_field(buf, content.x, y, content.width, label_col, field, focused, theme);
                y += 1;
            }
            y += 1;

            // OK button.
            if y < content.y + content.height {
                let ok_text = "  OK  ";
                let bx = content.x + content.width.saturating_sub(ok_text.len() as u16) / 2;
                let ok_style = if dialog.ok_focused() {
                    theme.dialog_button_active
                } else {
                    theme.dialog_button_inactive
                };
                buf.set_string(bx, y, ok_text, ok_style);
            }
        })
        .title(&self.dialog.title)
        .hints(hints)
        .render(area, buf);
    }
}

/// Render a single form field row: label + input control.
fn render_form_field(
    buf: &mut Buffer,
    x: u16, y: u16, width: u16, label_col: u16,
    field: &FormFieldState, focused: bool, theme: &Theme,
) {
    buf.set_line(x, y, &Line::from(Span::styled(field.label.as_str(), theme.info_label)), width);
    let vx = x + label_col;
    let vw = width.saturating_sub(label_col);
    if vw == 0 { return; }
    match &field.kind {
        FormFieldKind::Text { .. } | FormFieldKind::Number { .. } | FormFieldKind::Port => {
            let style = if focused { theme.filter } else { theme.info_value };
            let mut spans = vec![Span::styled(field.value.as_str(), style)];
            if focused { spans.push(Span::styled("\u{2588}", theme.filter)); }
            buf.set_line(vx, y, &Line::from(spans), vw);
        }
        FormFieldKind::Select { options } => {
            let idx: usize = field.value.parse().unwrap_or(0);
            let display = options.get(idx).map(|o| o.label.as_str()).unwrap_or("(none)");
            let style = if focused { theme.filter } else { theme.info_value };
            let arrows = if focused { (" \u{25c0} ", " \u{25b6}") } else { ("   ", "  ") };
            buf.set_line(vx, y, &Line::from(vec![
                Span::styled(arrows.0, theme.info_label),
                Span::styled(display, style),
                Span::styled(arrows.1, theme.info_label),
            ]), vw);
        }
    }
}
