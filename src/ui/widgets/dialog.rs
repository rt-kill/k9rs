use ratatui::{
    buffer::Buffer,
    layout::{Constraint, Layout, Rect},
    text::{Line, Span},
    widgets::{Block, Clear, Padding, Widget},
};

use unicode_width::UnicodeWidthStr;

use crate::app::{ConfirmDialog, FormDialog, FormFieldKind, FormFieldState};
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
        let msg_len = self.dialog.message.width() as u16 + 6; // padding
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
        let msg_x = inner.x + inner.width.saturating_sub(msg.width() as u16) / 2;
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
        // Sizing: width fits the longest label + value, height fits subtitle
        // (1) + blank (1) + each field (1) + blank (1) + button (1) + blank (1)
        // + hint (1) + borders (2). Clamp to the available area.
        let label_w = self
            .dialog
            .fields
            .iter()
            .map(|f| f.label.width() as u16)
            .max()
            .unwrap_or(0);
        let dialog_width = (label_w + 30).max(50).min(area.width.saturating_sub(4));
        let n_fields = self.dialog.fields.len() as u16;
        // subtitle + blank + n fields + blank + button + blank + hint + borders
        let dialog_height = (n_fields + 8).min(area.height.saturating_sub(2));

        let dialog_area = ConfirmDialogWidget::centered_rect(area, dialog_width, dialog_height);
        Clear.render(dialog_area, buf);

        let title = format!(" {} ", self.dialog.title);
        let block = Block::bordered()
            .title(title)
            .title_style(self.theme.title)
            .border_style(self.theme.dialog_border)
            .style(self.theme.dialog_bg)
            .padding(Padding::new(1, 1, 0, 0));

        let inner = block.inner(dialog_area);
        block.render(dialog_area, buf);

        if inner.height == 0 || inner.width == 0 {
            return;
        }

        let mut y = inner.y;

        // Subtitle (e.g. "namespace: default") — optional, skipped if empty.
        if !self.dialog.subtitle.is_empty() && y < inner.y + inner.height {
            let line = Line::from(Span::styled(
                self.dialog.subtitle.as_str(),
                self.theme.info_label,
            ));
            buf.set_line(inner.x, y, &line, inner.width);
            y = y.saturating_add(2); // subtitle + blank
        }

        // Render each field. Label is left-aligned to a fixed column so the
        // values line up across rows.
        let label_col = label_w + 2;
        for (idx, field) in self.dialog.fields.iter().enumerate() {
            if y >= inner.y + inner.height {
                break;
            }
            let focused = self.dialog.focused == idx;
            self.render_field(buf, inner.x, y, inner.width, label_col, field, focused);
            y = y.saturating_add(1);
        }

        // Blank row before the button.
        y = y.saturating_add(1);

        // OK button.
        if y < inner.y + inner.height {
            let ok_text = "  OK  ";
            let bx = inner.x + inner.width.saturating_sub(ok_text.len() as u16) / 2;
            let ok_style = if self.dialog.ok_focused() {
                self.theme.dialog_button_active
            } else {
                self.theme.dialog_button_inactive
            };
            buf.set_string(bx, y, ok_text, ok_style);
            y = y.saturating_add(2);
        }

        // Hint line at the bottom.
        let hint_y = inner.y + inner.height.saturating_sub(1);
        if hint_y >= y || hint_y == y.saturating_sub(1) {
            let hint = Line::from(vec![
                Span::styled("<Tab>", self.theme.status_bar_key),
                Span::styled(" next  ", self.theme.help_desc),
                Span::styled("<\u{2190}/\u{2192}>", self.theme.status_bar_key),
                Span::styled(" select  ", self.theme.help_desc),
                Span::styled("<Enter>", self.theme.status_bar_key),
                Span::styled(" submit  ", self.theme.help_desc),
                Span::styled("<Esc>", self.theme.status_bar_key),
                Span::styled(" cancel", self.theme.help_desc),
            ]);
            buf.set_line(inner.x, hint_y, &hint, inner.width);
        }
    }
}

impl FormDialogWidget<'_> {
    /// Render a single field row: label + input control. The input control
    /// dispatches on `FormFieldKind` so each field type has its own visual.
    fn render_field(
        &self,
        buf: &mut Buffer,
        x: u16,
        y: u16,
        width: u16,
        label_col: u16,
        field: &FormFieldState,
        focused: bool,
    ) {
        // Label column.
        let label_line = Line::from(Span::styled(field.label.as_str(), self.theme.info_label));
        buf.set_line(x, y, &label_line, width);

        // Value column.
        let vx = x + label_col;
        let vw = width.saturating_sub(label_col);
        if vw == 0 {
            return;
        }

        match &field.kind {
            FormFieldKind::Text { .. } | FormFieldKind::Number { .. } | FormFieldKind::Port => {
                let style = if focused { self.theme.filter } else { self.theme.info_value };
                let mut spans = vec![Span::styled(field.value.as_str(), style)];
                if focused {
                    spans.push(Span::styled("\u{2588}", self.theme.filter));
                }
                buf.set_line(vx, y, &Line::from(spans), vw);
            }
            FormFieldKind::Select { options } => {
                let idx: usize = field.value.parse().unwrap_or(0);
                let display = options
                    .get(idx)
                    .map(|(_, label)| label.as_str())
                    .unwrap_or("(none)");
                let style = if focused { self.theme.filter } else { self.theme.info_value };
                let arrows = if focused { (" ◀ ", " ▶") } else { ("   ", "  ") };
                let spans = vec![
                    Span::styled(arrows.0, self.theme.info_label),
                    Span::styled(display, style),
                    Span::styled(arrows.1, self.theme.info_label),
                ];
                buf.set_line(vx, y, &Line::from(spans), vw);
            }
        }
    }
}
