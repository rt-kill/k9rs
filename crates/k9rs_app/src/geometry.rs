//! How big a dialog's box has to be, in cells.
//!
//! ONE computation per dialog kind, consulted by the widget that draws it and
//! by `Element::extent`, which sizes the float around it. They were two
//! independent guesses at the same number, and a mismatch centres a dialog
//! inside a box that is not the size it thinks it is.
//!
//! These lived in `ui::widgets` and every one of them takes an `app` type and
//! returns a pair of `u16` — no ratatui anywhere — which is the tell that they
//! were misfiled. `ui` sits ABOVE `app`, so leaving them there meant the
//! element asking the renderer how big its own content was. `DIALOG_MARGIN`'s
//! own doc warned that the two halves of its arithmetic must not drift "in a
//! different crate module"; this is what stops that being possible.

use unicode_width::UnicodeWidthStr;

use crate::FormFieldKind;

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

/// The box a confirm dialog needs, in cells.
///
/// ONE computation, consulted by the widget that draws it and by
/// `Element::extent` which sizes the float around it — they were two
/// independent guesses at the same number, and a mismatch means the dialog
/// is centred inside a box that is not the size it thinks it is.
pub fn confirm_dialog_size(dialog: &crate::ConfirmDialog) -> (u16, u16) {
    let msg_lines: Vec<&str> = dialog.message.lines().collect();
    let msg_len = msg_lines.iter().map(|l| l.width()).max().unwrap_or(0) as u16 + 8;
    let extra_lines = msg_lines.len().saturating_sub(1) as u16;
    let button_width =
        " Cancel ".len() as u16 + format!(" {} ", dialog.action_label).len() as u16 + 4;
    (msg_len.max(button_width + 8).clamp(36, 72), 9 + extra_lines)
}

/// The box a container picker needs, in cells — the same computation the
/// renderer uses, so the float and the drawing agree on one rect.
pub fn container_picker_size(p: &crate::ContainerPicker) -> (u16, u16) {
    let widest = p.choices.iter().map(|c| c.label.len()).max().unwrap_or(10) as u16;
    ((widest + 6).clamp(30, 60), p.choices.len() as u16 + 4)
}

/// The box a form needs, in cells — one computation for the widget that
/// draws it and the extent that reserves space for it.
pub fn form_dialog_size(dialog: &crate::FormDialog) -> (u16, u16) {
    let label_w = dialog.fields.iter().map(|f| f.label.width() as u16).max().unwrap_or(0);
    let has_select = dialog.fields.iter().any(|f| matches!(f.kind, FormFieldKind::Select { .. }));
    // Mirrors the hint line the widget builds below; the arrow hints only
    // appear when a Select field is present, and they widen the box.
    let hint_width = if has_select { 52u16 } else { 38 } + 8;
    ((label_w + 30).max(50).max(hint_width), dialog.fields.len() as u16 + 9)
}
