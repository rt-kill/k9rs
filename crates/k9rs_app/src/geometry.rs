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

/// The hint bar a container picker shows, as `(key, label)` pairs.
///
/// Here, beside the size that has to fit it, for the reason [`form_hints`]
/// is: the picker's box was sized from its CHOICES alone, so when it moved
/// onto the shared `ModalOverlay` path — which spends `DIALOG_MARGIN` and
/// the block's own padding — a one-choice picker came out 30 columns wide,
/// 24 of them usable, and `<Esc> back` rendered as `<Esc> b`. Caught by a
/// test before it shipped; the same failure as the form's, found the same
/// way.
pub fn picker_hints() -> [(&'static str, &'static str); 2] {
    [("<Enter>", " select  "), ("<Esc>", " back")]
}

/// The columns [`picker_hints`] occupies.
pub fn picker_hints_width() -> u16 {
    picker_hints()
        .iter()
        .map(|(key, label)| (key.width() + label.width()) as u16)
        .sum()
}

/// The columns a dialog spends on its own chrome before any content: the
/// border, and the horizontal padding `ModalOverlay` gives its block.
///
/// Named because two sizes have to subtract it and the widget has to spend
/// it — the arithmetic that cancels, which `DIALOG_MARGIN`'s own doc warns
/// must not live in two files.
pub const DIALOG_CHROME: u16 = 2 + 4;

/// The box a container picker needs, in cells — the same computation the
/// renderer uses, so the float and the drawing agree on one rect.
pub(crate) fn container_picker_size(p: &crate::ContainerPicker) -> (u16, u16) {
    let widest = p.choices.iter().map(|c| c.label.width()).max().unwrap_or(10) as u16;
    let for_choices = (widest + DIALOG_CHROME).clamp(30, 60);
    (
        for_choices.max(picker_hints_width() + DIALOG_CHROME),
        p.choices.len() as u16 + 4,
    )
}

/// The hint bar a form dialog shows under its fields, as `(key, label)`
/// pairs for the widget to style.
///
/// Here rather than in the widget because the BOX has to be wide enough for
/// it, and that width was arrived at twice: `form_dialog_size` below
/// hardcoded `if has_select { 52 } else { 38 }` while the widget measured
/// the line it actually built. Measured, those lines are **41** and **55** —
/// so the model was three columns short either way, and on a port-forward
/// form (a Select plus a 14-column label) the box came out one cell under
/// what the hint needs and `<Esc> cancel` rendered as `<Esc> cance`. Losing
/// the key that gets you out is the specific failure `header::fit_bar`
/// exists to prevent.
///
/// The arrow hints appear only when a Select field is present, which is the
/// whole reason the width is conditional.
pub fn form_hints(dialog: &crate::FormDialog) -> Vec<(&'static str, &'static str)> {
    let mut hints = vec![("<Tab>", " next  ")];
    if dialog.fields.iter().any(|f| matches!(f.kind, FormFieldKind::Select { .. })) {
        hints.push(("<\u{2190}/\u{2192}>", " select  "));
    }
    hints.push(("<Enter>", " confirm  "));
    hints.push(("<Esc>", " cancel"));
    hints
}

/// The columns [`form_hints`] occupies.
pub fn form_hints_width(dialog: &crate::FormDialog) -> u16 {
    form_hints(dialog)
        .iter()
        .map(|(key, label)| (key.width() + label.width()) as u16)
        .sum()
}

/// The box a form needs, in cells — ONE computation, for the widget that
/// draws it and the extent that reserves space for it.
///
/// `pub`, like [`confirm_dialog_size`], because the widget has to be able to
/// consult it. The module doc's claim that each of these is "consulted by the
/// widget that draws it" was true only of the confirm: this was
/// `pub(crate)`, so `k9rs_ui` could not reach it and had its own copy of the
/// arithmetic.
pub fn form_dialog_size(dialog: &crate::FormDialog) -> (u16, u16) {
    let label_w = dialog.fields.iter().map(|f| f.label.width() as u16).max().unwrap_or(0);
    // `DIALOG_CHROME` plus two columns of slack, which the hint line has
    // always had and the clipping report measured against.
    let hint_width = form_hints_width(dialog) + DIALOG_CHROME + 2;
    ((label_w + 30).max(50).max(hint_width), dialog.fields.len() as u16 + 9)
}

#[cfg(test)]
#[path = "tests/geometry.rs"]
mod tests;
