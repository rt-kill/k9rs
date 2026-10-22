//! The generic form dialog — one dialog for every operation needing input.
//!
//! Split out of the former `app::types` grab-bag: this is a cohesive
//! unit with its own vocabulary, and it was only ever in `types.rs`
//! because that file was where types went.

use crate::kube::protocol::ObjectRef;

// ---------------------------------------------------------------------------
// Form dialog
// ---------------------------------------------------------------------------
//
// One generic dialog that handles every operation needing user input. The
// shape is built client-side per-operation in `session_handlers::build_*_form`
// from row context — for PortForward we read the pod's `containerPorts`
// off the selected `ResourceRow`; for Scale we read the current replica
// count.
//
// Submit dispatch is centralized: a single function pattern-matches on
// `FormDialog::kind` to build the typed `SessionCommand` from the collected
// field values. The widget never sees a wire command.

/// One field's live state inside an open `FormDialog`. `kind` decides what
/// input control the widget draws and what keystrokes the input handler
/// accepts; `value` is the user's current input — text-typed for every
/// kind so partial input (e.g. mid-typed numbers) parses cleanly.
#[derive(Debug, Clone)]
pub struct FormFieldState {
    /// Stable identifier — used by the per-OperationKind dispatcher to look
    /// the value up at submit time. Names come from the
    /// [`crate::kube::protocol::form_field_name`] constants module so the
    /// builder and dispatcher share a single source of truth.
    pub name: String,
    /// User-facing label rendered to the left of the input.
    pub label: String,
    /// Discriminator that decides which input control the widget renders
    /// and what keystrokes the input handler accepts.
    pub kind: FormFieldKind,
    /// Current text input — the characters the user has typed. Used by the
    /// text-like kinds (Text/Number/Port). For `Select` the chosen option is
    /// the typed `selected` index carried on the kind itself, and this field
    /// stays empty.
    pub value: crate::app::text_input::TextInput,
}

/// Field type discriminator. Owned client-side — there is no wire-level
/// equivalent (the daemon only sends a list of [`crate::kube::protocol::OperationKind`] and the
/// client builds the form shape from row context).
#[derive(Debug, Clone)]
pub enum FormFieldKind {
    /// Free-form text. `max_len` is advisory.
    Text { max_len: Option<usize> },
    /// Integer with explicit bounds. Input is digits-only.
    Number { min: i64, max: i64 },
    /// Network port (1..=65535). Input is digits-only.
    Port,
    /// One of a fixed set of choices, cycled with Left/Right. `selected`
    /// indexes `options` — typed, so the chosen option can't desync from a
    /// parallel digit-string the way the old `value`-encoded index could.
    Select { options: Vec<SelectOption>, selected: usize },
}

/// A single entry in a [`FormFieldKind::Select`]. `value` is what the
/// form submits; `label` is what the user sees. Named instead of the
/// prior `(String, String)` so call sites can't swap the positions.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SelectOption {
    pub value: String,
    pub label: String,
}

impl SelectOption {
    pub fn new(value: impl Into<String>, label: impl Into<String>) -> Self {
        Self { value: value.into(), label: label.into() }
    }
}

/// What happens when a form dialog is submitted. Each variant knows how
/// to extract field values and produce a wire command. The generic dialog
/// framework just collects input — it never knows what operation it's for.
///
/// Adding a new form-driven operation: add a variant here, implement its
/// `submit` arm in `dispatch_form_submit`, and write a builder function.
#[derive(Debug, Clone)]
pub enum FormSubmit {
    /// Scale a workload: reads the REPLICAS field → SessionCommand::Scale.
    Scale,
    /// Create a port-forward: reads CONTAINER_PORT + LOCAL_PORT fields
    /// → SessionCommand::PortForward.
    PortForward,
}

impl FormSubmit {
    /// Map an `OperationKind` to the corresponding `FormSubmit` variant.
    pub fn from_operation(op: crate::kube::protocol::OperationKind) -> Option<Self> {
        match op {
            crate::kube::protocol::OperationKind::Scale => Some(FormSubmit::Scale),
            crate::kube::protocol::OperationKind::PortForward => Some(FormSubmit::PortForward),
            _ => None,
        }
    }

    /// Human-readable label for flash messages.
    pub fn from_operation_label(&self) -> &'static str {
        match self {
            FormSubmit::Scale => "Scaling",
            FormSubmit::PortForward => "Port-forwarding",
        }
    }

    /// Build the wire command from form field values. Each variant knows
    /// which fields to extract and how to parse them. Returns the command
    /// to send to the daemon, or an error message for the user.
    pub fn build_command(
        &self,
        target: &crate::kube::protocol::ObjectRef,
        fields: &[FormFieldState],
    ) -> Result<crate::kube::protocol::SessionCommand, String> {
        use crate::kube::protocol::{SessionCommand, form_field_name};
        match self {
            FormSubmit::Scale => {
                let val = find_field_value(fields, form_field_name::REPLICAS)?;
                let replicas = val.parse::<u32>()
                    .map_err(|_| format!("Invalid replica count: {}", val))?;
                Ok(SessionCommand::Scale { target: target.clone(), replicas })
            }
            FormSubmit::PortForward => {
                let cp = parse_port_field(fields, form_field_name::CONTAINER_PORT)?;
                let lp = parse_port_field(fields, form_field_name::LOCAL_PORT)?;
                Ok(SessionCommand::PortForward {
                    target: target.clone(),
                    local_port: lp,
                    container_port: cp,
                })
            }
        }
    }
}

/// Extract a field's trimmed value by name.
fn find_field_value(fields: &[FormFieldState], name: &str) -> Result<String, String> {
    fields.iter()
        .find(|f| f.name == name)
        .map(|f| f.value.value().trim().to_string())
        .ok_or_else(|| format!("Missing field: {}", name))
}

/// Parse a port field — handles both Select (value is option index) and
/// direct Port input (value is the port number string).
fn parse_port_field(fields: &[FormFieldState], name: &str) -> Result<u16, String> {
    let field = fields.iter()
        .find(|f| f.name == name)
        .ok_or_else(|| format!("Missing field: {}", name))?;
    match &field.kind {
        FormFieldKind::Select { options, selected } => {
            options.get(*selected)
                .and_then(|opt| opt.value.parse::<u16>().ok())
                .ok_or_else(|| "Invalid port selection".to_string())
        }
        _ => field.value.value().trim().parse::<u16>()
            .map_err(|_| format!("Invalid port: {}", field.value.value().trim())),
    }
}

/// A modal form dialog gathering input for a single operation.
#[derive(Debug, Clone)]
pub struct FormDialog {
    /// What to do on submit.
    pub submit: FormSubmit,
    /// Title shown in the dialog border (e.g. "Scale: deploy/nginx").
    pub title: String,
    /// Optional context line under the title (e.g. "namespace: default").
    pub subtitle: String,
    /// The object the operation will run on. Carried through to dispatch.
    pub target: ObjectRef,
    /// Schema fields, in display order.
    pub fields: Vec<FormFieldState>,
    /// Where the caret is.
    ///
    /// Leaving a form — Esc, or Enter on Cancel — is the ACTION layer's job
    /// (`handler::handle_form_keys` → [`crate::app::actions::Action::Back`]),
    /// so there is ONE undo path rather than a second pop in the typing
    /// handler. That is what the old doc here got wrong: it claimed "Esc
    /// cancels regardless of focus" while the handler called
    /// `UiState::close_modal`, which clears the modal SLOT — and a form has
    /// been a nav element since `Modal::Form` was removed, so Esc cleared
    /// nothing and the dialog stayed on screen with OK as its only exit.
    pub focused: FormFocus,
}

/// Where the caret sits in a form: one of the fields, or one of the buttons.
///
/// This was a bare `usize` over `0..=fields.len()`, with `fields.len()`
/// *meaning* the OK button — an index space carrying a second meaning. That
/// is why there was exactly one button: a second one would have been
/// `fields.len() + 1`, leaving two predicates over arithmetic that could
/// disagree about which was focused. Naming the three cases makes the Tab
/// cycle explicit and the buttons exhaustive.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FormFocus {
    /// A schema field, by index into [`FormDialog::fields`].
    Field(usize),
    /// The OK button — submits the form.
    Ok,
    /// The Cancel button — leaves without submitting, exactly like Esc.
    Cancel,
}

impl FormDialog {
    /// Crumb label — the operation being configured.
    pub fn name(&self) -> &'static str {
        match self.submit {
            FormSubmit::Scale => "scale",
            FormSubmit::PortForward => "port-forward",
        }
    }

    /// Tab order: each field in display order, then OK, then Cancel, and
    /// round again. One function owns the cycle, so the two buttons cannot
    /// be reachable in one direction and not the other.
    pub fn focus_next(&mut self) {
        self.focused = match self.focused {
            FormFocus::Field(i) if i + 1 < self.fields.len() => FormFocus::Field(i + 1),
            FormFocus::Field(_) => FormFocus::Ok,
            FormFocus::Ok => FormFocus::Cancel,
            FormFocus::Cancel => self.first_focus(),
        };
    }

    /// The same cycle, backwards.
    pub fn focus_prev(&mut self) {
        self.focused = match self.focused {
            FormFocus::Field(0) => FormFocus::Cancel,
            FormFocus::Field(i) => FormFocus::Field(i.saturating_sub(1)),
            FormFocus::Ok => self.last_field_or_cancel(),
            FormFocus::Cancel => FormFocus::Ok,
        };
    }

    /// Where Tab lands after Cancel: the first field, or OK in a form that
    /// has no fields at all.
    fn first_focus(&self) -> FormFocus {
        if self.fields.is_empty() { FormFocus::Ok } else { FormFocus::Field(0) }
    }

    /// Where Shift-Tab lands from OK: the last field, or Cancel when there
    /// are none (the cycle is then just the two buttons).
    fn last_field_or_cancel(&self) -> FormFocus {
        match self.fields.len() {
            0 => FormFocus::Cancel,
            n => FormFocus::Field(n - 1),
        }
    }

    /// True if field `idx` holds the caret — for the renderer.
    pub fn field_focused(&self, idx: usize) -> bool {
        self.focused == FormFocus::Field(idx)
    }

    /// True if the OK button is currently focused.
    pub fn ok_focused(&self) -> bool {
        self.focused == FormFocus::Ok
    }

    /// True if the Cancel button is currently focused.
    pub fn cancel_focused(&self) -> bool {
        self.focused == FormFocus::Cancel
    }

    /// The focused field, if the caret is on a field rather than a button.
    /// A button is not a text sink, which is what the cursor-style gate and
    /// the editing keys both need to know.
    pub fn focused_field(&self) -> Option<&FormFieldState> {
        match self.focused {
            FormFocus::Field(i) => self.fields.get(i),
            FormFocus::Ok | FormFocus::Cancel => None,
        }
    }

    /// Mutably borrow the currently focused field, if any.
    pub fn current_field_mut(&mut self) -> Option<&mut FormFieldState> {
        match self.focused {
            FormFocus::Field(i) => self.fields.get_mut(i),
            FormFocus::Ok | FormFocus::Cancel => None,
        }
    }
}

impl FormFieldState {
    /// True if this field accepts text/digit input (i.e. the cursor sits
    /// inside an edit box, not on a Select picker or button).
    pub fn is_text_input(&self) -> bool {
        matches!(self.kind, FormFieldKind::Text { .. } | FormFieldKind::Number { .. } | FormFieldKind::Port)
    }
}

/// Shared state for YAML and Describe content views — one type, because the
/// two views differ in what they fetch, not in what they hold. (They were two
/// duplicated structs, then two `pub type` aliases kept "for backward
/// compatibility" in a binary that has no external consumers. Nothing named
/// them, and `pub` meant `dead_code` never said so.)
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ContentViewState {
    /// PRIVATE, with `describe_lines` and `line_count`, because the three are
    /// one fact in three shapes and only the two setters below keep them
    /// agreeing. While they were `pub`, a single `state.content = x` left the
    /// typed lines and the cached count stale — the same reason `UiState`
    /// keeps its modal slot private.
    content: String,
    /// Typed describe lines when this view is a structured *describe* (empty
    /// for YAML, aliases, and kubectl-fallback text). When non-empty the
    /// renderer styles by the producer's role tags instead of re-inferring
    /// structure from `content`.
    describe_lines: Vec<crate::kube::protocol::DescribeLine>,
    /// Vertical scroll relationship — content views never wrap, so one line is
    /// one row; the render publishes the extent back via `set_metrics`.
    pub viewport: crate::app::viewport::Viewport,
    pub search: Option<String>,
    pub search_matches: Vec<usize>,
    pub current_match: usize,
    pub search_input_active: bool,
    pub search_input: crate::app::text_input::TextInput,
    /// Cached line count — updated when content changes.
    line_count: usize,
}

impl ContentViewState {
    /// Set plain-text content (YAML, aliases, error text) and update the cached
    /// line count. Clears any typed describe lines so the renderer falls back
    /// to text inference for these genuinely-opaque views.
    pub fn set_content(&mut self, content: String) {
        self.line_count = content.lines().count();
        self.content = content;
        self.describe_lines.clear();
    }

    /// Set typed describe lines (a structured describe). Derives the flat
    /// `content` (line texts joined) so search / scroll / clipboard keep
    /// operating on a `String` unchanged.
    pub fn set_describe_lines(&mut self, lines: Vec<crate::kube::protocol::DescribeLine>) {
        self.content = crate::kube::protocol::describe_lines_text(&lines);
        self.line_count = lines.len();
        self.describe_lines = lines;
    }

    /// The flat text — search, scroll, clipboard and export all read this.
    pub fn content(&self) -> &str {
        &self.content
    }

    /// The typed lines, empty for the views that have none.
    pub fn describe_lines(&self) -> &[crate::kube::protocol::DescribeLine] {
        &self.describe_lines
    }

    /// Get the cached line count (O(1) instead of O(n)).
    pub fn line_count(&self) -> usize {
        self.line_count
    }
}

impl ContentViewState {
    /// Recompute search matches from current content (smartcase regex).
    pub fn update_search(&mut self) {
        self.search_matches.clear();
        self.current_match = 0;
        if let Some(ref term) = self.search {
            if term.is_empty() { return; }
            let pat = crate::util::SearchPattern::new(term);
            for (i, line) in self.content.lines().enumerate() {
                if pat.is_match(line) {
                    self.search_matches.push(i);
                }
            }
        }
    }

    pub fn next_match(&mut self) {
        if self.search_matches.is_empty() {
            return;
        }
        self.current_match = (self.current_match + 1) % self.search_matches.len();
        self.viewport.center_on(self.search_matches[self.current_match]);
    }

    pub fn prev_match(&mut self) {
        if self.search_matches.is_empty() {
            return;
        }
        self.current_match = if self.current_match == 0 {
            self.search_matches.len() - 1
        } else {
            self.current_match - 1
        };
        self.viewport.center_on(self.search_matches[self.current_match]);
    }

    /// Clear search state.
    pub fn clear_search(&mut self) {
        self.search = None;
        self.search_matches.clear();
        self.current_match = 0;
        self.search_input_active = false;
        self.search_input.clear();
    }
}

#[cfg(test)]
#[path = "../tests/app/form.rs"]
mod form_submit_tests_mirror;
