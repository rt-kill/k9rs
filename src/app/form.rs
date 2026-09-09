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
    pub value: String,
}

/// Field type discriminator. Owned client-side — there is no wire-level
/// equivalent (the daemon only sends a list of [`OperationKind`] and the
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
        .map(|f| f.value.trim().to_string())
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
        _ => field.value.trim().parse::<u16>()
            .map_err(|_| format!("Invalid port: {}", field.value.trim())),
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
    /// Currently focused position. `0..fields.len()` are field indices;
    /// `fields.len()` is the OK button. Esc cancels regardless of focus.
    pub focused: usize,
}

impl FormDialog {
    /// Number of focusable positions (one per field, plus the OK button).
    pub fn focus_count(&self) -> usize {
        self.fields.len() + 1
    }

    /// Move focus to the next position, wrapping around.
    pub fn focus_next(&mut self) {
        self.focused = (self.focused + 1) % self.focus_count();
    }

    /// Move focus to the previous position, wrapping around.
    pub fn focus_prev(&mut self) {
        let n = self.focus_count();
        self.focused = (self.focused + n - 1) % n;
    }

    /// True if the OK button is currently focused.
    pub fn ok_focused(&self) -> bool {
        self.focused == self.fields.len()
    }

    /// Mutably borrow the currently focused field, if any.
    pub fn current_field_mut(&mut self) -> Option<&mut FormFieldState> {
        self.fields.get_mut(self.focused)
    }

}

impl FormFieldState {
    /// True if this field accepts text/digit input (i.e. the cursor sits
    /// inside an edit box, not on a Select picker or button).
    pub fn is_text_input(&self) -> bool {
        matches!(self.kind, FormFieldKind::Text { .. } | FormFieldKind::Number { .. } | FormFieldKind::Port)
    }
}

/// Shared state for YAML and Describe content views (previously duplicated as
/// `YamlState` and `DescribeState`).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ContentViewState {
    pub content: String,
    /// Typed describe lines when this view is a structured *describe* (empty
    /// for YAML, aliases, and kubectl-fallback text). When non-empty the
    /// renderer styles by the producer's role tags instead of re-inferring
    /// structure from `content`.
    pub describe_lines: Vec<crate::kube::protocol::DescribeLine>,
    /// Vertical scroll relationship — content views never wrap, so one line is
    /// one row; the render publishes the extent back via `set_metrics`.
    pub viewport: crate::app::viewport::Viewport,
    pub search: Option<String>,
    pub search_matches: Vec<usize>,
    pub current_match: usize,
    pub search_input_active: bool,
    pub search_input: String,
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

/// Type alias for backward compatibility.
pub type YamlState = ContentViewState;
/// Type alias for backward compatibility.
pub type DescribeState = ContentViewState;

#[cfg(test)]
mod form_submit_tests {
    use super::*;
    use crate::kube::protocol::{Namespace, ObjectRef, ResourceId};
    use crate::kube::resource_def::BuiltInKind;

    fn pod_target() -> ObjectRef {
        ObjectRef::new(
            ResourceId::BuiltIn(BuiltInKind::Pod),
            "test-pod",
            Namespace::from_user_command("default"),
        )
    }

    #[test]
    fn scale_build_command_valid() {
        let target = pod_target();
        let fields = vec![FormFieldState {
            name: "replicas".into(),
            label: "Replicas".into(),
            kind: FormFieldKind::Number { min: 0, max: 100 },
            value: "3".into(),
        }];
        let cmd = FormSubmit::Scale.build_command(&target, &fields).unwrap();
        match cmd {
            crate::kube::protocol::SessionCommand::Scale { replicas, .. } => {
                assert_eq!(replicas, 3);
            }
            _ => panic!("expected Scale command"),
        }
    }

    #[test]
    fn scale_build_command_invalid() {
        let target = pod_target();
        let fields = vec![FormFieldState {
            name: "replicas".into(),
            label: "Replicas".into(),
            kind: FormFieldKind::Number { min: 0, max: 100 },
            value: "abc".into(),
        }];
        assert!(FormSubmit::Scale.build_command(&target, &fields).is_err());
    }

    #[test]
    fn port_forward_build_command_valid() {
        let target = pod_target();
        let fields = vec![
            FormFieldState {
                name: "container_port".into(),
                label: "".into(),
                kind: FormFieldKind::Port,
                value: "8080".into(),
            },
            FormFieldState {
                name: "local_port".into(),
                label: "".into(),
                kind: FormFieldKind::Port,
                value: "9090".into(),
            },
        ];
        let cmd = FormSubmit::PortForward.build_command(&target, &fields).unwrap();
        match cmd {
            crate::kube::protocol::SessionCommand::PortForward { local_port, container_port, .. } => {
                assert_eq!(container_port, 8080);
                assert_eq!(local_port, 9090);
            }
            _ => panic!("expected PortForward command"),
        }
    }

    #[test]
    fn port_forward_build_command_select_uses_typed_index() {
        let target = pod_target();
        // The pf_ports path builds a Select container_port. The chosen option is
        // the typed `selected` index — no digit-string round-trip — so selected=1
        // resolves to the second option ("8443").
        let fields = vec![
            FormFieldState {
                name: "container_port".into(),
                label: "".into(),
                kind: FormFieldKind::Select {
                    options: vec![
                        SelectOption::new("8080", "8080"),
                        SelectOption::new("8443", "8443"),
                    ],
                    selected: 1,
                },
                value: String::new(),
            },
            FormFieldState {
                name: "local_port".into(),
                label: "".into(),
                kind: FormFieldKind::Port,
                value: "9090".into(),
            },
        ];
        let cmd = FormSubmit::PortForward.build_command(&target, &fields).unwrap();
        match cmd {
            crate::kube::protocol::SessionCommand::PortForward { container_port, local_port, .. } => {
                assert_eq!(container_port, 8443);
                assert_eq!(local_port, 9090);
            }
            _ => panic!("expected PortForward command"),
        }
    }
}
