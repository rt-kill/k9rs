use std::collections::BTreeMap;

use super::ResourceTab;
use crate::kube::protocol::ResourceScope;

// ---------------------------------------------------------------------------
// NavFilter — the kinds of filters that can narrow a resource view
// ---------------------------------------------------------------------------

/// A filter applied at one level of the navigation stack.
#[derive(Debug, Clone)]
pub enum NavFilter {
    /// Text grep across all columns (what `/` does).
    Grep(String),
    /// Label selector: show only items whose labels contain ALL of these k=v pairs.
    /// Used for deployment→pods, statefulset→pods, etc.
    Labels(BTreeMap<String, String>),
    /// Field match: show only items where a named field equals a value.
    /// Used for node→pods (field="node", value="node-name").
    Field { field: String, value: String },
}

// ---------------------------------------------------------------------------
// DynamicSpec — CRD metadata needed to re-subscribe on pop
// ---------------------------------------------------------------------------

/// Metadata for a dynamic CRD resource subscription.
#[derive(Debug, Clone)]
pub struct DynamicSpec {
    pub group: String,
    pub version: String,
    pub kind: String,
    pub plural: String,
    pub scope: ResourceScope,
}

// ---------------------------------------------------------------------------
// NavStep — one level in the navigation stack
// ---------------------------------------------------------------------------

/// A single level in the navigation stack.
#[derive(Debug, Clone)]
pub struct NavStep {
    /// What resource tab this step is viewing.
    pub tab: ResourceTab,
    /// The filter applied at this level (if any).
    pub filter: Option<NavFilter>,
    /// For DynamicResource tab: the CRD metadata needed to re-subscribe.
    pub dynamic_spec: Option<DynamicSpec>,
    /// Saved table selection index so "back" restores cursor position.
    pub saved_selected: usize,
}

// ---------------------------------------------------------------------------
// NavStack — the full navigation state
// ---------------------------------------------------------------------------

/// Unified navigation state replacing filter, last_resource_tab, and dynamic_resource_* fields.
///
/// The stack is never empty — the bottom element is always the "root" (whatever
/// tab the user switched to via `:command` or tab key). Grep and drill-down
/// operations push new steps. Esc pops one step at a time.
#[derive(Debug)]
pub struct NavStack {
    steps: Vec<NavStep>,
}

impl NavStack {
    /// Create a new NavStack with a root step for the given tab.
    pub fn new(tab: ResourceTab) -> Self {
        Self {
            steps: vec![NavStep {
                tab,
                filter: None,
                dynamic_spec: None,
                saved_selected: 0,
            }],
        }
    }

    /// The current (topmost) step.
    pub fn current(&self) -> &NavStep {
        self.steps.last().expect("NavStack is never empty")
    }

    /// The current resource tab.
    pub fn tab(&self) -> ResourceTab {
        self.current().tab
    }

    /// The root tab (bottom of the stack — what the user originally navigated to).
    pub fn root_tab(&self) -> ResourceTab {
        self.steps[0].tab
    }

    /// Push a new step (grep or drill-down).
    pub fn push(&mut self, step: NavStep) {
        self.steps.push(step);
    }

    /// Pop one step. Returns the popped step, or None if at root.
    pub fn pop(&mut self) -> Option<NavStep> {
        if self.steps.len() <= 1 {
            return None;
        }
        self.steps.pop()
    }

    /// Replace the entire stack with a new root (`:command` or tab switch).
    pub fn reset(&mut self, tab: ResourceTab) {
        self.steps.clear();
        self.steps.push(NavStep {
            tab,
            filter: None,
            dynamic_spec: None,
            saved_selected: 0,
        });
    }

    /// Reset with a dynamic resource root.
    pub fn reset_dynamic(&mut self, spec: DynamicSpec) {
        self.steps.clear();
        self.steps.push(NavStep {
            tab: ResourceTab::DynamicResource,
            filter: None,
            dynamic_spec: Some(spec),
            saved_selected: 0,
        });
    }

    /// Collect all filters that apply to the current view.
    /// Walks backward from the top of the stack, collecting filters from
    /// consecutive steps that share the same tab as the current step.
    pub fn active_filters(&self) -> Vec<&NavFilter> {
        let current_tab = self.tab();
        let mut filters = Vec::new();
        for step in self.steps.iter().rev() {
            if step.tab != current_tab {
                break;
            }
            if let Some(ref f) = step.filter {
                filters.push(f);
            }
        }
        filters.reverse(); // oldest first — matches are AND'd in order
        filters
    }

    /// The number of steps (for breadcrumb rendering).
    pub fn depth(&self) -> usize {
        self.steps.len()
    }

    /// Whether we're deeper than root (any drill-down or grep active).
    pub fn is_drilled(&self) -> bool {
        self.steps.len() > 1
    }

    /// Pop steps from the top until we find and remove one whose grep filter
    /// contains the given substring. Returns true if found and removed.
    pub fn pop_grep_containing(&mut self, needle: &str) -> bool {
        // Search from the top for the matching step
        let mut found_idx = None;
        for i in (1..self.steps.len()).rev() {
            if let Some(NavFilter::Grep(ref text)) = self.steps[i].filter {
                if text.contains(needle) {
                    found_idx = Some(i);
                    break;
                }
            }
        }
        if let Some(idx) = found_idx {
            self.steps.remove(idx);
            true
        } else {
            false
        }
    }

    /// Save the current table selection into the topmost step.
    pub fn save_selected(&mut self, selected: usize) {
        if let Some(step) = self.steps.last_mut() {
            step.saved_selected = selected;
        }
    }

    /// Get the DynamicSpec from the current step (if any).
    pub fn current_dynamic_spec(&self) -> Option<&DynamicSpec> {
        self.current().dynamic_spec.as_ref()
    }

    /// Build a breadcrumb string for the table title.
    /// Examples: "", "/nginx", "deploy/my-app", "deploy/my-app > /running"
    pub fn breadcrumb(&self) -> String {
        if self.steps.len() <= 1 && self.steps[0].filter.is_none() {
            return String::new();
        }
        let mut parts = Vec::new();
        for (i, step) in self.steps.iter().enumerate() {
            if i == 0 && step.filter.is_none() {
                continue; // skip bare root
            }
            match &step.filter {
                Some(NavFilter::Grep(text)) => parts.push(format!("/{}", text)),
                Some(NavFilter::Labels(labels)) => {
                    let label_str: Vec<String> = labels.iter()
                        .map(|(k, v)| format!("{}={}", k, v))
                        .collect();
                    parts.push(label_str.join(","));
                }
                Some(NavFilter::Field { field, value }) => {
                    parts.push(format!("{}={}", field, value));
                }
                None => {
                    // Drill-down with no filter (e.g., CRD instances)
                    if let Some(ref spec) = step.dynamic_spec {
                        parts.push(spec.kind.clone());
                    } else {
                        parts.push(step.tab.label().to_string());
                    }
                }
            }
        }
        parts.join(" > ")
    }
}

// ---------------------------------------------------------------------------
// FilterInputState — the input widget state (while user is typing `/`)
// ---------------------------------------------------------------------------

/// Tracks the filter input widget state, separate from committed nav filters.
#[derive(Debug, Default, Clone)]
pub struct FilterInputState {
    /// Whether the filter bar is in edit mode (listening for keystrokes).
    pub active: bool,
    /// The text being typed (not yet committed to NavStack).
    pub text: String,
}
