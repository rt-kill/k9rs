use std::collections::BTreeMap;

use crate::kube::protocol::ResourceId;

/// What subscription changes a nav operation requires.
/// Returned by push/pop/reset so the session layer can subscribe/unsubscribe.
pub struct NavChange {
    pub unsubscribe: Option<ResourceId>,
    pub subscribe: Option<ResourceId>,
}

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
    /// Owner reference chain: show pods owned (directly or transitively) by
    /// the resource with this UID. Follows ownerReferences metadata for 100%
    /// accurate drill-down (Pod → ReplicaSet → Deployment).
    OwnerChain {
        /// UID of the owner resource to match.
        uid: String,
        /// Kind of the owner (for display in breadcrumb).
        kind: String,
        /// Display name of the owner resource.
        display_name: String,
    },
}

// ---------------------------------------------------------------------------
// NavStep — one level in the navigation stack
// ---------------------------------------------------------------------------

/// A single level in the navigation stack.
#[derive(Debug, Clone)]
pub struct NavStep {
    /// What resource this step is viewing (universal identity).
    pub resource: ResourceId,
    /// The filter applied at this level (if any).
    pub filter: Option<NavFilter>,
    /// Saved table selection index so "back" restores cursor position.
    pub saved_selected: usize,
    /// Uncommitted filter input for this frame.
    pub filter_input: FilterInputState,
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
    /// The root resource before the last `reset()`, used for
    /// "toggle last view" (Ctrl-^).
    prev_root: Option<ResourceId>,
}

impl NavStack {
    /// Create a new NavStack with a root step for the given resource.
    pub fn new(rid: ResourceId) -> Self {
        Self {
            steps: vec![NavStep {
                resource: rid,
                filter: None,
                saved_selected: 0,
                filter_input: FilterInputState::default(),
            }],
            prev_root: None,
        }
    }

    /// The current (topmost) step.
    pub fn current(&self) -> &NavStep {
        self.steps.last().expect("NavStack is never empty")
    }

    /// Mutable access to the current step.
    pub fn current_mut(&mut self) -> &mut NavStep {
        self.steps.last_mut().expect("NavStack is never empty")
    }

    /// The current resource id.
    pub fn resource_id(&self) -> &ResourceId {
        &self.current().resource
    }

    /// The root resource id (bottom of the stack).
    pub fn root_resource_id(&self) -> &ResourceId {
        &self.steps[0].resource
    }

    /// Push a new step (grep or drill-down). Returns subscription changes.
    pub fn push(&mut self, step: NavStep) -> NavChange {
        let old = self.resource_id().clone();
        self.steps.push(step);
        let new = self.resource_id().clone();
        if new != old {
            NavChange { unsubscribe: Some(old), subscribe: Some(new) }
        } else {
            NavChange { unsubscribe: None, subscribe: None }
        }
    }

    /// Pop one step. Returns (popped_step, subscription_changes).
    pub fn pop(&mut self) -> Option<(NavStep, NavChange)> {
        if self.steps.len() <= 1 {
            return None;
        }
        let old = self.resource_id().clone();
        let popped = self.steps.pop()?;
        let new = self.resource_id().clone();
        let change = if new != old {
            NavChange { unsubscribe: Some(old), subscribe: Some(new) }
        } else {
            NavChange { unsubscribe: None, subscribe: None }
        };
        Some((popped, change))
    }

    /// Replace the entire stack with a new root. Returns subscription changes.
    pub fn reset(&mut self, rid: ResourceId) -> NavChange {
        let old = self.resource_id().clone();
        self.prev_root = Some(self.root_resource_id().clone());
        self.steps.clear();
        self.steps.push(NavStep {
            resource: rid.clone(),
            filter: None,
            saved_selected: 0,
            filter_input: FilterInputState::default(),
        });
        if rid != old {
            NavChange { unsubscribe: Some(old), subscribe: Some(rid) }
        } else {
            NavChange { unsubscribe: None, subscribe: None }
        }
    }

    /// Collect all filters that apply to the current view.
    pub fn active_filters(&self) -> Vec<&NavFilter> {
        let current_rid = self.resource_id();
        let mut filters = Vec::new();
        for step in self.steps.iter().rev() {
            if step.resource != *current_rid {
                break;
            }
            if let Some(ref f) = step.filter {
                filters.push(f);
            }
        }
        filters.reverse();
        filters
    }

    pub fn depth(&self) -> usize { self.steps.len() }
    pub fn is_drilled(&self) -> bool { self.steps.len() > 1 }

    pub fn pop_grep_containing(&mut self, needle: &str) -> bool {
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

    pub fn has_fault_filter(&self) -> bool {
        self.steps.iter().any(|s| {
            matches!(&s.filter, Some(NavFilter::Grep(t)) if t.contains("crashloop"))
        })
    }

    pub fn save_selected(&mut self, selected: usize) {
        if let Some(step) = self.steps.last_mut() {
            step.saved_selected = selected;
        }
    }

    pub fn prev_root(&self) -> Option<&ResourceId> {
        self.prev_root.as_ref()
    }

    /// The current step's filter input state.
    pub fn filter_input(&self) -> &FilterInputState {
        &self.current().filter_input
    }

    /// Mutable access to the current step's filter input state.
    pub fn filter_input_mut(&mut self) -> &mut FilterInputState {
        &mut self.current_mut().filter_input
    }

    pub fn breadcrumb(&self) -> String {
        if self.steps.is_empty() {
            return String::new();
        }
        let mut parts = Vec::new();
        for (i, step) in self.steps.iter().enumerate() {
            if i == 0 && step.filter.is_none() {
                parts.push(step.resource.short_label().to_string());
                continue;
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
                Some(NavFilter::OwnerChain { display_name, kind, .. }) => {
                    parts.push(format!("{}/{}", kind, display_name));
                }
                None => {
                    parts.push(step.resource.short_label().to_string());
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

/// Convenience helper: look up a ResourceId by alias, panicking on unknown aliases.
/// Used for frequently-referenced built-in resources (pods, nodes, etc.).
pub fn rid(alias: &str) -> ResourceId {
    ResourceId::from_alias(alias)
        .unwrap_or_else(|| panic!("Unknown resource alias: {}", alias))
}
