use std::collections::HashMap;
use std::time::{Duration, Instant};
use std::collections::VecDeque;

use crate::kube::protocol::{ObjectRef, ResourceScope};

use super::actions;

// Named constants for configurable limits.
const MAX_LOG_LINES: usize = 50_000;
const KUBECTL_CACHE_CAPACITY: usize = 100;
const FLASH_EXPIRY_SECS: u64 = 5;

/// User config settings (loaded from ~/.config/k9rs/config.yaml).
#[derive(Debug, Clone, Copy)]
pub struct AppConfig {
    pub no_exit_on_ctrl_c: bool,
    pub read_only: bool,
}

/// Filtered and total item counts for a resource table.
#[derive(Debug, Clone, Copy)]
pub struct ItemCounts {
    pub filtered: usize,
    pub total: usize,
}

/// Result of processing a search input keystroke.
pub enum SearchInputResult {
    /// Key was not consumed (search not active).
    Ignored,
    /// Key was consumed, input updated (Char, Backspace).
    Updated,
    /// User pressed Esc — cancel search.
    Cancelled,
    /// User pressed Enter — commit search with this term (empty = clear).
    Committed(String),
}

/// Process a keystroke against a search input buffer.
/// Returns what happened so the caller can apply view-specific logic.
pub fn handle_search_key(
    active: bool,
    input: &mut String,
    key: crossterm::event::KeyCode,
) -> SearchInputResult {
    if !active { return SearchInputResult::Ignored; }
    match key {
        crossterm::event::KeyCode::Esc => {
            input.clear();
            SearchInputResult::Cancelled
        }
        crossterm::event::KeyCode::Enter => {
            let text = std::mem::take(input);
            SearchInputResult::Committed(text)
        }
        crossterm::event::KeyCode::Backspace => {
            input.pop();
            SearchInputResult::Updated
        }
        crossterm::event::KeyCode::Char(c) => {
            input.push(c);
            SearchInputResult::Updated
        }
        _ => SearchInputResult::Updated,
    }
}

/// Parse a formatted age string like "5m", "2h3m", "3d5h" to total seconds.
/// Used for correct age-column sorting (lexicographic comparison gets it wrong).
pub(crate) fn parse_age_seconds(s: &str) -> u64 {
    let mut total: u64 = 0;
    let mut num: u64 = 0;
    for c in s.chars() {
        match c {
            '0'..='9' => num = num * 10 + (c as u64 - '0' as u64),
            'd' => { total += num * 86400; num = 0; }
            'h' => { total += num * 3600; num = 0; }
            'm' => { total += num * 60; num = 0; }
            's' => { total += num; num = 0; }
            _ => {}
        }
    }
    total + num // handle trailing number without suffix
}

// ---------------------------------------------------------------------------
// CrdInfo — lightweight CRD metadata extracted from unified ResourceRow
// ---------------------------------------------------------------------------

/// Lightweight CRD metadata extracted from the extra bag of a ResourceRow.
/// Used by command parsing and CRD-instance drill-down.
#[derive(Debug, Clone)]
pub struct CrdInfo {
    pub group: String,
    pub version: String,
    pub kind: String,
    pub plural: String,
    pub scope: ResourceScope,
}

// ---------------------------------------------------------------------------
// ContainerRef
// ---------------------------------------------------------------------------

/// Reference to a specific container within a pod.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ContainerRef {
    pub pod: String,
    pub namespace: String,
    pub container: String,
}

impl ContainerRef {
    pub fn new(pod: impl Into<String>, namespace: impl Into<String>, container: impl Into<String>) -> Self {
        Self { pod: pod.into(), namespace: namespace.into(), container: container.into() }
    }
}

// ---------------------------------------------------------------------------
// Route
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Route {
    Overview,
    Resources,
    Yaml {
        target: ObjectRef,
        awaiting_response: bool,
        state: ContentViewState,
    },
    Describe {
        target: ObjectRef,
        awaiting_response: bool,
        state: ContentViewState,
    },
    Logs {
        target: ContainerRef,
        state: Box<LogState>,
    },
    Shell {
        target: ContainerRef,
        state: Box<LogState>,
    },
    Help,
    Contexts,
    ContainerSelect {
        pod: String,
        namespace: String,
        selected: usize,
        for_shell: bool,
    },
    Aliases { state: ContentViewState },
}

// ---------------------------------------------------------------------------
// InputMode — text input overlay state
// ---------------------------------------------------------------------------

/// Active text input overlay. Each variant carries only the state it needs.
#[derive(Debug, Clone)]
pub enum InputMode {
    /// No text input active.
    Normal,
    /// `:` command prompt.
    Command {
        input: String,
        history_index: Option<usize>,
    },
    /// Scale replica count prompt.
    Scale {
        input: String,
        target: ObjectRef,
    },
}

impl InputMode {
    pub fn is_active(&self) -> bool {
        !matches!(self, InputMode::Normal)
    }

    /// The text buffer for the active input, if any.
    pub fn input(&self) -> Option<&str> {
        match self {
            InputMode::Normal => None,
            InputMode::Command { input, .. } | InputMode::Scale { input, .. } => Some(input),
        }
    }

    /// The prompt string for the active input.
    pub fn prompt(&self) -> &'static str {
        match self {
            InputMode::Normal => "",
            InputMode::Command { .. } => ":",
            InputMode::Scale { .. } => "Replicas: ",
        }
    }
}

// ---------------------------------------------------------------------------
// Flash / Filter / Confirm / Log / Yaml / Describe state
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FlashLevel {
    Info,
    Warn,
    Error,
}

#[derive(Debug, Clone)]
pub struct FlashMessage {
    pub message: String,
    pub level: FlashLevel,
    pub created: Instant,
}

impl FlashMessage {
    pub fn info(msg: impl Into<String>) -> Self {
        Self { message: msg.into(), level: FlashLevel::Info, created: Instant::now() }
    }
    pub fn warn(msg: impl Into<String>) -> Self {
        Self { message: msg.into(), level: FlashLevel::Warn, created: Instant::now() }
    }
    pub fn error(msg: impl Into<String>) -> Self {
        Self { message: msg.into(), level: FlashLevel::Error, created: Instant::now() }
    }
    pub fn is_expired(&self) -> bool {
        self.created.elapsed().as_secs() >= FLASH_EXPIRY_SECS
    }
}

/// What a confirm dialog will do when confirmed.
#[derive(Debug, Clone)]
pub enum PendingAction {
    /// Single-target action (delete, restart, force-kill one resource).
    Single {
        action: actions::Action,
        target: ObjectRef,
    },
    /// Batch delete marked resources.
    BatchDelete(Vec<ObjectRef>),
    /// Batch restart marked resources.
    BatchRestart(Vec<ObjectRef>),
    /// Batch force-kill marked resources.
    BatchForceKill(Vec<ObjectRef>),
}

#[derive(Debug, Clone)]
pub struct ConfirmDialog {
    pub message: String,
    pub pending: PendingAction,
    pub yes_selected: bool,
}

/// Which field in the port-forward dialog is focused.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PortForwardField {
    LocalPort,
    ContainerPort,
    Ok,
    Cancel,
}

impl PortForwardField {
    pub fn next(self) -> Self {
        match self {
            Self::LocalPort => Self::ContainerPort,
            Self::ContainerPort => Self::Ok,
            Self::Ok => Self::Cancel,
            Self::Cancel => Self::LocalPort,
        }
    }
    pub fn prev(self) -> Self {
        match self {
            Self::LocalPort => Self::Cancel,
            Self::ContainerPort => Self::LocalPort,
            Self::Ok => Self::ContainerPort,
            Self::Cancel => Self::Ok,
        }
    }
    pub fn is_text_input(self) -> bool {
        matches!(self, Self::LocalPort | Self::ContainerPort)
    }
}

/// Port-forward dialog — centered modal showing target and port options.
#[derive(Debug, Clone)]
pub struct PortForwardDialog {
    pub target: String,
    pub namespace: String,
    pub available_ports: Vec<u16>,
    pub local_port: String,
    pub container_port: String,
    pub selected_field: PortForwardField,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogState {
    pub lines: VecDeque<String>,
    pub max_lines: usize,
    pub scroll: usize,
    pub follow: bool,
    pub wrap: bool,
    pub show_timestamps: bool,
    pub streaming: bool,
    pub since: Option<String>,
    pub tail_lines: u64,
    /// Stack of committed grep filters. Each narrows the visible lines further.
    pub filters: Vec<String>,
    /// Draft filter being typed (live preview). None = not in filter input mode.
    pub draft_filter: Option<String>,
    /// Indices into `lines` that pass all filters (committed + draft).
    pub filtered_indices: Vec<usize>,
}

impl Default for LogState {
    fn default() -> Self {
        Self {
            lines: VecDeque::new(),
            max_lines: MAX_LOG_LINES,
            scroll: 0,
            follow: false,
            wrap: false,
            show_timestamps: true,
            streaming: false,
            since: None,
            tail_lines: 500,
            filters: Vec::new(),
            draft_filter: None,
            filtered_indices: Vec::new(),
        }
    }
}

impl LogState {
    pub fn new() -> Self {
        Self { follow: true, show_timestamps: true, streaming: false, ..Default::default() }
    }
    pub fn push(&mut self, line: String) {
        let evicted = self.lines.len() >= self.max_lines;
        if evicted {
            self.lines.pop_front();
            if !self.follow {
                self.scroll = self.scroll.saturating_sub(1);
            }
            // Adjust all filtered_indices: decrement by 1, drop index 0.
            self.filtered_indices.retain_mut(|idx| {
                if *idx == 0 { return false; }
                *idx -= 1;
                true
            });
        }
        self.lines.push_back(line);
        // Incrementally check the new line against active filters.
        let new_idx = self.lines.len() - 1;
        let patterns: Vec<crate::util::SearchPattern> = self.filters.iter()
            .chain(self.draft_filter.iter())
            .filter(|s| !s.is_empty())
            .map(|s| crate::util::SearchPattern::new(s))
            .collect();
        if patterns.is_empty() || patterns.iter().all(|p| p.is_match(&self.lines[new_idx])) {
            self.filtered_indices.push(new_idx);
        }
    }
    pub fn clear(&mut self) {
        self.lines.clear();
        self.scroll = 0;
        self.filtered_indices.clear();
        self.filters.clear();
        self.draft_filter = None;
    }

    /// Rebuild filtered_indices from all committed filters + draft.
    pub fn rebuild_filter(&mut self) {
        let all_patterns: Vec<crate::util::SearchPattern> = self.filters.iter()
            .chain(self.draft_filter.iter())
            .filter(|s| !s.is_empty())
            .map(|s| crate::util::SearchPattern::new(s))
            .collect();

        if all_patterns.is_empty() {
            self.filtered_indices = (0..self.lines.len()).collect();
        } else {
            self.filtered_indices = (0..self.lines.len())
                .filter(|&i| {
                    let line = &self.lines[i];
                    all_patterns.iter().all(|p| p.is_match(line))
                })
                .collect();
        }
    }

    /// Whether we're in filter input mode.
    pub fn is_filtering(&self) -> bool {
        self.draft_filter.is_some()
    }

    /// Start a new draft filter (user pressed `/`).
    pub fn start_filter(&mut self) {
        self.draft_filter = Some(String::new());
    }

    /// Update the draft filter text (user typed a character).
    pub fn update_draft(&mut self, text: String) {
        self.draft_filter = Some(text);
        self.rebuild_filter();
    }

    /// Commit the draft filter (user pressed Enter).
    pub fn commit_filter(&mut self) {
        if let Some(draft) = self.draft_filter.take() {
            if !draft.is_empty() {
                self.filters.push(draft);
            }
            self.rebuild_filter();
        }
    }

    /// Cancel the draft filter (user pressed Esc).
    pub fn cancel_filter(&mut self) {
        self.draft_filter = None;
        self.rebuild_filter();
    }

    /// Pop the last committed filter (user pressed Esc with no draft).
    pub fn pop_filter(&mut self) -> bool {
        if self.filters.pop().is_some() {
            self.rebuild_filter();
            true
        } else {
            false
        }
    }

    /// The active filter patterns for highlighting (committed + draft).
    pub fn active_patterns(&self) -> Vec<String> {
        let mut all: Vec<String> = self.filters.clone();
        if let Some(ref d) = self.draft_filter {
            if !d.is_empty() { all.push(d.clone()); }
        }
        all
    }

    /// Total visible line count (after filtering).
    pub fn visible_count(&self) -> usize {
        self.filtered_indices.len()
    }
}

/// Shared state for YAML and Describe content views (previously duplicated as
/// `YamlState` and `DescribeState`).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ContentViewState {
    pub content: String,
    pub scroll: usize,
    pub search: Option<String>,
    pub search_matches: Vec<usize>,
    pub current_match: usize,
    pub search_input_active: bool,
    pub search_input: String,
    /// Cached line count — updated when content changes.
    line_count: usize,
}

impl ContentViewState {
    /// Set content and update cached line count.
    pub fn set_content(&mut self, content: String) {
        self.line_count = content.lines().count();
        self.content = content;
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

    /// Jump to the next search match, centering it in the viewport.
    pub fn next_match(&mut self, visible: usize) {
        if self.search_matches.is_empty() {
            return;
        }
        self.current_match = (self.current_match + 1) % self.search_matches.len();
        let target = self.search_matches[self.current_match];
        self.scroll = target.saturating_sub(visible / 2);
    }

    /// Jump to the previous search match, centering it in the viewport.
    pub fn prev_match(&mut self, visible: usize) {
        if self.search_matches.is_empty() {
            return;
        }
        self.current_match = if self.current_match == 0 {
            self.search_matches.len() - 1
        } else {
            self.current_match - 1
        };
        let target = self.search_matches[self.current_match];
        self.scroll = target.saturating_sub(visible / 2);
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

// ---------------------------------------------------------------------------
// KubectlCache — TTL cache for describe/yaml kubectl output
// ---------------------------------------------------------------------------

/// What kind of content is cached.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ContentKind {
    Yaml,
    Describe,
}

/// Key for the kubectl output cache.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CacheKey {
    pub resource: String,
    pub name: String,
    pub namespace: String,
    pub kind: ContentKind,
}

impl CacheKey {
    pub fn new(resource: impl Into<String>, name: impl Into<String>, namespace: impl Into<String>, kind: ContentKind) -> Self {
        Self { resource: resource.into(), name: name.into(), namespace: namespace.into(), kind }
    }
}

/// A cached content entry with its timestamp.
pub(crate) struct CacheEntry {
    pub(crate) content: String,
    pub(crate) cached_at: Instant,
}

pub struct KubectlCache {
    entries: HashMap<CacheKey, CacheEntry>,
    insertion_order: Vec<CacheKey>,
    ttl: Duration,
    max_capacity: usize,
}

impl KubectlCache {
    pub fn new(ttl: Duration) -> Self {
        Self {
            entries: HashMap::new(),
            insertion_order: Vec::new(),
            ttl,
            max_capacity: KUBECTL_CACHE_CAPACITY,
        }
    }

    pub fn get(&self, resource: &str, name: &str, namespace: &str, kind: ContentKind) -> Option<&str> {
        let key = CacheKey::new(resource, name, namespace, kind);
        self.entries
            .get(&key)
            .and_then(|entry| {
                if entry.cached_at.elapsed() < self.ttl {
                    Some(entry.content.as_str())
                } else {
                    None
                }
            })
    }

    pub fn insert(
        &mut self,
        resource: String,
        name: String,
        namespace: String,
        kind: ContentKind,
        content: String,
    ) {
        let key = CacheKey::new(resource, name, namespace, kind);
        // If the key already exists, just update the value (no change to insertion order).
        if self.entries.contains_key(&key) {
            self.entries.insert(key, CacheEntry { content, cached_at: std::time::Instant::now() });
            return;
        }
        // Evict the oldest entry if at capacity.
        if self.entries.len() >= self.max_capacity {
            if let Some(oldest_key) = self.insertion_order.first().cloned() {
                self.entries.remove(&oldest_key);
                self.insertion_order.remove(0);
            }
        }
        self.insertion_order.push(key.clone());
        self.entries.insert(key, CacheEntry { content, cached_at: Instant::now() });
    }

    pub fn clear(&mut self) {
        self.entries.clear();
        self.insertion_order.clear();
    }
}
