//! What `App` is made of: the display half and the cluster half.
//!
//! [`UiState`] is everything about presentation — the modal slot, the flash,
//! the animation tick. [`KubeState`] is everything about the cluster we are
//! pointed at. [`DeltaTracker`] sits between them: it remembers which rows
//! changed so the renderer can highlight them, and clears on a context or
//! namespace switch so a stale hash cannot flash a row in a new scope.
//!
//! This file was `types.rs`, 1468 lines and six concerns. A file named
//! `types` is a file named "stuff": config went to [`crate::config`], the
//! text-input gate to [`crate::text_input`], the link lifecycle to
//! [`crate::connection`] and the screen-takeover machinery to
//! [`crate::takeover`]. What is left is the state itself, named for it.

use std::collections::HashMap;
use std::hash::{DefaultHasher, Hash, Hasher};
use std::time::{Duration, Instant};

use crate::connection::ContextSwitchState;
use crate::kubectl_cache::KubectlCache;
use crate::takeover::{Modal, TakeoverOp};
// ---------------------------------------------------------------------------
// UiState — pure display/interaction state
// ---------------------------------------------------------------------------

/// Pure display and interaction state, factored out of `App` to reduce the
/// god-object surface. Fields here are only read/written by the UI layer
/// and the action handlers that respond to user input — they have no
/// cluster data semantics.
pub struct UiState {
    pub flash: Option<FlashMessage>,
    /// THE modal slot. One field, so "two modals at once" is unrepresentable
    /// rather than something 53 write sites each had to remember to prevent
    /// by clearing the other three. Private: reads go through the accessors
    /// below, writes through [`UiState::open`] / [`UiState::close_modal`],
    /// which is what makes the exclusivity structural instead of a
    /// convention. See [`Modal`].
    modal: Modal,
    pub show_header: bool,
    pub theme: k9rs_theme::Theme,
    pub tick_count: usize,
    pub column_level: k9rs_proto::resource_def::ColumnLevel,
    /// Self-contained animation clock for loading spinners — owns its phase,
    /// liveness, and cadence so it can't be perturbed by the data path.
    pub anim: crate::anim::Anim,
}

// ---------------------------------------------------------------------------
// KubeState — cluster/data state
// ---------------------------------------------------------------------------

/// Cluster and data state, factored out of `App`. Fields here are populated
/// from daemon events and read by the UI for display — they represent the
/// current cluster identity, namespace selection, metrics, and caches.
pub struct KubeState {
    /// The context we are actually on, or `None` before any has been
    /// confirmed — startup before the first `Ready`, or a launch with no
    /// resolvable context at all (the contexts picker is the whole UI then).
    /// An `Option` rather than an empty name so "not known yet" is a state
    /// the compiler makes every reader handle, instead of a blank string
    /// that renders as a context called "".
    pub context: Option<k9rs_proto::protocol::ContextName>,
    /// The context this session is being BROUGHT UP against, with the
    /// identity the kubeconfig says it has — read from disk, not confirmed by
    /// any daemon. `Some` from the moment a session starts connecting until
    /// it succeeds or is replaced.
    ///
    /// Separate from `context` because they answer different questions, and
    /// conflating them cost two bugs: the kubeconfig's candidate used to be
    /// written straight into `context` under an `is_none()` guard, so after a
    /// no-context start (where `context` stays `None`) a FAILED switch fell
    /// back to the context that had just failed and quit the app — and
    /// `identity` kept asserting the OLD cluster through every switch,
    /// because the candidate's identity arrived in the same event and was
    /// dropped with it.
    pub connecting: Option<(
        k9rs_proto::protocol::ContextName,
        k9rs_proto::protocol::ClusterIdentity,
    )>,
    pub identity: k9rs_proto::protocol::ClusterIdentity,
    pub selected_ns: k9rs_proto::protocol::Namespace,
    pub context_switch: ContextSwitchState,
    /// Latest metrics-server usage. Elements bind to it at construction
    /// (`store::MetricsBinding`); values overlay at derive
    /// time — rows are never mutated.
    pub metrics: std::sync::Arc<crate::store::MetricsHub>,
    pub kubectl_cache: KubectlCache,
}

impl KubeState {
    /// What the NEXT session should aim at, when one is being rebuilt.
    ///
    /// A different question from [`App::attachment`](crate::App::attachment),
    /// and named so nobody merges them: that one asks what the chrome should
    /// SAY, this one asks what to connect TO. The difference is `connecting`,
    /// which this deliberately ignores — it is the kubeconfig candidate
    /// produced BY a session build, so aiming the next build at it would be
    /// circular.
    ///
    /// A switch still in flight outranks the last-confirmed context: a
    /// mid-switch daemon blip must not silently abandon the switch and strand
    /// the user on the context they asked to leave.
    pub fn reconnect_target(&self) -> Option<k9rs_proto::protocol::ContextName> {
        self.context_switch
            .target()
            .cloned()
            .or_else(|| self.context.clone())
    }
}

impl UiState {
    /// Fresh UI state with nothing modal. A constructor rather than a
    /// struct literal so `modal` can stay private — which is what stops a
    /// fifth modal slot from being bolted on beside it later.
    pub fn new(theme: k9rs_theme::Theme) -> Self {
        Self {
            flash: None,
            modal: Modal::None,
            show_header: true,
            theme,
            tick_count: 0,
            column_level: k9rs_proto::resource_def::ColumnLevel::Default,
            anim: crate::anim::Anim::default(),
        }
    }

    /// The `:` prompt's buffer, if it is the open modal.
    pub fn command_input(&self) -> Option<(&str, Option<usize>)> {
        match &self.modal {
            Modal::Command { input, history_index } => Some((input.value(), *history_index)),
            _ => None,
        }
    }

    /// The `:` prompt's buffer itself, for the renderer — which needs the
    /// CARET, not just the text, to window a long command around it.
    pub fn command_text(&self) -> Option<&crate::text_input::TextInput> {
        match &self.modal {
            Modal::Command { input, .. } => Some(input),
            _ => None,
        }
    }

    /// Mutable access to the `:` prompt's buffer, for the editing keys.
    pub fn command_input_mut(
        &mut self,
    ) -> Option<(&mut crate::text_input::TextInput, &mut Option<usize>)> {
        match &mut self.modal {
            Modal::Command { input, history_index } => Some((input, history_index)),
            _ => None,
        }
    }

    /// Take whatever is open, leaving nothing. For flows that need to OWN
    /// the modal they are closing (the edit flow moves its `TempFile` out).
    pub fn take_modal(&mut self) -> Modal {
        std::mem::replace(&mut self.modal, Modal::None)
    }

    /// Close whatever is open.
    pub fn close_modal(&mut self) {
        self.modal = Modal::None;
    }

    /// Open a modal, replacing anything already there. The single write
    /// site — which is what stops two of them being open at once.
    pub fn open(&mut self, modal: Modal) {
        self.modal = modal;
    }

    /// What is open, for the render dispatch.
    pub fn modal(&self) -> &Modal {
        &self.modal
    }

    pub fn takeover(&self) -> Option<&TakeoverOp> {
        match &self.modal {
            Modal::Takeover(o) => Some(o),
            _ => None,
        }
    }

    pub fn takeover_mut(&mut self) -> Option<&mut TakeoverOp> {
        match &mut self.modal {
            Modal::Takeover(o) => Some(o),
            _ => None,
        }
    }
}

impl UiState {

    /// The `:` prompt's rendered prefix, or `""` when it isn't open.
    pub fn command_prompt(&self) -> &'static str {
        match &self.modal {
            Modal::Command { .. } => ":",
            _ => "",
        }
    }

    /// Whether ANY modal is up — the old
    /// `input_mode.is_active() || dialog.is_some() || …` chain.
    ///
    /// A question about DISPLAY, not about key routing. It was briefly wired
    /// as the fall-through guard in `try_handle_input_mode` and that was
    /// wrong in the most direct way possible: it is true for modals that
    /// already HAVE handlers, so it swallowed their keys before they could
    /// run and left every overlay — help, the container picker, a delete
    /// confirmation, a hanging shell connect — with no working Esc. Use
    /// [`Self::modal_without_key_handler`] for routing.
    pub fn is_modal(&self) -> bool {
        !matches!(self.modal, Modal::None)
    }

    /// Whether a modal is up whose keys NOTHING downstream will handle.
    ///
    /// This is the backstop the routing pass needs: a modal nobody handles
    /// must swallow input (visibly stuck) rather than leak it to the view
    /// underneath (invisibly wrong — keys keep driving the table, and Esc
    /// pops the nav stack out from under whatever is on screen).
    ///
    /// EXHAUSTIVE over `Modal` on purpose, and every arm is `false` today.
    /// That is the point: the value is not the answer, it is that adding a
    /// modal kind will not compile until someone states who handles its
    /// keys. A blanket `is_modal()` here answered a question nobody asked
    /// and broke every modal that was already correct.
    pub fn modal_without_key_handler(&self) -> bool {
        match &self.modal {
            Modal::None => false,
            // `Command` genuinely consumes every key it is handed.
            Modal::Command { .. } => false,
            // There is no `Form` arm because a form is not a modal: it is a
            // nav element, and `handler::handle_dialog_keys` owns its keys.
            // (The comment that used to sit here described a `Modal::Form`
            // variant that no longer exists, and broke off mid-sentence.)
            // That same fact is why Esc in a form could not be cancelled by
            // `close_modal` — see `FormDialog::focused`.
            //
            // Owned by `handler::handle_key_event`, whose `TakeoverOp` match is
            // wildcard-free — a new overlay variant is a compile error
            // there, which is the guard that actually holds.
            Modal::Takeover(_) => false,
        }
    }
}

// ---------------------------------------------------------------------------
// DeltaTracker — atomic ownership of row-change detection state
// ---------------------------------------------------------------------------

/// Owns the two maps that must stay in sync for row-change flash
/// highlights: per-row content hashes from the previous snapshot, and
/// timestamps of recently-changed rows. The only mutation paths go through
/// this struct's methods, so it's structurally impossible to update one
/// map without the other or to forget to clear both on a context switch.
///
/// Replaces the prior pair of loose `prev_rows: HashMap<ObjectKey, u64>` +
/// `changed_rows: HashMap<ObjectKey, Instant>` fields on `App`.
#[derive(Debug)]
pub struct DeltaTracker {
    prev_hashes: HashMap<k9rs_proto::protocol::ObjectKey, u64>,
    changed: HashMap<k9rs_proto::protocol::ObjectKey, Instant>,
}

impl Default for DeltaTracker {
    fn default() -> Self {
        Self::new()
    }
}

impl DeltaTracker {
    pub fn new() -> Self {
        Self {
            prev_hashes: HashMap::new(),
            changed: HashMap::new(),
        }
    }

    /// Baseline apply: compare incoming rows against the previous hashes.
    /// Rows whose content hash changed get a fresh timestamp in `changed`.
    /// The old hash map is replaced wholesale — rows that disappeared are
    /// implicitly forgotten. `prev_hashes` surviving across baselines is
    /// what gives cross-recovery highlight continuity (rows that changed
    /// while a watcher was down still flash after the recovery baseline)
    /// without mass-highlighting unchanged rows.
    pub fn rebaseline(&mut self, rows: &[k9rs_proto::row::ResourceRow]) {
        use k9rs_proto::protocol::ObjectKey;
        let now = Instant::now();
        let mut new_prev = HashMap::with_capacity(rows.len());
        for row in rows {
            let key = ObjectKey::new(
                row.namespace.clone().unwrap_or_default(),
                row.name.clone(),
            );
            let new_hash = Self::hash_cells(row);
            if let Some(prev_hash) = self.prev_hashes.get(&key) {
                if *prev_hash != new_hash {
                    self.changed.insert(key.clone(), now);
                }
            }
            new_prev.insert(key, new_hash);
        }
        self.prev_hashes = new_prev;
        // Prune change highlights for rows that no longer exist so stale
        // entries don't accumulate between expire() ticks.
        self.changed.retain(|k, _| self.prev_hashes.contains_key(k));
    }

    /// Delta apply: O(batch), replacing the per-snapshot full-store hash
    /// pass. The hash comparison stays — kube watches deliver metadata-only
    /// churn (heartbeats, resourceVersion bumps) as upserts with identical
    /// cells, and those must NOT flash. New keys don't flash (matches the
    /// baseline path's insert semantics); removes clear their state in
    /// O(1) (replacing the old O(n) retain).
    pub fn apply_changes(&mut self, changes: &[k9rs_proto::protocol::RowChange]) {
        use k9rs_proto::protocol::RowChange;
        let now = Instant::now();
        for change in changes {
            match change {
                RowChange::Upsert(row) => {
                    let key = change.key();
                    let new_hash = Self::hash_cells(row);
                    match self.prev_hashes.insert(key.clone(), new_hash) {
                        Some(prev) if prev != new_hash => {
                            self.changed.insert(key, now);
                        }
                        Some(_) => {} // metadata-only churn: no flash
                        None => {}    // insert: no flash (matches baseline)
                    }
                }
                RowChange::Remove(key) => {
                    self.prev_hashes.remove(key);
                    self.changed.remove(key);
                }
            }
        }
    }

    fn hash_cells(row: &k9rs_proto::row::ResourceRow) -> u64 {
        let mut hasher = DefaultHasher::new();
        for cell in &row.cells {
            cell.hash(&mut hasher);
        }
        hasher.finish()
    }

    /// Remove change highlights older than `max_age`. Returns `true` if
    /// any entries were removed (signals the UI should redraw).
    pub fn expire(&mut self, max_age: Duration) -> bool {
        let now = Instant::now();
        let before = self.changed.len();
        self.changed.retain(|_, ts| now.duration_since(*ts) < max_age);
        self.changed.len() != before
    }

    /// Clear all tracking state. Used on context and namespace switches
    /// so stale hashes from the old scope can't produce false change
    /// highlights in the new scope.
    pub fn clear(&mut self) {
        self.prev_hashes.clear();
        self.changed.clear();
    }

    /// Borrow the changed-rows map for the table widget's flash-highlight
    /// renderer. The map is keyed by `ObjectKey`; the widget looks up each
    /// visible row to decide whether to apply the highlight style.
    pub fn changed_rows(&self) -> &HashMap<k9rs_proto::protocol::ObjectKey, Instant> {
        &self.changed
    }
}

/// Filtered and total item counts for a resource table.
#[derive(Debug, Clone, Copy)]
pub struct ItemCounts {
    pub filtered: usize,
    pub total: usize,
}

// ---------------------------------------------------------------------------
// CrdInfo — lightweight CRD metadata extracted from unified ResourceRow
// ---------------------------------------------------------------------------

/// Lightweight CRD metadata extracted from the typed `crd_info` field of a
/// `ResourceRow`. Type alias over [`k9rs_proto::protocol::CrdRef`] — the
/// shape is identical and `find_crd_by_name` returns the row's stored
/// CrdRef directly instead of re-cloning fields.
pub type CrdInfo = k9rs_proto::protocol::CrdRef;

// (`ContainerRef` lived here: pod + namespace + `LogContainer`, carried
// beside a `LogSession`'s `LogInit` as its display identity. It was a
// second copy of what the spec already said, and a fan-out target —
// "3 pods", "deploy/api" — is precisely where the copy would have had to
// lie. The element now derives its header from the spec, and the
// container label lives on `LogContainer::label` next to the variants.)

// ---------------------------------------------------------------------------

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
    pub fn is_expired(&self, flash_config: &crate::config::FlashConfig) -> bool {
        let lifetime_secs = match self.level {
            FlashLevel::Info => flash_config.info_secs,
            FlashLevel::Warn => flash_config.warn_secs,
            FlashLevel::Error => flash_config.error_secs,
        };
        self.created.elapsed().as_secs() >= lifetime_secs
    }
}

#[cfg(test)]
#[path = "tests/state.rs"]
mod tests;
