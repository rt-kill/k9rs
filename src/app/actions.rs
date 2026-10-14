/// Which column the user wants to sort by. Replaces the prior
/// `Action::Sort(usize)` + `usize::MAX` sentinel for "last column" — the
/// dispatcher branches on the variant instead of comparing to a magic
/// integer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SortTarget {
    /// Sort by the column at the given DATA index.
    Column(usize),
    /// Sort by whatever column the cursor is on, resolved at DISPATCH time.
    ///
    /// The key handler used to do the visible→data conversion itself, which
    /// meant reading the last painted view from the key layer — cold on a
    /// fresh element, so `S` right after a drill produced no action at all.
    /// Naming the intent instead of computing an index also removes the one
    /// caller that had to know which of the two column index spaces
    /// `Column(usize)` is in.
    CursorColumn,
    /// Sort by the column carrying this header, resolved against the LIVE
    /// headers at apply time. `None` if the table has no such column.
    ///
    /// The key handler used to compute a data index for `N` (sort by name)
    /// from a single boolean — `if is_cluster_scoped { 0 } else { 1 }` — on
    /// the assumption that a namespaced table's second column is NAME. Events
    /// is namespaced and its headers are `[NAMESPACE, TYPE, REASON, …]`, so
    /// `N` sorted by event TYPE; the table has no NAME column at all. `A`
    /// (sort by age) had the same shape one line down, taking the LAST column
    /// — which is ACTIVE on the contexts table and RESTARTS on containers.
    ///
    /// A name is what the user means, so a name is what travels.
    Named(&'static str),
}

/// Canonical header names the semantic sort keys resolve against.
pub const COL_NAME: &str = "NAME";
pub const COL_AGE: &str = "AGE";

/// Represents every discrete user action that the application can handle.
/// Actions are produced by the event handler (keystroke → Action mapping;
/// k9rs is keyboard-only) and consumed by the application state machine.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Action {
    /// Quit the application.
    Quit,
    /// Navigate back (pop route stack).
    Back,
    /// Show the help screen.
    Help,
    /// Switch to the next resource tab.
    NextTab,
    /// Switch to the previous resource tab.
    PrevTab,
    /// Select the next item in the current list.
    NextItem,
    /// Select the previous item in the current list.
    PrevItem,
    /// Scroll up one page in the current list.
    PageUp,
    /// Scroll down one page in the current list.
    PageDown,
    /// Jump to the first item.
    Home,
    /// Jump to the last item.
    End,
    /// Activate/drill into the currently selected item.
    Enter,
    /// Delete the currently selected resource (requires confirmation).
    Delete,
    /// Show the describe view for the selected resource.
    Describe,
    /// Show the YAML view for the selected resource.
    Yaml,
    /// Open the log viewer for the selected pod.
    Logs,
    /// Open a shell into the selected pod/container.
    Shell,
    /// Open the selected resource in an editor.
    Edit,
    /// Scale the selected deployment/statefulset/replicaset.
    Scale,
    /// Apply a filter string to the current resource list.
    Filter(String),
    /// Clear the active filter.
    ClearFilter,
    /// Toggle log follow mode.
    ToggleLogFollow,
    /// Toggle log line wrapping.
    ToggleLogWrap,
    /// Toggle log timestamps display.
    /// Cycle how much per-line chrome the log view shows (`t`) — see
    /// `ui::widgets::Role::SHED_ORDER`. One action for the whole cycle, so a
    /// new role joins it without a new binding.
    ToggleLogTimestamps,
    /// Clear the log buffer.
    ClearLogs,
    /// Scroll up by N lines (log/detail view).
    ScrollUp(usize),
    /// Scroll down by N lines (log/detail view).
    ScrollDown(usize),
    /// Switch to a different namespace ("all" for all namespaces).
    SwitchNamespace(crate::kube::protocol::Namespace),
    /// Switch to a different Kubernetes context.
    SwitchContext(crate::kube::protocol::ContextName),
    /// Restart the selected deployment/statefulset/daemonset.
    Restart,
    /// Set up port-forwarding for the selected resource (Shift+F).
    PortForward,
    /// Show active port-forwards for the selected resource (f).
    ShowPortForwards,
    /// Toggle the header row visibility.
    ToggleHeader,
    /// Force-refresh the current resource data.
    Refresh,
    /// Copy the selected item or content to the system clipboard.
    Copy,
    /// Confirm a pending dialog action.
    Confirm,
    /// Cancel a pending dialog action.
    Cancel,
    /// Enter command mode (`:` prompt).
    CommandMode,
    /// Toggle the Yes/No selection in the confirmation dialog.
    ToggleDialogButton,
    /// Sort by a specific column.
    Sort(SortTarget),
    /// Toggle ascending/descending sort direction on the current column.
    ToggleSortDirection,
    /// Begin search in YAML/describe view.
    SearchStart,
    /// Execute a search query in YAML/describe view.
    SearchExec(String),
    /// Go to next search match.
    SearchNext,
    /// Go to previous search match.
    SearchPrev,
    /// Clear search in YAML/describe view.
    SearchClear,
    /// View previous container logs (--previous flag).
    PreviousLogs,
    /// Force-kill a pod (kubectl delete --force --grace-period=0).
    ForceKill,
    /// Show the node a pod is running on (switch to nodes view filtered by node name).
    ShowNode,
    /// Shell into a node via `kubectl debug node/<name>`.
    NodeShell,
    /// Toggle between the current and last resource tab view.
    ToggleLastView,
    /// Toggle mark/select on the currently selected row.
    ToggleMark,
    /// Save/dump the current table contents to a file.
    SaveTable,
    /// Show the alias view (all resource type shortcuts).
    ShowAliases,
    /// Change the log time range (--since flag). None = tail all logs.
    LogSince(Option<String>),
    /// Move left in this view: the column cursor on a table, a horizontal
    /// pan on a log. One action because it is one intent — each element kind
    /// answers it in its own units, which is why the log view did not need a
    /// second pair of keys or a second config binding.
    ColLeft,
    /// Move right in this view. See [`Action::ColLeft`].
    ColRight,
    /// Move the column cursor to the first (leftmost) column.
    ColFirst,
    /// Move the column cursor to the last (rightmost) column.
    ColLast,
    /// Pan a stride left / right (logs). The stride is screen-derived and
    /// published by the render — see `viewport::Pan`.
    PageLeft,
    PageRight,
    /// Open the filter input restricted to the currently hovered column.
    ColumnFilter,
    /// Toggle wide column mode.
    ToggleWide,
    /// Toggle fault filter (show only unhealthy resources).
    ToggleFaultFilter,
    /// Show an informational flash message to the user.
    FlashInfo(String),
    /// Decode a secret (base64 decode all values).
    DecodeSecret,
    /// Trigger a CronJob (create a Job from it).
    TriggerCronJob,
    /// Suspend/resume a CronJob.
    SuspendCronJob,
    /// Clear all marks on the current table.
    ClearMarks,
    /// Span-mark (select range from anchor to cursor).
    SpanMark,
    /// Jump to the owner of the selected row (e.g., Pod → ReplicaSet → Deployment).
    JumpToOwner,
    /// Save the current log view to a file.
    SaveLogs,
    /// Show which resources reference the selected row (reverse lookup).
    UsedBy,
    /// Show the events ABOUT the selected row — "why is this Pending".
    ShowEvents,
    /// TakeoverOp-defined capability. Carries only the capability name; the
    /// handler resolves implementation details from the overlay config.
    /// Currently only Drill (read-only navigation) is supported. If
    /// mutating capability types are added, update `is_mutating()`.
    OverlayCapability(String),
    /// Delete every marked row (select mode). Distinct from [`Self::Delete`]
    /// BY CONSTRUCTION: the keymap never produces these — only the
    /// select-mode gate does, by transforming the single-target action
    /// while marks are active. There is no "marked set non-empty?" branch
    /// at dispatch that could silently fall through to the hovered row.
    BatchDelete,
    /// Rollout-restart every marked row (select mode).
    BatchRestart,
    /// Force-kill every marked row (select mode).
    BatchForceKill,
    /// Copy ONLY the marked rows (select mode's `c`). Distinct from
    /// [`Action::Copy`], which dumps the whole visible table, for the
    /// same reason `BatchDelete` is distinct from `Delete`: single and
    /// batch must never be the same action wearing different data.
    BatchCopy,
    /// Stream the marked rows' logs as ONE merged view (select mode's
    /// log key). The only `Aggregate` action: where `BatchDelete` fans
    /// out to N independent commands, this collapses to a single
    /// substream the daemon merges — see [`crate::kube::protocol::LogTarget`].
    BatchLogs,
}

/// Operation-manifest → key-dispatch bridge. Lives HERE (client side), not
/// on `OperationKind`, so the wire module never imports client action types
/// — protocol stays pure shared substrate; the dependency flows
/// client → protocol only.
impl From<&crate::kube::protocol::OperationKind> for Action {
    fn from(op: &crate::kube::protocol::OperationKind) -> Self {
        use crate::kube::protocol::OperationKind as Op;
        match op {
            Op::Describe => Action::Describe,
            Op::Yaml => Action::Yaml,
            Op::Delete => Action::Delete,
            Op::Restart => Action::Restart,
            Op::Scale => Action::Scale,
            Op::StreamLogs => Action::Logs,
            Op::PreviousLogs => Action::PreviousLogs,
            Op::PortForward => Action::PortForward,
            Op::Shell => Action::Shell,
            Op::ShowNode => Action::ShowNode,
            Op::ForceKill => Action::ForceKill,
            Op::NodeShell => Action::NodeShell,
            Op::DecodeSecret => Action::DecodeSecret,
            Op::TriggerCronJob => Action::TriggerCronJob,
            Op::ToggleSuspendCronJob => Action::SuspendCronJob,
            Op::Custom(name) => Action::OverlayCapability(name.clone()),
            // Wire-only correlation variant; the user-facing flow that
            // produces an apply is Edit.
            Op::Apply => Action::Edit,
        }
    }
}

impl Action {
    /// Whether this action should be blocked in readonly mode with a local
    /// "Read-only mode" flash, BEFORE the wire round-trip.
    ///
    /// This is a strict **superset** of the server-side classification in
    /// `SessionCommand::is_mutating`:
    ///
    /// - `Shell` and `DecodeSecret` aren't mutations on the server (exec
    ///   can mutate but isn't gated; decode is a pure read). They're here
    ///   because "readonly mode" in the TUI is a user-facing stance about
    ///   *what the user wants to do*, not about what the wire command is —
    ///   readonly users don't want pop-up shells, and decoded secret values
    ///   shouldn't appear at all.
    /// - `Edit` is a local action that may trigger an `Apply` later; we
    ///   reject it up front before launching `$EDITOR` rather than after.
    ///
    /// The authoritative readonly gate lives on the server
    /// (`SessionCommand::is_mutating` + the centralized check in
    /// `handle_command`). If this list ever falls out of sync, the server
    /// still refuses — this check is UX polish to give instant feedback
    /// without a round-trip, not a second source of truth.
    pub fn is_mutating(&self) -> bool {
        matches!(
            self,
            Action::Delete
                | Action::Edit
                | Action::Scale
                | Action::Restart
                | Action::ForceKill
                | Action::Shell
                | Action::NodeShell
                | Action::PortForward
                | Action::DecodeSecret
                | Action::TriggerCronJob
                | Action::SuspendCronJob
                | Action::BatchDelete
                | Action::BatchRestart
                | Action::BatchForceKill
        )
    }
}
