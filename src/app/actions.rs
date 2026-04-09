/// Represents every discrete user action that the application can handle.
/// Actions are produced by the event handler (key/mouse -> Action mapping)
/// and consumed by the application state machine.
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
    SwitchContext(String),
    /// Restart the selected deployment/statefulset/daemonset.
    Restart,
    /// Set up port-forwarding for the selected resource (Shift+F).
    PortForward,
    /// Show active port-forwards for the selected resource (f).
    ShowPortForwards,
    /// Toggle the header row visibility.
    ToggleHeader,
    /// Toggle full-fetch mode (wait for complete list vs incremental loading).
    ToggleFullFetch,
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
    /// Sort by column index.
    Sort(usize),
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
}

impl Action {
    /// Whether this action mutates cluster state (requires write access).
    pub fn is_mutating(&self) -> bool {
        matches!(
            self,
            Action::Delete
                | Action::Edit
                | Action::Scale
                | Action::Restart
                | Action::ForceKill
                | Action::Shell
                | Action::PortForward
                | Action::DecodeSecret
                | Action::TriggerCronJob
                | Action::SuspendCronJob
        )
    }
}
