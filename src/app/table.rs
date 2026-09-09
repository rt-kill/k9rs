// ---------------------------------------------------------------------------
// TableDataState — lifecycle state for table data
// ---------------------------------------------------------------------------

/// Lifecycle state of a data source. Replaces the prior triple of
/// `has_data: bool` + `loading: bool` + `error: Option<String>` — same
/// pattern as [`crate::app::ContextSwitchState`] and
/// [`crate::app::EditState`].
///
/// Transitions:
/// - `Initializing` → `Ready` (first baseline arrives)
/// - `Ready` → `Initializing` (refresh / reused store cleared)
/// - `Ready` → `Failed` (subscription error arrives)
/// - `Failed` → `Initializing` (clear resets)
/// - `Initializing` → `Failed` (subscription fails before first baseline)
/// - `Ready` → `Stale` (the daemon's watch stopped feeding this store)
/// - `Stale` → `Ready` (the watch recovered, or any data arrived)
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub enum TableDataState {
    /// No data received yet. The UI shows a loading spinner.
    #[default]
    Initializing,
    /// At least one baseline has been received. The table may be empty
    /// (zero matching resources), but the server has responded.
    Ready,
    /// The subscription failed. The UI shows this error message instead
    /// of the loading spinner.
    Failed(String),
    /// Rows are resident and were true when they arrived, but the daemon's
    /// watch is no longer feeding them (cluster-side outage; it is
    /// retrying). Deliberately NOT `Initializing`: these rows are the last
    /// known truth and worth acting on, they are just not current — and
    /// deliberately NOT `Failed`: nothing is broken permanently.
    Stale(String),
}
