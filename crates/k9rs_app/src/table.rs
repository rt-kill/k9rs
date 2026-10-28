// ---------------------------------------------------------------------------
// TableDataState — lifecycle state for table data
// ---------------------------------------------------------------------------

/// Lifecycle state of a data source. Replaces the prior triple of
/// `has_data: bool` + `loading: bool` + `error: Option<String>` — same
/// pattern as [`crate::ContextSwitchState`] and
/// [`crate::EditState`].
///
/// Transitions. **Prose, not a mechanism**: `state` is written by eight bare
/// assignments in `store.rs` with no `transition()` to route them through, so
/// this list is a description of what the code does and nothing stops the
/// two parting ways. It had already parted ways — the first four edges below
/// were missing from it, including the self-heal one, which is the most
/// valuable edge in the table.
///
/// - `Failed` → `Ready` (a baseline arrives; the self-heal path — `Baseline`
///   assigns `Ready` unconditionally)
/// - `Stale` → `Failed` (an error arrives while the rows are already frozen)
/// - `Stale` → `Initializing` (clear, or `mark_reinitializing`)
/// - `Failed` → `Initializing` (`mark_reinitializing`, not only `clear`)
/// - `Initializing` → `Ready` (first baseline arrives)
/// - `Ready` → `Initializing` (refresh / reused store cleared)
/// - `Ready` → `Failed` (subscription error arrives)
/// - `Initializing` → `Failed` (subscription fails before first baseline)
/// - `Ready` → `Stale` (the daemon's watch stopped feeding this store)
/// - `Stale` → `Ready` (the watch recovered, or any data arrived)
///
/// A new VARIANT is caught — `liveness.rs`'s match over this enum is
/// exhaustive. A new EDGE is not caught by the compiler, but every edge
/// LISTED here is driven and asserted by
/// `every_documented_state_edge_lands_where_the_doc_says` in
/// `tests/store.rs`, so the list and the code can no longer part ways
/// silently — which they had, for four edges including the self-heal.
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
