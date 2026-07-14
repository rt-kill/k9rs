//! Exec-backed local resource source — periodically runs an external
//! command, parses its JSON stdout into rows, and publishes snapshots.
//!
//! The poll loop is a supervised [`LocalOperator`] on a
//! [`RunDelay::Schedule`] — the second operator after port-forward, and the
//! schedule-shaped one: re-running after the interval is the *normal* path,
//! so no failure-flavored lifecycle events are emitted (a failed poll warns
//! and keeps the last-known rows, exactly as before). Demand-driven
//! lifetime: subscribers hold the strong refs (`try_begin_grace`/`end_grace`
//! grace on the last drop), and the source's drop aborts the poll via its
//! [`OperatorGuard`].

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, OnceLock, Weak};
use std::time::Duration;

use serde::{Deserialize, Serialize};
use tokio::sync::watch;

use crate::kube::protocol::ResourceId;
use crate::kube::resources::row::{CellValue, ResourceRow, RowHealth};

use super::supervise::{
    supervise, AttemptHandle, LocalOperator, OperatorExit, OperatorGuard, RunDelay,
};
use super::LocalResourceSource;

// ---------------------------------------------------------------------------
// Config
// ---------------------------------------------------------------------------

/// User-facing configuration for a single exec resource source. Deserialized
/// from `~/.config/k9rs/config.yaml` under `k9rs.daemon.execResources`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ExecSourceConfig {
    /// Unique name for this resource (used as the resource plural and
    /// command-mode alias). E.g. `"node-costs"` → `:node-costs`.
    pub name: String,
    /// The command to run (e.g. `"kubectl"`, `"curl"`).
    pub command: String,
    /// Arguments passed to the command.
    #[serde(default)]
    pub args: Vec<String>,
    /// How often (in seconds) the command is re-run.
    #[serde(default = "default_poll_interval")]
    pub poll_interval_secs: u64,
    /// Column headers for the resulting table. Must match `json_field_keys`
    /// in length and order.
    pub headers: Vec<String>,
    /// JSON object keys to extract from each element of the stdout array.
    /// Each key becomes a `CellValue::Text` cell in the corresponding column.
    pub json_field_keys: Vec<String>,
}

fn default_poll_interval() -> u64 {
    30
}

// ---------------------------------------------------------------------------
// Internal entry
// ---------------------------------------------------------------------------

/// A single parsed entry from the command's JSON output.
struct ExecEntry {
    /// Unique name derived from the first field (or index if unnamed).
    name: String,
    /// Typed cells, one per header/field-key.
    cells: Vec<CellValue>,
    /// The original JSON object — kept for describe/yaml rendering.
    raw: serde_json::Value,
}

// ---------------------------------------------------------------------------
// Source
// ---------------------------------------------------------------------------

/// Per-context exec-backed resource source. Spawns a background task that
/// periodically runs a command, parses its JSON stdout, and publishes
/// snapshots to subscribed sessions.
pub struct ExecSource {
    rid: ResourceId,
    config: ExecSourceConfig,
    entries: Mutex<Vec<ExecEntry>>,
    tx: watch::Sender<crate::kube::protocol::TableBaseline>,
    _keep_rx: watch::Receiver<crate::kube::protocol::TableBaseline>,
    grace_in_flight: AtomicBool,
    /// RAII handle to the supervised poll loop, set once right after
    /// construction (`OnceLock` because `Arc::new_cyclic` runs before the
    /// task can exist). Source drop → guard drop → poll aborted — the old
    /// "loop exits when the `Weak` stops upgrading" made immediate.
    poll_guard: OnceLock<OperatorGuard>,
}

impl ExecSource {
    /// Construct a new exec source and start its supervised polling
    /// operator. The operator holds only a `Weak<Self>` so the source can
    /// be dropped when the last subscriber goes away.
    pub fn for_context(config: ExecSourceConfig) -> Arc<Self> {
        let rid = super::types::LocalResourceKind::Custom(config.name.clone()).to_resource_id();
        let headers = config.headers.clone();
        let empty = crate::kube::protocol::TableBaseline {
            resource: rid.clone(),
            headers,
            rows: Vec::new(),
        };
        let (tx, rx) = watch::channel(empty);
        let interval = Duration::from_secs(config.poll_interval_secs);
        let arc = Arc::new_cyclic(|_weak: &Weak<Self>| Self {
            rid,
            config,
            entries: Mutex::new(Vec::new()),
            tx,
            _keep_rx: rx,
            grace_in_flight: AtomicBool::new(false),
            poll_guard: OnceLock::new(),
        });
        let (guard, gate) = supervise(
            ExecPollOp { weak: Arc::downgrade(&arc) },
            RunDelay::Schedule(interval),
            // Exec has no lifecycle column — its rows ARE the output, and a
            // failed poll keeps last-known rows. The sink is a required
            // parameter, so "no state display" is this visible choice, not
            // a silently-defaulted hook.
            |_| {},
        );
        let _ = arc.poll_guard.set(guard);
        // No insert-race here (the guard's slot IS the source itself), so
        // arm immediately.
        gate.arm();
        arc
    }

    /// Fold one poll's outcome into the source: parse stdout, replace the
    /// entries, publish. Synchronous on purpose — the caller (the operator)
    /// upgrades its `Weak` only for this call, never across an await. A
    /// failed poll warns and returns without touching entries, so a
    /// transient command failure never blanks the display.
    fn ingest(&self, result: std::io::Result<std::process::Output>) {
        let output = match result {
            Ok(o) => o,
            Err(e) => {
                tracing::warn!(
                    "exec source: failed to run `{}`: {}",
                    self.config.command,
                    e
                );
                return;
            }
        };

        if !output.status.success() {
            tracing::warn!(
                "exec source: `{}` exited with {}",
                self.config.command,
                output.status
            );
            return;
        }

        let stdout = match String::from_utf8(output.stdout) {
            Ok(s) => s,
            Err(e) => {
                tracing::warn!("exec source: non-UTF8 stdout: {}", e);
                return;
            }
        };

        // Parse as a JSON array — this IS the external API boundary where
        // serde_json::Value is acceptable.
        let arr: Vec<serde_json::Value> = match serde_json::from_str(&stdout) {
            Ok(a) => a,
            Err(e) => {
                tracing::warn!("exec source: JSON parse failed: {}", e);
                return;
            }
        };

        let new_entries: Vec<ExecEntry> = arr
            .into_iter()
            .enumerate()
            .map(|(idx, obj)| self.parse_entry(idx, obj))
            .collect();

        {
            let mut locked = self.entries.lock().expect("exec entries mutex poisoned");
            *locked = new_entries;
        }

        self.publish();
    }

    /// Extract typed cells from a JSON object using the configured field keys.
    fn parse_entry(&self, idx: usize, obj: serde_json::Value) -> ExecEntry {
        let cells: Vec<CellValue> = self
            .config
            .json_field_keys
            .iter()
            .map(|key| {
                let val = obj.get(key);
                match val {
                    Some(serde_json::Value::String(s)) => CellValue::Text(s.clone()),
                    Some(serde_json::Value::Number(n)) => {
                        CellValue::Text(n.to_string())
                    }
                    Some(serde_json::Value::Bool(b)) => CellValue::Text(b.to_string()),
                    Some(serde_json::Value::Null) | None => CellValue::Text("-".into()),
                    Some(other) => CellValue::Text(other.to_string()),
                }
            })
            .collect();

        // Name: use the first field's text, or fall back to the index.
        let name = cells
            .first()
            .and_then(|c| match c {
                CellValue::Text(s) if !s.is_empty() && s != "-" => Some(s.clone()),
                _ => None,
            })
            .unwrap_or_else(|| format!("exec-{idx}"));

        ExecEntry {
            name,
            cells,
            raw: obj,
        }
    }

    fn publish(&self) {
        let locked = self.entries.lock().expect("exec entries mutex poisoned");
        let mut rows: Vec<ResourceRow> = locked
            .iter()
            .map(entry_to_row)
            .collect();
        rows.sort_by(|a, b| a.name.cmp(&b.name));
        let _ = self.tx.send(crate::kube::protocol::TableBaseline {
            resource: self.rid.clone(),
            headers: self.config.headers.clone(),
            rows,
        });
    }
}

/// The supervised poll: run the configured command once, fold the output
/// into the source. Holds `Weak<ExecSource>` and — per the operator
/// contract — never a strong ref across the command await: the command
/// line is copied out synchronously up front, and the source is
/// re-upgraded only for the synchronous `ingest`.
///
/// NOTE: the command runs with the **daemon's** environment, not any TUI
/// session's. An `ExecSource` is a daemon-owned, per-context resource
/// (defined in `k9rs.daemon.execResources`, shared across every session on
/// that context), so there is no single session identity to authenticate
/// as — a command that shells out to `kubectl`/cloud CLIs uses the
/// daemon's own credentials, by design. (Contrast the per-session
/// `kubectl` spawns, which carry the viewing session's env via
/// `SessionEnv`.)
struct ExecPollOp {
    weak: Weak<ExecSource>,
}

impl LocalOperator for ExecPollOp {
    async fn run_once(&self, _attempt: AttemptHandle) -> OperatorExit {
        let Some((command, args)) = self
            .weak
            .upgrade()
            .map(|t| (t.config.command.clone(), t.config.args.clone()))
        else {
            return OperatorExit::Gone;
        };
        let result = tokio::process::Command::new(&command)
            .args(&args)
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::null())
            .output()
            .await;
        let Some(this) = self.weak.upgrade() else {
            return OperatorExit::Gone;
        };
        this.ingest(result);
        // A poll "completing" — success or warned-and-kept-last-known — is
        // the schedule's normal path; run again next interval.
        OperatorExit::Continue(String::new())
    }
}

impl LocalResourceSource for ExecSource {
    fn resource_id(&self) -> &ResourceId {
        &self.rid
    }

    fn headers(&self) -> Vec<String> {
        self.config.headers.clone()
    }

    fn try_begin_grace(&self) -> bool {
        self.grace_in_flight
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
    }

    fn end_grace(&self) {
        self.grace_in_flight.store(false, Ordering::Release);
    }

    fn subscribe(&self) -> watch::Receiver<crate::kube::protocol::TableBaseline> {
        self.tx.subscribe()
    }

    fn delete(&self, name: &str) -> Result<(), String> {
        let mut locked = self.entries.lock().expect("exec entries mutex poisoned");
        let before = locked.len();
        locked.retain(|e| e.name != name);
        if locked.len() == before {
            return Err(format!("no exec entry named '{name}'"));
        }
        drop(locked);
        self.publish();
        Ok(())
    }

    fn describe(&self, name: &str) -> Option<Result<String, String>> {
        let locked = self.entries.lock().expect("exec entries mutex poisoned");
        let entry = locked.iter().find(|e| e.name == name);
        let entry = match entry {
            Some(e) => e,
            None => return Some(Err(format!("no exec entry named '{name}'"))),
        };
        let pretty = match serde_json::to_string_pretty(&entry.raw) {
            Ok(s) => s,
            Err(e) => return Some(Err(format!("JSON serialize error: {e}"))),
        };
        Some(Ok(pretty))
    }

    fn yaml(&self, name: &str) -> Option<Result<String, String>> {
        let locked = self.entries.lock().expect("exec entries mutex poisoned");
        let entry = locked.iter().find(|e| e.name == name);
        let entry = match entry {
            Some(e) => e,
            None => return Some(Err(format!("no exec entry named '{name}'"))),
        };
        let yaml = match serde_yaml::to_string(&entry.raw) {
            Ok(s) => s,
            Err(e) => return Some(Err(format!("YAML serialize error: {e}"))),
        };
        Some(Ok(yaml))
    }

    fn apply_yaml(&self, _name: &str, _yaml: &str) -> Result<String, String> {
        Err("exec resources are read-only".into())
    }
}

/// Convert an `ExecEntry` to a `ResourceRow`.
fn entry_to_row(entry: &ExecEntry) -> ResourceRow {
    ResourceRow {
        name: entry.name.clone(),
        namespace: None,
        cells: entry.cells.clone(),
        health: RowHealth::Normal,
        ..Default::default()
    }
}
