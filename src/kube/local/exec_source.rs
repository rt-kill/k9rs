//! Exec-backed local resource source — periodically runs an external
//! command, parses its JSON stdout into rows, and publishes snapshots.
//!
//! Follows the same per-context ownership model as [`super::port_forward::PortForwardSource`]:
//! `Arc::new_cyclic` constructor, `Weak<Self>` in spawned tasks,
//! `try_begin_grace` / `end_grace` with `AtomicBool`.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, Weak};

use serde::{Deserialize, Serialize};
use tokio::sync::watch;

use crate::event::ResourceUpdate;
use crate::kube::protocol::ResourceId;
use crate::kube::resources::row::{CellValue, ResourceRow, RowHealth};

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
    tx: watch::Sender<ResourceUpdate>,
    _keep_rx: watch::Receiver<ResourceUpdate>,
    grace_in_flight: AtomicBool,
}

impl ExecSource {
    /// Construct a new exec source and spawn the polling task. The task
    /// holds only a `Weak<Self>` so the source can be dropped when the
    /// last subscriber goes away.
    pub fn for_context(config: ExecSourceConfig) -> Arc<Self> {
        let rid = super::types::LocalResourceKind::Custom(config.name.clone()).to_resource_id();
        let headers = config.headers.clone();
        let empty = ResourceUpdate::Rows {
            resource: rid.clone(),
            headers,
            rows: Vec::new(),
        };
        let (tx, rx) = watch::channel(empty);
        let arc = Arc::new_cyclic(|_weak: &Weak<Self>| Self {
            rid,
            config,
            entries: Mutex::new(Vec::new()),
            tx,
            _keep_rx: rx,
            grace_in_flight: AtomicBool::new(false),
        });
        // Spawn the poll loop with a Weak ref.
        let weak = Arc::downgrade(&arc);
        tokio::spawn(Self::poll_loop(weak));
        arc
    }

    /// Background loop: run the command, parse stdout, update entries,
    /// publish, sleep, repeat. Exits when the `Weak` can no longer upgrade.
    async fn poll_loop(weak: Weak<Self>) {
        loop {
            let Some(this) = weak.upgrade() else { return };
            let interval_secs = this.config.poll_interval_secs;
            this.run_once().await;
            drop(this); // release strong ref before sleeping
            tokio::time::sleep(std::time::Duration::from_secs(interval_secs)).await;
        }
    }

    /// Execute the command once and update internal state.
    async fn run_once(&self) {
        let output = match tokio::process::Command::new(&self.config.command)
            .args(&self.config.args)
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::null())
            .output()
            .await
        {
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
        let _ = self.tx.send(ResourceUpdate::Rows {
            resource: self.rid.clone(),
            headers: self.config.headers.clone(),
            rows,
        });
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

    fn subscribe(&self) -> watch::Receiver<ResourceUpdate> {
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
