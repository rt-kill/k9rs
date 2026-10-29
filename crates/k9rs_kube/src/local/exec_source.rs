//! Exec-backed local resource source — periodically runs an external
//! command, parses its JSON stdout into rows, and publishes snapshots.
//!
//! The poll loop is a supervised `LocalOperator` on a
//! `RunDelay::Schedule` — the second operator after port-forward, and the
//! schedule-shaped one: re-running after the interval is the *normal* path,
//! so no failure-flavored lifecycle events are emitted (a failed poll warns
//! and keeps the last-known rows, exactly as before). Demand-driven
//! lifetime: subscribers hold the strong refs (its
//! [`GraceGate`](crate::grace::GraceGate) keeps it warm past the last drop),
//! and the source's drop aborts the poll via its `OperatorGuard`.

use std::sync::{Arc, Mutex, OnceLock, Weak};
use std::time::Duration;

use serde::{Deserialize, Serialize};
use tokio::sync::watch;

use k9rs_proto::protocol::ResourceId;
use k9rs_proto::row::{CellValue, ResourceRow, RowHealth};

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
    ///
    /// `NonZeroU64`, so `pollIntervalSecs: 0` is rejected by the config
    /// deserializer rather than by a guard someone has to remember to write.
    /// Zero was catastrophic and silent: it makes `RunDelay::Schedule(ZERO)`
    /// re-run with no delay AND makes the per-poll `timeout(interval, child)`
    /// fire before the child can emit a byte — so every poll `kill_on_drop`s
    /// its own subprocess, at CPU speed, forever, publishing no rows. With the
    /// exec sink deliberately `|_| {}`, a poller in that state is
    /// indistinguishable from a healthy one returning nothing.
    #[serde(default = "default_poll_interval")]
    pub poll_interval_secs: std::num::NonZeroU64,
    /// Column headers for the resulting table. Must match `json_field_keys`
    /// in length and order.
    pub headers: Vec<String>,
    /// JSON object keys to extract from each element of the stdout array.
    /// Each key becomes a `CellValue::Text` cell in the corresponding column.
    pub json_field_keys: Vec<String>,
}

impl ExecSourceConfig {
    /// Why this entry cannot serve a resource, or `Ok(())`.
    ///
    /// ONE validation, because the same config had TWO readers with
    /// different arity: the daemon applied five guards in a `filter`
    /// closure, and the client's `exec_resource_by_name` applied none — so
    /// an entry the daemon had skipped was still typeable and came back as
    /// `Unknown local resource: …` with no reason given.
    ///
    /// The ALIAS guard is new. `:`-command resolution tries
    /// `ResourceId::from_alias` first and exec-by-name fourth, so an entry
    /// named `pods`, `po`, `deploy`, `pf`, `ctx` or `a` could never be
    /// reached at all: its poll never started, nothing was ever shown, and
    /// nothing said why. That is the opposite of a shadow — the collision
    /// loses rather than wins — which is exactly why it was invisible.
    ///
    /// Duplicate names are NOT checked here: that is a property of the list,
    /// not of an entry. See [`Self::usable`].
    pub fn validate(&self) -> Result<(), String> {
        if self.name.is_empty() {
            return Err("has an empty name".to_string());
        }
        if let Some(existing) = k9rs_proto::protocol::ResourceId::from_alias(&self.name) {
            return Err(format!(
                "name '{}' is already a built-in alias for {} — `:{}` would never \
                 reach this resource",
                self.name,
                existing.plural(),
                self.name,
            ));
        }
        if self.command.is_empty() {
            return Err(format!("'{}' has an empty command", self.name));
        }
        if self.headers.is_empty() {
            return Err(format!("'{}' has no headers", self.name));
        }
        // The converter zips headers with json_field_keys positionally; a
        // length mismatch would silently drop columns or misalign data.
        if self.headers.len() != self.json_field_keys.len() {
            return Err(format!(
                "'{}' has {} headers but {} jsonFieldKeys — they must match in \
                 length and order",
                self.name,
                self.headers.len(),
                self.json_field_keys.len(),
            ));
        }
        Ok(())
    }

    /// The entries that can actually serve a resource, warning once per
    /// rejection.
    ///
    /// A malformed entry is skipped rather than fatal — one bad line should
    /// not take the others down, and a daemon is long-lived. Duplicate names
    /// are resolved first-wins, which is the only order a reader could
    /// predict.
    pub fn usable(configs: Vec<Self>) -> Vec<Self> {
        let mut seen = std::collections::HashSet::new();
        configs
            .into_iter()
            .filter(|c| {
                if let Err(why) = c.validate() {
                    tracing::warn!("exec resource config {why} — skipping");
                    return false;
                }
                if !seen.insert(c.name.clone()) {
                    tracing::warn!(
                        "exec resource config has duplicate name '{}' — skipping",
                        c.name,
                    );
                    return false;
                }
                true
            })
            .collect()
    }
}

fn default_poll_interval() -> std::num::NonZeroU64 {
    std::num::NonZeroU64::new(30).expect("30 is not zero")
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
    tx: watch::Sender<k9rs_proto::protocol::TableBaseline>,
    _keep_rx: watch::Receiver<k9rs_proto::protocol::TableBaseline>,
    grace: crate::grace::GraceGate,
    /// RAII handle to the supervised poll loop, set once right after
    /// construction (`OnceLock` because `Arc::new_cyclic` runs before the
    /// task can exist). Source drop → guard drop → poll aborted — the old
    /// "loop exits when the `Weak` stops upgrading" made immediate.
    poll_guard: OnceLock<OperatorGuard>,
}

/// How much stdout one exec poll may produce. Generous for a table's worth
/// of JSON; small enough that a runaway command cannot grow the daemon.
///
/// Overflow is a FAILED poll, not a truncated parse: half a JSON document is
/// not a shorter table, it is a parse error, and reporting it as one is the
/// honest outcome (last-known rows are kept, and the next tick tries again).
const MAX_EXEC_STDOUT: usize = 8 * 1024 * 1024;

/// `Command::output()` with a cap on stdout.
///
/// Reads through `Take`, so an over-long stream stops costing memory at the
/// limit rather than after it. stderr is read too — it is what explains a
/// non-zero exit.
async fn capped_output(
    child: std::io::Result<tokio::process::Child>,
) -> std::io::Result<std::process::Output> {
    use tokio::io::AsyncReadExt;
    let mut child = child?;
    let mut stdout = Vec::new();
    let mut stderr = Vec::new();
    if let Some(pipe) = child.stdout.take() {
        // One byte past the cap, so "exactly at the limit" is still an
        // overflow rather than a document that happens to end there.
        pipe.take(MAX_EXEC_STDOUT as u64 + 1).read_to_end(&mut stdout).await?;
    }
    if let Some(pipe) = child.stderr.take() {
        pipe.take(64 * 1024).read_to_end(&mut stderr).await?;
    }
    if stdout.len() > MAX_EXEC_STDOUT {
        // Stop the child rather than letting it keep writing into a pipe
        // nobody is draining.
        let _ = child.kill().await;
        return Err(std::io::Error::other(format!(
            "exec command produced more than {} bytes of stdout",
            MAX_EXEC_STDOUT
        )));
    }
    let status = child.wait().await?;
    Ok(std::process::Output { status, stdout, stderr })
}

impl ExecSource {
    /// Construct a new exec source and start its supervised polling
    /// operator. The operator holds only a `Weak<Self>` so the source can
    /// be dropped when the last subscriber goes away.
    pub fn for_context(config: ExecSourceConfig) -> Arc<Self> {
        let rid = super::types::LocalResourceKind::Custom(config.name.clone()).to_resource_id();
        let headers = config.headers.clone();
        let empty = k9rs_proto::protocol::TableBaseline {
            headers,
            rows: Vec::new(),
        };
        let (tx, rx) = watch::channel(empty);
        let interval = Duration::from_secs(config.poll_interval_secs.get());
        let arc = Arc::new_cyclic(|_weak: &Weak<Self>| Self {
            rid,
            config,
            entries: Mutex::new(Vec::new()),
            tx,
            _keep_rx: rx,
            grace: crate::grace::GraceGate::new(crate::grace::watcher_grace()),
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
            // With stderr: it is the only thing that explains the exit, and
            // it used to go to /dev/null.
            let why = String::from_utf8_lossy(&output.stderr);
            let why = why.trim();
            tracing::warn!(
                "exec source: `{}` exited with {}{}",
                self.config.command,
                output.status,
                if why.is_empty() { String::new() } else { format!(": {why}") },
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
        let _ = self.tx.send(k9rs_proto::protocol::TableBaseline {
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
        let Some((command, args, interval)) = self.weak.upgrade().map(|t| {
            (
                t.config.command.clone(),
                t.config.args.clone(),
                Duration::from_secs(t.config.poll_interval_secs.get()),
            )
        }) else {
            return OperatorExit::Gone;
        };
        // `kill_on_drop(true)`: if this operator is aborted mid-run
        // (source teardown, context death), the child is SIGKILLed rather
        // than orphaned to run to completion (or forever if wedged) —
        // honoring the ContextLocals "in-flight attempt drops its
        // kill_on_drop child" contract. A hung command must also not stall
        // the poller forever, so cap it at the poll interval: a command
        // that can't finish within its own cadence is treated as a failed
        // poll (keep last-known rows, try again next tick).
        // Output is CAPPED, and stderr is kept.
        //
        // `Command::output()` buffers the whole of stdout with no bound:
        // `kubectl get -o json` on a big cluster, or a runaway script, was
        // held entire and then copied twice more (`String::from_utf8`, then
        // `serde_json::from_str`) inside a long-lived daemon shared by every
        // session on the context. The only limit was the timeout, which does
        // not bound memory — a command can emit fast.
        //
        // And stderr went to /dev/null, so a non-zero exit was reported as
        // bare `status` with nothing saying why. It is the one thing that
        // would have explained the failure.
        let child = tokio::process::Command::new(&command)
            .args(&args)
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .kill_on_drop(true)
            .spawn();
        let result = match tokio::time::timeout(interval, capped_output(child)).await {
            Ok(r) => r,
            Err(_) => Err(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                format!("exec command exceeded {}s poll interval", interval.as_secs()),
            )),
        };
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

    fn grace_gate(&self) -> Option<&crate::grace::GraceGate> {
        Some(&self.grace)
    }

    fn subscribe(&self) -> watch::Receiver<k9rs_proto::protocol::TableBaseline> {
        self.tx.subscribe()
    }

    fn delete(&self, _name: &str) -> Result<(), String> {
        // Read-only, exactly as `apply_yaml` below already says.
        //
        // This used to remove the entry and report success — and then
        // `ingest` replaced the whole entry vector on the next poll and the
        // row came back. The rows ARE the command's output; there is nothing
        // here to delete. Reporting a success the next tick undoes is worse
        // than refusing.
        Err("exec resources are read-only (rows come from the command's output)".to_string())
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

#[cfg(test)]
#[path = "../tests/local/exec_source.rs"]
mod tests;
