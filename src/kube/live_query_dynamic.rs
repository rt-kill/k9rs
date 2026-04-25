//! Dynamic CRD instance watcher.
//!
//! Watches arbitrary CRD instances using `DynamicObject` and builds
//! `ResourceRow` snapshots via JSONPath extraction from printer columns.

use std::collections::HashMap;

use futures::{StreamExt, TryStreamExt};
use kube::api::{ApiResource, DynamicObject, GroupVersionKind};
use kube::runtime::watcher::{self, Event as WatcherEvent};
use kube::{Api, Client};
use tokio::sync::watch;
use tracing::{debug, warn};

use crate::event::ResourceUpdate;
use crate::kube::cache::PrinterColumn;
use crate::kube::protocol::{ObjectKey, ResourceScope};
use crate::kube::resources::row::{CellValue, RowHealth};

use super::live_query::{INIT_FLUSH_INTERVAL_MS, INITIAL_BACKOFF_MS, MAX_BACKOFF_MS, MAX_ELAPSED_MS, WATCHER_PAGE_SIZE, WatcherSnapshot};

/// Extracts the ObjectKey from a DynamicObject.
fn dyn_obj_key(obj: &DynamicObject) -> ObjectKey {
    let meta = &obj.metadata;
    ObjectKey::new(
        meta.namespace.clone().unwrap_or_default(),
        meta.name.clone().unwrap_or_default(),
    )
}

/// Watcher loop for dynamic CRD instances (DynamicObject -> ResourceRow).
pub(crate) async fn run_dynamic_live_watcher(
    client: Client,
    ns: crate::kube::protocol::Namespace,
    snapshot_tx: watch::Sender<WatcherSnapshot>,
    gvk: GroupVersionKind,
    plural: String,
    scope: ResourceScope,
    printer_columns: Vec<PrinterColumn>,
) {
    let ar = if plural.is_empty() {
        ApiResource::from_gvk(&gvk)
    } else {
        ApiResource::from_gvk_with_plural(&gvk, &plural)
    };
    // Delegate the scope routing to the centralized helper. Previously
    // this function re-inlined the `Api::all_with`/`Api::namespaced_with`
    // branches, one of the four copies task #102 was supposed to collapse.
    let api: Api<DynamicObject> = crate::kube::describe::dynamic_api_for(&client, &ar, scope, &ns);

    // Build a ResourceId for the dynamic type so Rows updates carry identity.
    // Dynamic watchers always serve runtime-discovered CRDs, never built-ins.
    let resource_id = crate::kube::protocol::ResourceId::crd(
        gvk.group.clone(), gvk.version.clone(), gvk.kind.clone(), plural.clone(), scope,
    );

    let watcher_config = watcher::Config::default()
        .page_size(WATCHER_PAGE_SIZE)
        .any_semantic();      // serve from API server cache (resourceVersion=0), much faster
    let mut stream = watcher::watcher(api, watcher_config).boxed();

    let mut store: HashMap<ObjectKey, DynamicObject> = HashMap::new();
    let mut backoff_ms: u64 = INITIAL_BACKOFF_MS;
    let mut backoff_start = std::time::Instant::now();
    let mut init_dirty = false;
    let mut steady_dirty = false;
    let mut had_success = false;

    let mut flush_timer = tokio::time::interval(std::time::Duration::from_millis(INIT_FLUSH_INTERVAL_MS));
    flush_timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    // Reason we exit the loop — same pattern as run_typed_watcher.
    let mut exit_reason: Option<String> = None;

    loop {
        tokio::select! {
            event_result = stream.try_next() => {
                match event_result {
                    Ok(Some(event)) => {
                        match event {
                            WatcherEvent::Init => {
                                store.clear();
                                backoff_start = std::time::Instant::now();
                            }
                            WatcherEvent::InitApply(obj) => {
                                let key = dyn_obj_key(&obj);
                                store.insert(key, obj);
                                init_dirty = true;
                            }
                            WatcherEvent::InitDone => {
                                had_success = true;
                                backoff_ms = INITIAL_BACKOFF_MS;
                                backoff_start = std::time::Instant::now();
                                debug!("live_query dynamic: initial list complete, {} items", store.len());
                                let snap = build_dynamic_snapshot(&store, &printer_columns, scope, &plural);
                                let _ = snapshot_tx.send(WatcherSnapshot::Live(ResourceUpdate::Rows {
                                    resource: resource_id.clone(),
                                    headers: snap.headers,
                                    rows: snap.rows,
                                }));
                            }
                            WatcherEvent::Apply(obj) => {
                                had_success = true;
                                backoff_ms = INITIAL_BACKOFF_MS;
                                backoff_start = std::time::Instant::now();
                                let key = dyn_obj_key(&obj);
                                store.insert(key, obj);
                                steady_dirty = true;
                            }
                            WatcherEvent::Delete(obj) => {
                                had_success = true;
                                backoff_ms = INITIAL_BACKOFF_MS;
                                backoff_start = std::time::Instant::now();
                                let key = dyn_obj_key(&obj);
                                store.remove(&key);
                                steady_dirty = true;
                            }
                        }
                    }
                    Ok(None) => {
                        debug!("live_query dynamic: stream ended");
                        break;
                    }
                    Err(e) => {
                        if !had_success {
                            warn!("live_query dynamic: initial load failed: {}", e);
                            exit_reason = Some(format!("{}", e));
                            break;
                        }
                        if backoff_start.elapsed().as_millis() as u64 > MAX_ELAPSED_MS {
                            warn!("live_query dynamic: watcher failed for over 2 minutes, giving up: {}", e);
                            exit_reason = Some(format!("{}", e));
                            break;
                        }
                        warn!("live_query dynamic: watcher error: {}, retrying in {}ms", e, backoff_ms);
                        tokio::time::sleep(std::time::Duration::from_millis(backoff_ms)).await;
                        backoff_ms = (backoff_ms * 2).min(MAX_BACKOFF_MS);
                    }
                }
            }
            _ = flush_timer.tick() => {
                if init_dirty && !store.is_empty() && store.len() <= super::live_query::INIT_FLUSH_ROW_LIMIT {
                    init_dirty = false;
                    let snap = build_dynamic_snapshot(&store, &printer_columns, scope, &plural);
                    let _ = snapshot_tx.send(WatcherSnapshot::Live(ResourceUpdate::Rows {
                        resource: resource_id.clone(),
                        headers: snap.headers,
                        rows: snap.rows,
                    }));
                }
                if steady_dirty {
                    steady_dirty = false;
                    let snap = build_dynamic_snapshot(&store, &printer_columns, scope, &plural);
                    let _ = snapshot_tx.send(WatcherSnapshot::Live(ResourceUpdate::Rows {
                        resource: resource_id.clone(),
                        headers: snap.headers,
                        rows: snap.rows,
                    }));
                }
            }
        }
    }

    // Terminal state — same pattern as run_typed_watcher.
    let reason = exit_reason.unwrap_or_else(|| "watcher stream ended".to_string());
    let _ = snapshot_tx.send(WatcherSnapshot::Dead(reason));
}

/// Table data built from a dynamic-watcher store: ordered column headers
/// and the sorted rows below them. The watcher feeds both into the
/// `ResourceUpdate::Rows` frame on every publish — they're always produced
/// together and always consumed together, so they ride through the codebase
/// as one value rather than an anonymous `(Vec<String>, Vec<ResourceRow>)`.
pub(crate) struct DynamicSnapshot {
    pub headers: Vec<String>,
    pub rows: Vec<crate::kube::resources::row::ResourceRow>,
}

/// Build a sorted snapshot of [`DynamicSnapshot`] from the dynamic store.
fn build_dynamic_snapshot(
    store: &HashMap<ObjectKey, DynamicObject>,
    printer_columns: &[PrinterColumn],
    scope: ResourceScope,
    plural: &str,
) -> DynamicSnapshot {
    // Use the authoritative scope from API discovery, not guessed from data.
    let is_namespaced = scope == ResourceScope::Namespaced;

    let mut all_columns: Vec<PrinterColumn> = Vec::new();
    if is_namespaced {
        all_columns.push(PrinterColumn {
            name: "NAMESPACE".into(),
            json_path: ".metadata.namespace".into(),
            column_type: crate::kube::cache::PrinterColumnType::String,
        });
    }
    all_columns.push(PrinterColumn {
        name: "NAME".into(),
        json_path: ".metadata.name".into(),
        column_type: crate::kube::cache::PrinterColumnType::String,
    });
    for pc in printer_columns {
        let upper = pc.name.to_uppercase();
        if upper == "NAME" || upper == "NAMESPACE" || upper == "AGE" { continue; }
        all_columns.push(pc.clone());
    }
    // Append user-defined overlay columns, skipping any that duplicate
    // an existing column name (case-insensitive).
    if let Some(overlay) = crate::kube::overlay::overlay_for(plural) {
        for oc in &overlay.columns {
            let upper = oc.header.to_uppercase();
            let already_exists = all_columns.iter().any(|c| c.name.to_uppercase() == upper);
            if already_exists { continue; }
            all_columns.push(PrinterColumn {
                name: oc.header.clone(),
                json_path: oc.jsonpath.clone(),
                column_type: crate::kube::cache::PrinterColumnType::String,
            });
        }
    }
    all_columns.push(PrinterColumn {
        name: "AGE".into(),
        json_path: ".metadata.creationTimestamp".into(),
        column_type: crate::kube::cache::PrinterColumnType::Date,
    });

    let headers: Vec<String> = all_columns.iter().map(|c| c.name.clone()).collect();

    let mut items: Vec<crate::kube::resources::row::ResourceRow> = store
        .values()
        .filter_map(|obj| {
            let meta = &obj.metadata;
            let namespace = meta.namespace.clone().unwrap_or_default();
            let name = meta.name.clone().unwrap_or_default();

            // Every column goes through JSONPath — no special cases. If the
            // object can't even serialize to a `Value` tree (shouldn't happen
            // for a well-formed `DynamicObject`, but would silently produce a
            // row full of empty cells under the old `unwrap_or(Null)` shape),
            // warn and drop the row instead of shipping an all-null ghost.
            let json_val = match serde_json::to_value(obj) {
                Ok(v) => v,
                Err(e) => {
                    warn!(
                        "live_query dynamic: failed to serialize {}/{} for JSONPath, dropping: {}",
                        namespace, name, e,
                    );
                    return None;
                }
            };
            let mut cells = Vec::with_capacity(all_columns.len());
            for col in &all_columns {
                let raw = resolve_json_path(&json_val, &col.json_path);
                let cell = if col.column_type.is_date() {
                    // Date columns become Age cells with epoch seconds.
                    if let Ok(ts) = chrono::DateTime::parse_from_rfc3339(&raw) {
                        CellValue::Age(Some(ts.with_timezone(&chrono::Utc).timestamp()))
                    } else {
                        CellValue::Text(raw)
                    }
                } else {
                    CellValue::Text(raw)
                };
                cells.push(cell);
            }            Some(crate::kube::resources::row::ResourceRow {
                name,
                namespace: Some(namespace),
                containers: Vec::new(),
                owner_refs: Vec::new(),
                pf_ports: Vec::new(),
                node: None,
                health: RowHealth::Normal,
                crd_info: None,
                drill_target: None,
                cells,
                ..Default::default()
            })
        })
        .collect();
    items.sort_by(|a, b| (&a.namespace, &a.name).cmp(&(&b.namespace, &b.name)));

    // Apply overlay coloring rules (Phase 1).
    if let Some(overlay) = crate::kube::overlay::overlay_for(plural) {
        if !overlay.coloring.is_empty() {
            for row in &mut items {
                crate::kube::overlay::evaluate_coloring(row, &headers, &overlay.coloring);
            }
        }
    }

    DynamicSnapshot { headers, rows: items }
}

/// Walk a JSONPath into a `serde_json::Value` tree.
///
/// Supports the forms K8s CRDs actually put in `additionalPrinterColumns`:
///
/// - Plain dot paths — `.spec.foo.bar`, `.status.phase`
/// - Array element filter — `.status.conditions[?(@.type=='Ready')].status`
///   (walks to the array at the path prefix, finds the first element whose
///   `type` field equals `'Ready'`, then continues the path on that element).
///   Both `==` and `!=` operators are accepted. String literals may be
///   wrapped in `'…'` or `"…"`.
///
/// This covers the condition-filter pattern used by cert-manager, ArgoCD,
/// Karpenter, Flux, and most operator CRDs. Anything more exotic —
/// conjunctions (`&&`, `||`), numeric comparisons, wildcards, array
/// slicing — still returns the empty string. Extend the filter parser
/// below as real CRDs force it; don't reach for a full JSONPath crate
/// unless the shape of the problem changes.
///
/// `serde_json::Value` is used intentionally here: CRDs are discovered at
/// runtime and have no compile-time schema, so the daemon holds a
/// `DynamicObject` that must be walked untyped. The value is local to
/// this function and immediately collapsed to a cell string — no `Value`
/// propagates into long-lived state.
fn resolve_json_path(obj: &serde_json::Value, path: &str) -> String {
    let mut current: Option<&serde_json::Value> = Some(obj);
    let mut remaining = path.trim_start_matches('.');

    while !remaining.is_empty() {
        let Some(val) = current else { return String::new(); };

        if let Some(filter_start) = remaining.find("[?(") {
            // Walk the dot path up to the filter.
            let (before, rest) = remaining.split_at(filter_start);
            let after_dot_path = walk_dot_path(val, before.trim_matches('.'));

            // Extract `<expr>)]<rest-of-path>`.
            let rest = &rest["[?(".len()..];
            let Some(filter_end) = rest.find(")]") else { return String::new(); };
            let filter_expr = &rest[..filter_end];
            remaining = rest[filter_end + ")]".len()..].trim_start_matches('.');

            // Apply the filter to the array at `after_dot_path`. `None`
            // from either step collapses the whole lookup to empty.
            current = after_dot_path.and_then(|v| apply_filter(v, filter_expr));
        } else {
            current = walk_dot_path(val, remaining);
            break;
        }
    }

    match current {
        Some(serde_json::Value::String(s)) => s.clone(),
        Some(serde_json::Value::Number(n)) => n.to_string(),
        Some(serde_json::Value::Bool(b)) => b.to_string(),
        Some(serde_json::Value::Null) | None => String::new(),
        Some(other) => other.to_string(),
    }
}

/// Walk a plain dot path on a `Value`. Empty path returns the input.
/// Missing segments collapse to `None`.
fn walk_dot_path<'a>(val: &'a serde_json::Value, path: &str) -> Option<&'a serde_json::Value> {
    let path = path.trim_matches('.');
    if path.is_empty() { return Some(val); }
    let mut current = val;
    for part in path.split('.') {
        current = current.get(part)?;
    }
    Some(current)
}

/// Apply a single `[?(@.<key><op><literal>)]` filter to an array.
/// Returns the first matching element, or `None` if the input isn't an
/// array, the expression doesn't match the supported shape, or no
/// element passes the predicate. Supported operators: `==`, `!=`.
fn apply_filter<'a>(val: &'a serde_json::Value, expr: &str) -> Option<&'a serde_json::Value> {
    let arr = val.as_array()?;
    let expr = expr.trim().strip_prefix("@.")?;

    // Find op. Order matters: `!=` must be checked before `=` to avoid
    // interpreting `!=` as `=` with a leading `!`.
    let (key, op_len, negate) = if let Some(p) = expr.find("!=") {
        (&expr[..p], 2, true)
    } else if let Some(p) = expr.find("==") {
        (&expr[..p], 2, false)
    } else {
        return None;
    };
    let key = key.trim();
    let literal = expr[key.len() + op_len..]
        .trim()
        .trim_matches(|c| c == '\'' || c == '"');

    arr.iter().find(|item| {
        let field_matches = match item.get(key) {
            Some(serde_json::Value::String(s)) => s == literal,
            Some(serde_json::Value::Bool(b)) => b.to_string() == literal,
            Some(serde_json::Value::Number(n)) => n.to_string() == literal,
            _ => false,
        };
        field_matches ^ negate
    })
}

#[cfg(test)]
mod json_path_tests {
    use super::resolve_json_path;
    use serde_json::json;

    #[test]
    fn plain_dot_path() {
        let obj = json!({ "spec": { "phase": "Running" } });
        assert_eq!(resolve_json_path(&obj, ".spec.phase"), "Running");
    }

    #[test]
    fn missing_segment_returns_empty() {
        let obj = json!({ "spec": { "phase": "Running" } });
        assert_eq!(resolve_json_path(&obj, ".spec.does.not.exist"), "");
    }

    #[test]
    fn number_and_bool_stringify() {
        let obj = json!({ "spec": { "replicas": 3, "paused": true } });
        assert_eq!(resolve_json_path(&obj, ".spec.replicas"), "3");
        assert_eq!(resolve_json_path(&obj, ".spec.paused"), "true");
    }

    #[test]
    fn condition_filter_eq_match() {
        // cert-manager Certificate shape
        let obj = json!({
            "status": {
                "conditions": [
                    { "type": "Issuing", "status": "False" },
                    { "type": "Ready",   "status": "True"  },
                ]
            }
        });
        assert_eq!(
            resolve_json_path(&obj, ".status.conditions[?(@.type=='Ready')].status"),
            "True"
        );
    }

    #[test]
    fn condition_filter_ne() {
        let obj = json!({
            "status": {
                "conditions": [
                    { "type": "Ready", "status": "True" },
                    { "type": "Degraded", "status": "False" },
                ]
            }
        });
        // First element whose type is NOT "Ready"
        assert_eq!(
            resolve_json_path(&obj, ".status.conditions[?(@.type!='Ready')].status"),
            "False"
        );
    }

    #[test]
    fn filter_accepts_double_quoted_literal() {
        let obj = json!({
            "status": { "conditions": [{ "type": "Ready", "status": "True" }] }
        });
        assert_eq!(
            resolve_json_path(&obj, r#".status.conditions[?(@.type=="Ready")].status"#),
            "True"
        );
    }

    #[test]
    fn filter_with_no_match_returns_empty() {
        let obj = json!({
            "status": { "conditions": [{ "type": "Issuing", "status": "False" }] }
        });
        assert_eq!(
            resolve_json_path(&obj, ".status.conditions[?(@.type=='Ready')].status"),
            ""
        );
    }

    #[test]
    fn unsupported_expression_returns_empty() {
        let obj = json!({
            "status": { "conditions": [{ "type": "Ready", "status": "True" }] }
        });
        // Conjunctions are not supported — collapse to empty rather than
        // returning a silently-wrong field.
        assert_eq!(
            resolve_json_path(&obj, ".status.conditions[?(@.type=='Ready' && @.status=='True')].status"),
            ""
        );
    }

    #[test]
    fn filter_on_non_array_returns_empty() {
        let obj = json!({ "status": { "conditions": { "type": "Ready" } } });
        assert_eq!(
            resolve_json_path(&obj, ".status.conditions[?(@.type=='Ready')].status"),
            ""
        );
    }
}
