//! Dynamic CRD instance watcher.
//!
//! Watches arbitrary CRD instances using `DynamicObject` and builds
//! `ResourceRow` snapshots via JSONPath extraction from printer columns.

use std::collections::HashMap;

use futures::{StreamExt, TryStreamExt};
use kube::api::{ApiResource, DynamicObject, GroupVersionKind};
use kube::runtime::watcher::{self, Event as WatcherEvent};
use kube::{Api, Client};
use tokio::sync::{mpsc, watch};
use tracing::{debug, warn};

use crate::event::ResourceUpdate;
use crate::kube::cache::PrinterColumn;
use crate::kube::protocol::{ObjectKey, ResourceScope};

use super::live_query::{INIT_FLUSH_INTERVAL_MS, INITIAL_BACKOFF_MS, MAX_BACKOFF_MS, MAX_ELAPSED_MS, WATCHER_PAGE_SIZE};

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
    snapshot_tx: watch::Sender<Option<ResourceUpdate>>,
    gvk: GroupVersionKind,
    plural: String,
    scope: ResourceScope,
    printer_columns: Vec<PrinterColumn>,
    last_error: std::sync::Arc<std::sync::Mutex<Option<String>>>,
) {
    let ar = if plural.is_empty() {
        ApiResource::from_gvk(&gvk)
    } else {
        ApiResource::from_gvk_with_plural(&gvk, &plural)
    };
    let api: Api<DynamicObject> = if scope == ResourceScope::Namespaced {
        match &ns {
            crate::kube::protocol::Namespace::All => Api::all_with(client, &ar),
            crate::kube::protocol::Namespace::Named(name) => Api::namespaced_with(client, name, &ar),
        }
    } else {
        Api::all_with(client, &ar)
    };

    // Build a ResourceId for the dynamic type so Rows updates carry identity.
    let resource_id = crate::kube::protocol::ResourceId::new(
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
    let mut had_success = false;

    let (snap_tx, mut snap_rx) = mpsc::channel::<()>(2);

    let mut init_flush = tokio::time::interval(std::time::Duration::from_millis(INIT_FLUSH_INTERVAL_MS));
    init_flush.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

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
                                let _ = snap_tx.try_send(());
                            }
                            WatcherEvent::Apply(obj) => {
                                had_success = true;
                                backoff_ms = INITIAL_BACKOFF_MS;
                                backoff_start = std::time::Instant::now();
                                let key = dyn_obj_key(&obj);
                                store.insert(key, obj);
                                let _ = snap_tx.try_send(());
                            }
                            WatcherEvent::Delete(obj) => {
                                had_success = true;
                                backoff_ms = INITIAL_BACKOFF_MS;
                                backoff_start = std::time::Instant::now();
                                let key = dyn_obj_key(&obj);
                                store.remove(&key);
                                let _ = snap_tx.try_send(());
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
                            *last_error.lock().unwrap() = Some(format!("{}", e));
                            break;
                        }
                        if backoff_start.elapsed().as_millis() as u64 > MAX_ELAPSED_MS {
                            warn!("live_query dynamic: watcher failed for over 2 minutes, giving up: {}", e);
                            *last_error.lock().unwrap() = Some(format!("{}", e));
                            break;
                        }
                        warn!("live_query dynamic: watcher error: {}, retrying in {}ms", e, backoff_ms);
                        tokio::time::sleep(std::time::Duration::from_millis(backoff_ms)).await;
                        backoff_ms = (backoff_ms * 2).min(MAX_BACKOFF_MS);
                    }
                }
            }
            _ = snap_rx.recv() => {
                let (headers, rows) = build_dynamic_snapshot(&store, &printer_columns, scope);
                let _ = snapshot_tx.send(Some(ResourceUpdate::Rows {
                    resource: resource_id.clone(),
                    headers,
                    rows,
                }));
            }
            _ = init_flush.tick() => {
                if init_dirty && !store.is_empty() {
                    init_dirty = false;
                    let (headers, rows) = build_dynamic_snapshot(&store, &printer_columns, scope);
                    let _ = snapshot_tx.send(Some(ResourceUpdate::Rows {
                        resource: resource_id.clone(),
                        headers,
                        rows,
                    }));
                }
            }
        }
    }

    // Signal that the watcher has died so the bridge detects it.
    let _ = snapshot_tx.send(None);
}

/// Build a sorted snapshot of ResourceRow from the dynamic store.
/// Returns (headers, rows) — headers are used by the Rows update to populate the descriptor.
fn build_dynamic_snapshot(
    store: &HashMap<ObjectKey, DynamicObject>,
    printer_columns: &[PrinterColumn],
    scope: ResourceScope,
) -> (Vec<String>, Vec<crate::kube::resources::row::ResourceRow>) {
    // Use the authoritative scope from API discovery, not guessed from data.
    let is_namespaced = scope == ResourceScope::Namespaced;

    let mut all_columns: Vec<PrinterColumn> = Vec::new();
    if is_namespaced {
        all_columns.push(PrinterColumn {
            name: "NAMESPACE".into(),
            json_path: ".metadata.namespace".into(),
            column_type: "string".into(),
        });
    }
    all_columns.push(PrinterColumn {
        name: "NAME".into(),
        json_path: ".metadata.name".into(),
        column_type: "string".into(),
    });
    for pc in printer_columns {
        let upper = pc.name.to_uppercase();
        if upper == "NAME" || upper == "NAMESPACE" || upper == "AGE" { continue; }
        all_columns.push(pc.clone());
    }
    all_columns.push(PrinterColumn {
        name: "AGE".into(),
        json_path: ".metadata.creationTimestamp".into(),
        column_type: "date".into(),
    });

    let headers: Vec<String> = all_columns.iter().map(|c| c.name.clone()).collect();

    let mut items: Vec<crate::kube::resources::row::ResourceRow> = store
        .values()
        .map(|obj| {
            let meta = &obj.metadata;
            let namespace = meta.namespace.clone().unwrap_or_default();
            let name = meta.name.clone().unwrap_or_default();

            // Every column goes through JSONPath — no special cases.
            let json_val = serde_json::to_value(obj).unwrap_or(serde_json::Value::Null);
            let mut cells = Vec::with_capacity(all_columns.len());
            for col in &all_columns {
                let val = if col.column_type == "date" {
                    // Format dates as age strings.
                    let raw = resolve_json_path(&json_val, &col.json_path);
                    if let Ok(ts) = chrono::DateTime::parse_from_rfc3339(&raw) {
                        crate::util::format_age(Some(ts.with_timezone(&chrono::Utc)))
                    } else {
                        raw
                    }
                } else {
                    resolve_json_path(&json_val, &col.json_path)
                };
                cells.push(val);
            }
            crate::kube::resources::row::ResourceRow {
                cells,
                name,
                namespace: Some(namespace),
                containers: Vec::new(),
                owner_refs: Vec::new(),
                pf_ports: Vec::new(),
                node: None,
                crd_info: None,
                drill_target: None,
            }
        })
        .collect();
    items.sort_by(|a, b| (&a.namespace, &a.name).cmp(&(&b.namespace, &b.name)));
    (headers, items)
}

/// Walk a simple dot-separated JSONPath into a `serde_json::Value` tree.
///
/// Handles the common K8s printer column paths like `.spec.foo.bar` and
/// `.status.phase`. Does NOT handle complex filter expressions like
/// `.status.conditions[?(@.type=='Ready')].status` — those return empty
/// for now and can be extended later.
fn resolve_json_path(obj: &serde_json::Value, path: &str) -> String {
    let path = path.trim_start_matches('.');
    let mut current = obj;
    for part in path.split('.') {
        match current.get(part) {
            Some(v) => current = v,
            None => return String::new(),
        }
    }
    match current {
        serde_json::Value::String(s) => s.clone(),
        serde_json::Value::Number(n) => n.to_string(),
        serde_json::Value::Bool(b) => b.to_string(),
        serde_json::Value::Null => String::new(),
        other => other.to_string(),
    }
}
