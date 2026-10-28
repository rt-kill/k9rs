//! Dynamic CRD watcher — same delta machinery as the typed path.
//!
//! Objects convert to [`ResourceRow`] AT INGEST (the column plan — printer
//! columns + overlay columns + NAMESPACE/NAME/AGE — is computed once at
//! spawn), so the store holds rows, not retained `DynamicObject`s. The old
//! per-flush `build_dynamic_snapshot` (which re-serialized EVERY object
//! every 200ms) and the `managedFields` strip (nothing is retained to
//! strip) died with the delta rework. The loop itself is
//! `live_query::run_typed_watcher`, generic
//! over the dynamic type — one skeleton, two ingests.

use kube::api::{ApiResource, DynamicObject, GroupVersionKind};
use kube::Client;
use tracing::warn;

use crate::cache::{DiscoveryCache, PrinterColumn};
use k9rs_proto::protocol::{ContextId, ResourceScope};
use k9rs_proto::row::{CellValue, ResourceRow, RowHealth};

/// Watcher for dynamic CRD instances. Thin wrapper: RESOLVE the column
/// plan (the watcher owns its whole definition — identity AND projection
/// — from birth; see [`resolve_printer_columns`]), build the Api, then
/// run the shared typed loop with a JSONPath-driven converter.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn run_dynamic_live_watcher(
    client: Client,
    ns: k9rs_proto::protocol::Namespace,
    delta_tx: tokio::sync::broadcast::Sender<std::sync::Arc<super::live_query::WatcherMsg>>,
    ask_rx: tokio::sync::mpsc::Receiver<super::live_query::BaselineAsk>,
    gvk: GroupVersionKind,
    plural: String,
    scope: ResourceScope,
    discovery: std::sync::Arc<DiscoveryCache>,
    context: ContextId,
    streaming_lists: bool,
) {
    // First act: resolve the projection recipe from its authoritative
    // source. Every watcher RUN re-resolves (death-retry, Ctrl-R force,
    // cache eviction), so a failed or raced resolution can never stick.
    // Cosmetic, and never fatal: a kind whose presentation metadata cannot
    // be read is still a kind worth watching. This used to send
    // `WatcherMsg::Dead` on any error but 403/404, on the stated grounds
    // that the bridge would respawn — see `resolve_printer_columns`, which
    // the bridge does not.
    let printer_columns =
        resolve_printer_columns(&client, &discovery, &context, &gvk.group, &plural).await;

    let ar = if plural.is_empty() {
        ApiResource::from_gvk(&gvk)
    } else {
        ApiResource::from_gvk_with_plural(&gvk, &plural)
    };
    let api: kube::Api<DynamicObject> =
        crate::describe::dynamic_api_for(&client, &ar, scope, &ns);

    let resource_id = k9rs_proto::protocol::ResourceId::crd(
        gvk.group.clone(), gvk.version.clone(), gvk.kind.clone(), plural.clone(), scope,
    );

    let columns = column_plan(&printer_columns, scope, &plural);
    let headers: Vec<String> = columns.iter().map(|c| c.name.clone()).collect();

    let convert = move |obj: DynamicObject| convert_dynamic(obj, &columns);

    // CRD watchers carry no server-side filter today (parity).
    let stream = super::live_query::watch_stream(api, &None, streaming_lists);
    super::live_query::run_typed_watcher(
        stream,
        delta_tx,
        ask_rx,
        convert,
        resource_id,
        headers,
        &ns,
    )
    .await;
}

/// Resolve a dynamic kind's projection recipe (`additionalPrinterColumns`)
/// from its authoritative source: the CRD definition object. The
/// discovery cache ACCELERATES this (warm after the bulk poll) but never
/// substitutes for it — on a miss we await one GET of the definition and
/// share the result back into the cache. The old
/// `printer_columns_for(..).unwrap_or_default()` — which silently turned
/// a cold cache into a gutted column plan — is unrepresentable now: no
/// path constructs a plan from absence of knowledge.
///
/// Typed outcomes:
/// - `Ok(cols)` — knowledge (possibly legitimately empty: CRDs may
///   declare no printer columns);
/// - 404 → `Ok(empty)` — no CRD object exists (aggregated-API resource):
///   the metadata plan IS the correct plan, a fact rather than a fallback;
/// - 403 → `Ok(empty)` + warning — the recipe exists but is forbidden:
///   explicit, logged degradation;
/// - anything else → `empty` + warning. A failure to read a kind's
///   PRESENTATION metadata is not a reason to stop watching the kind.
///
/// That last arm used to be `Err`, and the caller turned it into
/// `WatcherMsg::Dead` "honestly, because the bridge's retry machinery
/// respawns (and re-resolves)". The bridge does not: it gates resubscribe
/// on `sent_any_data`, and this failure happens at watcher BIRTH, before
/// any data — so the daemon writes `StreamEvent::Error` and returns, which
/// its own comment names as the permanent case and a test pins. A transient
/// 500, a timeout or a connection reset while fetching a CRD definition
/// therefore killed the subscription for good. The columns are cosmetic;
/// the rows are the point.
async fn resolve_printer_columns(
    client: &Client,
    discovery: &DiscoveryCache,
    context: &ContextId,
    group: &str,
    plural: &str,
) -> Vec<PrinterColumn> {
    if let Some(cols) = discovery.printer_columns_for(context, group, plural) {
        return cols;
    }
    use k8s_openapi::apiextensions_apiserver::pkg::apis::apiextensions::v1::CustomResourceDefinition;
    let api: kube::Api<CustomResourceDefinition> = kube::Api::all(client.clone());
    let name = if group.is_empty() {
        plural.to_string()
    } else {
        format!("{plural}.{group}")
    };
    match api.get(&name).await {
        Ok(crd) => match crate::cache::cached_crd_from_k8s(&crd) {
            Some(cached) => {
                let cols = cached.printer_columns.clone();
                discovery.merge_crd(context.clone(), cached);
                cols
            }
            // A CRD object without a name can't exist server-side; treat
            // an unparseable one as declaring no columns.
            None => Vec::new(),
        },
        Err(kube::Error::Api(ae)) if ae.code == 404 => {
            // Not a CRD at all (aggregated API server resource) — there
            // is no recipe to have; metadata columns are the truth.
            tracing::debug!("{name}: no CRD object (aggregated API?) — metadata columns");
            Vec::new()
        }
        Err(kube::Error::Api(ae)) if ae.code == 403 => {
            warn!("{name}: CRD definition read forbidden — printer columns unavailable, showing metadata columns");
            Vec::new()
        }
        Err(e) => {
            // Was a terminal `Err`. A transient failure here costs the
            // pretty columns for this run; the next one re-resolves.
            warn!("{name}: could not resolve printer columns ({e}) — showing metadata columns");
            Vec::new()
        }
    }
}

/// The full ordered column plan for a dynamic resource: NAMESPACE (if
/// namespaced), NAME, the CRD printer columns, user overlay columns, AGE.
/// Computed once at watcher spawn — headers are immutable per watcher
/// instance (a printer-column change arrives via a new watcher, which
/// re-plans and re-baselines).
fn column_plan(
    printer_columns: &[PrinterColumn],
    scope: ResourceScope,
    plural: &str,
) -> Vec<PrinterColumn> {
    let is_namespaced = scope == ResourceScope::Namespaced;
    let mut all_columns: Vec<PrinterColumn> = Vec::new();
    if is_namespaced {
        all_columns.push(PrinterColumn {
            name: "NAMESPACE".into(),
            json_path: ".metadata.namespace".into(),
            column_type: crate::cache::PrinterColumnType::String,
        });
    }
    all_columns.push(PrinterColumn {
        name: "NAME".into(),
        json_path: ".metadata.name".into(),
        column_type: crate::cache::PrinterColumnType::String,
    });
    for pc in printer_columns {
        let upper = pc.name.to_uppercase();
        if upper == "NAME" || upper == "NAMESPACE" || upper == "AGE" { continue; }
        all_columns.push(pc.clone());
    }
    // Append user-defined overlay columns, skipping any that duplicate
    // an existing column name (case-insensitive).
    if let Some(overlay) = k9rs_proto::overlay::overlay_for(plural) {
        for oc in &overlay.columns {
            // Only columns with a jsonpath add data (CRDs). Columns without
            // jsonpath are level overrides for built-in resources — skip here.
            let Some(ref jp) = oc.jsonpath else { continue };
            let upper = oc.header.to_uppercase();
            let already_exists = all_columns.iter().any(|c| c.name.to_uppercase() == upper);
            if already_exists { continue; }
            // Say so, once, at ingest. The column is still added — the
            // overlay system warns and continues — but a `jsonpath:` this
            // resolver cannot honour used to produce an all-empty column
            // with nothing logged anywhere, which is indistinguishable from
            // a field that is simply unset.
            if let Some(why) = unsupported_json_path(jp) {
                warn!(
                    plural = %plural,
                    column = %oc.header,
                    jsonpath = %jp,
                    "overlay column will be empty: {why}",
                );
            }
            all_columns.push(PrinterColumn {
                name: oc.header.clone(),
                json_path: jp.clone(),
                column_type: crate::cache::PrinterColumnType::String,
            });
        }
    }
    all_columns.push(PrinterColumn {
        name: "AGE".into(),
        json_path: ".metadata.creationTimestamp".into(),
        column_type: crate::cache::PrinterColumnType::Date,
    });
    all_columns
}

/// Convert one `DynamicObject` to a row via the column plan. Every column
/// goes through JSONPath against the object's JSON tree — this IS the
/// external API boundary where `serde_json::Value` is acceptable. A value
/// that fails to serialize (shouldn't happen for a well-formed
/// `DynamicObject`) yields a named row with empty cells + a warning,
/// rather than silently vanishing.
fn convert_dynamic(mut obj: DynamicObject, columns: &[PrinterColumn]) -> ResourceRow {
    let namespace = obj.metadata.namespace.clone().unwrap_or_default();
    let name = obj.metadata.name.clone().unwrap_or_default();

    // `managedFields` is frequently 10–50 KB/object and no printer column
    // addresses it — drop it before serializing to the JSON tree the
    // JSONPaths walk, so a 10k-instance CRD relist doesn't build (and
    // discard) that much extra tree per object.
    obj.metadata.managed_fields = None;

    let cells: Vec<CellValue> = match serde_json::to_value(&obj) {
        Ok(json_val) => columns
            .iter()
            .map(|col| {
                let raw = resolve_json_path(&json_val, &col.json_path);
                if col.column_type.is_date() {
                    // Date columns become Age cells with epoch seconds.
                    if let Ok(ts) = chrono::DateTime::parse_from_rfc3339(&raw) {
                        CellValue::Age(Some(ts.with_timezone(&chrono::Utc).timestamp()))
                    } else {
                        CellValue::Text(raw)
                    }
                } else {
                    CellValue::Text(raw)
                }
            })
            .collect(),
        Err(e) => {
            warn!(
                "live_query dynamic: failed to serialize {}/{} for JSONPath: {}",
                namespace, name, e,
            );
            Vec::new()
        }
    };

    ResourceRow {
        name,
        namespace: Some(namespace),
        health: RowHealth::Normal,
        cells,
        ..Default::default()
    }
}


/// Walk a JSONPath into a `serde_json::Value` tree.
///
/// Supports the forms K8s CRDs actually put in `additionalPrinterColumns`:
///
/// - Plain dot paths — `.spec.foo.bar`, `.status.phase`
/// - Escaped dots in keys — `.metadata.labels.karpenter\.sh/nodepool` resolves
///   the single key `karpenter.sh/nodepool` (label/annotation keys whose names
///   contain dots). A backslash escapes the following char and is itself
///   dropped, matching kubectl's `client-go/util/jsonpath` parser.
/// - Array element filter — `.status.conditions[?(@.type=='Ready')].status`
///   (walks to the array at the path prefix, finds the first element whose
///   `type` field equals `'Ready'`, then continues the path on that element).
///   Both `==` and `!=` operators are accepted. String literals may be
///   wrapped in `'…'` or `"…"`.
///
/// This covers the condition-filter and label-key patterns used by cert-manager,
/// ArgoCD, Karpenter, Flux, and most operator CRDs. NOT supported (these return
/// the empty string): bracket key access (`.metadata.labels['karpenter.sh/x']` —
/// the alternate form some CRDs emit), conjunctions (`&&`, `||`), numeric
/// comparisons, wildcards, array slicing. Extend the parser below as real CRDs
/// force it; don't reach for a full JSONPath crate unless the shape changes.
///
/// `serde_json::Value` is used intentionally here: CRDs are discovered at
/// runtime and have no compile-time schema, so the daemon holds a
/// `DynamicObject` that must be walked untyped. The value is local to
/// this function and immediately collapsed to a cell string — no `Value`
/// propagates into long-lived state.
/// Why this resolver cannot honour `path`, if it cannot.
///
/// SYNTAX only — an absent field is a legitimate empty cell, not an error.
/// The forms named here resolve to `""` for every object, for ever, and
/// nothing was logged at any level: the column header appeared, every cell
/// was blank, and there was no way to tell a typo from a field that is
/// genuinely unset. A user's overlay `jsonpath:` went in with no parse, no
/// validation and no diagnostic anywhere.
///
/// Warn-and-continue, like the rest of the overlay system: a column that
/// cannot resolve is still shown, because dropping it silently is the same
/// failure wearing a different hat.
pub(crate) fn unsupported_json_path(path: &str) -> Option<String> {
    if path.contains("['") || path.contains("[\"") {
        return Some(
            "bracket key access is not supported — escape the dots instead, \
             as in `.metadata.labels.app\\.kubernetes\\.io/name`"
                .to_string(),
        );
    }
    if path.contains("&&") || path.contains("||") {
        return Some("compound filter expressions (`&&`, `||`) are not supported".to_string());
    }
    if path.contains('*') {
        return Some("wildcards are not supported".to_string());
    }
    let opens = path.matches("[?(").count();
    if opens != path.matches(")]").count() {
        return Some("an unterminated `[?(...)]` filter".to_string());
    }
    // Inside a filter, only `@.key == 'literal'` / `!=` are understood.
    for seg in path.split("[?(").skip(1) {
        let expr = seg.split(")]").next().unwrap_or(seg).trim();
        if !expr.starts_with("@.") {
            return Some(format!("filter `{expr}` does not start with `@.`"));
        }
        if !expr.contains("==") && !expr.contains("!=") {
            return Some(format!(
                "filter `{expr}` uses an operator other than `==` or `!=`"
            ));
        }
    }
    None
}

pub(crate) fn resolve_json_path(obj: &serde_json::Value, path: &str) -> String {
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
    for part in split_escaped_dots(path) {
        current = current.get(part.as_str())?;
    }
    Some(current)
}

/// Split a dot path into key segments, treating `\.` as a literal dot *inside* a
/// key rather than a separator. CRD `additionalPrinterColumns` escape the dots
/// in label keys this way — e.g. Karpenter's
/// `.metadata.labels.karpenter\.sh/nodepool` must resolve the single key
/// `karpenter.sh/nodepool`, not three broken segments. Without this, every
/// label-based printer column (instance type, capacity type, zone, nodepool…)
/// silently renders empty.
fn split_escaped_dots(path: &str) -> Vec<String> {
    let mut segments = Vec::new();
    let mut cur = String::new();
    let mut chars = path.chars();
    while let Some(c) = chars.next() {
        match c {
            // A backslash escapes the next char and is itself dropped — matching
            // kubectl's `client-go/util/jsonpath` parser. So `\.` is a literal
            // dot *inside* a key (not a separator), and any other `\x` keeps `x`.
            // A trailing backslash is dropped.
            '\\' => {
                if let Some(next) = chars.next() {
                    cur.push(next);
                }
            }
            '.' => segments.push(std::mem::take(&mut cur)),
            other => cur.push(other),
        }
    }
    segments.push(cur);
    segments
}

/// Apply a single `[?(@.<key><op><literal>)]` filter to an array.
/// Returns the first matching element, or `None` if the input isn't an
/// array, the expression doesn't match the supported shape, or no
/// element passes the predicate. Supported operators: `==`, `!=`.
fn apply_filter<'a>(val: &'a serde_json::Value, expr: &str) -> Option<&'a serde_json::Value> {
    let arr = val.as_array()?;
    let expr = expr.trim().strip_prefix("@.")?;

    // Split ONCE, at the operator, so both halves are measured against the
    // same string. The key was re-trimmed and then its length used to index
    // back into the UNTRIMMED expression, so any whitespace before the
    // operator shifted the literal by that much: `@.type == 'Ready'` sliced
    // `"= 'Ready"` and matched nothing. Silently, and only for the spelling
    // with spaces — which is the one kubectl's own documentation uses, and
    // which a CRD's `additionalPrinterColumns` is free to write.
    //
    // Order matters: `!=` must be checked before `==` so that it is not read
    // as `=` with a leading `!`.
    let (raw_key, rest, negate) = if let Some((k, v)) = expr.split_once("!=") {
        (k, v, true)
    } else if let Some((k, v)) = expr.split_once("==") {
        (k, v, false)
    } else {
        return None;
    };
    let key = raw_key.trim();
    let literal = rest.trim().trim_matches(|c| c == '\'' || c == '"');

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
#[path = "tests/live_query_dynamic.rs"]
mod json_path_tests_mirror;
