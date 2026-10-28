//! Describe / YAML / discovery helpers for the daemon.
//!
//! Two entry points:
//! - [`fetch_describe`] / [`fetch_yaml`] — operate on a typed [`ObjectRef`]
//!   the daemon already holds. Built-ins skip discovery entirely; CRDs use
//!   their stored GVR if populated, falling back to discovery only when the
//!   client sent us an incomplete shape (`:nodeclaims` etc.). No string-
//!   keyed dispatch.
//! - [`api_resource_for`] — typed entrypoint that takes a [`ResourceId`]
//!   and returns the kube `ApiResource` + scope. Used by the Apply /
//!   Delete / YAML paths and by the subscribe path's CRD branch (it
//!   handles the incomplete-CrdRef → discovery fallback internally).
//! - [`built_in_api_resource`] — its synchronous half, for callers that
//!   already hold a `BuiltInKind` and so need no discovery. The daemon had
//!   a private copy of these four lines; this is the one.

use kube::api::{Api, ApiResource, DynamicObject, GroupVersionKind};
use kube::discovery::{self, Scope};

use k9rs_proto::protocol::{
    DescribeLine, DescribeLineKind, Namespace, ObjectRef, ResourceId, ResourceScope,
};
use crate::session_env::SessionEnv;

/// Build an `Api<DynamicObject>` for the given ApiResource + scope +
/// namespace. Centralizes the four-way `match scope { Cluster, Namespaced
/// (with empty special case), Namespaced }` that every mutating handler
/// otherwise duplicates. Takes a typed `Namespace` rather than a raw
/// string so the empty-vs-None semantics are encoded once at the boundary.
pub fn dynamic_api_for(
    client: &kube::Client,
    ar: &ApiResource,
    scope: ResourceScope,
    namespace: &Namespace,
) -> Api<DynamicObject> {
    match (scope, namespace.as_option()) {
        (ResourceScope::Cluster, _) | (ResourceScope::Namespaced, None) => {
            // Cluster-scoped resources always use all(). Namespaced resources
            // with Namespace::All (as_option() = None) also use all() to
            // list across all namespaces.
            Api::all_with(client.clone(), ar)
        }
        (ResourceScope::Namespaced, Some(ns)) => Api::namespaced_with(client.clone(), ns, ar),
    }
}

// ---------------------------------------------------------------------------
// Public API — typed `ObjectRef` entry points
// ---------------------------------------------------------------------------

/// Fetch a describe view for a resource via `kubectl describe`. We shell
/// out because kubectl's describe output (events, replica sets, rolling
/// update strategy, conditions) is comprehensive in a way that's tedious
/// to reproduce against the raw API. We try the API first (fast — reuses
/// the session's authenticated `kube::Client`) and fall back to kubectl
/// only on failure.
pub async fn fetch_describe(
    client: &kube::Client,
    target: &ObjectRef,
    context: &k9rs_proto::protocol::ContextName,
    session_env: &SessionEnv,
) -> Vec<DescribeLine> {
    match fetch_describe_via_api(client, target).await {
        Ok(lines) => lines,
        Err(_) => fetch_describe_via_kubectl(target, context, session_env).await,
    }
}

/// Classify one line of *opaque* text (kubectl describe output, a decoded
/// secret, a local-resource describe) into a [`DescribeLineKind`]. This is the
/// same best-effort heuristic the UI used to run at render time, lifted to the
/// producer side — for text whose structure isn't otherwise known, inference is
/// legitimate (nothing the producer knows is being discarded). Structured
/// producers like `format_describe` tag precisely instead of calling this.
pub fn classify_describe_line(text: &str) -> DescribeLineKind {
    let trimmed = text.trim();
    if trimmed.is_empty() {
        DescribeLineKind::Plain
    } else if trimmed.ends_with(':') && trimmed.len() > 1 {
        DescribeLineKind::Section
    } else if let Some(rel) = trimmed.find(':') {
        // `key_end` is the colon's byte index in the full (indented) line.
        let indent = text.len() - text.trim_start().len();
        DescribeLineKind::Field { key_end: indent + rel }
    } else {
        DescribeLineKind::Plain
    }
}

/// Split opaque multi-line text into classified [`DescribeLine`]s. Used by the
/// kubectl-fallback, secret-decode, and local-resource describe paths.
pub fn describe_lines_from_text(text: &str) -> Vec<DescribeLine> {
    text.lines().map(classified).collect()
}

/// Build one describe line by classifying its visible text.
///
/// Cleaning and classification are ONE step, not two: `key_end` is a byte
/// index into this string, so cleaning afterwards — which is what the
/// kubectl path used to do one layer up with `strip_ansi` — can slide the
/// index off its colon. Built together, the index and the string it indexes
/// cannot disagree. [`field_line`] now cleans first for the same reason;
/// this paragraph used to name it as a live exception.
///
/// The owning `into_owned` is free here: `DescribeLine.text` is a `String`,
/// so the `&str` callers were allocating anyway.
fn classified(text: &str) -> DescribeLine {
    let text = k9rs_util::text::visible_text(text).into_owned();
    let kind = classify_describe_line(&text);
    DescribeLine { text, kind }
}

/// Build a `key: value` field line, locating the structural colon the producer
/// just wrote (the label always precedes the value, so the first `:` is it).
///
/// Cleans FIRST, for the reason [`classified`] gives: `key_end` is a byte
/// index into the string it ships with, so cleaning afterwards can slide the
/// index off its colon. This path computed the index on raw text while its
/// sibling's doc described that order as the defect — and the text here is
/// label and annotation keys and values, i.e. arbitrary cluster-supplied
/// UTF-8 that may carry escapes. The renderer guards the index, so the
/// symptom was a mis-placed key/value split rather than a panic; the order
/// is still the thing that makes the index meaningful.
fn field_line(text: String) -> DescribeLine {
    let text = k9rs_util::text::visible_text(&text).into_owned();
    let key_end = text.find(':').unwrap_or(text.len());
    DescribeLine::field(text, key_end)
}

/// Fetch YAML for a resource. Tries the kube API first (fast, no subprocess),
/// falls back to kubectl on any failure.
pub async fn fetch_yaml(
    client: &kube::Client,
    target: &ObjectRef,
    context: &k9rs_proto::protocol::ContextName,
    session_env: &SessionEnv,
) -> String {
    match fetch_yaml_via_api(client, target).await {
        Ok(yaml) => yaml,
        Err(_) => fetch_yaml_via_kubectl(target, context, session_env).await,
    }
}

// ---------------------------------------------------------------------------
// Typed API resource resolution
// ---------------------------------------------------------------------------

/// Build the kube `ApiResource` for a BUILT-IN kind, from the def's
/// `&'static Gvr` — no discovery roundtrip, no string allocations.
///
/// Synchronous, which is why it is worth naming separately from
/// [`api_resource_for`]: that one is `async` for the CRD-discovery fallback,
/// and callers that already know the kind should not have to be. It was
/// copied, byte for byte, into the daemon as a private
/// `built_in_api_resource` whose doc said "all mutating handlers go through
/// this so the typed `BuiltInKind` is the single source of truth" — it had
/// one caller, which had one caller, and the Apply/Delete/YAML paths all
/// came through here instead. One copy now, in the crate that owns the
/// mapping.
pub fn built_in_api_resource(
    kind: k9rs_proto::resource_def::BuiltInKind,
) -> (ApiResource, ResourceScope) {
    let g = *kind.def().gvr();
    let gvk = GroupVersionKind::gvk(g.group, g.version, g.kind);
    (ApiResource::from_gvk_with_plural(&gvk, g.plural), g.scope)
}

/// Build the kube `ApiResource` for any `ResourceId`. Built-ins resolve
/// through the registry's `&'static Gvr` with no allocation and no HTTP
/// call. CRDs use their stored GVR if populated; only the incomplete-shape
/// case (`group`/`version` empty) hits discovery.
///
/// Errors only on truly unknown resources or local rids (which have no K8s
/// API representation by definition).
pub async fn api_resource_for(
    client: &kube::Client,
    rid: &ResourceId,
) -> anyhow::Result<(ApiResource, ResourceScope)> {
    match rid {
        ResourceId::BuiltIn(kind) => Ok(built_in_api_resource(*kind)),
        ResourceId::Crd(crd_ref) => {
            let gvk = GroupVersionKind::gvk(&crd_ref.group, &crd_ref.version, &crd_ref.kind);
            Ok((ApiResource::from_gvk_with_plural(&gvk, &crd_ref.plural), crd_ref.scope))
        }
        ResourceId::CrdUnresolved(plural) => {
            // Unresolved CRD ref (e.g. user typed `:nodeclaims` without
            // group): fall back to discovery to fill in the GVR.
            resolve_via_discovery(client, plural).await
        }
        ResourceId::Local(_) => {
            anyhow::bail!("local resources have no K8s API resource descriptor")
        }
    }
}

async fn resolve_via_discovery(
    client: &kube::Client,
    resource: &str,
) -> anyhow::Result<(ApiResource, ResourceScope)> {
    use ResourceScope::{Cluster, Namespaced};

    let discovery = discovery::Discovery::new(client.clone()).run().await?;

    if resource.contains('.') {
        let parts: Vec<&str> = resource.splitn(2, '.').collect();
        let plural = parts[0];
        let group = parts.get(1).unwrap_or(&"");
        for api_group in discovery.groups() {
            for (ar, caps) in api_group.recommended_resources() {
                if ar.plural == plural && ar.group == *group {
                    let scope = if caps.scope == Scope::Cluster { Cluster } else { Namespaced };
                    return Ok((ar, scope));
                }
            }
        }
    } else {
        let lower = resource.to_lowercase();
        for api_group in discovery.groups() {
            for (ar, caps) in api_group.recommended_resources() {
                if ar.plural.to_lowercase() == lower
                    || ar.kind.to_lowercase() == lower
                    || format!("{}s", ar.kind.to_lowercase()) == lower
                {
                    let scope = if caps.scope == Scope::Cluster { Cluster } else { Namespaced };
                    return Ok((ar, scope));
                }
            }
        }
    }

    Err(anyhow::anyhow!("Resource not found: {}", resource))
}

// ---------------------------------------------------------------------------
// YAML via kube API (no subprocess)
// ---------------------------------------------------------------------------

async fn fetch_yaml_via_api(
    client: &kube::Client,
    target: &ObjectRef,
) -> anyhow::Result<String> {
    let (ar, scope) = api_resource_for(client, &target.resource).await?;
    let api = dynamic_api_for(client, &ar, scope, &target.namespace);
    let mut obj = api.get(&target.name).await?;
    // Strip server-managed fields that the API rejects on apply.
    // `managedFields` in particular causes "metadata.managedFields must be
    // nil" on any write-back. Users editing YAML shouldn't have to manually
    // delete these — the apply path sends the full object and the server
    // fills them back in.
    obj.metadata.managed_fields = None;
    let yaml = serde_yaml::to_string(&obj)?;
    Ok(yaml)
}

// ---------------------------------------------------------------------------
// Describe via kube API (no subprocess)
// ---------------------------------------------------------------------------

/// Fetch a resource and format key sections in a describe-like layout.
/// Not as rich as kubectl's describe (no events, no related resources), but
/// covers the core fields and is ~50ms instead of 1-5s (no subprocess, no
/// re-authentication).
async fn fetch_describe_via_api(
    client: &kube::Client,
    target: &ObjectRef,
) -> anyhow::Result<Vec<DescribeLine>> {
    let (ar, scope) = api_resource_for(client, &target.resource).await?;
    let api = dynamic_api_for(client, &ar, scope, &target.namespace);
    let obj = api.get(&target.name).await?;
    Ok(format_describe(&obj))
}

/// Format a DynamicObject into a human-readable describe view. Each line is
/// emitted with its role tagged: the producer knows what it is writing (a
/// header field, a section, a continuation, nested YAML), so it never has to be
/// re-inferred downstream. Top-level fields are tagged precisely; nested YAML
/// sub-content is classified (its inner structure isn't known here).
fn format_describe(obj: &kube::api::DynamicObject) -> Vec<DescribeLine> {
    let mut lines: Vec<DescribeLine> = Vec::new();
    let meta = &obj.metadata;
    let blank = || DescribeLine::plain(String::new());

    // Header
    let kind = obj.types.as_ref().map(|t| t.kind.as_str()).unwrap_or("Resource");
    lines.push(field_line(format!("Name:         {}", meta.name.as_deref().unwrap_or(""))));
    lines.push(field_line(format!("Namespace:    {}", meta.namespace.as_deref().unwrap_or("<none>"))));
    if let Some(uid) = &meta.uid {
        lines.push(field_line(format!("UID:          {}", uid)));
    }
    if let Some(ref ts) = meta.creation_timestamp {
        lines.push(field_line(format!("Created:      {}", ts.0.to_rfc3339())));
    }

    // Labels
    lines.push(blank());
    if let Some(ref labels) = meta.labels {
        if labels.is_empty() {
            lines.push(field_line("Labels:       <none>".to_string()));
        } else {
            for (i, (k, v)) in labels.iter().enumerate() {
                if i == 0 {
                    lines.push(field_line(format!("Labels:       {}={}", k, v)));
                } else {
                    // Continuation value — no structural key.
                    lines.push(DescribeLine::plain(format!("              {}={}", k, v)));
                }
            }
        }
    } else {
        lines.push(field_line("Labels:       <none>".to_string()));
    }

    // Annotations
    if let Some(ref annotations) = meta.annotations {
        if annotations.is_empty() {
            lines.push(field_line("Annotations:  <none>".to_string()));
        } else {
            for (i, (k, v)) in annotations.iter().enumerate() {
                // Char-boundary-safe truncation: annotation values are arbitrary
                // UTF-8, so a byte-index slice (`&v[..77]`) would panic mid-codepoint
                // and crash this describe task daemon-side.
                let display_v = k9rs_util::width::truncate(v, 80);
                if i == 0 {
                    lines.push(field_line(format!("Annotations:  {}={}", k, display_v)));
                } else {
                    lines.push(DescribeLine::plain(format!("              {}={}", k, display_v)));
                }
            }
        }
    } else {
        lines.push(field_line("Annotations:  <none>".to_string()));
    }

    // Owner references
    if let Some(ref owners) = meta.owner_references {
        lines.push(blank());
        for owner in owners {
            lines.push(field_line(format!("Controlled By:  {}/{}", owner.kind, owner.name)));
        }
    }

    // Spec + Status as YAML subsections
    if let Some(spec) = obj.data.get("spec") {
        lines.push(blank());
        lines.push(DescribeLine::section("Spec:"));
        if let Ok(yaml) = serde_yaml::to_string(spec) {
            for line in yaml.lines() {
                lines.push(classified(&format!("  {}", line)));
            }
        }
    }
    if let Some(status) = obj.data.get("status") {
        lines.push(blank());
        lines.push(DescribeLine::section("Status:"));
        if let Ok(yaml) = serde_yaml::to_string(status) {
            for line in yaml.lines() {
                lines.push(classified(&format!("  {}", line)));
            }
        }
    }

    // Type header for context
    lines.push(blank());
    lines.push(field_line(format!("Kind:         {}", kind)));

    lines
}

// ---------------------------------------------------------------------------
// kubectl fallbacks
// ---------------------------------------------------------------------------

async fn fetch_describe_via_kubectl(target: &ObjectRef, context: &k9rs_proto::protocol::ContextName, session_env: &SessionEnv) -> Vec<DescribeLine> {
    let mut cmd = session_env.kubectl_once().await;
    cmd.arg("describe").arg(target.kubectl_target());
    cmd.arg("--context").arg(context.as_str());
    if let Some(ns) = target.namespace.as_option() { cmd.arg("-n").arg(ns); }
    // kubectl's output has no producer-side structure for us — classify each
    // line (same as the UI used to, now done once here).
    let text = match cmd.output().await {
        Ok(output) if output.status.success() => String::from_utf8_lossy(&output.stdout).into_owned(),
        Ok(output) => format!("Error running kubectl describe:\n{}", String::from_utf8_lossy(&output.stderr)),
        Err(e) => format!("Failed to run kubectl: {}", e),
    };
    describe_lines_from_text(&text)
}

async fn fetch_yaml_via_kubectl(target: &ObjectRef, context: &k9rs_proto::protocol::ContextName, session_env: &SessionEnv) -> String {
    let mut cmd = session_env.kubectl_once().await;
    cmd.arg("get").arg(target.kubectl_target()).arg("-o").arg("yaml");
    cmd.arg("--context").arg(context.as_str());
    if let Some(ns) = target.namespace.as_option() { cmd.arg("-n").arg(ns); }
    match cmd.output().await {
        Ok(output) if output.status.success() => String::from_utf8_lossy(&output.stdout).to_string(),
        Ok(output) => format!("Error fetching YAML:\n{}", String::from_utf8_lossy(&output.stderr)),
        Err(e) => format!("Failed to run kubectl: {}", e),
    }
}

#[cfg(test)]
#[path = "tests/describe.rs"]
mod describe_line_tests;
