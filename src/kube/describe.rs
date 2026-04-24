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

use kube::api::{Api, ApiResource, DynamicObject, GroupVersionKind};
use kube::discovery::{self, Scope};

use crate::kube::protocol::{Namespace, ObjectRef, ResourceId, ResourceScope};

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
        (ResourceScope::Cluster, _) => Api::all_with(client.clone(), ar),
        (ResourceScope::Namespaced, None) => Api::default_namespaced_with(client.clone(), ar),
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
    context: &crate::kube::protocol::ContextName,
) -> String {
    match fetch_describe_via_api(client, target).await {
        Ok(content) => content,
        Err(_) => fetch_describe_via_kubectl(target, context).await,
    }
}

/// Fetch YAML for a resource. Tries the kube API first (fast, no subprocess),
/// falls back to kubectl on any failure.
pub async fn fetch_yaml(
    client: &kube::Client,
    target: &ObjectRef,
    context: &crate::kube::protocol::ContextName,
) -> String {
    match fetch_yaml_via_api(client, target).await {
        Ok(yaml) => yaml,
        Err(_) => fetch_yaml_via_kubectl(target, context).await,
    }
}

// ---------------------------------------------------------------------------
// Typed API resource resolution
// ---------------------------------------------------------------------------

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
        ResourceId::BuiltIn(kind) => {
            let g = crate::kube::resource_defs::REGISTRY.by_kind(*kind).gvr();
            let gvk = GroupVersionKind::gvk(g.group, g.version, g.kind);
            Ok((ApiResource::from_gvk_with_plural(&gvk, g.plural), g.scope))
        }
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
) -> anyhow::Result<String> {
    let (ar, scope) = api_resource_for(client, &target.resource).await?;
    let api = dynamic_api_for(client, &ar, scope, &target.namespace);
    let obj = api.get(&target.name).await?;
    Ok(format_describe(&obj))
}

/// Format a DynamicObject into a human-readable describe view.
fn format_describe(obj: &kube::api::DynamicObject) -> String {
    use std::fmt::Write;
    let mut out = String::new();
    let meta = &obj.metadata;

    // Header
    let kind = obj.types.as_ref().map(|t| t.kind.as_str()).unwrap_or("Resource");
    let _ = writeln!(out, "Name:         {}", meta.name.as_deref().unwrap_or(""));
    let _ = writeln!(out, "Namespace:    {}", meta.namespace.as_deref().unwrap_or("<none>"));
    if let Some(uid) = &meta.uid {
        let _ = writeln!(out, "UID:          {}", uid);
    }
    if let Some(ref ts) = meta.creation_timestamp {
        let _ = writeln!(out, "Created:      {}", ts.0.to_rfc3339());
    }

    // Labels
    let _ = writeln!(out);
    if let Some(ref labels) = meta.labels {
        if labels.is_empty() {
            let _ = writeln!(out, "Labels:       <none>");
        } else {
            for (i, (k, v)) in labels.iter().enumerate() {
                if i == 0 {
                    let _ = writeln!(out, "Labels:       {}={}", k, v);
                } else {
                    let _ = writeln!(out, "              {}={}", k, v);
                }
            }
        }
    } else {
        let _ = writeln!(out, "Labels:       <none>");
    }

    // Annotations
    if let Some(ref annotations) = meta.annotations {
        if annotations.is_empty() {
            let _ = writeln!(out, "Annotations:  <none>");
        } else {
            for (i, (k, v)) in annotations.iter().enumerate() {
                let display_v = if v.len() > 80 { format!("{}...", &v[..77]) } else { v.clone() };
                if i == 0 {
                    let _ = writeln!(out, "Annotations:  {}={}", k, display_v);
                } else {
                    let _ = writeln!(out, "              {}={}", k, display_v);
                }
            }
        }
    } else {
        let _ = writeln!(out, "Annotations:  <none>");
    }

    // Owner references
    if let Some(ref owners) = meta.owner_references {
        let _ = writeln!(out);
        for owner in owners {
            let _ = writeln!(out, "Controlled By:  {}/{}", owner.kind, owner.name);
        }
    }

    // Spec + Status as YAML subsections
    if let Some(spec) = obj.data.get("spec") {
        let _ = writeln!(out);
        let _ = writeln!(out, "Spec:");
        if let Ok(yaml) = serde_yaml::to_string(spec) {
            for line in yaml.lines() {
                let _ = writeln!(out, "  {}", line);
            }
        }
    }
    if let Some(status) = obj.data.get("status") {
        let _ = writeln!(out);
        let _ = writeln!(out, "Status:");
        if let Ok(yaml) = serde_yaml::to_string(status) {
            for line in yaml.lines() {
                let _ = writeln!(out, "  {}", line);
            }
        }
    }

    // Type header for context
    let _ = writeln!(out);
    let _ = writeln!(out, "Kind:         {}", kind);

    out
}

// ---------------------------------------------------------------------------
// kubectl fallbacks
// ---------------------------------------------------------------------------

async fn fetch_describe_via_kubectl(target: &ObjectRef, context: &crate::kube::protocol::ContextName) -> String {
    let mut cmd = tokio::process::Command::new("kubectl");
    cmd.arg("describe").arg(target.kubectl_target());
    if !context.is_empty() { cmd.arg("--context").arg(context.as_str()); }
    if let Some(ns) = target.namespace.as_option() { cmd.arg("-n").arg(ns); }
    cmd.kill_on_drop(true);
    match cmd.output().await {
        Ok(output) if output.status.success() => {
            crate::util::strip_ansi(&String::from_utf8_lossy(&output.stdout))
        }
        Ok(output) => format!("Error running kubectl describe:\n{}", String::from_utf8_lossy(&output.stderr)),
        Err(e) => format!("Failed to run kubectl: {}", e),
    }
}

async fn fetch_yaml_via_kubectl(target: &ObjectRef, context: &crate::kube::protocol::ContextName) -> String {
    let mut cmd = tokio::process::Command::new("kubectl");
    cmd.arg("get").arg(target.kubectl_target()).arg("-o").arg("yaml");
    if !context.is_empty() { cmd.arg("--context").arg(context.as_str()); }
    if let Some(ns) = target.namespace.as_option() { cmd.arg("-n").arg(ns); }
    cmd.kill_on_drop(true);
    match cmd.output().await {
        Ok(output) if output.status.success() => String::from_utf8_lossy(&output.stdout).to_string(),
        Ok(output) => format!("Error fetching YAML:\n{}", String::from_utf8_lossy(&output.stderr)),
        Err(e) => format!("Failed to run kubectl: {}", e),
    }
}
