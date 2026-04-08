use std::collections::BTreeMap;

use k8s_openapi::api::apps::v1::Deployment;
use k8s_openapi::api::core::v1::{Node, Pod, Service};
use kube::api::{Api, DynamicObject, ObjectMeta};
use kube::Resource;
use serde::Deserialize;

use crate::kube::protocol::ResourceScope;

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Fetch a describe-like view for a resource.
/// Uses typed k8s-openapi structs for known types, DynamicObject for CRDs.
pub async fn fetch_describe_native(
    client: &kube::Client,
    resource: &str,
    name: &str,
    namespace: &str,
    context: &str,
) -> String {
    match fetch_describe_typed(client, resource, name, namespace).await {
        Ok(describe) => describe,
        Err(_) => fetch_describe_via_kubectl(resource, name, namespace, context).await,
    }
}

/// Fetch YAML for a resource.
pub async fn fetch_yaml_native(
    client: &kube::Client,
    resource: &str,
    name: &str,
    namespace: &str,
    context: &str,
) -> String {
    match fetch_yaml_via_discovery(client, resource, name, namespace).await {
        Ok(yaml) => yaml,
        Err(_) => fetch_yaml_via_kubectl(resource, name, namespace, context).await,
    }
}

/// Resolve a resource type string to an ApiResource.
pub async fn resolve_api_resource(
    client: &kube::Client,
    resource: &str,
) -> anyhow::Result<(kube::api::ApiResource, ResourceScope)> {
    use kube::api::ApiResource;
    use kube::discovery::{self, Scope};
    use ResourceScope::{Cluster, Namespaced};

    if let Some(meta) = crate::kube::resource_types::find_by_name(resource) {
        let gvk = kube::api::GroupVersionKind::gvk(meta.group, meta.version, meta.kind);
        let ar = ApiResource::from_gvk_with_plural(&gvk, meta.plural);
        return Ok((ar, meta.scope));
    }

    // Use API discovery to find the resource by plural name (with or without group).
    let discovery = discovery::Discovery::new(client.clone()).run().await?;

    if resource.contains('.') {
        // "nodeclaims.karpenter.sh" → plural=nodeclaims, group=karpenter.sh
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
        // "nodeclaims" → search all groups for a matching plural or kind
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
// Typed describe: uses k8s-openapi structs for known types
// ---------------------------------------------------------------------------

async fn fetch_describe_typed(
    client: &kube::Client,
    resource: &str,
    name: &str,
    namespace: &str,
) -> anyhow::Result<String> {
    match resource {
        "pod" => {
            let api = ns_api::<Pod>(client, namespace);
            let pod = api.get(name).await?;
            Ok(format_pod(&pod))
        }
        "deployment" => {
            let api = ns_api::<Deployment>(client, namespace);
            let dep = api.get(name).await?;
            Ok(format_deployment(&dep))
        }
        "service" => {
            let api = ns_api::<Service>(client, namespace);
            let svc = api.get(name).await?;
            Ok(format_service(&svc))
        }
        "node" => {
            let api: Api<Node> = Api::all(client.clone());
            let node = api.get(name).await?;
            Ok(format_node(&node))
        }
        _ => {
            // Unknown/CRD: fetch as DynamicObject, deserialize data into generic struct
            let (ar, scope) = resolve_api_resource(client, resource).await?;
            let api: Api<DynamicObject> = match scope {
                ResourceScope::Cluster => Api::all_with(client.clone(), &ar),
                ResourceScope::Namespaced if namespace.is_empty() => Api::default_namespaced_with(client.clone(), &ar),
                ResourceScope::Namespaced => Api::namespaced_with(client.clone(), namespace, &ar),
            };
            let obj = api.get(name).await?;
            Ok(format_generic(&obj))
        }
    }
}

fn ns_api<K>(client: &kube::Client, namespace: &str) -> Api<K>
where
    K: Resource<Scope = k8s_openapi::NamespaceResourceScope>,
    <K as Resource>::DynamicType: Default,
{
    if namespace.is_empty() {
        Api::default_namespaced(client.clone())
    } else {
        Api::namespaced(client.clone(), namespace)
    }
}

// ---------------------------------------------------------------------------
// Common metadata formatting
// ---------------------------------------------------------------------------

fn format_metadata(meta: &ObjectMeta, out: &mut String) {
    if let Some(ref name) = meta.name {
        out.push_str(&format!("Name:         {}\n", name));
    }
    if let Some(ref ns) = meta.namespace {
        out.push_str(&format!("Namespace:    {}\n", ns));
    }
    if let Some(ref ts) = meta.creation_timestamp {
        out.push_str(&format!("Created:      {}\n", ts.0.format("%Y-%m-%dT%H:%M:%SZ")));
    }
    format_labels(meta.labels.as_ref(), out);
    format_annotations(meta.annotations.as_ref(), out);
    out.push('\n');
}

fn format_labels(labels: Option<&BTreeMap<String, String>>, out: &mut String) {
    match labels {
        Some(l) if !l.is_empty() => {
            out.push_str("Labels:\n");
            for (k, v) in l { out.push_str(&format!("              {}={}\n", k, v)); }
        }
        _ => out.push_str("Labels:       <none>\n"),
    }
}

fn format_annotations(annotations: Option<&BTreeMap<String, String>>, out: &mut String) {
    match annotations {
        Some(a) if !a.is_empty() => {
            out.push_str("Annotations:\n");
            for (k, v) in a { out.push_str(&format!("              {}={}\n", k, v)); }
        }
        _ => out.push_str("Annotations:  <none>\n"),
    }
}

// ---------------------------------------------------------------------------
// Pod describe (fully typed)
// ---------------------------------------------------------------------------

fn format_pod(pod: &Pod) -> String {
    let mut out = String::new();
    format_metadata(&pod.metadata, &mut out);

    let status = pod.status.as_ref();
    let spec = pod.spec.as_ref();

    if let Some(phase) = status.and_then(|s| s.phase.as_deref()) {
        out.push_str(&format!("Status:       {}\n", phase));
    }
    if let Some(node) = spec.and_then(|s| s.node_name.as_deref()) {
        out.push_str(&format!("Node:         {}\n", node));
    }
    if let Some(ip) = status.and_then(|s| s.pod_ip.as_deref()) {
        out.push_str(&format!("IP:           {}\n", ip));
    }
    if let Some(sa) = spec.and_then(|s| s.service_account_name.as_deref()) {
        out.push_str(&format!("Service Account: {}\n", sa));
    }

    out.push('\n');

    // Containers
    if let Some(containers) = spec.map(|s| &s.containers) {
        let statuses = status.and_then(|s| s.container_statuses.as_ref());
        out.push_str("Containers:\n");
        for c in containers {
            out.push_str(&format!("  {}:\n", c.name));
            out.push_str(&format!("    Image:    {}\n", c.image.as_deref().unwrap_or("?")));
            if let Some(ref ports) = c.ports {
                let ps: Vec<String> = ports.iter().map(|p| {
                    format!("{}/{}", p.container_port, p.protocol.as_deref().unwrap_or("TCP"))
                }).collect();
                out.push_str(&format!("    Ports:    {}\n", ps.join(", ")));
            }
            if let Some(ref res) = c.resources {
                if let Some(ref limits) = res.limits {
                    let parts: Vec<String> = limits.iter().map(|(k, v)| format!("{}={}", k, v.0)).collect();
                    out.push_str(&format!("    Limits:   {}\n", parts.join(", ")));
                }
                if let Some(ref requests) = res.requests {
                    let parts: Vec<String> = requests.iter().map(|(k, v)| format!("{}={}", k, v.0)).collect();
                    out.push_str(&format!("    Requests: {}\n", parts.join(", ")));
                }
            }
            // Match container status by name
            if let Some(statuses) = statuses {
                if let Some(cs) = statuses.iter().find(|s| s.name == c.name) {
                    out.push_str(&format!("    Ready:    {}\n", cs.ready));
                    out.push_str(&format!("    Restarts: {}\n", cs.restart_count));
                    if let Some(ref state) = cs.state {
                        if state.running.is_some() {
                            out.push_str("    State:    Running\n");
                        } else if let Some(ref w) = state.waiting {
                            out.push_str(&format!("    State:    Waiting ({})\n", w.reason.as_deref().unwrap_or("?")));
                        } else if let Some(ref t) = state.terminated {
                            out.push_str(&format!("    State:    Terminated ({})\n", t.reason.as_deref().unwrap_or("?")));
                        }
                    }
                }
            }
        }
    }

    // Init containers
    if let Some(init_containers) = spec.and_then(|s| s.init_containers.as_ref()) {
        if !init_containers.is_empty() {
            out.push_str("\nInit Containers:\n");
            for c in init_containers {
                out.push_str(&format!("  {}:\n", c.name));
                out.push_str(&format!("    Image:    {}\n", c.image.as_deref().unwrap_or("?")));
            }
        }
    }

    out.push('\n');

    // Conditions
    if let Some(conditions) = status.and_then(|s| s.conditions.as_ref()) {
        out.push_str("Conditions:\n");
        out.push_str("  Type                 Status\n");
        out.push_str("  ----                 ------\n");
        for c in conditions {
            out.push_str(&format!("  {:<22}{}\n", c.type_, c.status));
        }
    }

    // Volumes
    if let Some(volumes) = spec.and_then(|s| s.volumes.as_ref()) {
        out.push_str("\nVolumes:\n");
        for v in volumes {
            out.push_str(&format!("  {}:\n", v.name));
            // Show the first volume source type
            if v.config_map.is_some() { out.push_str("    Type: ConfigMap\n"); }
            else if v.secret.is_some() { out.push_str("    Type: Secret\n"); }
            else if v.empty_dir.is_some() { out.push_str("    Type: EmptyDir\n"); }
            else if v.host_path.is_some() { out.push_str("    Type: HostPath\n"); }
            else if v.persistent_volume_claim.is_some() { out.push_str("    Type: PVC\n"); }
            else if v.projected.is_some() { out.push_str("    Type: Projected\n"); }
            else if v.downward_api.is_some() { out.push_str("    Type: DownwardAPI\n"); }
        }
    }

    // Tolerations
    if let Some(tolerations) = spec.and_then(|s| s.tolerations.as_ref()) {
        out.push_str("\nTolerations:\n");
        for t in tolerations {
            let key = t.key.as_deref().unwrap_or("");
            let op = t.operator.as_deref().unwrap_or("Equal");
            let effect = t.effect.as_deref().unwrap_or("");
            let value = t.value.as_deref().unwrap_or("");
            if key.is_empty() {
                out.push_str(&format!("  op={}\n", op));
            } else if value.is_empty() {
                out.push_str(&format!("  {}:{} for {}\n", key, op, if effect.is_empty() { "*" } else { effect }));
            } else {
                out.push_str(&format!("  {}={}:{}\n", key, value, if effect.is_empty() { "*" } else { effect }));
            }
        }
    }

    out
}

// ---------------------------------------------------------------------------
// Deployment describe (fully typed)
// ---------------------------------------------------------------------------

fn format_deployment(dep: &Deployment) -> String {
    let mut out = String::new();
    format_metadata(&dep.metadata, &mut out);

    let spec = dep.spec.as_ref();
    let status = dep.status.as_ref();

    if let Some(replicas) = spec.and_then(|s| s.replicas) {
        let ready = status.and_then(|s| s.ready_replicas).unwrap_or(0);
        let available = status.and_then(|s| s.available_replicas).unwrap_or(0);
        let updated = status.and_then(|s| s.updated_replicas).unwrap_or(0);
        out.push_str(&format!("Replicas:     {} desired | {} updated | {} total | {} available | {} ready\n",
            replicas, updated, replicas, available, ready));
    }

    if let Some(ref strategy) = spec.and_then(|s| s.strategy.as_ref()).and_then(|s| s.type_.as_ref()) {
        out.push_str(&format!("Strategy:     {}\n", strategy));
    }

    if let Some(ref match_labels) = spec.and_then(|s| s.selector.match_labels.as_ref()) {
        let parts: Vec<String> = match_labels.iter().map(|(k, v)| format!("{}={}", k, v)).collect();
        out.push_str(&format!("Selector:     {}\n", parts.join(",")));
    }

    out.push('\n');

    // Pod template containers
    if let Some(containers) = spec.and_then(|s| s.template.spec.as_ref()).map(|s| &s.containers) {
        out.push_str("Pod Template:\n");
        for c in containers {
            out.push_str(&format!("  {}:\n", c.name));
            out.push_str(&format!("    Image:    {}\n", c.image.as_deref().unwrap_or("?")));
            if let Some(ref ports) = c.ports {
                let ps: Vec<String> = ports.iter().map(|p| {
                    format!("{}/{}", p.container_port, p.protocol.as_deref().unwrap_or("TCP"))
                }).collect();
                out.push_str(&format!("    Ports:    {}\n", ps.join(", ")));
            }
            if let Some(ref env) = c.env {
                out.push_str("    Environment:\n");
                for e in env {
                    if let Some(ref value) = e.value {
                        out.push_str(&format!("      {}={}\n", e.name, value));
                    } else if e.value_from.is_some() {
                        out.push_str(&format!("      {} (from ref)\n", e.name));
                    }
                }
            }
        }
    }

    out.push('\n');

    if let Some(conditions) = status.and_then(|s| s.conditions.as_ref()) {
        out.push_str("Conditions:\n");
        out.push_str("  Type                 Status  Reason\n");
        out.push_str("  ----                 ------  ------\n");
        for c in conditions {
            out.push_str(&format!("  {:<22}{:<8}{}\n", c.type_, c.status, c.reason.as_deref().unwrap_or("")));
        }
    }

    out
}

// ---------------------------------------------------------------------------
// Service describe (fully typed)
// ---------------------------------------------------------------------------

fn format_service(svc: &Service) -> String {
    let mut out = String::new();
    format_metadata(&svc.metadata, &mut out);

    let spec = svc.spec.as_ref();
    let status = svc.status.as_ref();

    if let Some(svc_type) = spec.and_then(|s| s.type_.as_deref()) {
        out.push_str(&format!("Type:         {}\n", svc_type));
    }
    if let Some(cluster_ip) = spec.and_then(|s| s.cluster_ip.as_deref()) {
        out.push_str(&format!("ClusterIP:    {}\n", cluster_ip));
    }
    if let Some(ref external_ips) = spec.and_then(|s| s.external_ips.as_ref()) {
        out.push_str(&format!("ExternalIPs:  {}\n", external_ips.join(", ")));
    }
    if let Some(ref ingress) = status.and_then(|s| s.load_balancer.as_ref()).and_then(|lb| lb.ingress.as_ref()) {
        let ips: Vec<String> = ingress.iter().map(|i| {
            i.ip.as_deref().or(i.hostname.as_deref()).unwrap_or("").to_string()
        }).collect();
        if !ips.is_empty() {
            out.push_str(&format!("LoadBalancer: {}\n", ips.join(", ")));
        }
    }

    if let Some(ref selector) = spec.and_then(|s| s.selector.as_ref()) {
        let parts: Vec<String> = selector.iter().map(|(k, v)| format!("{}={}", k, v)).collect();
        out.push_str(&format!("Selector:     {}\n", parts.join(",")));
    }

    if let Some(ref ports) = spec.and_then(|s| s.ports.as_ref()) {
        out.push_str("\nPorts:\n");
        for p in *ports {
            let name = p.name.as_deref().unwrap_or("<unnamed>");
            let target = p.target_port.as_ref()
                .map(|tp| match tp {
                    k8s_openapi::apimachinery::pkg::util::intstr::IntOrString::Int(n) => n.to_string(),
                    k8s_openapi::apimachinery::pkg::util::intstr::IntOrString::String(s) => s.clone(),
                })
                .unwrap_or_else(|| "?".to_string());
            out.push_str(&format!("  {} {}/{} -> {}\n", name, p.port, p.protocol.as_deref().unwrap_or("TCP"), target));
        }
    }

    if let Some(affinity) = spec.and_then(|s| s.session_affinity.as_deref()) {
        out.push_str(&format!("Session Affinity: {}\n", affinity));
    }

    out
}

// ---------------------------------------------------------------------------
// Node describe (fully typed)
// ---------------------------------------------------------------------------

fn format_node(node: &Node) -> String {
    let mut out = String::new();
    format_metadata(&node.metadata, &mut out);

    let status = node.status.as_ref();

    // Roles from labels
    if let Some(ref labels) = node.metadata.labels {
        let roles: Vec<&str> = labels.keys()
            .filter_map(|k| k.strip_prefix("node-role.kubernetes.io/"))
            .collect();
        if !roles.is_empty() {
            out.push_str(&format!("Roles:        {}\n", roles.join(", ")));
        }
    }

    if let Some(ref addresses) = status.and_then(|s| s.addresses.as_ref()) {
        out.push_str("Addresses:\n");
        for a in *addresses {
            out.push_str(&format!("  {}: {}\n", a.type_, a.address));
        }
    }

    if let Some(capacity) = status.and_then(|s| s.capacity.as_ref()) {
        out.push_str("Capacity:\n");
        for (k, v) in capacity { out.push_str(&format!("  {}: {}\n", k, v.0)); }
    }

    if let Some(allocatable) = status.and_then(|s| s.allocatable.as_ref()) {
        out.push_str("Allocatable:\n");
        for (k, v) in allocatable { out.push_str(&format!("  {}: {}\n", k, v.0)); }
    }

    if let Some(ref info) = status.and_then(|s| s.node_info.as_ref()) {
        out.push_str("\nSystem Info:\n");
        out.push_str(&format!("  kubeletVersion: {}\n", info.kubelet_version));
        out.push_str(&format!("  containerRuntimeVersion: {}\n", info.container_runtime_version));
        out.push_str(&format!("  osImage: {}\n", info.os_image));
        out.push_str(&format!("  architecture: {}\n", info.architecture));
        out.push_str(&format!("  operatingSystem: {}\n", info.operating_system));
        out.push_str(&format!("  kernelVersion: {}\n", info.kernel_version));
    }

    out.push('\n');

    if let Some(ref conditions) = status.and_then(|s| s.conditions.as_ref()) {
        out.push_str("Conditions:\n");
        out.push_str("  Type                   Status  Reason                   Message\n");
        out.push_str("  ----                   ------  ------                   -------\n");
        for c in *conditions {
            out.push_str(&format!("  {:<24}{:<8}{:<25}{}\n",
                c.type_, c.status, c.reason.as_deref().unwrap_or(""), c.message.as_deref().unwrap_or("")));
        }
    }

    out
}

// ---------------------------------------------------------------------------
// Generic describe (DynamicObject — for CRDs and unknown resources)
// ---------------------------------------------------------------------------

/// Typed wrapper for common DynamicObject fields.
#[derive(Deserialize, Default)]
struct GenericResourceData {
    #[serde(default)]
    spec: Option<serde_yaml::Value>,
    #[serde(default)]
    status: Option<serde_yaml::Value>,
}

impl From<&serde_json::Value> for GenericResourceData {
    fn from(data: &serde_json::Value) -> Self {
        serde_json::from_value(data.clone()).unwrap_or_default()
    }
}

fn format_generic(obj: &DynamicObject) -> String {
    let mut out = String::new();
    format_metadata(&obj.metadata, &mut out);

    let data = GenericResourceData::from(&obj.data);

    if let Some(ref spec) = data.spec {
        out.push_str("Spec:\n");
        if let Ok(yaml) = serde_yaml::to_string(spec) {
            for line in yaml.lines() {
                out.push_str(&format!("  {}\n", line));
            }
        }
        out.push('\n');
    }

    if let Some(ref status) = data.status {
        out.push_str("Status:\n");
        if let Ok(yaml) = serde_yaml::to_string(status) {
            for line in yaml.lines() {
                out.push_str(&format!("  {}\n", line));
            }
        }
        out.push('\n');
    }

    out
}

// ---------------------------------------------------------------------------
// kubectl fallbacks
// ---------------------------------------------------------------------------

async fn fetch_yaml_via_discovery(
    client: &kube::Client,
    resource: &str,
    name: &str,
    namespace: &str,
) -> anyhow::Result<String> {
    let (ar, scope) = resolve_api_resource(client, resource).await?;
    let api: Api<DynamicObject> = match scope {
        ResourceScope::Cluster => Api::all_with(client.clone(), &ar),
        ResourceScope::Namespaced if namespace.is_empty() => Api::default_namespaced_with(client.clone(), &ar),
        ResourceScope::Namespaced => Api::namespaced_with(client.clone(), namespace, &ar),
    };
    let obj = api.get(name).await?;
    let yaml = serde_yaml::to_string(&obj)?;
    Ok(yaml)
}

async fn fetch_describe_via_kubectl(resource: &str, name: &str, namespace: &str, context: &str) -> String {
    let mut cmd = tokio::process::Command::new("kubectl");
    cmd.arg("describe").arg(resource).arg(name);
    if !context.is_empty() { cmd.arg("--context").arg(context); }
    if !namespace.is_empty() { cmd.arg("-n").arg(namespace); }
    match cmd.output().await {
        Ok(output) if output.status.success() => {
            crate::util::strip_ansi(&String::from_utf8_lossy(&output.stdout))
        }
        Ok(output) => format!("Error running kubectl describe:\n{}", String::from_utf8_lossy(&output.stderr)),
        Err(e) => format!("Failed to run kubectl: {}", e),
    }
}

async fn fetch_yaml_via_kubectl(resource: &str, name: &str, namespace: &str, context: &str) -> String {
    let mut cmd = tokio::process::Command::new("kubectl");
    cmd.arg("get").arg(resource).arg(name).arg("-o").arg("yaml");
    if !context.is_empty() { cmd.arg("--context").arg(context); }
    if !namespace.is_empty() { cmd.arg("-n").arg(namespace); }
    match cmd.output().await {
        Ok(output) if output.status.success() => String::from_utf8_lossy(&output.stdout).to_string(),
        Ok(output) => format!("Error fetching YAML:\n{}", String::from_utf8_lossy(&output.stderr)),
        Err(e) => format!("Failed to run kubectl: {}", e),
    }
}
