// ---------------------------------------------------------------------------
// Shared K8s API value constants — strings that the Kubernetes API returns as
// condition types, condition statuses, phase names, address types, etc.
// Only values referenced by two or more resource converter files live here;
// single-file constants stay file-local.
// ---------------------------------------------------------------------------
pub(crate) mod k8s_const {
    // -- Condition types --
    pub const COND_READY: &str = "Ready";

    // -- Condition / field statuses --
    pub const STATUS_TRUE: &str = "True";

    // -- Pod / namespace phases & reasons --
    pub const PHASE_RUNNING: &str = "Running";
    pub const PHASE_PENDING: &str = "Pending";
    pub const PHASE_SUCCEEDED: &str = "Succeeded";
    pub const PHASE_COMPLETED: &str = "Completed";
    pub const PHASE_UNKNOWN: &str = "Unknown";
    pub const REASON_NOT_READY: &str = "NotReady";
    pub const REASON_TERMINATING: &str = "Terminating";

    // -- PV / PVC phases --
    pub const PHASE_BOUND: &str = "Bound";
    pub const PHASE_AVAILABLE: &str = "Available";

    // -- Restart policy --
    pub const RESTART_ALWAYS: &str = "Always";

    // -- Node address types --
    pub const ADDR_INTERNAL_IP: &str = "InternalIP";
    pub const ADDR_EXTERNAL_IP: &str = "ExternalIP";
}

pub mod row;
pub mod pods;
pub mod deployments;
pub mod services;
pub mod statefulsets;
pub mod daemonsets;
pub mod jobs;
pub mod cronjobs;
pub mod replicasets;
pub mod nodes;
pub mod configmaps;
pub mod secrets;
pub mod ingresses;
pub mod networkpolicies;
pub mod serviceaccounts;
pub mod namespaces;
pub mod pvs;
pub mod pvcs;
pub mod storageclasses;
pub mod events;
pub mod roles;
pub mod clusterroles;
pub mod rolebindings;
pub mod clusterrolebindings;
pub mod hpa;
pub mod endpoints;
pub mod limitranges;
pub mod resourcequotas;
pub mod pdb;
pub mod crds;
pub mod endpointslices;
pub mod leases;
pub mod priorityclasses;
pub mod webhooks;

/// The contract for anything that can live inside a `StatefulTable`. The
/// only implementor is `ResourceRow`; this trait is the bound used by
/// generic table machinery (`StatefulTable<T>`, `live_query`'s sort
/// helpers) so they don't have to depend on the concrete row type.
pub trait KubeResource: Clone + std::fmt::Debug + Send + Sync + 'static {
    /// The typed cell values for this row, in header order.
    fn cells(&self) -> &[row::CellValue];
    /// Resource name (cached for O(1) access in sorts/filters).
    fn name(&self) -> &str;
    /// Resource namespace, or `""` for cluster-scoped rows.
    fn namespace(&self) -> &str;
}

pub fn access_mode_short(mode: &str) -> &str {
    match mode {
        "ReadWriteOnce" => "RWO",
        "ReadOnlyMany" => "ROX",
        "ReadWriteMany" => "RWX",
        "ReadWriteOncePod" => "RWOP",
        other => other,
    }
}

// ---------------------------------------------------------------------------
// Per-resource converter helpers — dedup the "pull metadata" and "summarize
// a PodSpec" incantations that every workload file used to repeat verbatim.
// ---------------------------------------------------------------------------

/// Shape every converter starts from: the common `ObjectMeta` fields plus
/// a pre-joined label string (the cells vec wants a display `String`, not
/// the parsed map).
///
/// `from_k8s` is consuming because `ObjectMeta` is a plain value type and
/// converters want to take ownership — no reason to borrow + clone.
pub(crate) struct CommonMeta {
    pub namespace: String,
    pub name: String,
    pub uid: String,
    pub labels: std::collections::BTreeMap<String, String>,
    /// Pre-joined `k=v,...` display string — a few converters want it in
    /// their cells vec and we build it here once so they don't all repeat
    /// the same `iter().map().join()` incantation.
    pub labels_str: String,
    pub age: Option<chrono::DateTime<chrono::Utc>>,
}

impl CommonMeta {
    pub(crate) fn from_k8s(
        meta: k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta,
    ) -> Self {
        let namespace = meta.namespace.unwrap_or_default();
        let name = meta.name.unwrap_or_default();
        let uid = meta.uid.unwrap_or_default();
        let labels = meta.labels.unwrap_or_default();
        let labels_str = labels
            .iter()
            .map(|(k, v)| format!("{}={}", k, v))
            .collect::<Vec<_>>()
            .join(",");
        let age = meta.creation_timestamp.map(|t| t.0);
        Self { namespace, name, uid, labels, labels_str, age }
    }
}

/// Container-level summary pulled off a `PodSpec` (or a nested template's
/// `PodSpec`): comma-joined names + images for display cells, plus the
/// non-UDP container ports used to populate the port-forward form's
/// `container_port` field.
pub(crate) struct WorkloadContainers {
    pub names: String,
    pub images: String,
    pub tcp_ports: Vec<u16>,
}

impl WorkloadContainers {
    pub(crate) fn from_pod_spec(
        spec: Option<&k8s_openapi::api::core::v1::PodSpec>,
    ) -> Self {
        let Some(ps) = spec else {
            return Self { names: String::new(), images: String::new(), tcp_ports: Vec::new() };
        };
        let names = ps.containers.iter().map(|c| c.name.clone())
            .collect::<Vec<_>>().join(",");
        let images = ps.containers.iter()
            .map(|c| c.image.clone().unwrap_or_default())
            .collect::<Vec<_>>().join(",");
        let tcp_ports = ps.containers.iter()
            .flat_map(|c| c.ports.as_ref().into_iter().flatten())
            .filter(|p| p.protocol.as_deref() != Some("UDP"))
            .map(|p| p.container_port as u16)
            .collect();
        Self { names, images, tcp_ports }
    }
}
