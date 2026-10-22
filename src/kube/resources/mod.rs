// ---------------------------------------------------------------------------
// Shared K8s API value constants — strings that the Kubernetes API returns as
// condition types, condition statuses, phase names, address types, etc.
// Only values referenced by two or more resource converter files live here;
// single-file constants stay file-local.
// ---------------------------------------------------------------------------
pub(crate) mod k8s_const {
    // -- Condition types --
    pub(crate) const COND_READY: &str = "Ready";

    // -- Condition / field statuses --
    pub(crate) const STATUS_TRUE: &str = "True";

    // -- Pod / namespace phases & reasons --
        /// Event `type` — a two-valued field, both values named.
    pub(crate) const EVENT_TYPE_NORMAL: &str = "Normal";
    pub(crate) const EVENT_TYPE_WARNING: &str = "Warning";
    /// ConditionStatus. `STATUS_TRUE` already lived here; its counterpart
    /// was re-spelled raw in `hpa.rs`.
    pub(crate) const STATUS_FALSE: &str = "False";

    pub(crate) const PHASE_RUNNING: &str = "Running";
    pub(crate) const PHASE_PENDING: &str = "Pending";
    pub(crate) const PHASE_SUCCEEDED: &str = "Succeeded";
    pub(crate) const PHASE_COMPLETED: &str = "Completed";
    pub(crate) const PHASE_UNKNOWN: &str = "Unknown";
    pub(crate) const REASON_NOT_READY: &str = "NotReady";
    pub(crate) const REASON_TERMINATING: &str = "Terminating";

    // -- PV / PVC phases --
    pub(crate) const PHASE_BOUND: &str = "Bound";
    pub(crate) const PHASE_AVAILABLE: &str = "Available";

    // -- Restart policy --
    pub(crate) const RESTART_ALWAYS: &str = "Always";

    // -- Node address types --
    pub(crate) const ADDR_INTERNAL_IP: &str = "InternalIP";
    pub(crate) const ADDR_EXTERNAL_IP: &str = "ExternalIP";
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

/// The replica counts a workload row's health is decided from — the one place
/// that decision lives. Deployment, StatefulSet, ReplicaSet and DaemonSet
/// each spell these fields differently in their status (`readyReplicas` vs
/// `numberReady`, `replicas` vs `currentNumberScheduled`), so the *mapping*
/// is each call site's job; the *verdict* is this type's. It was written out
/// by hand in all four converters, which is how a fourth clause would have
/// become three-quarters of a rule.
pub(crate) struct WorkloadTally {
    /// The scale target: how many pods there are supposed to be.
    pub desired: i32,
    /// Pods passing their readiness probe, across every revision.
    pub ready: i32,
    /// Non-terminated pods that exist, across every revision. Exceeds
    /// `desired` during a surge, trails it while pods are still being
    /// created, and lags it while a scale-down drains.
    pub total: i32,
    /// Pods running the current pod template. `None` for a workload with no
    /// revision axis to be behind on (a ReplicaSet *is* one revision), and
    /// for one whose table shows no column that would explain the verdict —
    /// see [`WorkloadTally::rollout_in_flight`].
    pub up_to_date: Option<i32>,
    /// What `desired == 0` means for this kind — see [`AtZero`].
    pub at_zero: AtZero,
}

/// What a scale target of zero MEANS, which differs by workload kind and is
/// not recoverable from the numbers.
///
/// It used to be one rule for all four: `desired == 0` → `Pending`. For a
/// Deployment that is right — someone scaled it down, and a parked workload
/// is worth marking. For a ReplicaSet it is wrong: a Deployment keeps its old
/// ReplicaSets at zero per `revisionHistoryLimit`, so `:rs` in any real
/// cluster is mostly retired ones and the whole view went yellow — diluting
/// the yellow that now means "a rollout is in flight".
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum AtZero {
    /// Someone scaled it to zero. Parked, not broken, and worth saying so.
    /// k9s agrees for Deployments, keying `PendingColor` on READY's
    /// denominator being `0` (`render/dp.go`).
    Parked,
    /// The controller left it behind — history, not a state anyone needs
    /// flagged. k9s shows these normal too: its ReplicaSet check is
    /// `status.replicas != status.readyReplicas`, and `0 != 0` is false.
    Retired,
}

impl WorkloadTally {
    /// `true` while the fleet is not settled: some pods are still on the old
    /// template, or there are more (or fewer) pods than the scale target.
    ///
    /// Both halves are load-bearing. At one to three replicas a Deployment's
    /// default strategy works out to `maxUnavailable: 0` / `maxSurge: 1`
    /// (25% rounds *down* for unavailable and *up* for surge), so the new pod
    /// is created alongside the old and `up_to_date` reaches `desired` the
    /// instant it exists — only `total` sees that rollout at all. Above that,
    /// a rollout steps through states where every pod is present but some are
    /// still the old template, which only `up_to_date` sees.
    fn rollout_in_flight(&self) -> bool {
        self.total != self.desired || self.up_to_date.is_some_and(|u| u < self.desired)
    }

    /// The row's health, in precedence order:
    ///
    /// 1. **Scaled to zero** → whatever zero means for this kind
    ///    ([`AtZero`]): `Pending` for a parked workload, `Normal` for a
    ///    retired ReplicaSet.
    /// 2. **Fewer ready pods than asked for** → `Failed`. Less serving
    ///    capacity than requested, *whatever the reason*. A rollout being the
    ///    cause does not make the shortfall untrue, and a rollout wedged at
    ///    `3/4` must not read as "relax, it's in progress".
    /// 3. **Full capacity, fleet unsettled** → `Pending`. A rollout in
    ///    flight, or a scale-down still draining. Yellow, not red: this is a
    ///    transient, not a fault. k9s flattens this into (2) — its hidden
    ///    VALID column compares `status.replicas` against
    ///    `status.availableReplicas`, so it paints the row red for the whole
    ///    rollout.
    /// 4. Otherwise `Normal`.
    ///
    /// Clause 3 is the *only* indication during the window where a surge pod
    /// has been created but is not yet ready: at one replica `READY` still
    /// reads `1/1` and `UP-TO-DATE` still reads `1`. Colouring that window is
    /// the point — it is the state no number in the row can carry.
    pub(crate) fn health(&self) -> row::RowHealth {
        use row::RowHealth;
        if self.desired == 0 {
            match self.at_zero {
                AtZero::Parked => RowHealth::Pending,
                AtZero::Retired => RowHealth::Normal,
            }
        } else if self.ready < self.desired {
            RowHealth::Failed
        } else if self.rollout_in_flight() {
            RowHealth::Pending
        } else {
            RowHealth::Normal
        }
    }
}

#[cfg(test)]
#[path = "../../tests/kube/resources.rs"]
mod tests;
