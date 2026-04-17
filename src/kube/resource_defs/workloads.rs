//! Workload resource definitions: Pod, Deployment, StatefulSet, DaemonSet,
//! ReplicaSet, Job, CronJob.

use crate::kube::protocol::ResourceScope;
use crate::kube::resource_def::*;
use crate::kube::resources::row::ResourceRow;

use k8s_openapi::api::apps::v1::{DaemonSet, Deployment, ReplicaSet, StatefulSet};
use k8s_openapi::api::batch::v1::{CronJob, Job};
use k8s_openapi::api::core::v1::Pod;

// ---------------------------------------------------------------------------
// Pod
// ---------------------------------------------------------------------------

pub struct PodDef;

impl ResourceDef for PodDef {
    fn kind(&self) -> BuiltInKind { BuiltInKind::Pod }
    fn gvr(&self) -> &'static Gvr {
        const G: Gvr = Gvr {
            group: "", version: "v1", kind: "Pod",
            plural: "pods", scope: ResourceScope::Namespaced,
        };
        &G
    }
    fn aliases(&self) -> &[&str] { &["po", "pod", "pods"] }
    fn short_label(&self) -> &str { "Pods" }
    fn default_headers(&self) -> Vec<String> {
        ["NAMESPACE", "NAME", "READY", "STATUS", "RESTARTS", "LAST RESTART",
         "CPU", "MEM", "IP", "NODE", "QOS", "SERVICE-ACCOUNT",
         "READINESS GATES", "LABELS", "AGE"]
            .into_iter().map(String::from).collect()
    }
    fn metrics_kind(&self) -> Option<MetricsKind> { Some(MetricsKind::Pod) }

    fn column_defs(&self) -> Vec<ColumnDef> {
        use ColumnDef as C;
        use MetricsColumn::*;
        vec![
            C::new("NAMESPACE"), C::new("NAME"), C::new("READY"), C::new("STATUS"),
            C::new("RESTARTS"), C::extra_age("LAST RESTART"),
            C::extra("CPU").with_metrics(Cpu), C::extra("MEM").with_metrics(Mem),
            C::new("IP"), C::extra("NODE"), C::extra("QOS"),
            C::extra("SERVICE-ACCOUNT"), C::extra("READINESS GATES"),
            C::extra("LABELS"), C::age("AGE"),
        ]
    }

    fn is_loggable(&self) -> bool { true }
    fn is_shellable(&self) -> bool { true }
    fn is_show_nodeable(&self) -> bool { true }
    fn is_port_forwardable(&self) -> bool { true }
}

impl ConvertToRow<Pod> for PodDef {
    fn convert(obj: Pod) -> ResourceRow {
        crate::kube::resources::pods::pod_to_row(obj)
    }
}

// ---------------------------------------------------------------------------
// Deployment
// ---------------------------------------------------------------------------

pub struct DeploymentDef;

impl ResourceDef for DeploymentDef {
    fn kind(&self) -> BuiltInKind { BuiltInKind::Deployment }
    fn gvr(&self) -> &'static Gvr {
        const G: Gvr = Gvr {
            group: "apps", version: "v1", kind: "Deployment",
            plural: "deployments", scope: ResourceScope::Namespaced,
        };
        &G
    }
    fn aliases(&self) -> &[&str] { &["dp", "deploy", "deployment", "deployments"] }
    fn short_label(&self) -> &str { "Deploy" }
    fn default_headers(&self) -> Vec<String> {
        ["NAMESPACE", "NAME", "READY", "UP-TO-DATE", "AVAILABLE",
         "CONTAINERS", "IMAGES", "LABELS", "AGE"]
            .into_iter().map(String::from).collect()
    }

    fn is_loggable(&self) -> bool { true }
    fn is_scaleable(&self) -> bool { true }
    fn is_restartable(&self) -> bool { true }
    fn is_port_forwardable(&self) -> bool { true }
}

impl ConvertToRow<Deployment> for DeploymentDef {
    fn convert(obj: Deployment) -> ResourceRow {
        crate::kube::resources::deployments::deployment_to_row(obj)
    }
}

// ---------------------------------------------------------------------------
// StatefulSet
// ---------------------------------------------------------------------------

pub struct StatefulSetDef;

impl ResourceDef for StatefulSetDef {
    fn kind(&self) -> BuiltInKind { BuiltInKind::StatefulSet }
    fn gvr(&self) -> &'static Gvr {
        const G: Gvr = Gvr {
            group: "apps", version: "v1", kind: "StatefulSet",
            plural: "statefulsets", scope: ResourceScope::Namespaced,
        };
        &G
    }
    fn aliases(&self) -> &[&str] { &["sts", "statefulset", "statefulsets"] }
    fn short_label(&self) -> &str { "STS" }
    fn default_headers(&self) -> Vec<String> {
        ["NAMESPACE", "NAME", "READY", "SERVICE", "CONTAINERS", "IMAGES", "LABELS", "AGE"]
            .into_iter().map(String::from).collect()
    }

    fn is_loggable(&self) -> bool { true }
    fn is_scaleable(&self) -> bool { true }
    fn is_restartable(&self) -> bool { true }
    fn is_port_forwardable(&self) -> bool { true }
}

impl ConvertToRow<StatefulSet> for StatefulSetDef {
    fn convert(obj: StatefulSet) -> ResourceRow {
        crate::kube::resources::statefulsets::statefulset_to_row(obj)
    }
}

// ---------------------------------------------------------------------------
// DaemonSet
// ---------------------------------------------------------------------------

pub struct DaemonSetDef;

impl ResourceDef for DaemonSetDef {
    fn kind(&self) -> BuiltInKind { BuiltInKind::DaemonSet }
    fn gvr(&self) -> &'static Gvr {
        const G: Gvr = Gvr {
            group: "apps", version: "v1", kind: "DaemonSet",
            plural: "daemonsets", scope: ResourceScope::Namespaced,
        };
        &G
    }
    fn aliases(&self) -> &[&str] { &["ds", "daemonset", "daemonsets"] }
    fn short_label(&self) -> &str { "DS" }
    fn default_headers(&self) -> Vec<String> {
        ["NAMESPACE", "NAME", "DESIRED", "CURRENT", "READY", "UP-TO-DATE",
         "AVAILABLE", "NODE SELECTOR", "LABELS", "AGE"]
            .into_iter().map(String::from).collect()
    }

    fn is_loggable(&self) -> bool { true }
    fn is_restartable(&self) -> bool { true }
    fn is_port_forwardable(&self) -> bool { true }
}

impl ConvertToRow<DaemonSet> for DaemonSetDef {
    fn convert(obj: DaemonSet) -> ResourceRow {
        crate::kube::resources::daemonsets::daemonset_to_row(obj)
    }
}

// ---------------------------------------------------------------------------
// ReplicaSet
// ---------------------------------------------------------------------------

pub struct ReplicaSetDef;

impl ResourceDef for ReplicaSetDef {
    fn kind(&self) -> BuiltInKind { BuiltInKind::ReplicaSet }
    fn gvr(&self) -> &'static Gvr {
        const G: Gvr = Gvr {
            group: "apps", version: "v1", kind: "ReplicaSet",
            plural: "replicasets", scope: ResourceScope::Namespaced,
        };
        &G
    }
    fn aliases(&self) -> &[&str] { &["rs", "replicaset", "replicasets"] }
    fn short_label(&self) -> &str { "RS" }
    fn default_headers(&self) -> Vec<String> {
        ["NAMESPACE", "NAME", "DESIRED", "CURRENT", "READY", "LABELS", "AGE"]
            .into_iter().map(String::from).collect()
    }

    fn is_loggable(&self) -> bool { true }
    fn is_scaleable(&self) -> bool { true }
    fn is_port_forwardable(&self) -> bool { true }
}

impl ConvertToRow<ReplicaSet> for ReplicaSetDef {
    fn convert(obj: ReplicaSet) -> ResourceRow {
        crate::kube::resources::replicasets::replicaset_to_row(obj)
    }
}

// ---------------------------------------------------------------------------
// Job
// ---------------------------------------------------------------------------

pub struct JobDef;

impl ResourceDef for JobDef {
    fn kind(&self) -> BuiltInKind { BuiltInKind::Job }
    fn gvr(&self) -> &'static Gvr {
        const G: Gvr = Gvr {
            group: "batch", version: "v1", kind: "Job",
            plural: "jobs", scope: ResourceScope::Namespaced,
        };
        &G
    }
    fn aliases(&self) -> &[&str] { &["job", "jobs"] }
    fn short_label(&self) -> &str { "Jobs" }
    fn default_headers(&self) -> Vec<String> {
        ["NAMESPACE", "NAME", "COMPLETIONS", "DURATION", "CONTAINERS", "IMAGES", "LABELS", "AGE"]
            .into_iter().map(String::from).collect()
    }

    fn is_loggable(&self) -> bool { true }
    fn is_port_forwardable(&self) -> bool { true }
}

impl ConvertToRow<Job> for JobDef {
    fn convert(obj: Job) -> ResourceRow {
        crate::kube::resources::jobs::job_to_row(obj)
    }
}

// ---------------------------------------------------------------------------
// CronJob
// ---------------------------------------------------------------------------

pub struct CronJobDef;

impl ResourceDef for CronJobDef {
    fn kind(&self) -> BuiltInKind { BuiltInKind::CronJob }
    fn gvr(&self) -> &'static Gvr {
        const G: Gvr = Gvr {
            group: "batch", version: "v1", kind: "CronJob",
            plural: "cronjobs", scope: ResourceScope::Namespaced,
        };
        &G
    }
    fn aliases(&self) -> &[&str] { &["cj", "cronjob", "cronjobs"] }
    fn short_label(&self) -> &str { "CronJobs" }
    fn default_headers(&self) -> Vec<String> {
        ["NAMESPACE", "NAME", "SCHEDULE", "SUSPEND", "ACTIVE", "LAST SCHEDULE",
         "CONTAINERS", "IMAGES", "LABELS", "AGE"]
            .into_iter().map(String::from).collect()
    }

    fn is_loggable(&self) -> bool { true }
}

impl ConvertToRow<CronJob> for CronJobDef {
    fn convert(obj: CronJob) -> ResourceRow {
        crate::kube::resources::cronjobs::cronjob_to_row(obj)
    }
}
