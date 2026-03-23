use std::borrow::Cow;

use chrono::{DateTime, Utc};
use k8s_openapi::api::core::v1::Event;

use crate::util::format_age;
use super::KubeResource;

#[derive(Debug, Clone)]
pub struct KubeEvent {
    pub namespace: String,
    pub last_seen: String,
    pub event_type: String,
    pub reason: String,
    pub involved_object: String,
    pub message: String,
    pub source: String,
    pub count: i32,
    pub age: Option<DateTime<Utc>>,
}

impl KubeResource for KubeEvent {
    fn headers() -> &'static [&'static str] {
        &[
            "NAMESPACE",
            "TYPE",
            "REASON",
            "OBJECT",
            "MESSAGE",
            "SOURCE",
            "COUNT",
            "AGE",
        ]
    }

    fn row(&self) -> Vec<Cow<'_, str>> {
        vec![
            Cow::Borrowed(&self.namespace),
            Cow::Borrowed(&self.event_type),
            Cow::Borrowed(&self.reason),
            Cow::Borrowed(&self.involved_object),
            Cow::Borrowed(&self.message),
            Cow::Borrowed(&self.source),
            Cow::Owned(self.count.to_string()),
            Cow::Owned(format_age(self.age)),
        ]
    }

    fn name(&self) -> &str {
        &self.involved_object
    }

    fn namespace(&self) -> &str {
        &self.namespace
    }

    fn kind() -> &'static str {
        "event"
    }
}

impl From<Event> for KubeEvent {
    fn from(ev: Event) -> Self {
        let metadata = ev.metadata;
        let namespace = metadata.namespace.unwrap_or_default();
        let age = metadata.creation_timestamp.map(|t| t.0);

        let event_type = ev.type_.unwrap_or_default();
        let reason = ev.reason.unwrap_or_default();
        let message = ev.message.unwrap_or_default();
        let count = ev.count.unwrap_or(0);

        // last_seen: prefer last_timestamp, fall back to event_time
        let last_seen = ev
            .last_timestamp
            .map(|t| format_age(Some(t.0)))
            .or_else(|| ev.event_time.map(|t| format_age(Some(t.0))))
            .unwrap_or_else(|| format_age(age));

        // Object: "kind/name"
        let obj = ev.involved_object;
        let kind = obj.kind.unwrap_or_default();
        let obj_name = obj.name.unwrap_or_default();
        let involved_object = if kind.is_empty() {
            obj_name
        } else {
            format!("{}/{}", kind.to_lowercase(), obj_name)
        };

        // Source: component
        let source = ev
            .source
            .and_then(|s| s.component)
            .unwrap_or_default();

        KubeEvent {
            namespace,
            last_seen,
            event_type,
            reason,
            involved_object,
            message,
            source,
            count,
            age,
        }
    }
}
