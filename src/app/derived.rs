//! Projection functions for derived views.
//!
//! Each [`crate::kube::resources::row::DerivedViewKind`] variant has a corresponding `project_*` function
//! that converts a parent [`ResourceRow`] into child rows for the derived
//! table. Adding a new derived view means adding a function here and a
//! match arm in [`DerivedViewKind::project`](crate::app::view::DerivedViewKind::project).

use crate::kube::resources::row::{CellValue, ResourceRow};

/// Project a pod's containers into individual table rows.
pub(crate) fn project_containers(parent: &ResourceRow) -> Vec<ResourceRow> {
    parent.containers.iter().map(|c| {
        let display_name = c.display_name();

        let health = c.health;

        let cells = vec![
            CellValue::Text(display_name.clone()),
            CellValue::Text(c.image.clone()),
            CellValue::Status { text: c.status_label().to_string(), health },
            CellValue::Bool(c.ready),
            CellValue::Count(c.restart_count as i64),
        ];

        ResourceRow {
            name: c.name.clone(),
            namespace: parent.namespace.clone(),
            cells,
            health,
            // A container runs wherever its pod runs, so `o` (show node)
            // means the same thing one level down. This is the ONLY
            // non-pod row that gets a node, and it gets one because the
            // answer is carried in, not inferred: `parent` IS the pod.
            node: parent.node.clone(),
            ..Default::default()
        }
    }).collect()
}
