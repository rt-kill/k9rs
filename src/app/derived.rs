//! Projection functions for derived views.
//!
//! Each [`DerivedViewKind`] variant has a corresponding `project_*` function
//! that converts a parent [`ResourceRow`] into child rows for the derived
//! table. Adding a new derived view means adding a function here and a
//! match arm in [`DerivedViewKind::project`].

use crate::kube::resources::row::{CellValue, ContainerInfo, ContainerKind, ResourceRow, RowHealth};

/// Classify container health from ready state, status string, and restart
/// count. Single source of truth — used for both the row health AND the
/// Status cell health so they can't diverge.
fn container_health(c: &ContainerInfo) -> RowHealth {
    if c.ready {
        RowHealth::Normal
    } else if c.status == "Running" {
        // Running but not ready — starting up.
        RowHealth::Pending
    } else if c.status == "Completed" {
        RowHealth::Normal
    } else if c.restart_count > 0 {
        // Not ready + has restarts → likely crash-looping.
        RowHealth::Failed
    } else {
        // Not ready, no restarts — waiting to start or terminated cleanly.
        RowHealth::Pending
    }
}

/// Project a pod's containers into individual table rows.
pub(crate) fn project_containers(parent: &ResourceRow) -> Vec<ResourceRow> {
    parent.containers.iter().map(|c| {
        let display_name = match c.kind {
            ContainerKind::Init => format!("init:{}", c.name),
            ContainerKind::Regular => c.name.clone(),
        };

        let health = container_health(c);

        let cells = vec![
            CellValue::Text(display_name.clone()),
            CellValue::Text(c.image.clone()),
            CellValue::Status { text: c.status.clone(), health },
            CellValue::Bool(c.ready),
            CellValue::Count(c.restart_count as i64),
        ];

        ResourceRow {
            name: c.name.clone(),
            namespace: parent.namespace.clone(),
            cells,
            health,
            ..Default::default()
        }
    }).collect()
}
