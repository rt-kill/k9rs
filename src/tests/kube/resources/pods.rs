use super::*;
use crate::kube::resource_def::{ConvertToRow, ResourceDef};

/// Pods are the highest-risk converter: 24 columns and a metrics overlay
/// that writes cells *by index* derived from `column_defs()`. Guard that
/// the `cells` vec aligns with the headers so a one-off drift can't silently
/// land a CPU value under the wrong column. (The headers↔column_defs half is
/// covered registry-wide in `resource_defs::registry::tests`.)
#[test]
fn pod_cells_align_with_headers() {
    let row = PodDef::convert(k8s_openapi::api::core::v1::Pod::default());
    assert_eq!(
        row.cells.len(),
        PodDef.default_headers().len(),
        "pod cells vec must align with default_headers",
    );
}
