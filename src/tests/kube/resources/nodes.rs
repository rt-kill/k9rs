use super::*;
use crate::kube::resource_def::{ConvertToRow, ResourceDef};

/// The three parallel lists — `default_headers`, `column_defs`, and the
/// per-row `cells` vec — must stay positionally aligned. A drift silently
/// renders data under the wrong header. This guards all three.
#[test]
fn node_headers_columns_cells_aligned() {
    let headers = NodeDef.default_headers();
    let cols = NodeDef.column_defs();
    assert_eq!(headers.len(), cols.len(), "default_headers vs column_defs length");
    for (h, c) in headers.iter().zip(&cols) {
        assert_eq!(h.as_str(), c.header, "header text mismatch with column_defs");
    }
    let row = NodeDef::convert(k8s_openapi::api::core::v1::Node::default());
    assert_eq!(row.cells.len(), headers.len(), "cells vec must align with headers");
}
