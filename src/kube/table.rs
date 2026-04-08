//! K8s server-side Table format support for column discovery.
//!
//! Instead of hardcoding column headers for each resource type, we make a
//! one-shot Table API call to ask the server what columns it wants to display.
//! This is how kubectl gets its column definitions.

use kube::Client;
use serde::Deserialize;

/// Column definition from the K8s Table API response.
#[derive(Debug, Clone, Deserialize)]
pub struct ColumnDefinition {
    pub name: String,
    #[serde(rename = "type")]
    pub type_: String,
    #[serde(default)]
    pub priority: i32,
}

/// Minimal Table API response — we only care about column definitions.
#[derive(Debug, Deserialize)]
struct TableResponse {
    #[serde(rename = "columnDefinitions")]
    #[serde(default)]
    column_definitions: Vec<ColumnDefinition>,
}

/// Fetch column definitions from the K8s Table API for a given resource.
/// Makes a single LIST call with `limit=1` and `includeObject=None` to minimize data transfer.
pub async fn fetch_table_columns(
    client: &Client,
    group: &str,
    version: &str,
    plural: &str,
    namespace: Option<&str>,
) -> anyhow::Result<Vec<ColumnDefinition>> {
    let base = if group.is_empty() {
        format!("/api/{}", version)
    } else {
        format!("/apis/{}/{}", group, version)
    };

    let path = if let Some(ns) = namespace {
        format!("{}/namespaces/{}/{}?limit=1&includeObject=None", base, ns, plural)
    } else {
        format!("{}/{}?limit=1&includeObject=None", base, plural)
    };

    let request = http::Request::get(&path)
        .header(
            "Accept",
            "application/json;as=Table;v=v1;g=meta.k8s.io",
        )
        .body(vec![])
        .map_err(|e| anyhow::anyhow!("Failed to build Table request: {}", e))?;

    let response: TableResponse = client.request(request).await?;
    Ok(response.column_definitions)
}

/// Convert server-provided column definitions to header strings.
/// Filters by priority: 0 = always shown, >0 = only in wide mode.
pub fn columns_to_headers(columns: &[ColumnDefinition], wide: bool) -> Vec<String> {
    columns
        .iter()
        .filter(|c| wide || c.priority == 0)
        .map(|c| c.name.to_uppercase())
        .collect()
}
