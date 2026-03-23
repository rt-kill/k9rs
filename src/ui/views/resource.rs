use std::collections::HashSet;

use ratatui::{
    layout::{Constraint, Layout, Rect},
    style::Modifier,
    text::{Line, Span},
    widgets::Block,
    Frame,
};

use crate::app::{App, ResourceTab, StatefulTable};
use crate::kube::resources::KubeResource;
use crate::ui::header;
use crate::ui::theme::Theme;
use crate::ui::widgets::{FilterBar, ResourceTable, ResourceTableState, TabBar};

// ---------------------------------------------------------------------------
// Key hints (displayed in header center panel)
// ---------------------------------------------------------------------------

fn key_hints_for_tab(tab: ResourceTab) -> Vec<(&'static str, &'static str)> {
    let mut hints = vec![
        (":", "cmd"),
        ("/", "filter"),
        ("d", "desc"),
        ("y", "yaml"),
        ("Ctrl-d", "del"),
        ("?", "help"),
    ];
    match tab {
        ResourceTab::Pods => {
            hints.insert(4, ("l", "logs"));
            hints.insert(5, ("s", "shell"));
            hints.insert(6, ("f", "pf"));
            hints.insert(7, ("p", "prev-logs"));
            hints.insert(8, ("o", "node"));
        }
        ResourceTab::Deployments | ResourceTab::StatefulSets => {
            hints.insert(4, ("l", "logs"));
            hints.insert(5, ("r", "restart"));
            hints.insert(6, ("s", "scale"));
        }
        ResourceTab::DaemonSets => {
            hints.insert(4, ("l", "logs"));
            hints.insert(5, ("r", "restart"));
        }
        ResourceTab::ReplicaSets => {
            hints.insert(4, ("l", "logs"));
            hints.insert(5, ("s", "scale"));
        }
        ResourceTab::Jobs | ResourceTab::CronJobs => {
            hints.insert(4, ("l", "logs"));
        }
        ResourceTab::Services => {}
        ResourceTab::Crds | ResourceTab::DynamicResource => {}
        _ => {}
    }
    hints.push(("Space", "mark"));
    hints.push(("q", "quit"));
    hints
}

// ---------------------------------------------------------------------------
// Render a generic resource table given headers and row data.
// ---------------------------------------------------------------------------

fn draw_resource_table<T: Clone>(
    f: &mut Frame,
    area: Rect,
    title: &str,
    headers: Vec<&str>,
    rows: &[Vec<String>],
    table: &StatefulTable<T>,
    namespace: &str,
    filter_text: &str,
    theme: &Theme,
    marked_filtered: &HashSet<usize>,
    display_sort_col: Option<usize>,
) {
    let rt = ResourceTable::new(headers, rows, title, theme)
        .sort(display_sort_col, table.sort_ascending)
        .namespace(namespace)
        .filter_text(filter_text)
        .marked(marked_filtered);

    let mut state = ResourceTableState {
        selected: table.selected,
        offset: table.offset,
        filtered_count: 0,
    };

    f.render_stateful_widget(rt, area, &mut state);
}

/// Helper: draw a resource table for any StatefulTable<T> where T: KubeResource
fn draw_typed_table<T: Clone + KubeResource>(
    f: &mut Frame,
    area: Rect,
    title: &str,
    table: &StatefulTable<T>,
    namespace: &str,
    filter_text: &str,
    theme: &Theme,
    _tick_count: usize,
) {
    // Append loading indicator to title when still receiving initial data
    let display_title = if table.loading {
        format!("{} (loading...)", title)
    } else {
        title.to_string()
    };
    let title = &display_title;

    // When viewing a specific namespace (not "all"), hide the NAMESPACE column
    // since it's redundant. Cluster-scoped resources (nodes, namespaces, PVs,
    // storage classes, cluster roles, cluster role bindings) don't have a
    // NAMESPACE column, so the check on headers().first() handles them naturally.
    let skip_ns = namespace != "all"
        && !namespace.is_empty()
        && T::headers().first() == Some(&"NAMESPACE");

    let headers: Vec<&str> = if skip_ns {
        T::headers()[1..].to_vec()
    } else {
        T::headers().to_vec()
    };

    // Show a spinner + message when the table has no items.
    // Use has_data to distinguish "still loading" from "genuinely empty."
    if table.items.is_empty() {
        let loading_text = if table.has_data {
            // Data was received before but is now empty (e.g. all resources deleted)
            format!("No {} found.", title.to_lowercase())
        } else {
            // Never received data yet — show spinner
            crate::util::loading_bar("Loading...")
        };
        let loading_line = Line::from(Span::styled(loading_text, theme.info_value));
        // Draw the bordered block with title so it looks consistent
        let title_ns = if !namespace.is_empty() {
            format!(" {}({})[0]", title.to_lowercase(), namespace)
        } else {
            format!(" {}[0]", title.to_lowercase())
        };
        let block = Block::bordered()
            .title(Span::styled(title_ns, theme.title))
            .border_style(theme.border);
        let inner = block.inner(area);
        f.render_widget(block, area);
        if inner.height > 0 && inner.width > 0 {
            // Center the loading text vertically
            let center_y = inner.y + inner.height / 2;
            let center_x = inner.x
                + inner.width.saturating_sub(loading_line.width() as u16) / 2;
            f.render_widget(
                loading_line,
                Rect::new(center_x, center_y, inner.width, 1),
            );
        }
        return;
    }

    let rows: Vec<Vec<String>> = table
        .filtered_indices
        .iter()
        .filter_map(|&i| table.items.get(i))
        .map(|r| {
            let row: Vec<String> = r.row().into_iter().map(|c| c.into_owned()).collect();
            if skip_ns && !row.is_empty() {
                row[1..].to_vec()
            } else {
                row
            }
        })
        .collect();

    // Convert marked real indices to filtered-list positions for the table widget
    let marked_filtered: HashSet<usize> = table.filtered_indices
        .iter()
        .enumerate()
        .filter(|(_pos, &real_idx)| table.marked.contains(&real_idx))
        .map(|(pos, _)| pos)
        .collect();

    // Adjust the sort column index when the NAMESPACE column is stripped so
    // the sort indicator points at the correct rendered column.
    let display_sort_col = if skip_ns {
        if table.sort_column == 0 { None } else { Some(table.sort_column - 1) }
    } else {
        Some(table.sort_column)
    };

    draw_resource_table(f, area, title, headers, &rows, table, namespace, filter_text, theme, &marked_filtered, display_sort_col);
}

// ---------------------------------------------------------------------------
// Main entry point: draw_resources
// ---------------------------------------------------------------------------

/// Draw the main resource browser view matching k9s layout:
///
/// ```text
/// +--header (7 lines)------------------------------------------+
/// | cluster info  |  key hints  |  k9rs logo                   |
/// +--command prompt (3 lines, optional)-------------------------+
/// | :pods                                                       |
/// +--filter (3 lines, optional)---------------------------------+
/// | /filter_text                                                |
/// +--table (fills remaining)------------------------------------+
/// | pods(default)[42]  </:filter>                               |
/// | NAMESPACE  NAME  ...                                        |
/// +--breadcrumbs (1 line)--------------------------------------+
/// +--flash (1 line)--------------------------------------------+
/// ```
pub fn draw_resources(f: &mut Frame, app: &App, area: Rect) {
    let theme = &app.theme;

    // Determine dynamic section heights
    let header_height: u16 = if app.show_header { 7 } else { 0 };
    let command_height: u16 = if app.command_mode || app.scale_mode { 3 } else { 0 };
    // Only show the filter bar box while actively typing; when committed
    // (inactive but text non-empty), the table title shows `</:filter_text>`.
    let filter_visible = app.filter.active;
    let filter_height: u16 = if filter_visible { 3 } else { 0 };

    let chunks = Layout::vertical([
        Constraint::Length(header_height),      // header
        Constraint::Length(command_height),     // command prompt
        Constraint::Length(filter_height),      // filter
        Constraint::Fill(1),                   // table
        Constraint::Length(1),                 // breadcrumbs
        Constraint::Length(1),                 // flash
    ])
    .split(area);

    let header_area = chunks[0];
    let command_area = chunks[1];
    let filter_area = chunks[2];
    let table_area = chunks[3];
    let breadcrumb_area = chunks[4];
    let flash_area = chunks[5];

    // 1. Header section: 3 columns (only when visible)
    if app.show_header {
        let tab = app.resource_tab;
        header::draw_header(f, app, header_area, theme, |f, area, theme| {
            draw_key_hints(f, tab, area, theme);
        });
    }

    // 2. Command prompt (only when command mode active)
    if app.command_mode || app.scale_mode {
        draw_command_prompt(f, app, command_area, theme);
    }

    // 3. Filter prompt
    if filter_visible {
        let (filtered, total) = app.active_table_items_count();
        let match_count = if app.filter.text.is_empty() { total } else { filtered };
        let filter_bar = FilterBar::new(
            app.filter.active,
            &app.filter.text,
            match_count,
            total,
            theme,
        );
        f.render_widget(filter_bar, filter_area);
    }

    // 4. Resource table
    let ns = &app.selected_ns;
    // Use the per-table filter text (not the global app.filter.text) so the
    // filter indicator in the table title is correct after switching tabs.
    let ft = app.active_filter_text();
    let tc = app.tick_count;
    match app.resource_tab {
        ResourceTab::Pods => draw_typed_table(f, table_area, "Pods", &app.data.pods, ns, ft, theme, tc),
        ResourceTab::Deployments => draw_typed_table(f, table_area, "Deployments", &app.data.deployments, ns, ft, theme, tc),
        ResourceTab::Services => draw_typed_table(f, table_area, "Services", &app.data.services, ns, ft, theme, tc),
        ResourceTab::StatefulSets => draw_typed_table(f, table_area, "StatefulSets", &app.data.statefulsets, ns, ft, theme, tc),
        ResourceTab::DaemonSets => draw_typed_table(f, table_area, "DaemonSets", &app.data.daemonsets, ns, ft, theme, tc),
        ResourceTab::Jobs => draw_typed_table(f, table_area, "Jobs", &app.data.jobs, ns, ft, theme, tc),
        ResourceTab::CronJobs => draw_typed_table(f, table_area, "CronJobs", &app.data.cronjobs, ns, ft, theme, tc),
        ResourceTab::ConfigMaps => draw_typed_table(f, table_area, "ConfigMaps", &app.data.configmaps, ns, ft, theme, tc),
        ResourceTab::Secrets => draw_typed_table(f, table_area, "Secrets", &app.data.secrets, ns, ft, theme, tc),
        ResourceTab::Nodes => draw_typed_table(f, table_area, "Nodes", &app.data.nodes, ns, ft, theme, tc),
        ResourceTab::Namespaces => draw_typed_table(f, table_area, "Namespaces", &app.data.namespaces, ns, ft, theme, tc),
        ResourceTab::Ingresses => draw_typed_table(f, table_area, "Ingresses", &app.data.ingresses, ns, ft, theme, tc),
        ResourceTab::ReplicaSets => draw_typed_table(f, table_area, "ReplicaSets", &app.data.replicasets, ns, ft, theme, tc),
        ResourceTab::Pvs => draw_typed_table(f, table_area, "PVs", &app.data.pvs, ns, ft, theme, tc),
        ResourceTab::Pvcs => draw_typed_table(f, table_area, "PVCs", &app.data.pvcs, ns, ft, theme, tc),
        ResourceTab::StorageClasses => draw_typed_table(f, table_area, "StorageClasses", &app.data.storage_classes, ns, ft, theme, tc),
        ResourceTab::ServiceAccounts => draw_typed_table(f, table_area, "ServiceAccounts", &app.data.service_accounts, ns, ft, theme, tc),
        ResourceTab::NetworkPolicies => draw_typed_table(f, table_area, "NetworkPolicies", &app.data.network_policies, ns, ft, theme, tc),
        ResourceTab::Events => draw_typed_table(f, table_area, "Events", &app.data.events, ns, ft, theme, tc),
        ResourceTab::Roles => draw_typed_table(f, table_area, "Roles", &app.data.roles, ns, ft, theme, tc),
        ResourceTab::ClusterRoles => draw_typed_table(f, table_area, "ClusterRoles", &app.data.cluster_roles, ns, ft, theme, tc),
        ResourceTab::RoleBindings => draw_typed_table(f, table_area, "RoleBindings", &app.data.role_bindings, ns, ft, theme, tc),
        ResourceTab::ClusterRoleBindings => draw_typed_table(f, table_area, "ClusterRoleBindings", &app.data.cluster_role_bindings, ns, ft, theme, tc),
        ResourceTab::Hpa => draw_typed_table(f, table_area, "HPA", &app.data.hpa, ns, ft, theme, tc),
        ResourceTab::Endpoints => draw_typed_table(f, table_area, "Endpoints", &app.data.endpoints, ns, ft, theme, tc),
        ResourceTab::LimitRanges => draw_typed_table(f, table_area, "LimitRanges", &app.data.limit_ranges, ns, ft, theme, tc),
        ResourceTab::ResourceQuotas => draw_typed_table(f, table_area, "ResourceQuotas", &app.data.resource_quotas, ns, ft, theme, tc),
        ResourceTab::Pdb => draw_typed_table(f, table_area, "PDB", &app.data.pdb, ns, ft, theme, tc),
        ResourceTab::Crds => draw_typed_table(f, table_area, "CRDs", &app.data.crds, ns, ft, theme, tc),
        ResourceTab::DynamicResource => {
            let title = &app.dynamic_resource_name;
            let display_title = if title.is_empty() { "Dynamic" } else { title };
            draw_typed_table(f, table_area, display_title, &app.data.dynamic_resources, ns, ft, theme, tc);
        }
    }

    // 5. Breadcrumbs
    let all_tabs = ResourceTab::all();
    let tab_bar = TabBar::new(all_tabs, app.resource_tab, 0, theme)
        .namespace(&app.selected_ns);
    f.render_widget(tab_bar, breadcrumb_area);

    // 6. Flash message area (handled by ui/mod.rs overlay, but we reserve the line)
    // Draw a subtle flash area background
    if flash_area.width > 0 && flash_area.height > 0 {
        let empty = Line::raw("");
        f.render_widget(empty, flash_area);
    }
}

// ---------------------------------------------------------------------------
// Header: key hints center panel (delegates to shared header module)
// ---------------------------------------------------------------------------

/// Center panel: compact key hint grid for the resource view.
fn draw_key_hints(f: &mut Frame, tab: ResourceTab, area: Rect, theme: &Theme) {
    let hints = key_hints_for_tab(tab);
    header::draw_key_hint_grid(f, area, &hints, theme);
}

// ---------------------------------------------------------------------------
// Command prompt
// ---------------------------------------------------------------------------

fn draw_command_prompt(f: &mut Frame, app: &App, area: Rect, theme: &Theme) {
    if area.height < 3 || area.width == 0 {
        return;
    }

    let block = Block::bordered()
        .border_style(theme.border_focused);
    let inner = block.inner(area);
    f.render_widget(block, area);

    if inner.height == 0 || inner.width == 0 {
        return;
    }

    let input = &app.command_input;
    let ghost = app.best_completion()
        .and_then(|c| {
            if c.len() > input.len() {
                Some(c[input.len()..].to_string())
            } else {
                None
            }
        })
        .unwrap_or_default();

    // Fish-style rendering: typed text (bright) followed immediately by ghost
    // text (dim/italic) with no block cursor in between. The terminal cursor
    // is placed right after the typed text via set_cursor_position.
    let prefix = if app.scale_mode { "Replicas: " } else { ":" };
    let prefix_len: u16 = prefix.len() as u16;
    let typed_len = input.len() as u16;

    let mut spans = vec![
        Span::styled(prefix, theme.command.add_modifier(Modifier::BOLD)),
        Span::styled(input, theme.command),
    ];

    if !ghost.is_empty() && !app.scale_mode {
        spans.push(Span::styled(
            ghost,
            theme.command_suggestion.add_modifier(Modifier::DIM | Modifier::ITALIC),
        ));
    }

    let line = Line::from(spans);
    f.render_widget(line, inner);

    // Place the real terminal cursor right after the typed text (thin line /
    // blinking bar depending on the user's terminal emulator settings).
    let cursor_x = inner.x + prefix_len + typed_len;
    let cursor_y = inner.y;
    if cursor_x < inner.x + inner.width {
        f.set_cursor_position((cursor_x, cursor_y));
    }
}

