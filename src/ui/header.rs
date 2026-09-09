use ratatui::{
    layout::{Constraint, Layout, Rect},
    text::{Line, Span},
    widgets::Paragraph,
    Frame,
};

use crate::app::App;
use crate::ui::theme::Theme;

// ---------------------------------------------------------------------------
// k9rs ASCII art logo (rendered in orange)
// ---------------------------------------------------------------------------

pub const LOGO: &[&str] = &[
    r" _     ___            ",
    r"| | __/ _ \ _ __ ___  ",
    r"| |/ / (_) | '__/ __| ",
    r"|   < \__, | |  \__ \ ",
    r"|_|\_\  /_/|_|  |___/ ",
];

// ---------------------------------------------------------------------------
// Header: cluster info (left), key hints (center), logo (right)
// ---------------------------------------------------------------------------

/// Compact header: context/cluster/user stacked vertically on the left,
/// What the chrome should call the active context.
///
/// ONE function because the header and the overview both need it and both
/// used to work it out separately — which is how the picker came to say
/// "connecting…" while nothing was connecting.
///
/// A context name only means something together with the link state, and
/// there are three distinct stories the field has to tell:
/// - **No context at all.** Nothing is being attempted and nothing will be
///   until the user picks one. Saying "connecting…" here is the same lie the
///   connecting screen exists to stop telling.
/// - **A switch in flight.** `kube.context` is the LAST-CONFIRMED context,
///   kept deliberately so a failed switch can fall back to it — but right
///   now we are attached to neither it nor the target. Naming it reads as
///   "you are on prod" when prod is precisely what you just left, so name
///   the TARGET instead: that is what the screen is waiting for.
/// - **Otherwise** the confirmed context, marked while the link is down.
pub fn context_label(app: &App) -> String {
    use crate::app::Liveness;
    let connecting_to = |name: &dyn std::fmt::Display| format!("{} (connecting…)", name);
    match Liveness::of_link(&app.conn) {
        Liveness::NoContext => "none — press Enter to select".to_string(),
        live if live.shows_data() => match &app.kube.context {
            Some(c) => c.to_string(),
            // Linked but nothing confirmed yet: the initial handshake.
            None => "connecting...".to_string(),
        },
        // Link down. What we are AIMING at outranks what we last confirmed:
        // a switch target first, then the session's candidate from disk, and
        // only then the last-confirmed name.
        _ => match (
            app.kube.context_switch.target(),
            app.kube.connecting.as_ref(),
            &app.kube.context,
        ) {
            (Some(target), _, _) => connecting_to(target),
            (None, Some((candidate, _)), _) => connecting_to(candidate),
            (None, None, Some(c)) => connecting_to(c),
            (None, None, None) => "connecting...".to_string(),
        },
    }
}

/// The cluster/user/version to display beside [`context_label`].
///
/// While the link is down, `kube.identity` still describes the context we are
/// LEAVING — so pairing it with the target's name reads as "you are on
/// staging" above prod's cluster and user. The candidate carries the
/// kubeconfig's own view of the target, which is the honest thing to show
/// until the daemon confirms.
pub fn display_identity(app: &App) -> &crate::kube::protocol::ClusterIdentity {
    match (
        crate::app::Liveness::of_link(&app.conn).shows_data(),
        app.kube.connecting.as_ref(),
    ) {
        (false, Some((_, identity))) => identity,
        _ => &app.kube.identity,
    }
}

/// k9rs logo on the right. No key hints — those live in the ? help dialog.
pub fn draw_header(
    f: &mut Frame,
    app: &App,
    area: Rect,
    theme: &Theme,
) {
    if area.height == 0 || area.width == 0 {
        return;
    }

    let ctx = context_label(app);
    let id = display_identity(app);
    let cluster = if id.cluster.is_empty() { "n/a" } else { &id.cluster };
    let user = if id.user.is_empty() { "n/a" } else { &id.user };
    let k8s_ver = if id.k8s_version.is_empty() { "n/a" } else { &id.k8s_version };

    let logo_width = LOGO.iter().map(|l| l.len()).max().unwrap_or(0) as u16 + 2;
    let cols = Layout::horizontal([
        Constraint::Fill(1),
        Constraint::Length(logo_width),
    ]).split(area);

    // Left: context / cluster / user / k8s version stacked vertically.
    let info = Paragraph::new(vec![
        Line::from(vec![
            Span::styled(" Context: ", theme.info_label),
            Span::styled(ctx, theme.info_value),
        ]),
        Line::from(vec![
            Span::styled(" Cluster: ", theme.info_label),
            Span::styled(cluster, theme.info_value),
        ]),
        Line::from(vec![
            Span::styled(" User:    ", theme.info_label),
            Span::styled(user, theme.info_value),
        ]),
        Line::from(vec![
            Span::styled(" K8s:     ", theme.info_label),
            Span::styled(k8s_ver, theme.info_value),
        ]),
    ]);
    f.render_widget(info, cols[0]);

    // Right: k9rs logo.
    let logo_lines: Vec<Line> = LOGO.iter()
        .map(|l| Line::from(Span::styled(*l, theme.logo)))
        .collect();
    let logo = Paragraph::new(logo_lines)
        .alignment(ratatui::layout::Alignment::Right);
    f.render_widget(logo, cols[1]);
}

/// Build a keybinding bar `Line` from a list of `(key, description)` pairs.
/// Used by describe, yaml, log, and context views for their bottom status bars.
pub fn render_keybinding_bar(hints: &[(&str, &str)], theme: &Theme) -> Line<'static> {
    let mut spans = Vec::new();
    spans.push(Span::styled(" ", theme.status_bar));
    for (i, (key, desc)) in hints.iter().enumerate() {
        spans.push(Span::styled(format!("<{}>", key), theme.status_bar_key));
        spans.push(Span::styled(format!(" {} ", desc), theme.status_bar));
        if i < hints.len() - 1 {
            spans.push(Span::styled("\u{2502}", theme.status_bar));
        }
    }
    Line::from(spans)
}
