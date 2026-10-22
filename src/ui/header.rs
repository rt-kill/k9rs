use ratatui::{
    layout::{Constraint, Layout, Rect},
    text::{Line, Span},
    widgets::Paragraph,
    Frame,
};

use unicode_width::UnicodeWidthStr;

use crate::app::App;
use crate::theme::Theme;

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
/// How an unknown [`ClusterIdentity`](crate::kube::protocol::ClusterIdentity)
/// field reads. Same word the table side uses — `CellValue::Placeholder`
/// renders exactly this — so a context with no `user:` says the same thing
/// wherever you look at it.
pub const UNKNOWN_FIELD: &str = "n/a";

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
    let cluster = id.cluster().unwrap_or(UNKNOWN_FIELD);
    let user = id.user().unwrap_or(UNKNOWN_FIELD);
    let k8s_ver = id.k8s_version().unwrap_or(UNKNOWN_FIELD);

    let logo_width = LOGO.iter().map(|l| l.len()).max().unwrap_or(0) as u16 + 2;
    let cols = Layout::horizontal([
        Constraint::Fill(1),
        Constraint::Length(logo_width),
    ]).split(area);

    // Left: context / cluster / user / k8s version stacked vertically.
    //
    // Each value is elided from the LEFT, because these are identifiers whose
    // distinguishing part is at the END: `aws eks update-kubeconfig` without
    // `--alias` writes a context of
    // `arn:aws:eks:us-west-2:123456789012:cluster/prod-usw2`, where every
    // column before `prod-usw2` is boilerplate shared with every other
    // context you have. The `Paragraph` clipped it on the right instead, so
    // below ~80 columns the four lines read as four copies of the same ARN
    // prefix — same reasoning as the nav breadcrumb, which is also a path.
    let label_w = " Context: ".width();
    let value_w = (cols[0].width as usize).saturating_sub(label_w);
    let elide = |v: &str| crate::util::truncate_left(v, value_w).into_owned();
    let info = Paragraph::new(vec![
        Line::from(vec![
            Span::styled(" Context: ", theme.info_label),
            Span::styled(elide(&ctx), theme.info_value),
        ]),
        Line::from(vec![
            Span::styled(" Cluster: ", theme.info_label),
            Span::styled(elide(cluster), theme.info_value),
        ]),
        Line::from(vec![
            Span::styled(" User:    ", theme.info_label),
            Span::styled(elide(user), theme.info_value),
        ]),
        Line::from(vec![
            Span::styled(" K8s:     ", theme.info_label),
            Span::styled(elide(k8s_ver), theme.info_value),
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

/// Fit a one-line bar into `width`, dropping whole groups from the right and
/// saying that it did.
///
/// A bar is a sequence of GROUPS — a separator, a label, a value — and a
/// group is indivisible: half of `<q> back` is worse than none of it. So the
/// unit of dropping is the group, and an `…` marks that any were dropped.
///
/// Shared, because this is the one mistake both bars made. ratatui truncates
/// a `Line` at the right edge in silence, so the log hint bar (115 columns
/// against a 97-column terminal) lost `1h`, `24h` and `back` — the last being
/// the key that gets you out — and the log STATUS bar could lose `Since` and
/// `Pan` the same way at 80. Each surface measuring its own content was how
/// one of them got a fix and the other did not.
pub fn fit_bar(groups: Vec<Vec<Span<'static>>>, width: u16, theme: &Theme) -> Line<'static> {
    let width = width as usize;
    let mut spans: Vec<Span<'static>> = Vec::new();
    let mut used = 0usize;
    let mut dropped = false;
    for (i, group) in groups.iter().enumerate() {
        let cost: usize = group.iter().map(|s| s.content.width()).sum();
        // Keep a column for the `…` unless this is the last group.
        let reserve = usize::from(i + 1 < groups.len());
        if used + cost + reserve > width {
            dropped = true;
            break;
        }
        spans.extend(group.iter().cloned());
        used += cost;
    }
    if dropped {
        spans.push(Span::styled("\u{2026}", theme.status_bar));
    }
    Line::from(spans)
}

/// Build a keybinding bar `Line` from a list of `(key, description)` pairs.
/// Used by describe, yaml, log, and context views for their bottom status bars.
pub fn render_keybinding_bar(
    hints: &[(impl AsRef<str>, impl AsRef<str>)],
    width: u16,
    theme: &Theme,
) -> Line<'static> {
    let groups: Vec<Vec<Span<'static>>> = hints
        .iter()
        .enumerate()
        .map(|(i, (key, desc))| {
            // One column of lead-in each: a space before the first, a `│`
            // between the rest.
            let lead = if i == 0 { " " } else { "\u{2502}" };
            vec![
                Span::styled(lead, theme.status_bar),
                Span::styled(format!("<{}>", key.as_ref()), theme.status_bar_key),
                Span::styled(format!(" {} ", desc.as_ref()), theme.status_bar),
            ]
        })
        .collect();
    fit_bar(groups, width, theme)
}
