use ratatui::{
    layout::{Constraint, Layout, Rect},
    text::{Line, Span},
    widgets::Paragraph,
    Frame,
};

use unicode_width::UnicodeWidthStr;

use k9rs_app::{App, Attachment};
use k9rs_theme::Theme;

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
/// It no longer decides WHICH context that is. That is
/// [`Attachment`]'s job, in the model, because the
/// identity drawn beneath this string has to describe the same context and
/// used to be derived independently. All that is left here is the wording.
pub fn context_label(at: &Attachment) -> String {
    match at {
        // Nothing is being attempted and nothing will be until the user picks
        // one. Saying "connecting…" here is the same lie the connecting screen
        // exists to stop telling.
        Attachment::NoContext => "none — press Enter to select".to_string(),
        Attachment::Live(a) => a.context().to_string(),
        Attachment::Connecting(a) => format!("{} (connecting…)", a.context()),
        Attachment::Opening => "connecting...".to_string(),
    }
}

/// How an unknown [`ClusterIdentity`](k9rs_proto::protocol::ClusterIdentity)
/// field reads. Same word the table side uses — `CellValue::Placeholder`
/// renders exactly this — so a context with no `user:` says the same thing
/// wherever you look at it.
pub const UNKNOWN_FIELD: &str = "n/a";

/// k9rs logo on the right. No key hints — those live in the ? help dialog.
/// The four identity fields, each value elided from the LEFT to fit
/// `value_w` columns.
///
/// ONE projection for the header and the overview. Both already start from
/// `app.attachment()` — that half was unified after the header named one
/// context above another's cluster — but they then FITTED the values
/// independently. Elision has to go leftward here: these are identifiers
/// whose distinguishing part is at the END, and `aws eks update-kubeconfig`
/// without `--alias` writes a context of
/// `arn:aws:eks:us-west-2:123456789012:cluster/prod-usw2`, where every
/// column before `prod-usw2` is shared with every other context you have.
/// The overview handed its raw strings to a centred `Line`, which clips on
/// the RIGHT — so the landing page showed four copies of one ARN prefix,
/// which is precisely the bug `the_header_keeps_the_telling_end_of_an_arn`
/// pins for the header.
///
/// Field NAMES, not formatted labels: the header pads them into a column and
/// the overview centres each line, so the padding is the caller's.
pub fn identity_fields(
    at: &k9rs_app::Attachment,
    value_w: usize,
) -> [(&'static str, String); 4] {
    let id = at.identity();
    let elide = |v: &str| k9rs_util::width::truncate_left(v, value_w).into_owned();
    [
        ("Context", elide(&context_label(at))),
        ("Cluster", elide(id.cluster().unwrap_or(UNKNOWN_FIELD))),
        ("User", elide(id.user().unwrap_or(UNKNOWN_FIELD))),
        ("K8s", elide(id.k8s_version().unwrap_or(UNKNOWN_FIELD))),
    ]
}

pub fn draw_header(
    f: &mut Frame,
    app: &App,
    area: Rect,
    theme: &Theme,
) {
    if area.height == 0 || area.width == 0 {
        return;
    }

    // ONE resolution, so the name and the three lines beneath it cannot come
    // from different arms of different matches.
    let at = app.attachment();

    let logo_width = LOGO.iter().map(|l| l.len()).max().unwrap_or(0) as u16 + 2;
    let cols = Layout::horizontal([
        Constraint::Fill(1),
        Constraint::Length(logo_width),
    ]).split(area);

    // Left: context / cluster / user / k8s version stacked vertically.
    //
    // Values elided from the left — see `identity_fields`, which the
    // overview shares.
    let label_w = " Context: ".width();
    let value_w = (cols[0].width as usize).saturating_sub(label_w);
    let info = Paragraph::new(
        identity_fields(&at, value_w)
            .into_iter()
            .map(|(field, value)| {
                // Padded into a column: ` Context: `, ` User:    `.
                let label = format!(" {field}:{:width$}", "", width = 8 - field.len() - 1);
                Line::from(vec![
                    Span::styled(label, theme.info_label),
                    Span::styled(value, theme.info_value),
                ])
            })
            .collect::<Vec<_>>(),
    );
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

#[cfg(test)]
#[path = "tests/header.rs"]
mod tests;
