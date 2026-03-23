use ratatui::{
    buffer::Buffer,
    layout::{Constraint, Layout, Rect},
    text::{Line, Span},
    widgets::{Block, Clear, Padding, Widget},
};

use crate::ui::theme::Theme;

/// A keybinding entry for the help overlay.
struct HelpEntry {
    key: &'static str,
    description: &'static str,
}

/// A section of keybindings.
struct HelpSection {
    title: &'static str,
    entries: Vec<HelpEntry>,
}

/// Help overlay widget styled like k9s.
/// Clean two-column layout: key in blue, description in white.
/// Rendered as a modal overlay, dismissable with ? or Esc.
pub struct HelpOverlay<'a> {
    theme: &'a Theme,
    scroll: usize,
}

impl<'a> HelpOverlay<'a> {
    pub fn new(theme: &'a Theme, scroll: usize) -> Self {
        Self { theme, scroll }
    }

    fn sections() -> Vec<HelpSection> {
        vec![
            HelpSection {
                title: "Navigation",
                entries: vec![
                    HelpEntry {
                        key: "j / \u{2193}",
                        description: "Move down",
                    },
                    HelpEntry {
                        key: "k / \u{2191}",
                        description: "Move up",
                    },
                    HelpEntry {
                        key: "PgDn / PgUp",
                        description: "Page down / up",
                    },
                    HelpEntry {
                        key: "g",
                        description: "Home / top",
                    },
                    HelpEntry {
                        key: "G",
                        description: "End / bottom",
                    },
                    HelpEntry {
                        key: "Esc",
                        description: "Back / clear filter",
                    },
                    HelpEntry {
                        key: "-",
                        description: "Toggle last view",
                    },
                ],
            },
            HelpSection {
                title: "Actions",
                entries: vec![
                    HelpEntry {
                        key: "Enter",
                        description: "View / drill-down",
                    },
                    HelpEntry {
                        key: "d",
                        description: "Describe resource",
                    },
                    HelpEntry {
                        key: "y",
                        description: "View YAML",
                    },
                    HelpEntry {
                        key: "e",
                        description: "Edit resource",
                    },
                    HelpEntry {
                        key: "Ctrl-d",
                        description: "Delete resource",
                    },
                    HelpEntry {
                        key: "Ctrl-k",
                        description: "Force-kill (pods)",
                    },
                    HelpEntry {
                        key: "Ctrl-r",
                        description: "Refresh",
                    },
                    HelpEntry {
                        key: "f",
                        description: "Port forward (pods/deploy/sts/ds/svc)",
                    },
                    HelpEntry {
                        key: "c",
                        description: "Copy",
                    },
                    HelpEntry {
                        key: "Space",
                        description: "Mark / select row",
                    },
                    HelpEntry {
                        key: "O",
                        description: "Sort (toggle direction)",
                    },
                    HelpEntry {
                        key: "N",
                        description: "Sort by name",
                    },
                    HelpEntry {
                        key: "A",
                        description: "Sort by age",
                    },
                    HelpEntry {
                        key: "S",
                        description: "Sort by status (pods)",
                    },
                ],
            },
            HelpSection {
                title: "Pods & Workloads",
                entries: vec![
                    HelpEntry {
                        key: "l",
                        description: "View logs",
                    },
                    HelpEntry {
                        key: "s",
                        description: "Shell (pods) / Scale",
                    },
                    HelpEntry {
                        key: "p",
                        description: "Previous logs (--previous)",
                    },
                    HelpEntry {
                        key: "o",
                        description: "Show node (pods)",
                    },
                ],
            },
            HelpSection {
                title: "Deploy/STS/DS",
                entries: vec![
                    HelpEntry {
                        key: "r",
                        description: "Restart (deploy/sts/ds)",
                    },
                    HelpEntry {
                        key: "s",
                        description: "Scale (deploy/sts/rs)",
                    },
                ],
            },
            HelpSection {
                title: "Commands",
                entries: vec![
                    HelpEntry {
                        key: ":",
                        description: "Command mode",
                    },
                    HelpEntry {
                        key: "/",
                        description: "Filter",
                    },
                    HelpEntry {
                        key: "q",
                        description: "Back / clear filter",
                    },
                    HelpEntry {
                        key: "Ctrl-c",
                        description: "Quit",
                    },
                    HelpEntry {
                        key: "Ctrl-e",
                        description: "Toggle header",
                    },
                    HelpEntry {
                        key: "Ctrl-s",
                        description: "Save table to file",
                    },
                    HelpEntry {
                        key: "Ctrl-w",
                        description: "Toggle wide mode",
                    },
                    HelpEntry {
                        key: "Ctrl-z",
                        description: "Toggle fault filter",
                    },
                    HelpEntry {
                        key: "Ctrl-a",
                        description: "Show aliases",
                    },
                    HelpEntry {
                        key: "?",
                        description: "Help",
                    },
                    HelpEntry {
                        key: ":ctx",
                        description: "Switch context",
                    },
                    HelpEntry {
                        key: ":ns name",
                        description: "Switch namespace",
                    },
                ],
            },
            HelpSection {
                title: "Log View",
                entries: vec![
                    HelpEntry {
                        key: "s",
                        description: "Toggle follow",
                    },
                    HelpEntry {
                        key: "w",
                        description: "Toggle wrap",
                    },
                    HelpEntry {
                        key: "t",
                        description: "Toggle timestamps",
                    },
                    HelpEntry {
                        key: "Shift-C",
                        description: "Clear logs",
                    },
                    HelpEntry {
                        key: "0-6",
                        description: "Set log time range (0:tail, 1:1m, 2:5m, 3:15m, 4:30m, 5:1h, 6:24h)",
                    },
                    HelpEntry {
                        key: "q",
                        description: "Back",
                    },
                ],
            },
            HelpSection {
                title: "Detail Views (YAML/Describe)",
                entries: vec![
                    HelpEntry {
                        key: "Ctrl-d",
                        description: "Half-page down",
                    },
                    HelpEntry {
                        key: "Ctrl-u",
                        description: "Half-page up",
                    },
                    HelpEntry {
                        key: "/",
                        description: "Search",
                    },
                    HelpEntry {
                        key: "n",
                        description: "Next search match",
                    },
                    HelpEntry {
                        key: "N",
                        description: "Prev search match",
                    },
                ],
            },
        ]
    }

    fn centered_rect(area: Rect, percent_x: u16, percent_y: u16) -> Rect {
        let vert = Layout::vertical([
            Constraint::Percentage((100 - percent_y) / 2),
            Constraint::Percentage(percent_y),
            Constraint::Percentage((100 - percent_y) / 2),
        ])
        .split(area);

        let horiz = Layout::horizontal([
            Constraint::Percentage((100 - percent_x) / 2),
            Constraint::Percentage(percent_x),
            Constraint::Percentage((100 - percent_x) / 2),
        ])
        .split(vert[1]);

        horiz[1]
    }
}

impl Widget for HelpOverlay<'_> {
    fn render(self, area: Rect, buf: &mut Buffer) {
        let dialog_area = Self::centered_rect(area, 70, 80);

        // Clear background
        Clear.render(dialog_area, buf);

        let block = Block::bordered()
            .title(" Help - Press ? or Esc to close ")
            .title_style(self.theme.title)
            .border_style(self.theme.border)
            .style(self.theme.dialog_bg)
            .padding(Padding::new(2, 2, 1, 1));

        let inner = block.inner(dialog_area);
        block.render(dialog_area, buf);

        if inner.height == 0 || inner.width == 0 {
            return;
        }

        let sections = Self::sections();
        let max_y = inner.y + inner.height;

        // Pre-build all logical lines for scroll support
        let mut all_lines: Vec<(Line<'_>, Option<(Line<'_>, u16)>)> = Vec::new();
        let col_width = inner.width / 2;

        for (si, section) in sections.iter().enumerate() {
            // Section title
            let title_line = Line::from(Span::styled(
                format!("--- {} ---", section.title),
                self.theme.title,
            ));
            all_lines.push((title_line, None));

            // Two-column layout
            let entries_per_col = (section.entries.len() + 1) / 2;
            for row in 0..entries_per_col {
                let left = if row < section.entries.len() {
                    let entry = &section.entries[row];
                    Line::from(vec![
                        Span::styled(
                            format!("{:<14}", entry.key),
                            self.theme.help_key,
                        ),
                        Span::styled(entry.description, self.theme.help_desc),
                    ])
                } else {
                    Line::raw("")
                };

                let right = {
                    let right_idx = row + entries_per_col;
                    if right_idx < section.entries.len() {
                        let entry = &section.entries[right_idx];
                        Some((Line::from(vec![
                            Span::styled(
                                format!("{:<14}", entry.key),
                                self.theme.help_key,
                            ),
                            Span::styled(entry.description, self.theme.help_desc),
                        ]), col_width))
                    } else {
                        None
                    }
                };

                all_lines.push((left, right));
            }

            // Blank line between sections (except after last)
            if si + 1 < sections.len() {
                all_lines.push((Line::raw(""), None));
            }
        }

        // Clamp scroll
        let total = all_lines.len();
        let visible = inner.height as usize;
        let scroll = self.scroll.min(total.saturating_sub(visible.max(1)));

        let mut y = inner.y;
        for (left, right) in all_lines.iter().skip(scroll) {
            if y >= max_y {
                break;
            }
            buf.set_line(inner.x, y, left, col_width);
            if let Some((ref r, cw)) = right {
                buf.set_line(inner.x + *cw, y, r, *cw);
            }
            y += 1;
        }
    }
}
