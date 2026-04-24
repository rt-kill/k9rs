use ratatui::{
    buffer::Buffer,
    layout::Rect,
    style::Modifier,
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

/// Help overlay widget.
/// Clean two-column layout: key in blue, description in white.
/// Rendered as a modal overlay, dismissable with ? or Esc.
pub struct HelpOverlay<'a> {
    theme: &'a Theme,
    scroll: usize,
    caps: Option<crate::kube::protocol::ResourceCapabilities>,
}

impl<'a> HelpOverlay<'a> {
    pub fn new(theme: &'a Theme, scroll: usize, caps: Option<crate::kube::protocol::ResourceCapabilities>) -> Self {
        Self { theme, scroll, caps }
    }

    /// Total rendered line count for the help content (section titles +
    /// entries + blank separators between sections). Used internally by
    /// [`Self::max_scroll`].
    fn total_lines(&self) -> usize {
        let sections = self.sections();
        let mut total = 0usize;
        for (si, section) in sections.iter().enumerate() {
            total += 1; // section title
            total += section.entries.len();
            if si + 1 < sections.len() {
                total += 1; // blank separator
            }
        }
        total
    }

    /// Maximum sensible value for `help_scroll` given the current terminal
    /// height. Mirrors the render-time clamp at line 350 so action handlers
    /// can store a stable max instead of `usize::MAX` (which makes PrevItem
    /// decrements appear to do nothing for ~`visible_height` keystrokes
    /// before they overcome the difference).
    ///
    /// Returns 0 if the content fits without scrolling.
    pub fn max_scroll(terminal_height: u16, caps: Option<&crate::kube::protocol::ResourceCapabilities>) -> usize {
        // Dialog is `centered_rect(area, 42, 85)` — 85% of terminal height.
        // Block overhead is 3 rows (2 borders + 1 top pad). See render.
        let dialog_height = (terminal_height as usize) * 85 / 100;
        let visible_height = dialog_height.saturating_sub(3).max(1);
        // Compute total lines from a temporary instance. The theme is
        // only needed for rendering, not for counting lines, so we use
        // a stack-local theme whose lifetime is confined to this call.
        let theme = Theme::load();
        let total = {
            let tmp = HelpOverlay { theme: &theme, scroll: 0, caps: caps.cloned() };
            tmp.total_lines()
        };
        total.saturating_sub(visible_height)
    }

    fn sections(&self) -> Vec<HelpSection> {
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
                    HelpEntry {
                        key: "0",
                        description: "All namespaces",
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
                ],
            },
            HelpSection {
                title: "Sorting",
                entries: vec![
                    HelpEntry {
                        key: "Shift-O",
                        description: "Sort / toggle direction",
                    },
                    HelpEntry {
                        key: "Shift-N",
                        description: "Sort by name",
                    },
                    HelpEntry {
                        key: "Shift-A",
                        description: "Sort by age",
                    },
                    HelpEntry {
                        key: "Shift-S",
                        description: "Sort by status (pods)",
                    },
                ],
            },
            self.resource_actions_section(),
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
                        key: "Ctrl-l",
                        description: "Toggle full-fetch mode",
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

    /// Build the "Resource Actions" section dynamically from the current
    /// resource's capabilities. Falls back to a generic section listing
    /// all possible resource-specific keys when no caps are provided.
    fn resource_actions_section(&self) -> HelpSection {
        use crate::kube::protocol::OperationKind;

        let mut entries = Vec::new();

        if let Some(ref caps) = self.caps {
            if caps.supports(OperationKind::StreamLogs) {
                entries.push(HelpEntry { key: "Shift-L", description: "View logs" });
            }
            if caps.supports(OperationKind::Shell) {
                entries.push(HelpEntry { key: "s", description: "Shell" });
            }
            if caps.supports(OperationKind::Scale) {
                entries.push(HelpEntry { key: "s", description: "Scale" });
            }
            if caps.supports(OperationKind::Restart) {
                entries.push(HelpEntry { key: "r", description: "Restart" });
            }
            if caps.supports(OperationKind::ShowNode) {
                entries.push(HelpEntry { key: "o", description: "Show node" });
            }
            if caps.supports(OperationKind::PreviousLogs) {
                entries.push(HelpEntry { key: "p", description: "Previous logs" });
            }
            if caps.supports(OperationKind::DecodeSecret) {
                entries.push(HelpEntry { key: "x", description: "Decode" });
            }
            if caps.supports(OperationKind::ForceKill) {
                entries.push(HelpEntry { key: "Ctrl-k", description: "Force kill" });
            }
            if caps.supports(OperationKind::PortForward) {
                entries.push(HelpEntry { key: "f", description: "Port forward" });
            }
            if caps.supports(OperationKind::NodeShell) {
                entries.push(HelpEntry { key: "s", description: "Node shell" });
            }
            if caps.supports(OperationKind::TriggerCronJob) {
                entries.push(HelpEntry { key: "t", description: "Trigger job" });
            }
            if caps.supports(OperationKind::ToggleSuspendCronJob) {
                entries.push(HelpEntry { key: "s", description: "Toggle suspend" });
            }
        } else {
            // No caps available — show a generic combined section
            entries.push(HelpEntry { key: "Shift-L", description: "View logs" });
            entries.push(HelpEntry { key: "s", description: "Shell / Scale" });
            entries.push(HelpEntry { key: "r", description: "Restart" });
            entries.push(HelpEntry { key: "p", description: "Previous logs" });
            entries.push(HelpEntry { key: "o", description: "Show node" });
            entries.push(HelpEntry { key: "f", description: "Port forward" });
        }

        HelpSection {
            title: "Resource Actions",
            entries,
        }
    }

}

impl Widget for HelpOverlay<'_> {
    fn render(self, area: Rect, buf: &mut Buffer) {
        // Percentage-based centering (42% width, 85% height).
        let w = (area.width as u32 * 42 / 100) as u16;
        let h = (area.height as u32 * 85 / 100) as u16;
        let dialog_area = crate::ui::centered_rect(area, w, h);

        // Clear + guaranteed-visible bg (consistent with ModalOverlay).
        Clear.render(dialog_area, buf);
        crate::ui::fill_dialog_bg(buf, dialog_area);

        // Build all lines first to know total count
        let sections = self.sections();
        let mut all_lines: Vec<Line<'_>> = Vec::new();

        for (si, section) in sections.iter().enumerate() {
            // Section title — highlighted
            all_lines.push(Line::from(Span::styled(
                format!("  {} ", section.title),
                self.theme.title.add_modifier(Modifier::BOLD),
            )));

            // Single-column layout — one entry per line, easy to read top-to-bottom
            for entry in &section.entries {
                all_lines.push(Line::from(vec![
                    Span::styled(
                        format!("  {:<14} ", entry.key),
                        self.theme.help_key,
                    ),
                    Span::styled(entry.description, self.theme.help_desc),
                ]));
            }

            // Blank line between sections
            if si + 1 < sections.len() {
                all_lines.push(Line::raw(""));
            }
        }

        let total = all_lines.len();

        // Clamp scroll BEFORE formatting the title — Action::End sets
        // help_scroll to usize::MAX as a sentinel, so `scroll + 1` would
        // overflow if we used `self.scroll` directly.
        //
        // Block overhead is 2 border rows + 1 top-padding row + 0 bottom-
        // padding row = 3 (matches `Padding::new(1, 1, 1, 0)` below).
        let visible_height = dialog_area.height.saturating_sub(3) as usize;
        let has_more = total > visible_height;
        let scroll = self.scroll.min(total.saturating_sub(visible_height.max(1)));
        let title = if has_more {
            format!(" Help [j/k to scroll] [{}/{}] ", scroll + 1, total)
        } else {
            " Help — press ? or Esc to close ".to_string()
        };

        let block = Block::bordered()
            .title(title)
            .title_style(self.theme.title)
            .border_style(self.theme.border)
            .style(self.theme.dialog_bg)
            .padding(Padding::new(1, 1, 1, 0));

        let inner = block.inner(dialog_area);
        block.render(dialog_area, buf);

        if inner.height == 0 || inner.width == 0 {
            return;
        }

        let max_y = inner.y + inner.height;
        let visible = inner.height as usize;

        let mut y = inner.y;
        for line in all_lines.iter().skip(scroll) {
            if y >= max_y {
                break;
            }
            buf.set_line(inner.x, y, line, inner.width);
            y += 1;
        }

        // Show scroll indicator arrows at the edges
        if scroll > 0 {
            buf.set_string(
                inner.x + inner.width.saturating_sub(3),
                inner.y,
                " ▲ ",
                self.theme.title,
            );
        }
        if scroll + visible < total {
            buf.set_string(
                inner.x + inner.width.saturating_sub(3),
                max_y.saturating_sub(1),
                " ▼ ",
                self.theme.title,
            );
        }
    }
}
