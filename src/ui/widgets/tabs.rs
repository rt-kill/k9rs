use ratatui::{
    buffer::Buffer,
    layout::Rect,
    widgets::Widget,
};

use crate::app::ResourceTab;
use crate::ui::theme::Theme;

/// k9s-style breadcrumb bar showing the current resource type and namespace.
///
/// Instead of a numbered tab list, this renders breadcrumbs like:
///   < pods > < default >
/// Active crumb: black on orange, inactive: black on steelblue.
/// Navigation is via `:` commands and number hotkeys.
pub struct TabBar<'a> {
    #[allow(dead_code)]
    tabs: &'a [ResourceTab],
    active: ResourceTab,
    namespace: &'a str,
    #[allow(dead_code)]
    offset: usize,
    theme: &'a Theme,
}

impl<'a> TabBar<'a> {
    pub fn new(
        tabs: &'a [ResourceTab],
        active: ResourceTab,
        offset: usize,
        theme: &'a Theme,
    ) -> Self {
        Self {
            tabs,
            active,
            offset,
            namespace: "",
            theme,
        }
    }

    pub fn namespace(mut self, ns: &'a str) -> Self {
        self.namespace = ns;
        self
    }

    /// Calculate the offset needed to make the active tab visible.
    /// Kept for API compat but breadcrumbs don't need scrolling.
    pub fn visible_offset(_tabs: &[ResourceTab], _active: ResourceTab, _area_width: u16) -> usize {
        0
    }
}

impl Widget for TabBar<'_> {
    fn render(self, area: Rect, buf: &mut Buffer) {
        if area.height == 0 || area.width == 0 {
            return;
        }

        let mut x = area.x;
        let max_x = area.x + area.width;

        // Resource type breadcrumb (active: black on orange)
        let resource_label = self.active.label().to_lowercase();
        let crumb_style = self.theme.breadcrumb_active;

        // " <pods> "
        if x < max_x {
            buf.set_string(x, area.y, " ", self.theme.breadcrumb_inactive);
            x += 1;
        }
        let crumb_text = format!("<{}>", resource_label);
        let crumb_len = crumb_text.len().min((max_x - x) as usize);
        if crumb_len > 0 {
            buf.set_string(x, area.y, &crumb_text[..crumb_len], crumb_style);
            x += crumb_len as u16;
        }

        // Namespace breadcrumb (inactive: black on steelblue)
        if !self.namespace.is_empty() {
            let ns_style = self.theme.breadcrumb_inactive;

            if x < max_x {
                buf.set_string(x, area.y, " ", ns_style);
                x += 1;
            }
            let ns_text = format!("<{}>", self.namespace);
            let ns_len = ns_text.len().min((max_x - x) as usize);
            if ns_len > 0 {
                buf.set_string(x, area.y, &ns_text[..ns_len], ns_style);
                x += ns_len as u16;
            }
        }

        // Fill remaining space with status bar background
        while x < max_x {
            buf.set_string(x, area.y, " ", self.theme.status_bar);
            x += 1;
        }
    }
}
