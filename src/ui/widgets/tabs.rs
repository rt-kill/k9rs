use ratatui::{
    buffer::Buffer,
    layout::Rect,
    widgets::Widget,
};

use unicode_width::UnicodeWidthStr;

use crate::kube::protocol::ResourceId;
use crate::ui::theme::Theme;

/// Breadcrumb bar showing the current resource type and namespace.
///
/// Instead of a numbered tab list, this renders breadcrumbs like:
///   < pods > < default >
/// Active crumb: black on orange, inactive: black on steelblue.
/// Navigation is via `:` commands and number hotkeys.
pub struct TabBar<'a> {
    active: &'a ResourceId,
    namespace: &'a str,
    item_count: Option<usize>,
    theme: &'a Theme,
}

impl<'a> TabBar<'a> {
    pub fn new(
        active: &'a ResourceId,
        theme: &'a Theme,
    ) -> Self {
        Self {
            active,
            namespace: "",
            item_count: None,
            theme,
        }
    }

    pub fn namespace(mut self, ns: &'a str) -> Self {
        self.namespace = ns;
        self
    }

    pub fn item_count(mut self, count: usize) -> Self {
        self.item_count = Some(count);
        self
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
        let resource_label = self.active.short_label().to_lowercase();
        let crumb_style = self.theme.breadcrumb_active;

        // " <pods> "
        if x < max_x {
            buf.set_string(x, area.y, " ", self.theme.breadcrumb_inactive);
            x += 1;
        }
        let crumb_text = if let Some(count) = self.item_count {
            format!("<{}[{}]>", resource_label, count)
        } else {
            format!("<{}>", resource_label)
        };
        // Width-bounded, char-boundary-safe truncation (a byte-index slice on
        // a multi-byte label would panic), and advance by display width.
        let crumb = crate::util::truncate_to_width(&crumb_text, max_x.saturating_sub(x) as usize);
        if !crumb.is_empty() {
            buf.set_string(x, area.y, crumb, crumb_style);
            x += UnicodeWidthStr::width(crumb) as u16;
        }

        // Namespace breadcrumb (inactive: black on steelblue)
        if !self.namespace.is_empty() {
            let ns_style = self.theme.breadcrumb_inactive;

            if x < max_x {
                buf.set_string(x, area.y, " ", ns_style);
                x += 1;
            }
            let ns_text = format!("<{}>", self.namespace);
            let ns = crate::util::truncate_to_width(&ns_text, max_x.saturating_sub(x) as usize);
            if !ns.is_empty() {
                buf.set_string(x, area.y, ns, ns_style);
                x += UnicodeWidthStr::width(ns) as u16;
            }
        }

        // Fill remaining space with status bar background
        while x < max_x {
            buf.set_string(x, area.y, " ", self.theme.status_bar);
            x += 1;
        }
    }
}
