use std::path::Path;

use ratatui::style::{Color, Modifier, Style};
use serde::Deserialize;

// ---------------------------------------------------------------------------
// Default color palette (approximate RGB values)
// ---------------------------------------------------------------------------

/// DodgerBlue: #1E90FF
const DODGER_BLUE: Color = Color::Rgb(30, 144, 255);
/// Aqua/Cyan: #00FFFF
const AQUA: Color = Color::Rgb(0, 255, 255);
/// Orange: #FFA500
const ORANGE: Color = Color::Rgb(255, 165, 0);
/// Fuchsia/Magenta: #FF00FF
const FUCHSIA: Color = Color::Rgb(255, 0, 255);
/// PapayaWhip: #FFEFD5
const PAPAYA_WHIP: Color = Color::Rgb(255, 239, 213);
/// SteelBlue: #4682B4
const STEEL_BLUE: Color = Color::Rgb(70, 130, 180);
/// LawnGreen: #7CFC00
const LAWN_GREEN: Color = Color::Rgb(124, 252, 0);
/// OrangeRed: #FF4500
const ORANGE_RED: Color = Color::Rgb(255, 69, 0);
/// DarkOrange: #FF8C00
const DARK_ORANGE: Color = Color::Rgb(255, 140, 0);
/// CadetBlue: #5F9EA0
const CADET_BLUE: Color = Color::Rgb(95, 158, 160);

// ---------------------------------------------------------------------------
// Helpers to parse colors from skin YAML
// ---------------------------------------------------------------------------

/// Parse a color string from a skin YAML value.
/// Supports hex colors (#RRGGBB), named colors, and "default" (mapped to Reset).
fn parse_color(s: &str) -> Option<Color> {
    let s = s.trim();
    if s == "default" || s.is_empty() {
        return Some(Color::Reset);
    }
    if s.starts_with('#') && s.len() == 7 {
        let r = u8::from_str_radix(&s[1..3], 16).ok()?;
        let g = u8::from_str_radix(&s[3..5], 16).ok()?;
        let b = u8::from_str_radix(&s[5..7], 16).ok()?;
        return Some(Color::Rgb(r, g, b));
    }
    match s.to_lowercase().as_str() {
        "black" => Some(Color::Black),
        "red" => Some(Color::Red),
        "green" => Some(Color::Green),
        "yellow" => Some(Color::Yellow),
        "blue" => Some(Color::Blue),
        "magenta" => Some(Color::Magenta),
        "cyan" => Some(Color::Cyan),
        "white" => Some(Color::White),
        "darkgray" | "darkgrey" => Some(Color::DarkGray),
        "lightred" => Some(Color::LightRed),
        "lightgreen" => Some(Color::LightGreen),
        "lightyellow" => Some(Color::LightYellow),
        "lightblue" => Some(Color::LightBlue),
        "lightmagenta" => Some(Color::LightMagenta),
        "lightcyan" => Some(Color::LightCyan),
        "gray" | "grey" => Some(Color::Gray),
        _ => None,
    }
}

/// Helper: set fg on a style if the color is present.
fn with_fg(style: Style, color: Option<Color>) -> Style {
    match color {
        Some(c) => style.fg(c),
        None => style,
    }
}

/// Helper: set bg on a style if the color is present.
fn with_bg(style: Style, color: Option<Color>) -> Style {
    match color {
        Some(c) => style.bg(c),
        None => style,
    }
}

// ---------------------------------------------------------------------------
// Serde-based skin YAML schema
// ---------------------------------------------------------------------------

/// A color value from a skin YAML. Deserializes from a string like "#FF0000",
/// "red", or "default".
#[derive(Debug, Clone)]
struct SkinColor(Color);

impl SkinColor {
    fn get(&self) -> Color {
        self.0
    }
}

impl<'de> serde::Deserialize<'de> for SkinColor {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let s = String::deserialize(d)?;
        parse_color(&s)
            .map(SkinColor)
            .ok_or_else(|| serde::de::Error::custom(format!("invalid color: {}", s)))
    }
}

/// Helper to extract an `Option<Color>` from an `Option<SkinColor>`.
fn skin_color(sc: &Option<SkinColor>) -> Option<Color> {
    sc.as_ref().map(|c| c.get())
}

#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct SkinSchema {
    body: SkinBody,
    prompt: SkinPrompt,
    info: SkinInfo,
    dialog: SkinDialog,
    frame: SkinFrame,
    views: SkinViews,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
struct SkinBody {
    fg_color: Option<SkinColor>,
    bg_color: Option<SkinColor>,
    logo_color: Option<SkinColor>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
struct SkinPrompt {
    fg_color: Option<SkinColor>,
    suggest_color: Option<SkinColor>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
struct SkinInfo {
    fg_color: Option<SkinColor>,
    section_color: Option<SkinColor>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
struct SkinDialog {
    bg_color: Option<SkinColor>,
    button_focus_fg_color: Option<SkinColor>,
    button_focus_bg_color: Option<SkinColor>,
    button_fg_color: Option<SkinColor>,
    button_bg_color: Option<SkinColor>,
    label_fg_color: Option<SkinColor>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct SkinFrame {
    border: SkinFrameBorder,
    menu: SkinFrameMenu,
    crumbs: SkinFrameCrumbs,
    status: SkinFrameStatus,
    title: SkinFrameTitle,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
struct SkinFrameBorder {
    fg_color: Option<SkinColor>,
    focus_color: Option<SkinColor>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
struct SkinFrameMenu {
    fg_color: Option<SkinColor>,
    key_color: Option<SkinColor>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
struct SkinFrameCrumbs {
    fg_color: Option<SkinColor>,
    bg_color: Option<SkinColor>,
    active_color: Option<SkinColor>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
struct SkinFrameStatus {
    new_color: Option<SkinColor>,
    add_color: Option<SkinColor>,
    modify_color: Option<SkinColor>,
    error_color: Option<SkinColor>,
    completed_color: Option<SkinColor>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
struct SkinFrameTitle {
    fg_color: Option<SkinColor>,
    highlight_color: Option<SkinColor>,
    counter_color: Option<SkinColor>,
    filter_color: Option<SkinColor>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct SkinViews {
    table: SkinViewsTable,
    yaml: SkinViewsYaml,
    logs: SkinViewsLogs,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
struct SkinViewsTable {
    fg_color: Option<SkinColor>,
    bg_color: Option<SkinColor>,
    cursor_fg_color: Option<SkinColor>,
    cursor_bg_color: Option<SkinColor>,
    header: SkinViewsTableHeader,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
struct SkinViewsTableHeader {
    fg_color: Option<SkinColor>,
    bg_color: Option<SkinColor>,
    sorter_color: Option<SkinColor>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
struct SkinViewsYaml {
    key_color: Option<SkinColor>,
    value_color: Option<SkinColor>,
    colon_color: Option<SkinColor>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
struct SkinViewsLogs {
    fg_color: Option<SkinColor>,
    bg_color: Option<SkinColor>,
}

impl SkinSchema {
    /// Apply skin overrides to an existing theme.
    fn apply_to(&self, theme: &mut Theme) {
        // -- body --
        let fg = skin_color(&self.body.fg_color);
        let bg = skin_color(&self.body.bg_color);
        theme.row_normal = with_bg(with_fg(theme.row_normal, fg), bg);
        theme.info_value = with_fg(theme.info_value, fg);
        if let Some(c) = skin_color(&self.body.logo_color) {
            theme.logo = theme.logo.fg(c);
        }

        // -- prompt --
        theme.command = with_fg(theme.command, skin_color(&self.prompt.fg_color));
        if let Some(c) = skin_color(&self.prompt.suggest_color) {
            theme.command_suggestion = theme.command_suggestion.fg(c);
        }

        // -- info --
        theme.info_value = with_fg(theme.info_value, skin_color(&self.info.fg_color));
        if let Some(c) = skin_color(&self.info.section_color) {
            theme.info_label = theme.info_label.fg(c);
        }

        // -- dialog --
        theme.dialog_bg = with_bg(theme.dialog_bg, skin_color(&self.dialog.bg_color));
        if let Some(c) = skin_color(&self.dialog.button_focus_fg_color) {
            theme.dialog_button_active = theme.dialog_button_active.fg(c);
        }
        if let Some(c) = skin_color(&self.dialog.button_focus_bg_color) {
            theme.dialog_button_active = theme.dialog_button_active.bg(c);
        }
        if let Some(c) = skin_color(&self.dialog.button_fg_color) {
            theme.dialog_button_inactive = theme.dialog_button_inactive.fg(c);
        }
        if let Some(c) = skin_color(&self.dialog.button_bg_color) {
            theme.dialog_button_inactive = theme.dialog_button_inactive.bg(c);
        }
        if let Some(c) = skin_color(&self.dialog.label_fg_color) {
            theme.dialog_border = theme.dialog_border.fg(c);
        }

        // -- frame.border --
        if let Some(c) = skin_color(&self.frame.border.fg_color) {
            theme.border = theme.border.fg(c);
        }
        if let Some(c) = skin_color(&self.frame.border.focus_color) {
            theme.border_focused = theme.border_focused.fg(c);
        }

        // -- frame.menu --
        theme.status_bar = with_fg(theme.status_bar, skin_color(&self.frame.menu.fg_color));
        if let Some(c) = skin_color(&self.frame.menu.key_color) {
            theme.status_bar_key = theme.status_bar_key.fg(c);
            theme.help_key = theme.help_key.fg(c);
        }

        // -- frame.crumbs --
        let crumb_fg = skin_color(&self.frame.crumbs.fg_color);
        let crumb_bg = skin_color(&self.frame.crumbs.bg_color);
        theme.breadcrumb_inactive =
            with_bg(with_fg(theme.breadcrumb_inactive, crumb_fg), crumb_bg);
        if let Some(c) = skin_color(&self.frame.crumbs.active_color) {
            theme.breadcrumb_active = with_fg(theme.breadcrumb_active, crumb_fg).bg(c);
        }

        // -- frame.status --
        if let Some(c) = skin_color(&self.frame.status.new_color) {
            theme.status_running = theme.status_running.fg(c);
        }
        if let Some(c) = skin_color(&self.frame.status.add_color) {
            theme.flash_info = theme.flash_info.fg(c);
        }
        if let Some(c) = skin_color(&self.frame.status.modify_color) {
            theme.status_pending = theme.status_pending.fg(c);
            theme.flash_warn = theme.flash_warn.fg(c);
        }
        if let Some(c) = skin_color(&self.frame.status.error_color) {
            theme.status_failed = theme.status_failed.fg(c);
            theme.flash_error = theme.flash_error.fg(c);
        }
        if let Some(c) = skin_color(&self.frame.status.completed_color) {
            theme.status_succeeded = theme.status_succeeded.fg(c);
        }

        // -- frame.title --
        theme.title = with_fg(theme.title, skin_color(&self.frame.title.fg_color));
        if let Some(c) = skin_color(&self.frame.title.highlight_color) {
            theme.title_namespace = theme.title_namespace.fg(c);
            theme.namespace_label = theme.namespace_label.fg(c);
        }
        if let Some(c) = skin_color(&self.frame.title.counter_color) {
            theme.title_counter = theme.title_counter.fg(c);
        }
        if let Some(c) = skin_color(&self.frame.title.filter_color) {
            theme.title_filter_indicator = theme.title_filter_indicator.fg(c);
            theme.filter = theme.filter.fg(c);
        }

        // -- views.table --
        let table_fg = skin_color(&self.views.table.fg_color);
        let table_bg = skin_color(&self.views.table.bg_color);
        theme.row_normal = with_bg(with_fg(theme.row_normal, table_fg), table_bg);
        if let Some(c) = skin_color(&self.views.table.cursor_fg_color) {
            theme.selected = theme.selected.fg(c);
        }
        if let Some(c) = skin_color(&self.views.table.cursor_bg_color) {
            theme.selected = theme.selected.bg(c);
        }

        // -- views.table.header --
        let hdr_fg = skin_color(&self.views.table.header.fg_color);
        let hdr_bg = skin_color(&self.views.table.header.bg_color);
        theme.header = with_bg(with_fg(theme.header, hdr_fg), hdr_bg);
        if let Some(c) = skin_color(&self.views.table.header.sorter_color) {
            theme.sort_indicator = theme.sort_indicator.fg(c);
        }

        // -- views.yaml --
        if let Some(c) = skin_color(&self.views.yaml.key_color) {
            theme.yaml_key = theme.yaml_key.fg(c);
        }
        if let Some(c) = skin_color(&self.views.yaml.value_color) {
            theme.yaml_string = theme.yaml_string.fg(c);
        }
        if let Some(c) = skin_color(&self.views.yaml.colon_color) {
            theme.yaml_number = theme.yaml_number.fg(c);
        }

        // -- views.logs --
        let log_fg = skin_color(&self.views.logs.fg_color);
        let log_bg = skin_color(&self.views.logs.bg_color);
        theme.log_text = with_bg(with_fg(theme.log_text, log_fg), log_bg);
    }
}

/// Color scheme and styling for the k9rs TUI.
/// Default dark theme.
pub struct Theme {
    // Table
    pub header: Style,
    pub selected: Style,
    pub row_normal: Style,

    // Status colors
    pub status_running: Style,
    pub status_pending: Style,
    pub status_failed: Style,
    pub status_succeeded: Style,

    // Borders
    pub border: Style,
    pub border_focused: Style,

    // Title bar
    pub title: Style,
    pub title_namespace: Style,
    pub title_counter: Style,
    pub title_filter_indicator: Style,
    pub sort_indicator: Style,

    // Filter
    pub filter: Style,

    // Flash
    pub flash_info: Style,
    pub flash_warn: Style,
    pub flash_error: Style,

    // Breadcrumbs
    pub breadcrumb_active: Style,
    pub breadcrumb_inactive: Style,

    // YAML viewer
    pub yaml_key: Style,
    pub yaml_string: Style,
    pub yaml_number: Style,

    // Command prompt
    pub command: Style,
    pub command_suggestion: Style,

    // Status bar
    pub status_bar: Style,
    pub status_bar_key: Style,

    // Header panel (cluster info)
    pub info_label: Style,
    pub info_value: Style,
    pub logo: Style,

    // Namespace / context
    pub namespace_label: Style,
    pub context_label: Style,

    // Help overlay
    pub help_key: Style,
    pub help_desc: Style,

    // Dialog
    pub dialog_border: Style,
    pub dialog_bg: Style,
    pub dialog_button_active: Style,
    pub dialog_button_inactive: Style,

    // Log viewer
    pub log_timestamp: Style,
    pub log_text: Style,
    pub line_number: Style,
    pub search_match: Style,

    // Info "n/a" values (OrangeRed bold)
    pub info_na: Style,

    // Marked/selected rows (gold/yellow text)
    pub marked_row: Style,
    // Cursor on a marked row: gold text, inherits cursor bg from fill
    pub selected_marked: Style,

    // Delta tracking: rows that changed since last update
    pub delta_changed: Style,

    // Column cursor: a subtle background tint applied to the entire
    // selected column (header + all visible rows). Must not interfere
    // with foreground colors (red/yellow/green health indicators).
    pub col_highlight: Style,
}

impl Default for Theme {
    fn default() -> Self {
        Self {
            // Table header: white bold on black
            header: Style::default()
                .fg(Color::White)
                .add_modifier(Modifier::BOLD),
            // Selected row: black text on aqua, bold
            selected: Style::default()
                .fg(Color::Black)
                .bg(AQUA)
                .add_modifier(Modifier::BOLD),
            // Normal row text: dodgerblue on black
            row_normal: Style::default()
                .fg(DODGER_BLUE),

            // Status colors
            status_running: Style::default()
                .fg(LAWN_GREEN),
            status_pending: Style::default()
                .fg(DARK_ORANGE),
            status_failed: Style::default()
                .fg(ORANGE_RED),
            status_succeeded: Style::default()
                .fg(LAWN_GREEN)
                .add_modifier(Modifier::DIM),

            // Borders: dodgerblue, focused = aqua
            border: Style::default()
                .fg(DODGER_BLUE),
            border_focused: Style::default()
                .fg(AQUA),

            // Table title: aqua text
            title: Style::default()
                .fg(AQUA)
                .add_modifier(Modifier::BOLD),
            // Namespace highlight in title: fuchsia
            title_namespace: Style::default()
                .fg(FUCHSIA)
                .add_modifier(Modifier::BOLD),
            // Counter in title: papayawhip
            title_counter: Style::default()
                .fg(PAPAYA_WHIP),
            // Filter indicator in title: steelblue
            title_filter_indicator: Style::default()
                .fg(STEEL_BLUE),
            // Sort indicator: orange
            sort_indicator: Style::default()
                .fg(ORANGE)
                .add_modifier(Modifier::BOLD),

            // Filter prompt: steelblue
            filter: Style::default()
                .fg(STEEL_BLUE),

            // Flash messages
            flash_info: Style::default()
                .fg(Color::Rgb(255, 222, 173)) // NavajoWhite
                .add_modifier(Modifier::BOLD),
            flash_warn: Style::default()
                .fg(DARK_ORANGE)
                .add_modifier(Modifier::BOLD),
            flash_error: Style::default()
                .fg(ORANGE_RED)
                .add_modifier(Modifier::BOLD),

            // Breadcrumbs
            breadcrumb_active: Style::default()
                .fg(Color::Black)
                .bg(ORANGE),
            breadcrumb_inactive: Style::default()
                .fg(Color::Black)
                .bg(STEEL_BLUE),

            // YAML
            yaml_key: Style::default()
                .fg(AQUA),
            yaml_string: Style::default()
                .fg(LAWN_GREEN),
            yaml_number: Style::default()
                .fg(FUCHSIA),

            // Command prompt
            command: Style::default()
                .fg(CADET_BLUE),
            command_suggestion: Style::default()
                .fg(DODGER_BLUE),

            // Status bar
            status_bar: Style::default()
                .fg(Color::White)
                .bg(Color::Rgb(30, 30, 40)),
            status_bar_key: Style::default()
                .fg(AQUA)
                .bg(Color::Rgb(30, 30, 40))
                .add_modifier(Modifier::BOLD),

            // Header panel
            info_label: Style::default()
                .fg(DODGER_BLUE)
                .add_modifier(Modifier::BOLD),
            info_value: Style::default()
                .fg(DODGER_BLUE),
            logo: Style::default()
                .fg(ORANGE)
                .add_modifier(Modifier::BOLD),

            // Namespace/context labels
            namespace_label: Style::default()
                .fg(FUCHSIA)
                .add_modifier(Modifier::BOLD),
            context_label: Style::default()
                .fg(DODGER_BLUE)
                .add_modifier(Modifier::BOLD),

            // Help overlay
            help_key: Style::default()
                .fg(DODGER_BLUE)
                .add_modifier(Modifier::BOLD),
            help_desc: Style::default()
                .fg(Color::White),

            // Dialog — dark bg pops from terminal, not pure black
            dialog_border: Style::default()
                .fg(ORANGE_RED),
            dialog_bg: Style::default()
                .bg(Color::Rgb(25, 28, 38)),
            dialog_button_active: Style::default()
                .fg(Color::Black)
                .bg(AQUA)
                .add_modifier(Modifier::BOLD),
            dialog_button_inactive: Style::default()
                .fg(Color::DarkGray),

            // Log viewer
            log_timestamp: Style::default()
                .fg(STEEL_BLUE),
            log_text: Style::default()
                .fg(DODGER_BLUE),
            line_number: Style::default()
                .fg(Color::DarkGray),
            search_match: Style::default()
                .fg(Color::Black)
                .bg(ORANGE),

            // Info "n/a" values
            info_na: Style::default()
                .fg(ORANGE_RED)
                .add_modifier(Modifier::BOLD),

            // Marked/selected rows: gold/yellow text bold
            marked_row: Style::default()
                .fg(Color::Rgb(255, 215, 0)) // Gold
                .add_modifier(Modifier::BOLD),
            // Cursor on a marked row: gold text, bg inherited from fill
            selected_marked: Style::default()
                .fg(Color::Rgb(255, 215, 0)) // Gold
                .add_modifier(Modifier::BOLD),

            // Delta changed rows: italic with a subtle color tint
            delta_changed: Style::default()
                .fg(Color::Rgb(135, 206, 250)) // LightSkyBlue
                .add_modifier(Modifier::ITALIC),

            // Column cursor: very subtle dark-gray background. Doesn't
            // change foreground color so red/yellow/green health text
            // stays readable.
            col_highlight: Style::default()
                .bg(Color::Rgb(35, 38, 45)),

        }
    }
}

impl Theme {
    /// Try to load the user's skin, falling back to the stock theme.
    ///
    /// 1. Read `~/.config/k9rs/config.yaml` to find the skin name under `k9rs.ui.skin`.
    /// 2. Look for `~/.config/k9rs/skins/<name>.yaml`.
    /// 3. If found, parse it and override colors.
    /// 4. Fall back to `Theme::default()`.
    /// Load the theme. The skin name comes from AppConfig (already
    /// deserialized via serde). Searches k9rs then k9s skins dirs.
    pub fn load(skin_name: Option<&str>) -> Self {
        let Some(name) = skin_name.filter(|s| !s.is_empty()) else {
            return Self::default();
        };
        let home = match std::env::var("HOME") {
            Ok(h) => h,
            Err(_) => return Self::default(),
        };
        // Try k9rs skins dir first, fall back to k9s.
        let skin_file = format!("{}.yaml", name);
        let k9rs_path = Path::new(&home).join(".config/k9rs/skins").join(&skin_file);
        if k9rs_path.exists() {
            return Self::from_skin_file(&k9rs_path).unwrap_or_default();
        }
        let k9s_path = Path::new(&home).join(".config/k9s/skins").join(&skin_file);
        if k9s_path.exists() {
            return Self::from_skin_file(&k9s_path).unwrap_or_default();
        }
        Self::default()
    }

    /// Load a skin YAML file (compatible with k9s skin format) and produce a Theme
    /// with overridden colors. Returns `None` if the file cannot be read or parsed.
    pub fn from_skin_file(path: &Path) -> Option<Self> {
        let content = std::fs::read_to_string(path).ok()?;
        let yaml: serde_yaml::Value = serde_yaml::from_str(&content).ok()?;
        let k9s_val = yaml.get("k9s")?.clone();
        let skin: SkinSchema = serde_yaml::from_value(k9s_val).ok()?;

        let mut theme = Self::default();
        skin.apply_to(&mut theme);
        Some(theme)
    }
}
