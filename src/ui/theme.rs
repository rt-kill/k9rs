use std::path::Path;

use ratatui::style::{Color, Modifier, Style};
use serde::Deserialize;

// ---------------------------------------------------------------------------
// Palettes
// ---------------------------------------------------------------------------
//
// Every style in [`Theme`] is derived from a [`Palette`], so a color scheme is
// a palette and nothing else — the style construction lives once, in
// [`Theme::from_palette`]. Two ship in-tree: [`DARK`] (Nord, matching the k9s
// "foot" skin) and [`LIGHT`], its counterpart for light terminals. Skin files
// override individual styles on top of whichever palette was picked.

/// The color slots a [`Theme`] is built from.
struct Palette {
    /// Body text: titles, log lines, info values, command prompt.
    text: Color,
    /// Brighter than `text` — table headers.
    text_bright: Color,
    /// Low-contrast text: borders, timestamps, line numbers, "n/a".
    muted: Color,
    /// Accent: focused borders, YAML keys, filter prompt, info labels.
    blue: Color,
    /// Default row color for healthy resources (k9s `StdColor`).
    teal: Color,
    /// Healthy / running / completed.
    green: Color,
    /// Warning, pending, marked rows, changed rows.
    yellow: Color,
    /// Error, failed.
    red: Color,
    /// Logo, counters, key hints.
    mauve: Color,
    /// Row cursor bar, and the text drawn on top of it.
    cursor_bg: Color,
    cursor_fg: Color,
    /// Breadcrumb pills — the trailing (active) crumb and the rest.
    crumb_bg: Color,
    crumb_fg: Color,
    crumb_active_bg: Color,
    crumb_active_fg: Color,
    /// Search hit highlight.
    match_bg: Color,
    match_fg: Color,
    /// Dialog interior fill. Painted unconditionally, so it has to stay
    /// legible against ANY terminal background.
    dialog_bg: Color,
    /// Column cursor tint: a background wash laid under whatever foreground
    /// the cell already has (red/yellow/green health), so it must be subtle.
    col_highlight: Color,
}

/// Nord — the default dark palette (matches the k9s "foot" skin).
const DARK: Palette = Palette {
    // Snow Storm (text)
    text: Color::Rgb(216, 222, 232),            // #d8dee8
    text_bright: Color::Rgb(229, 233, 240),     // #e5e9f0
    // Polar Night (muted)
    muted: Color::Rgb(75, 82, 98),              // #4b5262
    // Frost + Aurora
    blue: Color::Rgb(129, 161, 193),            // #81a1c1
    teal: Color::Rgb(137, 208, 186),            // #89d0ba
    green: Color::Rgb(163, 190, 140),           // #a3be8c
    yellow: Color::Rgb(235, 203, 139),          // #ebcb8b
    red: Color::Rgb(191, 97, 106),              // #bf616a
    mauve: Color::Rgb(180, 142, 173),           // #b48ead
    // Filled bars: dark Polar Night text on a bright accent.
    cursor_bg: Color::Rgb(129, 161, 193),
    cursor_fg: Color::Rgb(47, 52, 63),          // #2f343f
    crumb_bg: Color::Rgb(129, 161, 193),
    crumb_fg: Color::Rgb(47, 52, 63),
    crumb_active_bg: Color::Rgb(180, 142, 173),
    crumb_active_fg: Color::Rgb(47, 52, 63),
    match_bg: Color::Rgb(235, 203, 139),
    match_fg: Color::Rgb(47, 52, 63),
    dialog_bg: Color::Rgb(25, 28, 38),
    col_highlight: Color::Rgb(40, 44, 55),
};

/// Light counterpart to [`DARK`]. Same hues, darkened until every foreground
/// clears 4.5:1 against a white background (pure Aurora yellow/teal on white
/// is unreadable), and the filled bars flip: dark text on a pale accent wash
/// instead of Nord's dark-on-bright.
const LIGHT: Palette = Palette {
    text: Color::Rgb(46, 52, 64),               // #2e3440
    text_bright: Color::Rgb(28, 32, 40),        // #1c2028
    muted: Color::Rgb(98, 107, 120),            // #626b78
    blue: Color::Rgb(43, 108, 176),             // #2b6cb0
    teal: Color::Rgb(15, 118, 110),             // #0f766e
    green: Color::Rgb(21, 128, 61),             // #15803d
    yellow: Color::Rgb(180, 83, 9),             // #b45309 — amber, not yellow
    red: Color::Rgb(185, 28, 28),               // #b91c1c
    mauve: Color::Rgb(126, 34, 206),            // #7e22ce
    cursor_bg: Color::Rgb(203, 224, 245),       // #cbe0f5
    cursor_fg: Color::Rgb(28, 32, 40),
    crumb_bg: Color::Rgb(219, 230, 244),        // #dbe6f4
    crumb_fg: Color::Rgb(28, 32, 40),
    crumb_active_bg: Color::Rgb(230, 214, 245), // #e6d6f5
    crumb_active_fg: Color::Rgb(28, 32, 40),
    match_bg: Color::Rgb(255, 224, 138),        // #ffe08a
    match_fg: Color::Rgb(28, 32, 40),
    dialog_bg: Color::Rgb(242, 244, 248),       // #f2f4f8
    col_highlight: Color::Rgb(232, 236, 243),   // #e8ecf3
};

// ---------------------------------------------------------------------------
// Theme mode
// ---------------------------------------------------------------------------

/// Which palette to build the theme from (`ui.theme` in config, `--theme` on
/// the command line).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize, clap::ValueEnum)]
#[serde(rename_all = "lowercase")]
pub enum ThemeMode {
    /// Ask the terminal for its background color and pick to match; falls
    /// back to `Dark` when it doesn't answer. See [`crate::ui::term_bg`].
    #[default]
    Auto,
    Dark,
    Light,
}

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
    // `is_ascii` guards the byte-index slices below: a 7-*byte* string could hold
    // a multi-byte char, and `&s[1..3]` would panic on a non-char-boundary cut.
    if s.starts_with('#') && s.len() == 7 && s.is_ascii() {
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
        // body.fgColor applies to info/text, NOT row_normal.
        // k9s sets StdColor (row default) from frame.status.newColor.
        theme.info_value = with_fg(theme.info_value, skin_color(&self.body.fg_color));
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
        // k9s uses newColor as StdColor (default row color for all
        // healthy resources). Maps to both row_normal and status_running.
        if let Some(c) = skin_color(&self.frame.status.new_color) {
            theme.row_normal = theme.row_normal.fg(c);
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
        // views.table.fgColor does NOT map to row_normal — k9s uses
        // frame.status.newColor as StdColor (row default).
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
    /// Opaque fill for dialog interiors. Not a `Style` and not
    /// skin-overridable on purpose: dialogs paint it under everything so a
    /// skin's `dialog.bgColor: default` can't render them invisible against
    /// the terminal background. See [`crate::ui::fill_dialog_bg`].
    pub dialog_fill: Color,
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
    /// The stock dark (Nord) theme — the historical default, and what every
    /// non-rendering caller (line counting, tests) wants.
    fn default() -> Self {
        Self::from_palette(&DARK)
    }
}

impl Theme {
    /// The stock dark (Nord) theme.
    pub fn dark() -> Self {
        Self::from_palette(&DARK)
    }

    /// The stock light theme.
    pub fn light() -> Self {
        Self::from_palette(&LIGHT)
    }

    /// Resolve a [`ThemeMode`] to a theme. `Auto` asks the terminal for its
    /// background color (once per process — the answer is cached).
    pub fn for_mode(mode: ThemeMode) -> Self {
        match mode {
            ThemeMode::Dark => Self::dark(),
            ThemeMode::Light => Self::light(),
            ThemeMode::Auto => match crate::ui::term_bg::detect() {
                crate::ui::term_bg::Appearance::Light => Self::light(),
                crate::ui::term_bg::Appearance::Dark => Self::dark(),
            },
        }
    }

    /// Build the full style set from a palette. Every color a widget can
    /// reach comes from here, so a palette swap recolors the whole TUI.
    fn from_palette(p: &Palette) -> Self {
        Self {
            // Table header: brightest text, bold
            header: Style::default()
                .fg(p.text_bright)
                .add_modifier(Modifier::BOLD),
            // Selected row: cursor bar
            selected: Style::default()
                .fg(p.cursor_fg)
                .bg(p.cursor_bg)
                .add_modifier(Modifier::BOLD),
            // Normal row text: teal (k9s uses frame.status.newColor
            // as StdColor — the default row color for healthy resources).
            row_normal: Style::default()
                .fg(p.teal),

            // Status colors
            status_running: Style::default()
                .fg(p.green),
            status_pending: Style::default()
                .fg(p.yellow),
            status_failed: Style::default()
                .fg(p.red),
            status_succeeded: Style::default()
                .fg(p.green)
                .add_modifier(Modifier::DIM),

            // Borders: muted, focused = accent blue
            border: Style::default()
                .fg(p.muted),
            border_focused: Style::default()
                .fg(p.blue),

            // Table title: body text
            title: Style::default()
                .fg(p.text)
                .add_modifier(Modifier::BOLD),
            // Namespace highlight in title
            title_namespace: Style::default()
                .fg(p.yellow)
                .add_modifier(Modifier::BOLD),
            // Counter in title
            title_counter: Style::default()
                .fg(p.mauve),
            // Filter indicator in title
            title_filter_indicator: Style::default()
                .fg(p.green),
            // Sort indicator
            sort_indicator: Style::default()
                .fg(p.teal)
                .add_modifier(Modifier::BOLD),

            // Filter prompt
            filter: Style::default()
                .fg(p.blue),

            // Flash messages
            flash_info: Style::default()
                .fg(p.teal)
                .add_modifier(Modifier::BOLD),
            flash_warn: Style::default()
                .fg(p.yellow)
                .add_modifier(Modifier::BOLD),
            flash_error: Style::default()
                .fg(p.red)
                .add_modifier(Modifier::BOLD),

            // Breadcrumbs: pills, active one in the accent hue
            breadcrumb_active: Style::default()
                .fg(p.crumb_active_fg)
                .bg(p.crumb_active_bg),
            breadcrumb_inactive: Style::default()
                .fg(p.crumb_fg)
                .bg(p.crumb_bg),

            // YAML syntax
            yaml_key: Style::default()
                .fg(p.blue),
            yaml_string: Style::default()
                .fg(p.green),
            yaml_number: Style::default()
                .fg(p.mauve),

            // Command prompt
            command: Style::default()
                .fg(p.text),
            command_suggestion: Style::default()
                .fg(p.blue),

            // Status bar
            status_bar: Style::default()
                .fg(p.text),
            status_bar_key: Style::default()
                .fg(p.mauve)
                .add_modifier(Modifier::BOLD),

            // Header panel (cluster info)
            info_label: Style::default()
                .fg(p.blue)
                .add_modifier(Modifier::BOLD),
            info_value: Style::default()
                .fg(p.text),
            logo: Style::default()
                .fg(p.mauve)
                .add_modifier(Modifier::BOLD),

            // Namespace/context labels
            namespace_label: Style::default()
                .fg(p.yellow)
                .add_modifier(Modifier::BOLD),
            context_label: Style::default()
                .fg(p.blue)
                .add_modifier(Modifier::BOLD),

            // Help overlay
            help_key: Style::default()
                .fg(p.mauve)
                .add_modifier(Modifier::BOLD),
            help_desc: Style::default()
                .fg(p.text),

            // Dialog
            dialog_border: Style::default()
                .fg(p.blue),
            dialog_bg: Style::default()
                .bg(p.dialog_bg),
            dialog_fill: p.dialog_bg,
            dialog_button_active: Style::default()
                .fg(p.cursor_fg)
                .bg(p.cursor_bg)
                .add_modifier(Modifier::BOLD),
            dialog_button_inactive: Style::default()
                .fg(p.muted),

            // Log viewer
            log_timestamp: Style::default()
                .fg(p.muted),
            log_text: Style::default()
                .fg(p.text),
            line_number: Style::default()
                .fg(p.muted),
            search_match: Style::default()
                .fg(p.match_fg)
                .bg(p.match_bg),

            // Info "n/a" values
            info_na: Style::default()
                .fg(p.muted),

            // Marked/selected rows
            marked_row: Style::default()
                .fg(p.yellow)
                .add_modifier(Modifier::BOLD),
            selected_marked: Style::default()
                .fg(p.yellow)
                .add_modifier(Modifier::BOLD),

            // Delta changed rows
            delta_changed: Style::default()
                .fg(p.yellow),

            // Column cursor: subtle background tint
            col_highlight: Style::default()
                .bg(p.col_highlight),
        }
    }

    /// Load the theme: the palette picked by `mode`, with the user's skin
    /// (if any) overriding colors on top of it.
    ///
    /// 1. `mode` selects the base palette — dark, light, or auto-detected
    ///    from the terminal background.
    /// 2. The skin name comes from AppConfig (already deserialized via
    ///    serde); an empty/absent name means "stock palette, no overrides".
    /// 3. `~/.config/k9rs/skins/<name>.yaml` is searched first, then
    ///    `~/.config/k9s/skins/<name>.yaml`.
    /// 4. A missing or unparsable skin falls back to the bare palette.
    pub fn load(skin_name: Option<&str>, mode: ThemeMode) -> Self {
        let Some(name) = skin_name.filter(|s| !s.is_empty()) else {
            return Self::for_mode(mode);
        };
        let home = match std::env::var("HOME") {
            Ok(h) => h,
            Err(_) => return Self::for_mode(mode),
        };
        // Try k9rs skins dir first, fall back to k9s.
        let skin_file = format!("{}.yaml", name);
        let k9rs_path = Path::new(&home).join(".config/k9rs/skins").join(&skin_file);
        if k9rs_path.exists() {
            return Self::from_skin_file(&k9rs_path, mode).unwrap_or_else(|| Self::for_mode(mode));
        }
        let k9s_path = Path::new(&home).join(".config/k9s/skins").join(&skin_file);
        if k9s_path.exists() {
            return Self::from_skin_file(&k9s_path, mode).unwrap_or_else(|| Self::for_mode(mode));
        }
        Self::for_mode(mode)
    }

    /// Load a skin YAML file (compatible with k9s skin format) and produce a
    /// Theme with overridden colors, layered over `mode`'s palette. Returns
    /// `None` if the file cannot be read or parsed.
    pub fn from_skin_file(path: &Path, mode: ThemeMode) -> Option<Self> {
        let content = std::fs::read_to_string(path).ok()?;
        let yaml: serde_yaml::Value = serde_yaml::from_str(&content).ok()?;
        let k9s_val = yaml.get("k9s")?.clone();
        let skin: SkinSchema = serde_yaml::from_value(k9s_val).ok()?;

        let mut theme = Self::for_mode(mode);
        skin.apply_to(&mut theme);
        Some(theme)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// WCAG relative luminance.
    fn luminance(c: Color) -> f64 {
        let Color::Rgb(r, g, b) = c else {
            panic!("palette colors are always RGB, got {c:?}")
        };
        let lin = |v: u8| {
            let v = v as f64 / 255.0;
            if v <= 0.04045 { v / 12.92 } else { ((v + 0.055) / 1.055).powf(2.4) }
        };
        0.2126 * lin(r) + 0.7152 * lin(g) + 0.0722 * lin(b)
    }

    /// WCAG contrast ratio between two colors (1.0 = identical, 21.0 = black
    /// on white).
    fn contrast(a: Color, b: Color) -> f64 {
        let (x, y) = (luminance(a), luminance(b));
        let (hi, lo) = if x > y { (x, y) } else { (y, x) };
        (hi + 0.05) / (lo + 0.05)
    }

    /// The light palette's whole point: it has to stay readable on a light
    /// terminal. Every foreground clears WCAG AA (4.5:1) against both pure
    /// white and the dialog fill — `muted` (borders, timestamps) is the
    /// floor, which is why it is a mid gray and not a pale one.
    #[test]
    fn light_palette_foregrounds_are_readable_on_light_backgrounds() {
        let p = &LIGHT;
        let fgs = [
            ("text", p.text), ("text_bright", p.text_bright), ("muted", p.muted),
            ("blue", p.blue), ("teal", p.teal), ("green", p.green),
            ("yellow", p.yellow), ("red", p.red), ("mauve", p.mauve),
        ];
        for (name, fg) in fgs {
            for (bg_name, bg) in [("white", Color::Rgb(255, 255, 255)), ("dialog", p.dialog_bg)] {
                let ratio = contrast(fg, bg);
                assert!(ratio >= 4.5, "light {name} on {bg_name}: {ratio:.2}:1 < 4.5:1");
            }
        }
    }

    /// Filled bars (row cursor, breadcrumbs, search hits) carry their own
    /// foreground, so they must be legible against THAT, not the terminal.
    /// The light palette is held to AA (4.5:1); the dark pairings are Nord's
    /// own, kept as-is for fidelity with the k9s "foot" skin, and its mauve
    /// active crumb is the floor at 4.4:1.
    #[test]
    fn filled_bars_are_readable_in_both_palettes() {
        for (name, p, floor) in [("dark", &DARK, 4.0), ("light", &LIGHT, 4.5)] {
            for (what, fg, bg) in [
                ("cursor", p.cursor_fg, p.cursor_bg),
                ("crumb", p.crumb_fg, p.crumb_bg),
                ("crumb_active", p.crumb_active_fg, p.crumb_active_bg),
                ("search_match", p.match_fg, p.match_bg),
            ] {
                let ratio = contrast(fg, bg);
                assert!(ratio >= floor, "{name} {what}: {ratio:.2}:1 < {floor}:1");
            }
        }
    }

    #[test]
    fn light_and_dark_are_actually_different_themes() {
        let (dark, light) = (Theme::dark(), Theme::light());
        assert_ne!(dark.row_normal.fg, light.row_normal.fg);
        assert_ne!(dark.dialog_fill, light.dialog_fill);
        // Default stays dark: the historical behavior, and what non-rendering
        // callers (help line counting, tests) get.
        assert_eq!(Theme::default().row_normal.fg, dark.row_normal.fg);
    }

    #[test]
    fn theme_mode_parses_from_config() {
        assert_eq!(serde_yaml::from_str::<ThemeMode>("auto").unwrap(), ThemeMode::Auto);
        assert_eq!(serde_yaml::from_str::<ThemeMode>("dark").unwrap(), ThemeMode::Dark);
        assert_eq!(serde_yaml::from_str::<ThemeMode>("light").unwrap(), ThemeMode::Light);
        assert!(serde_yaml::from_str::<ThemeMode>("solarized").is_err());
        assert_eq!(ThemeMode::default(), ThemeMode::Auto);
    }

    /// A skin overrides individual colors ON TOP of the mode's palette —
    /// picking light doesn't discard the skin, and a skin that only sets a
    /// few keys doesn't drag the rest back to the dark defaults.
    #[test]
    fn skin_overrides_layer_over_the_selected_palette() {
        let path = std::env::temp_dir()
            .join(format!("k9rs-skin-test-{}.yaml", std::process::id()));
        std::fs::write(
            &path,
            "k9s:\n  frame:\n    border:\n      focusColor: \"#ff00ff\"\n",
        )
        .unwrap();

        let light = Theme::from_skin_file(&path, ThemeMode::Light).unwrap();
        let dark = Theme::from_skin_file(&path, ThemeMode::Dark).unwrap();
        let _ = std::fs::remove_file(&path);

        // The skinned color wins in both.
        assert_eq!(light.border_focused.fg, Some(Color::Rgb(255, 0, 255)));
        assert_eq!(dark.border_focused.fg, Some(Color::Rgb(255, 0, 255)));
        // Everything the skin left alone still comes from the palette.
        assert_eq!(light.row_normal.fg, Some(LIGHT.teal));
        assert_eq!(dark.row_normal.fg, Some(DARK.teal));
    }
}
