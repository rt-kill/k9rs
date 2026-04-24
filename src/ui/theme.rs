use std::path::Path;

use ratatui::style::{Color, Modifier, Style};

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

/// Helper: extract a color from a YAML mapping by key name.
fn yaml_color(map: &serde_yaml::Value, key: &str) -> Option<Color> {
    map.get(key)?.as_str().and_then(parse_color)
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
    pub fn load() -> Self {
        let home = match std::env::var("HOME") {
            Ok(h) => h,
            Err(_) => return Self::default(),
        };
        // Try k9rs config first, fall back to k9s for compatibility.
        let (skin_name, base_dir) = Self::find_skin_config(&home)
            .unwrap_or_default();
        if skin_name.is_empty() {
            return Self::default();
        }
        let skin_path = Path::new(&base_dir)
            .join("skins")
            .join(format!("{}.yaml", skin_name));
        Self::from_skin_file(&skin_path).unwrap_or_default()
    }

    /// Find skin name and config base directory, trying k9rs then k9s.
    fn find_skin_config(home: &str) -> Option<(String, String)> {
        // Try ~/.config/k9rs/ first
        let k9rs_dir = Path::new(home).join(".config/k9rs");
        let k9rs_config = k9rs_dir.join("config.yaml");
        if let Some(name) = Self::read_skin_name(&k9rs_config, "k9rs") {
            return Some((name, k9rs_dir.to_string_lossy().to_string()));
        }
        // Fall back to ~/.config/k9s/
        let k9s_dir = Path::new(home).join(".config/k9s");
        let k9s_config = k9s_dir.join("config.yaml");
        if let Some(name) = Self::read_skin_name(&k9s_config, "k9s") {
            return Some((name, k9s_dir.to_string_lossy().to_string()));
        }
        None
    }

    /// Read the skin name from a config.yaml file under the given YAML key.
    fn read_skin_name(path: &Path, key: &str) -> Option<String> {
        let content = std::fs::read_to_string(path).ok()?;
        let yaml: serde_yaml::Value = serde_yaml::from_str(&content).ok()?;
        yaml.get(key)?
            .get("ui")?
            .get("skin")?
            .as_str()
            .map(|s| s.to_string())
    }

    /// Load a skin YAML file (compatible with k9s skin format) and produce a Theme
    /// with overridden colors. Returns `None` if the file cannot be read or parsed.
    pub fn from_skin_file(path: &Path) -> Option<Self> {
        let content = std::fs::read_to_string(path).ok()?;
        let yaml: serde_yaml::Value = serde_yaml::from_str(&content).ok()?;
        let k9s = yaml.get("k9s")?;

        let mut theme = Self::default();

        // -- body --
        if let Some(body) = k9s.get("body") {
            let fg = yaml_color(body, "fgColor");
            let bg = yaml_color(body, "bgColor");
            // Body fg/bg affects general text styles
            theme.row_normal = with_bg(with_fg(theme.row_normal, fg), bg);
            theme.info_value = with_fg(theme.info_value, fg);
            if let Some(logo_color) = yaml_color(body, "logoColor") {
                theme.logo = theme.logo.fg(logo_color);
            }
        }

        // -- prompt --
        if let Some(prompt) = k9s.get("prompt") {
            let fg = yaml_color(prompt, "fgColor");
            theme.command = with_fg(theme.command, fg);
            if let Some(suggest) = yaml_color(prompt, "suggestColor") {
                theme.command_suggestion = theme.command_suggestion.fg(suggest);
            }
        }

        // -- info --
        if let Some(info) = k9s.get("info") {
            let fg = yaml_color(info, "fgColor");
            theme.info_value = with_fg(theme.info_value, fg);
            if let Some(section) = yaml_color(info, "sectionColor") {
                theme.info_label = theme.info_label.fg(section);
            }
        }

        // -- dialog --
        if let Some(dialog) = k9s.get("dialog") {
            let bg = yaml_color(dialog, "bgColor");
            theme.dialog_bg = with_bg(theme.dialog_bg, bg);
            if let Some(focus_fg) = yaml_color(dialog, "buttonFocusFgColor") {
                theme.dialog_button_active = theme.dialog_button_active.fg(focus_fg);
            }
            if let Some(focus_bg) = yaml_color(dialog, "buttonFocusBgColor") {
                theme.dialog_button_active = theme.dialog_button_active.bg(focus_bg);
            }
            if let Some(btn_fg) = yaml_color(dialog, "buttonFgColor") {
                theme.dialog_button_inactive = theme.dialog_button_inactive.fg(btn_fg);
            }
            if let Some(btn_bg) = yaml_color(dialog, "buttonBgColor") {
                theme.dialog_button_inactive = theme.dialog_button_inactive.bg(btn_bg);
            }
            if let Some(label_fg) = yaml_color(dialog, "labelFgColor") {
                theme.dialog_border = theme.dialog_border.fg(label_fg);
            }
        }

        // -- frame --
        if let Some(frame) = k9s.get("frame") {
            // frame.border
            if let Some(border) = frame.get("border") {
                if let Some(fg) = yaml_color(border, "fgColor") {
                    theme.border = theme.border.fg(fg);
                }
                if let Some(focus) = yaml_color(border, "focusColor") {
                    theme.border_focused = theme.border_focused.fg(focus);
                }
            }

            // frame.menu
            if let Some(menu) = frame.get("menu") {
                let fg = yaml_color(menu, "fgColor");
                theme.status_bar = with_fg(theme.status_bar, fg);
                if let Some(key_color) = yaml_color(menu, "keyColor") {
                    theme.status_bar_key = theme.status_bar_key.fg(key_color);
                    theme.help_key = theme.help_key.fg(key_color);
                }
            }

            // frame.crumbs
            if let Some(crumbs) = frame.get("crumbs") {
                let fg = yaml_color(crumbs, "fgColor");
                let bg = yaml_color(crumbs, "bgColor");
                theme.breadcrumb_inactive = with_bg(with_fg(theme.breadcrumb_inactive, fg), bg);
                if let Some(active) = yaml_color(crumbs, "activeColor") {
                    theme.breadcrumb_active = with_fg(theme.breadcrumb_active, fg).bg(active);
                }
            }

            // frame.status
            if let Some(status) = frame.get("status") {
                if let Some(c) = yaml_color(status, "newColor") {
                    theme.status_running = theme.status_running.fg(c);
                }
                if let Some(c) = yaml_color(status, "addColor") {
                    theme.flash_info = theme.flash_info.fg(c);
                }
                if let Some(c) = yaml_color(status, "modifyColor") {
                    theme.status_pending = theme.status_pending.fg(c);
                    theme.flash_warn = theme.flash_warn.fg(c);
                }
                if let Some(c) = yaml_color(status, "errorColor") {
                    theme.status_failed = theme.status_failed.fg(c);
                    theme.flash_error = theme.flash_error.fg(c);
                }
                if let Some(c) = yaml_color(status, "completedColor") {
                    theme.status_succeeded = theme.status_succeeded.fg(c);
                }
            }

            // frame.title
            if let Some(title) = frame.get("title") {
                let fg = yaml_color(title, "fgColor");
                theme.title = with_fg(theme.title, fg);
                if let Some(hl) = yaml_color(title, "highlightColor") {
                    theme.title_namespace = theme.title_namespace.fg(hl);
                    theme.namespace_label = theme.namespace_label.fg(hl);
                }
                if let Some(counter) = yaml_color(title, "counterColor") {
                    theme.title_counter = theme.title_counter.fg(counter);
                }
                if let Some(filter) = yaml_color(title, "filterColor") {
                    theme.title_filter_indicator = theme.title_filter_indicator.fg(filter);
                    theme.filter = theme.filter.fg(filter);
                }
            }
        }

        // -- views --
        if let Some(views) = k9s.get("views") {
            // views.table
            if let Some(table) = views.get("table") {
                let fg = yaml_color(table, "fgColor");
                let bg = yaml_color(table, "bgColor");
                theme.row_normal = with_bg(with_fg(theme.row_normal, fg), bg);

                if let Some(cursor_fg) = yaml_color(table, "cursorFgColor") {
                    theme.selected = theme.selected.fg(cursor_fg);
                }
                if let Some(cursor_bg) = yaml_color(table, "cursorBgColor") {
                    theme.selected = theme.selected.bg(cursor_bg);
                }

                // views.table.header
                if let Some(header) = table.get("header") {
                    let hfg = yaml_color(header, "fgColor");
                    let hbg = yaml_color(header, "bgColor");
                    theme.header = with_bg(with_fg(theme.header, hfg), hbg);
                    if let Some(sorter) = yaml_color(header, "sorterColor") {
                        theme.sort_indicator = theme.sort_indicator.fg(sorter);
                    }
                }
            }

            // views.yaml
            if let Some(yaml) = views.get("yaml") {
                if let Some(key) = yaml_color(yaml, "keyColor") {
                    theme.yaml_key = theme.yaml_key.fg(key);
                }
                if let Some(value) = yaml_color(yaml, "valueColor") {
                    theme.yaml_string = theme.yaml_string.fg(value);
                }
                if let Some(colon) = yaml_color(yaml, "colonColor") {
                    theme.yaml_number = theme.yaml_number.fg(colon);
                }
            }

            // views.logs
            if let Some(logs) = views.get("logs") {
                let fg = yaml_color(logs, "fgColor");
                let bg = yaml_color(logs, "bgColor");
                theme.log_text = with_bg(with_fg(theme.log_text, fg), bg);
            }
        }

        Some(theme)
    }
}
