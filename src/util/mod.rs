use chrono::{DateTime, Utc};
use unicode_width::UnicodeWidthChar;

/// Generates a snake-style loading bar string: `[  ===   ]`
/// The snake is 3 chars wide sliding across an 8-char bar.
pub fn loading_bar(label: &str) -> String {
    let bar_width = 8usize;
    let snake_len = 4usize;
    let elapsed = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as usize;
    let pos = (elapsed / 200) % (bar_width + snake_len);
    let bar: String = (0..bar_width)
        .map(|i| {
            if i >= pos.saturating_sub(snake_len) && i < pos { '=' } else { ' ' }
        })
        .collect();
    format!("[{}] {}", bar, label)
}

/// Strip ANSI escape sequences from a string.
/// Handles CSI sequences (ESC[...m), OSC sequences (ESC]...BEL/ST), and simple ESC sequences.
pub fn strip_ansi(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut chars = s.chars().peekable();
    while let Some(c) = chars.next() {
        if c == '\x1b' {
            // ESC character — consume the escape sequence
            match chars.peek() {
                Some('[') => {
                    chars.next(); // consume '['
                    // CSI sequence: consume until a letter (0x40-0x7E)
                    while let Some(&nc) = chars.peek() {
                        chars.next();
                        if nc.is_ascii_alphabetic() || nc == '~' || nc == '@' {
                            break;
                        }
                    }
                }
                Some(']') => {
                    chars.next(); // consume ']'
                    // OSC sequence: consume until BEL (\x07) or ST (ESC \)
                    while let Some(&nc) = chars.peek() {
                        chars.next();
                        if nc == '\x07' { break; }
                        if nc == '\x1b' {
                            if chars.peek() == Some(&'\\') { chars.next(); }
                            break;
                        }
                    }
                }
                _ => {
                    // Simple ESC sequence — skip next char
                    chars.next();
                }
            }
        } else {
            out.push(c);
        }
    }
    out
}

/// Truncate a string to fit within `max_width` display columns.
///
/// Uses `UnicodeWidthChar` so that multi-byte / wide characters (emoji, CJK)
/// are measured correctly and we never slice in the middle of a UTF-8 sequence.
pub fn truncate_to_width(s: &str, max_width: usize) -> &str {
    let mut width = 0;
    for (i, c) in s.char_indices() {
        let w = UnicodeWidthChar::width(c).unwrap_or(0);
        if width + w > max_width {
            return &s[..i];
        }
        width += w;
    }
    s
}

/// Formats a Kubernetes timestamp into a human-readable age string like "2d3h", "5m", "10s".
/// Returns "<unknown>" if the timestamp is None.
pub fn format_age(timestamp: Option<DateTime<Utc>>) -> String {
    let ts = match timestamp {
        Some(t) => t,
        None => return "<unknown>".to_string(),
    };

    let now = Utc::now();
    let duration = now.signed_duration_since(ts);

    if duration.num_seconds() < 0 {
        return "0s".to_string();
    }

    let total_secs = duration.num_seconds();

    let days = total_secs / 86400;
    let hours = (total_secs % 86400) / 3600;
    let minutes = (total_secs % 3600) / 60;
    let seconds = total_secs % 60;

    if days > 0 {
        if hours > 0 {
            format!("{}d{}h", days, hours)
        } else {
            format!("{}d", days)
        }
    } else if hours > 0 {
        if minutes > 0 {
            format!("{}h{}m", hours, minutes)
        } else {
            format!("{}h", hours)
        }
    } else if minutes > 0 {
        if seconds > 0 {
            format!("{}m{}s", minutes, seconds)
        } else {
            format!("{}m", minutes)
        }
    } else {
        format!("{}s", seconds)
    }
}

/// Formats CPU quantities from Kubernetes resource strings.
///
/// Handles:
/// - Nanocores: "250000000n" -> "250m"
/// - Millicores: "500m" -> "500m"
/// - Whole cores: "2" -> "2000m"
/// - Empty or unparseable: returns the original string or "0".
pub fn format_cpu(cpu_str: &str) -> String {
    let s = cpu_str.trim();
    if s.is_empty() {
        return "0".to_string();
    }

    // Nanocores: e.g. "250000000n"
    if let Some(nano_str) = s.strip_suffix('n') {
        if let Ok(nano) = nano_str.parse::<u64>() {
            let milli = nano / 1_000_000;
            if milli >= 1000 && milli % 1000 == 0 {
                return format!("{}", milli / 1000);
            }
            return format!("{}m", milli);
        }
        return s.to_string();
    }

    // Millicores: e.g. "500m"
    if let Some(milli_str) = s.strip_suffix('m') {
        if let Ok(milli) = milli_str.parse::<u64>() {
            if milli >= 1000 && milli % 1000 == 0 {
                return format!("{}", milli / 1000);
            }
            return format!("{}m", milli);
        }
        return s.to_string();
    }

    // Whole cores: e.g. "2" or "1.5"
    if let Ok(cores) = s.parse::<f64>() {
        let milli = (cores * 1000.0) as u64;
        if milli >= 1000 && milli % 1000 == 0 {
            return format!("{}", milli / 1000);
        }
        return format!("{}m", milli);
    }

    s.to_string()
}

/// Formats memory quantities from Kubernetes resource strings.
///
/// Handles:
/// - Ki (kibibytes): "131072Ki" -> "128Mi"
/// - Mi (mebibytes): "256Mi" -> "256Mi"
/// - Gi (gibibytes): "2Gi" -> "2Gi"
/// - Ti (tebibytes): "1Ti" -> "1Ti"
/// - Bare bytes: "1073741824" -> "1Gi"
/// - "e" notation: "128974848" or "129e6" -> "123Mi"
pub fn format_mem(mem_str: &str) -> String {
    let s = mem_str.trim();
    if s.is_empty() {
        return "0".to_string();
    }

    let bytes: f64 = if let Some(val) = s.strip_suffix("Ti") {
        match val.parse::<f64>() {
            Ok(v) => v * 1024.0 * 1024.0 * 1024.0 * 1024.0,
            Err(_) => return s.to_string(),
        }
    } else if let Some(val) = s.strip_suffix("Gi") {
        match val.parse::<f64>() {
            Ok(v) => v * 1024.0 * 1024.0 * 1024.0,
            Err(_) => return s.to_string(),
        }
    } else if let Some(val) = s.strip_suffix("Mi") {
        match val.parse::<f64>() {
            Ok(v) => v * 1024.0 * 1024.0,
            Err(_) => return s.to_string(),
        }
    } else if let Some(val) = s.strip_suffix("Ki") {
        match val.parse::<f64>() {
            Ok(v) => v * 1024.0,
            Err(_) => return s.to_string(),
        }
    } else if let Some(val) = s.strip_suffix('T') {
        match val.parse::<f64>() {
            Ok(v) => v * 1_000_000_000_000.0,
            Err(_) => return s.to_string(),
        }
    } else if let Some(val) = s.strip_suffix('G') {
        match val.parse::<f64>() {
            Ok(v) => v * 1_000_000_000.0,
            Err(_) => return s.to_string(),
        }
    } else if let Some(val) = s.strip_suffix('M') {
        match val.parse::<f64>() {
            Ok(v) => v * 1_000_000.0,
            Err(_) => return s.to_string(),
        }
    } else if let Some(val) = s.strip_suffix('k') {
        match val.parse::<f64>() {
            Ok(v) => v * 1_000.0,
            Err(_) => return s.to_string(),
        }
    } else if s.contains('e') || s.contains('E') {
        match s.parse::<f64>() {
            Ok(v) => v,
            Err(_) => return s.to_string(),
        }
    } else {
        match s.parse::<f64>() {
            Ok(v) => v,
            Err(_) => return s.to_string(),
        }
    };

    format_bytes(bytes)
}

/// Converts a byte count into the most appropriate human-readable unit.
fn format_bytes(bytes: f64) -> String {
    const TI: f64 = 1024.0 * 1024.0 * 1024.0 * 1024.0;
    const GI: f64 = 1024.0 * 1024.0 * 1024.0;
    const MI: f64 = 1024.0 * 1024.0;
    const KI: f64 = 1024.0;

    if bytes >= TI {
        let val = bytes / TI;
        if val == val.floor() {
            format!("{}Ti", val as u64)
        } else {
            format!("{:.1}Ti", val)
        }
    } else if bytes >= GI {
        let val = bytes / GI;
        if val == val.floor() {
            format!("{}Gi", val as u64)
        } else {
            format!("{:.1}Gi", val)
        }
    } else if bytes >= MI {
        let val = bytes / MI;
        if val == val.floor() {
            format!("{}Mi", val as u64)
        } else {
            format!("{:.1}Mi", val)
        }
    } else if bytes >= KI {
        let val = bytes / KI;
        if val == val.floor() {
            format!("{}Ki", val as u64)
        } else {
            format!("{:.1}Ki", val)
        }
    } else {
        format!("{}", bytes as u64)
    }
}

/// Truncates a string to the given maximum number of characters.
/// If truncated, appends an ellipsis character.
pub fn truncate(s: &str, max: usize) -> String {
    if max == 0 {
        return String::new();
    }
    let chars: Vec<char> = s.chars().collect();
    if chars.len() <= max {
        s.to_string()
    } else if max <= 1 {
        "\u{2026}".to_string()
    } else {
        let mut result: String = chars[..max - 1].iter().collect();
        result.push('\u{2026}');
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Duration;

    #[test]
    fn test_format_age_none() {
        assert_eq!(format_age(None), "<unknown>");
    }

    #[test]
    fn test_format_age_seconds() {
        let ts = Utc::now() - Duration::seconds(30);
        assert_eq!(format_age(Some(ts)), "30s");
    }

    #[test]
    fn test_format_age_minutes() {
        let ts = Utc::now() - Duration::minutes(5) - Duration::seconds(10);
        assert_eq!(format_age(Some(ts)), "5m10s");
    }

    #[test]
    fn test_format_age_hours() {
        let ts = Utc::now() - Duration::hours(3) - Duration::minutes(15);
        assert_eq!(format_age(Some(ts)), "3h15m");
    }

    #[test]
    fn test_format_age_days() {
        let ts = Utc::now() - Duration::days(2) - Duration::hours(5);
        assert_eq!(format_age(Some(ts)), "2d5h");
    }

    #[test]
    fn test_format_cpu_nanocores() {
        assert_eq!(format_cpu("250000000n"), "250m");
    }

    #[test]
    fn test_format_cpu_millicores() {
        assert_eq!(format_cpu("500m"), "500m");
    }

    #[test]
    fn test_format_cpu_whole_cores() {
        assert_eq!(format_cpu("2"), "2");
    }

    #[test]
    fn test_format_mem_ki() {
        assert_eq!(format_mem("131072Ki"), "128Mi");
    }

    #[test]
    fn test_format_mem_mi() {
        assert_eq!(format_mem("256Mi"), "256Mi");
    }

    #[test]
    fn test_format_mem_gi() {
        assert_eq!(format_mem("2Gi"), "2Gi");
    }

    #[test]
    fn test_truncate_short() {
        assert_eq!(truncate("hello", 10), "hello");
    }

    #[test]
    fn test_truncate_exact() {
        assert_eq!(truncate("hello", 5), "hello");
    }

    #[test]
    fn test_truncate_long() {
        let result = truncate("hello world", 8);
        assert_eq!(result, "hello w\u{2026}");
    }

    #[test]
    fn test_truncate_zero() {
        assert_eq!(truncate("hello", 0), "");
    }
}
