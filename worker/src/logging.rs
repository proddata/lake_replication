//! Dependency-free JSON-lines logging shared by every worker binary.

use std::time::{SystemTime, UNIX_EPOCH};

/// Emit one structured event. Informational events use stdout; warnings and
/// errors use stderr so existing process supervisors preserve severity.
///
/// Field values are strings deliberately: this keeps the logger dependency-free
/// and gives every line the same shape for ingestion by journald, Vector, or a
/// file tailer. Stable numeric telemetry belongs in `lake_replication.metrics`.
pub fn event(
    component: &str,
    level: &str,
    event_name: &str,
    message: &str,
    fields: &[(&str, String)],
) {
    let timestamp_unix_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    let mut line = format!(
        "{{\"timestamp_unix_ms\":{timestamp_unix_ms},\"level\":\"{}\",\"component\":\"{}\",\"event\":\"{}\",\"pid\":{},\"message\":\"{}\"",
        escape(level),
        escape(component),
        escape(event_name),
        std::process::id(),
        escape(message),
    );
    for (name, value) in fields {
        line.push_str(",\"");
        line.push_str(&escape(name));
        line.push_str("\":\"");
        line.push_str(&escape(value));
        line.push('"');
    }
    line.push('}');
    if matches!(level, "WARN" | "ERROR") {
        eprintln!("{line}");
    } else {
        println!("{line}");
    }
}

fn escape(value: &str) -> String {
    let mut escaped = String::with_capacity(value.len());
    for character in value.chars() {
        match character {
            '"' => escaped.push_str("\\\""),
            '\\' => escaped.push_str("\\\\"),
            '\n' => escaped.push_str("\\n"),
            '\r' => escaped.push_str("\\r"),
            '\t' => escaped.push_str("\\t"),
            character if character <= '\u{001f}' => {
                use std::fmt::Write;
                write!(escaped, "\\u{:04x}", character as u32).expect("writing to String");
            }
            character => escaped.push(character),
        }
    }
    escaped
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn escapes_json_strings_without_a_dependency() {
        assert_eq!(
            escape("quote=\" slash=\\ newline=\n tab=\t"),
            "quote=\\\" slash=\\\\ newline=\\n tab=\\t"
        );
        assert_eq!(escape("\u{0001}"), "\\u0001");
    }
}
