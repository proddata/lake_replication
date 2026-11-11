//! SQL literals for the simple-query and replication-command paths.

/// Explicit escape strings have the same meaning with either value of
/// standard_conforming_strings. Escape backslashes before quoting apostrophes.
pub fn sql_literal(value: &str) -> String {
    format!("E'{}'", value.replace('\\', "\\\\").replace('\'', "''"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn escapes_backslashes_before_quotes() {
        assert_eq!(sql_literal(""), "E''");
        assert_eq!(sql_literal("O'Reilly"), "E'O''Reilly'");
        assert_eq!(sql_literal(r"\'; SELECT 1; --"), r"E'\\''; SELECT 1; --'");
        assert_eq!(sql_literal("Grüße\n世界"), "E'Grüße\n世界'");
    }
}
