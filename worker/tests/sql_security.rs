//! Run against a disposable PostgreSQL server configured with
//! standard_conforming_strings=off and client_encoding=LATIN1:
//! LAKE_SQL_TEST_ADDRESS=127.0.0.1:5432 cargo test --test sql_security -- --ignored
use lake_replication_worker::sql::sql_literal;
use lake_replication_worker::wire::Client;

#[test]
#[ignore = "requires a disposable PostgreSQL server; see module documentation"]
fn startup_overrides_unsafe_defaults_and_literals_round_trip_under_both_modes() {
    let address = std::env::var("LAKE_SQL_TEST_ADDRESS").expect("test server address");
    let user = std::env::var("LAKE_SQL_TEST_USER").unwrap_or_else(|_| "postgres".into());
    let mut client = Client::connect(address.as_str(), &user, "postgres", false).unwrap();
    assert_eq!(
        client
            .simple_query("SHOW standard_conforming_strings")
            .unwrap(),
        vec![vec![Some("on".into())]]
    );
    assert_eq!(
        client.simple_query("SHOW client_encoding").unwrap(),
        vec![vec![Some("UTF8".into())]]
    );
    client
        .simple_query("CREATE TEMP TABLE injection_guard (id int)")
        .unwrap();
    let values = [
        "",
        "O'Reilly",
        "\\",
        "\\\\",
        r"\'; DROP TABLE injection_guard; --",
        "Grüße\n世界",
        r#"{"value":"backslash\\ and quote'"}"#,
    ];
    for mode in ["off", "on"] {
        client
            .simple_query(&format!("SET standard_conforming_strings={mode}"))
            .unwrap();
        for value in values {
            assert_eq!(
                client
                    .simple_query(&format!("SELECT {}", sql_literal(value)))
                    .unwrap(),
                vec![vec![Some(value.to_owned())]],
                "literal changed with standard_conforming_strings={mode}"
            );
        }
        assert_eq!(
            client
                .simple_query("SELECT count(*) FROM injection_guard")
                .unwrap(),
            vec![vec![Some("0".into())]]
        );
    }
    // Positive control: the old quote-only helper really does execute the
    // payload under the nondefault setting. Only a temporary table is dropped.
    client
        .simple_query("SET standard_conforming_strings=off")
        .unwrap();
    let payload = r"\'; DROP TABLE injection_guard; --";
    let legacy_literal = format!("'{}'", payload.replace('\'', "''"));
    client
        .simple_query(&format!("SELECT {legacy_literal}"))
        .unwrap();
    assert_eq!(
        client
            .simple_query("SELECT to_regclass('pg_temp.injection_guard')")
            .unwrap(),
        vec![vec![None]]
    );
    // Replication connections also use startup options, without an extra SQL
    // command that would be invalid for the replication protocol.
    let mut replication = Client::connect(address.as_str(), &user, "postgres", true).unwrap();
    assert_eq!(
        replication.simple_query("IDENTIFY_SYSTEM").unwrap().len(),
        1
    );
}
