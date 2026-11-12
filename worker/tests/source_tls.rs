//! Invoked by tests/security/probe_tls.py against disposable PostgreSQL 18.
use lake_replication_worker::credentials::Settings;
use lake_replication_worker::wire::{Client, TlsConfig};

#[test]
#[ignore = "requires the disposable TLS fixture"]
fn managed_workers_use_connection_policy_despite_environment_overrides() {
    let address = std::env::var("LAKE_SQL_TEST_ADDRESS").unwrap();
    let subscription = std::env::var("LAKE_SUBSCRIPTION_ID")
        .unwrap()
        .parse()
        .unwrap();
    let mut settings = Settings::default();
    settings
        .load_subscription_source_credentials(
            subscription,
            &address,
            "postgres",
            "postgres",
            None,
            None,
        )
        .unwrap();
    assert_eq!(settings.value("LAKE_SOURCE_TLS_MODE", ""), "verify-full");
    assert_eq!(
        settings.value("LAKE_SOURCE_TLS_ROOT_CERT", ""),
        "/tmp/tls-certs/ca.crt"
    );
    assert_eq!(
        settings.value("LAKE_SOURCE_TLS_SERVER_NAME", ""),
        "source.test"
    );
    let root = std::env::var("LAKE_TEST_CA_FILE").unwrap();
    // The CA path is shared with libpq on the destination host in production.
    // The fixture copies that public certificate out for this host-native test.
    let tls = TlsConfig::verify_full(
        &root,
        Some(settings.value("LAKE_SOURCE_TLS_SERVER_NAME", "")),
    );
    let mut client =
        Client::connect_secure(&address, "postgres", "postgres", false, None, Some(&tls)).unwrap();
    assert_eq!(
        client
            .simple_query("SELECT ssl::text FROM pg_stat_ssl WHERE pid=pg_backend_pid()")
            .unwrap(),
        vec![vec![Some("true".into())]]
    );
    for (ca, name, endpoint) in [
        (
            std::env::var("LAKE_TEST_BAD_CA_FILE").unwrap(),
            "source.test",
            address.clone(),
        ),
        (root.clone(), "wrong.test", address.clone()),
        (
            root,
            "source.test",
            std::env::var("LAKE_TEST_PLAIN_ADDRESS").unwrap(),
        ),
    ] {
        assert!(
            Client::connect_secure(
                &endpoint,
                "postgres",
                "postgres",
                false,
                None,
                Some(&TlsConfig::verify_full(ca, Some(name.into())))
            )
            .is_err()
        );
    }
}
