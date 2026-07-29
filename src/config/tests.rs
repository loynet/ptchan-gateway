use std::{collections::HashMap, time::Duration};

use super::{file, validation, LogFormat};

#[test]
fn validates_runtime_address() {
    let mut config = valid_file();
    config.runtime.http_addr = "not-an-address".to_string();

    let err = validation::validate(&config).unwrap_err();

    assert!(err.to_string().contains("runtime.http_addr is invalid"));
}

#[test]
fn validates_posting_name_after_resolving_tripcode() {
    let raw = config_with_posting(Some("ptchan-gateway"));
    let file = file::parse(&raw).unwrap();
    let secrets = HashMap::from([
        ("PTCHAN_INTEGRATION_EXAMPLE_SECRET", "integration-secret"),
        (
            "PTCHAN_INTEGRATION_EXAMPLE_TRIPCODE",
            "this-is-an-example-ok",
        ),
        ("PTCHAN_INTEGRATION_EXAMPLE_POST_PASSWORD", "password"),
    ]);

    let err = file::resolve(file, |name, _| Ok(secrets.get(name).unwrap().to_string()))
        .err()
        .unwrap();

    assert!(err
        .to_string()
        .contains("integration example posting name is"));
}

#[test]
fn resolves_only_secrets_required_by_enabled_capabilities() {
    let file = file::parse(&config_with_posting(None)).unwrap();
    let requested = std::cell::RefCell::new(Vec::new());
    let config = file::resolve(file, |name, _| {
        requested.borrow_mut().push(name.to_string());
        Ok(match name {
            "PTCHAN_INTEGRATION_EXAMPLE_SECRET" => "integration-secret",
            "PTCHAN_INTEGRATION_EXAMPLE_TRIPCODE" => "trip-secret",
            "PTCHAN_INTEGRATION_EXAMPLE_POST_PASSWORD" => "post-password",
            _ => panic!("unexpected secret {name}"),
        }
        .to_string())
    })
    .unwrap();

    assert_eq!(
        requested.into_inner(),
        [
            "PTCHAN_INTEGRATION_EXAMPLE_SECRET",
            "PTCHAN_INTEGRATION_EXAMPLE_TRIPCODE",
            "PTCHAN_INTEGRATION_EXAMPLE_POST_PASSWORD",
        ]
    );
    assert_eq!(config.integrations.len(), 1);
    assert_eq!(config.postings.len(), 1);
    assert!(config.webhooks.is_empty());
    assert!(config.fingerprint_secret.is_none());
    assert_eq!(config.postings[0].form_name(), "example##trip-secret");
    assert_eq!(config.postings[0].tripcode_secret, "trip-secret");
    assert_eq!(config.postings[0].public_tripcode, "!!X8NXmAS44=");
}

#[test]
fn resolves_fingerprint_secret_only_when_requested() {
    let file = file::parse(
        r#"
[ptchan]
base_url = "https://ptchan.test"

[[integration]]
name = "example"

[integration.webhook]
url = "https://integration.test/events"
include_poster_fingerprint = true

[integration.posting]
public_tripcode = "!!X8NXmAS44="

[storage]
sqlite_path = "data/test.db"
"#,
    )
    .unwrap();
    let requested = std::cell::RefCell::new(Vec::new());

    let config = file::resolve(file, |name, _| {
        requested.borrow_mut().push(name.to_string());
        Ok(match name {
            "PTCHAN_INTEGRATION_EXAMPLE_TRIPCODE" => "trip-secret",
            "PTCHAN_INTEGRATION_EXAMPLE_POST_PASSWORD" => "post-password",
            "PTCHAN_INTEGRATION_EXAMPLE_SECRET" | "PTCHAN_FINGERPRINT_SECRET" => "secret",
            _ => panic!("unexpected secret {name}"),
        }
        .to_string())
    })
    .unwrap();

    assert_eq!(
        requested.into_inner(),
        [
            "PTCHAN_INTEGRATION_EXAMPLE_SECRET",
            "PTCHAN_INTEGRATION_EXAMPLE_TRIPCODE",
            "PTCHAN_INTEGRATION_EXAMPLE_POST_PASSWORD",
            "PTCHAN_FINGERPRINT_SECRET",
        ]
    );
    assert!(config.fingerprint_secret.is_some());
    assert_eq!(config.postings[0].post_password, "post-password");
}

#[test]
fn validates_public_tripcode_shape_and_uniqueness() {
    let mut config = valid_file();
    config.integrations[0].posting = Some(posting("not-a-tripcode"));

    assert!(validation::validate(&config)
        .unwrap_err()
        .to_string()
        .contains("public_tripcode must be the 12-character"));

    config.integrations[0].posting = Some(posting("!!X8NXmAS44="));
    let mut duplicate = config.integrations[0].clone();
    duplicate.name = "other".to_string();
    config.integrations.push(duplicate);

    assert!(validation::validate(&config)
        .unwrap_err()
        .to_string()
        .contains("public_tripcode conflicts"));
}

#[test]
fn validates_integration_rate_limit() {
    let mut config = valid_file();
    config.integrations[0].rate_limit.reading.burst = 0;
    assert!(validation::validate(&config)
        .unwrap_err()
        .to_string()
        .contains("integration example rate_limit.reading burst"));

    config.integrations[0].rate_limit.reading.burst =
        config.integrations[0].rate_limit.reading.requests + 1;
    assert!(validation::validate(&config)
        .unwrap_err()
        .to_string()
        .contains("integration example rate_limit.reading burst"));

    config.integrations[0].rate_limit.reading.burst = 1;
    config.integrations[0].rate_limit.reading.requests = 2_000_000_000;
    config.integrations[0].rate_limit.reading.window = Duration::from_secs(1);
    assert!(validation::validate(&config)
        .unwrap_err()
        .to_string()
        .contains("integration example rate_limit.reading window is too small"));

    let mut config = valid_file();
    config.integrations[0].rate_limit.posting.burst = 0;
    assert!(validation::validate(&config)
        .unwrap_err()
        .to_string()
        .contains("integration example rate_limit.posting burst"));
}

#[test]
fn validates_integration_names_for_env_and_metrics() {
    let mut config = valid_file();
    config.integrations[0].name = "bad name".to_string();

    assert!(validation::validate(&config)
        .unwrap_err()
        .to_string()
        .contains("integration name bad name is invalid"));
}

#[test]
fn rejects_integration_env_name_collisions() {
    let mut config = valid_file();
    let mut duplicate = config.integrations[0].clone();
    duplicate.name = "example-test".to_string();
    duplicate.webhook = None;
    config.integrations.push(duplicate);
    config.integrations[0].name = "example_test".to_string();

    assert!(validation::validate(&config)
        .unwrap_err()
        .to_string()
        .contains("conflicts with another integration environment name"));
}

#[test]
fn defaults_runtime_and_logging_sections() {
    let raw = r#"
[ptchan]
base_url = "https://ptchan.test"

[storage]
sqlite_path = "data/test.db"
"#;

    let config = file::parse(raw).unwrap();

    assert_eq!(config.runtime.http_addr, "0.0.0.0:8080");
    assert_eq!(config.runtime.logging.level, "info");
    assert!(matches!(config.runtime.logging.format, LogFormat::Json));
    assert_eq!(config.runtime.rate_limit.reading.requests, 1_000);
    assert_eq!(config.runtime.rate_limit.reading.burst, 200);
    assert_eq!(config.runtime.rate_limit.posting.requests, 100);
    assert_eq!(config.runtime.rate_limit.posting.burst, 20);
}

#[test]
fn reading_capability_is_enabled_by_section_presence() {
    let raw = r#"
[ptchan]
base_url = "https://ptchan.test"

[[integration]]
name = "example"

[integration.reading]

[storage]
sqlite_path = "data/test.db"
"#;

    let file = file::parse(raw).unwrap();
    let config = file::resolve(file, |_, _| Ok("secret".to_string())).unwrap();

    assert!(config.integrations[0].reading);
    assert_eq!(config.integrations[0].rate_limit.reading.requests, 120);
    assert_eq!(
        config.integrations[0].rate_limit.reading.window,
        Duration::from_secs(60)
    );
    assert_eq!(config.integrations[0].rate_limit.reading.burst, 30);
    assert!(file::parse(
        r#"
[ptchan]
base_url = "https://ptchan.test"

[[integration]]
name = "example"

[integration.reading]
enabled = true

[storage]
sqlite_path = "data/test.db"
"#
    )
    .is_err());
}

fn valid_file() -> file::FileConfig {
    file::parse(
        r#"
[ptchan]
base_url = "https://ptchan.test"

[[integration]]
name = "example"

[integration.reading]

[integration.webhook]
url = "http://127.0.0.1:8081/events"

[storage]
sqlite_path = "data/test.db"
"#,
    )
    .unwrap()
}

fn posting(public_tripcode: &str) -> file::FilePosting {
    file::FilePosting {
        display_name: None,
        public_tripcode: public_tripcode.to_string(),
    }
}

fn config_with_posting(display_name: Option<&str>) -> String {
    let display_name = display_name
        .map(|name| format!("display_name = {name:?}\n"))
        .unwrap_or_default();
    format!(
        r#"
[ptchan]
base_url = "https://ptchan.test"

[[integration]]
name = "example"

[integration.posting]
{display_name}public_tripcode = "!!X8NXmAS44="

[storage]
sqlite_path = "data/test.db"
"#
    )
}
