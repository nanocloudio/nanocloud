use std::collections::HashMap;

use nanocloud::nanocloud::engine::profile::Profile;

#[test]
fn profile_to_json_roundtrip_preserves_data() {
    let mut options = HashMap::new();
    options.insert("region".to_string(), "us-central".to_string());
    options.insert("secret_token".to_string(), "s3cr3t".to_string());

    let profile = Profile::from_options(&options).expect("build profile");
    let json = profile.to_json().expect("serialize profile");
    let decoded = Profile::from_json(json.into_bytes()).expect("deserialize profile");

    let (key, config) = decoded
        .to_serialized_fields()
        .expect("extract serialized fields");
    assert!(!key.is_empty(), "wrapped profile key should be present");
    assert_eq!(config.get("region"), Some(&"us-central".to_string()));
    assert_eq!(config.get("secret_token"), Some(&"s3cr3t".to_string()));
}

#[test]
fn export_components_separates_secrets_from_options() {
    let mut options = HashMap::new();
    options.insert("region".to_string(), "us-west".to_string());
    options.insert("secret_api_key".to_string(), "apisecret".to_string());

    let profile = Profile::from_options(&options).expect("build profile");
    let (profile_key, non_secret, secrets) = profile
        .export_components()
        .expect("export components");

    assert!(
        !profile_key.is_empty(),
        "exported profile key should not be empty"
    );
    assert_eq!(
        non_secret.get("region"),
        Some(&"us-west".to_string()),
        "non-secret options should be preserved"
    );
    assert!(
        !non_secret.contains_key("secret_api_key"),
        "secret entries should be excluded from the public options map"
    );
    assert_eq!(secrets.len(), 1, "expected single secret entry");
    let secret = &secrets[0];
    assert_eq!(secret.name, "secret_api_key");
    assert!(
        !secret.cipher_text.is_empty(),
        "cipher text should be populated for secret exports"
    );
    assert!(
        !secret.key_id.is_empty(),
        "key identifier should be recorded for secret exports"
    );
}
