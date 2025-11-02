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
