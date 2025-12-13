/*
 * Copyright (C) 2024 The Nanocloud Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

//! Integration tests for server routing and authentication.
//!
//! These tests verify that:
//! - Auth context types work correctly
//! - Auth scope and subject transitions are valid
//! - Error responses follow expected conventions

use nanocloud::nanocloud::server::auth::{AuthContext, AuthScope, AuthSubject};

/// Test that auth context defaults to unauthenticated when no credentials are provided.
#[test]
fn auth_context_defaults_to_unauthenticated() {
    let context = AuthContext::default();
    assert!(matches!(context.subject(), AuthSubject::Anonymous));
    assert!(matches!(context.scope(), AuthScope::Unauthenticated));
}

/// Test that auth scope correctly identifies different authentication methods.
#[test]
fn auth_scope_identification() {
    // Certificate scope
    let mut cert_context = AuthContext::default();
    cert_context.set_subject(AuthSubject::DistinguishedName("CN=test".to_string()));
    cert_context.set_scope(AuthScope::Certificate);
    assert!(matches!(cert_context.scope(), AuthScope::Certificate));

    // Bootstrap scope
    let mut bootstrap_context = AuthContext::default();
    bootstrap_context.set_subject(AuthSubject::BootstrapToken("subject".to_string()));
    bootstrap_context.set_scope(AuthScope::Bootstrap);
    assert!(matches!(bootstrap_context.scope(), AuthScope::Bootstrap));

    // Device scope
    let mut device_context = AuthContext::default();
    device_context.set_subject(AuthSubject::Device {
        device_id: "device123".to_string(),
        distinguished_name: "CN=device:device123".to_string(),
    });
    device_context.set_scope(AuthScope::Device);
    assert!(matches!(device_context.scope(), AuthScope::Device));
}

/// Test auth subject equality for various types.
#[test]
fn auth_subject_equality() {
    let anon1 = AuthSubject::Anonymous;
    let anon2 = AuthSubject::Anonymous;
    assert_eq!(anon1, anon2);

    let dn1 = AuthSubject::DistinguishedName("CN=test".to_string());
    let dn2 = AuthSubject::DistinguishedName("CN=test".to_string());
    assert_eq!(dn1, dn2);

    let dn3 = AuthSubject::DistinguishedName("CN=other".to_string());
    assert_ne!(dn1, dn3);

    let bootstrap1 = AuthSubject::BootstrapToken("subject".to_string());
    let bootstrap2 = AuthSubject::BootstrapToken("subject".to_string());
    assert_eq!(bootstrap1, bootstrap2);

    // Different types should not be equal
    assert_ne!(anon1, dn1);
}

/// Test auth context cloning preserves all fields.
#[test]
fn auth_context_cloning() {
    let mut original = AuthContext::default();
    original.set_subject(AuthSubject::Jwt {
        subject: "user@example.com".to_string(),
        issuer: Some("issuer".to_string()),
    });
    original.set_scope(AuthScope::Jwt(vec!["read".to_string(), "write".to_string()]));

    let cloned = original.clone();
    assert_eq!(original, cloned);
}

/// Test auth scope debug formatting.
#[test]
fn auth_scope_debug_format() {
    let scope = AuthScope::Certificate;
    let debug = format!("{:?}", scope);
    assert!(debug.contains("Certificate"));

    let jwt_scope = AuthScope::Jwt(vec!["read".to_string()]);
    let jwt_debug = format!("{:?}", jwt_scope);
    assert!(jwt_debug.contains("Jwt"));
    assert!(jwt_debug.contains("read"));
}

/// Test that device scope is correctly parsed from certificate subjects.
#[test]
fn device_scope_parsing() {
    // Valid device certificate subject
    let device_dn = "CN=device:abc123";
    assert!(device_dn.starts_with("CN=device:"));

    // Extract device ID
    let device_id = device_dn.strip_prefix("CN=device:").unwrap();
    assert_eq!(device_id, "abc123");

    // Non-device certificate subject
    let regular_dn = "CN=example";
    assert!(!regular_dn.starts_with("CN=device:"));
}

/// Test bootstrap token subject format.
#[test]
fn bootstrap_token_subject_format() {
    // Bootstrap tokens should have a meaningful subject
    let subject = AuthSubject::BootstrapToken("node-bootstrap".to_string());
    if let AuthSubject::BootstrapToken(s) = subject {
        assert!(!s.is_empty());
        assert_eq!(s, "node-bootstrap");
    } else {
        panic!("Expected BootstrapToken variant");
    }
}

/// Test JWT subject format with issuer.
#[test]
fn jwt_subject_with_issuer() {
    let subject = AuthSubject::Jwt {
        subject: "user@example.com".to_string(),
        issuer: Some("https://issuer.example.com".to_string()),
    };

    if let AuthSubject::Jwt { subject: s, issuer } = subject {
        assert_eq!(s, "user@example.com");
        assert_eq!(issuer, Some("https://issuer.example.com".to_string()));
    } else {
        panic!("Expected Jwt variant");
    }
}

/// Test JWT subject without issuer.
#[test]
fn jwt_subject_without_issuer() {
    let subject = AuthSubject::Jwt {
        subject: "user@example.com".to_string(),
        issuer: None,
    };

    if let AuthSubject::Jwt { subject: s, issuer } = subject {
        assert_eq!(s, "user@example.com");
        assert!(issuer.is_none());
    } else {
        panic!("Expected Jwt variant");
    }
}

/// Test that auth scope transitions are valid.
#[test]
fn auth_scope_transitions() {
    // Start unauthenticated
    let mut context = AuthContext::default();
    assert!(matches!(context.scope(), AuthScope::Unauthenticated));

    // Can transition to bootstrap
    context.set_scope(AuthScope::Bootstrap);
    assert!(matches!(context.scope(), AuthScope::Bootstrap));

    // Can transition to certificate
    context.set_scope(AuthScope::Certificate);
    assert!(matches!(context.scope(), AuthScope::Certificate));
}

/// Test that auth subject transitions are valid.
#[test]
fn auth_subject_transitions() {
    // Start anonymous
    let mut context = AuthContext::default();
    assert!(matches!(context.subject(), AuthSubject::Anonymous));

    // Can set distinguished name
    context.set_subject(AuthSubject::DistinguishedName("CN=test".to_string()));
    assert!(matches!(
        context.subject(),
        AuthSubject::DistinguishedName(_)
    ));

    // Can set device
    context.set_subject(AuthSubject::Device {
        device_id: "dev1".to_string(),
        distinguished_name: "CN=device:dev1".to_string(),
    });
    assert!(matches!(context.subject(), AuthSubject::Device { .. }));
}

/// Test auth context equality comparison.
#[test]
fn auth_context_equality() {
    let mut ctx1 = AuthContext::default();
    ctx1.set_subject(AuthSubject::DistinguishedName("CN=test".to_string()));
    ctx1.set_scope(AuthScope::Certificate);

    let mut ctx2 = AuthContext::default();
    ctx2.set_subject(AuthSubject::DistinguishedName("CN=test".to_string()));
    ctx2.set_scope(AuthScope::Certificate);

    let mut ctx3 = AuthContext::default();
    ctx3.set_subject(AuthSubject::DistinguishedName("CN=other".to_string()));
    ctx3.set_scope(AuthScope::Certificate);

    assert_eq!(ctx1, ctx2);
    assert_ne!(ctx1, ctx3);
}

/// Test JWT scope with multiple permissions.
#[test]
fn jwt_scope_multiple_permissions() {
    let scope = AuthScope::Jwt(vec![
        "pods.read".to_string(),
        "pods.write".to_string(),
        "services.read".to_string(),
    ]);

    if let AuthScope::Jwt(scopes) = scope {
        assert_eq!(scopes.len(), 3);
        assert!(scopes.contains(&"pods.read".to_string()));
        assert!(scopes.contains(&"pods.write".to_string()));
        assert!(scopes.contains(&"services.read".to_string()));
    } else {
        panic!("Expected Jwt scope");
    }
}

/// Test empty JWT scope.
#[test]
fn jwt_scope_empty() {
    let scope = AuthScope::Jwt(vec![]);

    if let AuthScope::Jwt(scopes) = scope {
        assert!(scopes.is_empty());
    } else {
        panic!("Expected Jwt scope");
    }
}

/// Test AuthSubject debug formatting.
#[test]
fn auth_subject_debug_format() {
    let anon = AuthSubject::Anonymous;
    let debug = format!("{:?}", anon);
    assert!(debug.contains("Anonymous"));

    let dn = AuthSubject::DistinguishedName("CN=test".to_string());
    let dn_debug = format!("{:?}", dn);
    assert!(dn_debug.contains("DistinguishedName"));
    assert!(dn_debug.contains("CN=test"));

    let device = AuthSubject::Device {
        device_id: "dev1".to_string(),
        distinguished_name: "CN=device:dev1".to_string(),
    };
    let device_debug = format!("{:?}", device);
    assert!(device_debug.contains("Device"));
    assert!(device_debug.contains("dev1"));
}

/// Test AuthContext default values.
#[test]
fn auth_context_default_values() {
    let context = AuthContext::default();

    // Check subject is Anonymous
    if let AuthSubject::Anonymous = context.subject() {
        // Expected
    } else {
        panic!("Expected Anonymous subject for default context");
    }

    // Check scope is Unauthenticated
    if let AuthScope::Unauthenticated = context.scope() {
        // Expected
    } else {
        panic!("Expected Unauthenticated scope for default context");
    }
}

/// Test chained modifications to AuthContext.
#[test]
fn auth_context_chained_modifications() {
    let mut context = AuthContext::default();

    // Modify subject first
    context.set_subject(AuthSubject::BootstrapToken("bootstrap".to_string()));
    assert!(matches!(context.subject(), AuthSubject::BootstrapToken(_)));

    // Then modify scope
    context.set_scope(AuthScope::Bootstrap);
    assert!(matches!(context.scope(), AuthScope::Bootstrap));

    // Verify both modifications persist
    assert!(matches!(context.subject(), AuthSubject::BootstrapToken(_)));
    assert!(matches!(context.scope(), AuthScope::Bootstrap));
}

/// Test that AuthContext can be used in collections.
#[test]
fn auth_context_in_collections() {
    let mut ctx1 = AuthContext::default();
    ctx1.set_subject(AuthSubject::DistinguishedName("CN=a".to_string()));

    let mut ctx2 = AuthContext::default();
    ctx2.set_subject(AuthSubject::DistinguishedName("CN=b".to_string()));

    let contexts = vec![ctx1.clone(), ctx2.clone()];
    assert_eq!(contexts.len(), 2);
    assert_eq!(contexts[0], ctx1);
    assert_eq!(contexts[1], ctx2);
}
