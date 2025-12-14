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

//! Security-sensitive helpers used by Nanocloud.
//!
//! - [`SecureAssets`] manages the CA certificate, service RSA key, and volume keys on disk.
//!   Call [`SecureAssets::generate`](SecureAssets::generate) during setup to ensure keys exist
//!   before invoking TLS helpers.
//! - [`TlsInfo`] issues end-entity certificates (ECC P-256) signed by the on-disk CA and supports
//!   DNS/IP/URI SANs for SPIFFE identities.
//! - [`EncryptionKey`] and the [`kms`] module encapsulate envelope encryption: data keys are
//!   32-byte AES-256-GCM secrets that can be wrapped either via the local secure assets key
//!   (default) or via an external KMS implementation plugged in through [`kms::register_global_kms`].
//!
//! ## Guarantees
//!
//! * Private keys generated here are never written outside the secure-assets directory and are
//!   persisted with `0700` permissions.
//! * TLS certificates use SHA-256 signatures and default to a 100-year validity for the CA.
//! * Symmetric encryption always uses AES-256-GCM; legacy RSA/AES-CBC paths remain available only
//!   for decoding pre-existing secrets and are wrapped behind the same helpers.
//!
//! ## Limitations and guidance
//!
//! * Helpers are synchronous and perform filesystem IO; when invoking them from async code,
//!   call [`run_blocking_security`] so the work executes in a blocking thread without repeating
//!   `tokio::task::spawn_blocking` boilerplate.
//! * When running in environments with an external KMS, initialise it early via
//!   [`kms::register_global_kms`] so that [`EncryptionKey::new(None)`] can request new data keys.
//! * The utilities intentionally avoid touching production KMS endpoints during tests; integration
//!   tests should point `NANOCLOUD_SECURE_ASSETS` at a temporary directory and call
//!   [`SecureAssets::generate`] before creating TLS material.

mod assets;
mod blocking;
mod cert;
mod crypto;
pub mod kms;
mod tls;
pub mod volume;
#[cfg(all(test, feature = "security-test-noop"))]
pub mod noop;

#[cfg(test)]
#[allow(unused_imports)]
pub(crate) use assets::clear_asset_caches;
pub(crate) use assets::load_ca;
pub(crate) use assets::load_secret_key as load_service_secret_key;
pub use assets::{SecureAssets, VolumeKeyMetadata};
#[allow(unused_imports)]
pub use blocking::run_blocking_security;
pub(crate) use cert::sign_csr;
pub use crypto::EncryptionKey;
pub use tls::{JsonTlsInfo, TlsInfo};
