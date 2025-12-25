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

//! Shared helpers that power Nanocloud subsystems.
//!
//! - `error` and `keyspace` provide general-purpose utilities that can be used
//!   from most modules.
//! - The `security` submodule houses TLS/crypto helpers and should only be used
//!   when code already deals with sensitive material. It documents its own
//!   guarantees and limitations to prevent accidental misuse.
//!
//! Keeping these responsibilities in a single crate simplifies imports while
//! the documentation above makes the intended boundaries explicit.
//!
//! ## Future reorganization
//!
//! Security-heavy helpers will eventually graduate to a dedicated module or
//! crate so that audits can reason about cryptographic boundaries without
//! scanning general-purpose utilities. Until that split happens, code touching
//! TLS or encryption primitives should import from `nanocloud::util::security`
//! directly and avoid re-exporting sensitive wrappers through unrelated modules.

pub mod error;
mod keyspace;
pub mod security;

pub(crate) use keyspace::reset_partition_watch;
#[allow(unused_imports)]
pub use keyspace::{
    is_missing_value_error, Keyspace, KeyspaceEvent, KeyspaceEventType, SingleUseTokenOutcome,
};
