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

//! Thin error helpers used throughout the codebase.
//!
//! `new_error` converts user-facing strings into boxed errors, while
//! `with_context` wraps existing errors with additional context describing the
//! failing operation. The helpers keep diagnostics consistent and lightweight
//! without forcing every crate to define bespoke error types.

use std::error::Error;
use std::fmt;

#[derive(Debug)]
struct ContextError {
    context: String,
    source: Box<dyn Error + Send + Sync>,
}

impl ContextError {
    fn new(context: impl Into<String>, source: impl Into<Box<dyn Error + Send + Sync>>) -> Self {
        Self {
            context: normalize_message(context),
            source: source.into(),
        }
    }
}

impl fmt::Display for ContextError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.context, self.source)
    }
}

impl Error for ContextError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(self.source.as_ref())
    }
}

#[derive(Debug)]
struct SimpleError(String);

impl SimpleError {
    fn new(message: impl Into<String>) -> Self {
        Self(normalize_message(message))
    }
}

impl fmt::Display for SimpleError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl Error for SimpleError {}

/// Wraps an existing error with additional context.
///
/// Use this helper when propagating IO/network failures so that logs clearly
/// call out which step failed.
///
/// # Examples
///
/// ```
/// # use nanocloud::nanocloud::util::error::with_context;
/// # use std::fs::File;
/// # fn demo() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
/// let file = File::open("/tmp/missing").map_err(|err| {
///     with_context(err, "Failed to load cached configuration")
/// })?;
/// # let _ = file;
/// # Ok(())
/// # }
/// ```
pub fn with_context<E>(error: E, context: impl Into<String>) -> Box<dyn Error + Send + Sync>
where
    E: Into<Box<dyn Error + Send + Sync>>,
{
    Box::new(ContextError::new(context, error))
}

/// Creates a boxed error with a normalized, user-facing message.
///
/// Prefer this helper over ad-hoc `Box::<dyn Error>::from(...)` conversions so
/// that error strings remain consistently trimmed and easy to compare in tests.
///
/// # Examples
///
/// ```
/// # use nanocloud::nanocloud::util::error::new_error;
/// let err = new_error("key is missing");
/// assert!(err.to_string().contains("key is missing"));
/// ```
pub fn new_error(message: impl Into<String>) -> Box<dyn Error + Send + Sync> {
    Box::new(SimpleError::new(message))
}

fn normalize_message(message: impl Into<String>) -> String {
    let trimmed = message.into();
    let trimmed = trimmed.trim();
    if trimmed.is_empty() {
        "operation failed".to_string()
    } else {
        trimmed.to_string()
    }
}
