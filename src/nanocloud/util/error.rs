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
            context: context.into(),
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
        Self(message.into())
    }
}

impl fmt::Display for SimpleError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl Error for SimpleError {}

pub fn with_context<E>(error: E, context: impl Into<String>) -> Box<dyn Error + Send + Sync>
where
    E: Into<Box<dyn Error + Send + Sync>>,
{
    Box::new(ContextError::new(context, error))
}

pub fn new_error(message: impl Into<String>) -> Box<dyn Error + Send + Sync> {
    Box::new(SimpleError::new(message))
}
