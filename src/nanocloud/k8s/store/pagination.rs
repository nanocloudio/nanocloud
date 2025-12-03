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

use base64::engine::general_purpose::STANDARD as BASE64_ENGINE;
use base64::Engine;
use serde::{Deserialize, Serialize};
use std::error::Error;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ListCursor {
    pub key: String,
    #[serde(rename = "resourceVersion", skip_serializing_if = "Option::is_none")]
    pub resource_version: Option<String>,
}

#[derive(Debug)]
pub enum PaginationError {
    InvalidContinue(String),
    InvalidLimit(String),
}

impl std::fmt::Display for PaginationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PaginationError::InvalidContinue(msg) => write!(f, "{msg}"),
            PaginationError::InvalidLimit(msg) => write!(f, "{msg}"),
        }
    }
}

impl Error for PaginationError {}

#[derive(Debug)]
pub struct PaginatedResult<T> {
    pub items: Vec<T>,
    pub next_cursor: Option<ListCursor>,
    pub remaining: usize,
}

pub fn encode_continue_token(kind: &str, cursor: &ListCursor) -> String {
    let payload = serde_json::json!({
        "kind": kind,
        "key": cursor.key,
        "resourceVersion": cursor.resource_version,
    });
    BASE64_ENGINE.encode(payload.to_string())
}

pub fn decode_continue_token(
    token: &str,
    expected_kind: &str,
) -> Result<ListCursor, PaginationError> {
    let decoded = BASE64_ENGINE.decode(token).map_err(|_| {
        PaginationError::InvalidContinue("continue token is not valid base64".to_string())
    })?;
    let value: serde_json::Value = serde_json::from_slice(&decoded).map_err(|_| {
        PaginationError::InvalidContinue("continue token payload is not valid JSON".to_string())
    })?;

    let kind = value
        .get("kind")
        .and_then(|kind| kind.as_str())
        .ok_or_else(|| {
            PaginationError::InvalidContinue("continue token missing kind".to_string())
        })?;
    if kind != expected_kind {
        return Err(PaginationError::InvalidContinue(format!(
            "continue token was issued for '{}', not '{}'",
            kind, expected_kind
        )));
    }

    let key = value
        .get("key")
        .and_then(|key| key.as_str())
        .ok_or_else(|| {
            PaginationError::InvalidContinue("continue token missing key".to_string())
        })?;
    let resource_version = value
        .get("resourceVersion")
        .and_then(|rv| rv.as_str())
        .map(|rv| rv.to_string());

    Ok(ListCursor {
        key: key.to_string(),
        resource_version,
    })
}

pub fn paginate_entries<T>(
    mut entries: Vec<(String, T, Option<String>)>,
    cursor: Option<&ListCursor>,
    limit: Option<u32>,
) -> Result<PaginatedResult<T>, PaginationError> {
    if let Some(limit) = limit {
        if limit == 0 {
            return Err(PaginationError::InvalidLimit(
                "limit must be greater than 0".to_string(),
            ));
        }
    }

    entries.sort_by(|a, b| a.0.cmp(&b.0));

    let start_index = if let Some(cursor) = cursor {
        match entries.iter().position(|(key, _, _)| key == &cursor.key) {
            Some(index) => index.saturating_add(1),
            None => {
                return Err(PaginationError::InvalidContinue(
                    "continue token no longer matches available items".to_string(),
                ))
            }
        }
    } else {
        0
    };

    let total_after_start = entries.len().saturating_sub(start_index);

    let take_count = match limit {
        Some(limit) => std::cmp::min(limit as usize, total_after_start),
        None => total_after_start,
    };

    let mut items = Vec::with_capacity(take_count);
    let mut last_key: Option<String> = None;
    let mut last_rv: Option<String> = None;

    for (index, (key, value, rv)) in entries.into_iter().skip(start_index).enumerate() {
        if index >= take_count {
            break;
        }
        last_key = Some(key);
        last_rv = rv;
        items.push(value);
    }

    let remaining = total_after_start.saturating_sub(items.len());
    let next_cursor = if remaining > 0 {
        last_key.map(|key| ListCursor {
            key,
            resource_version: last_rv,
        })
    } else {
        None
    };

    Ok(PaginatedResult {
        items,
        next_cursor,
        remaining,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn paginate_entries_without_limit_returns_all() {
        let entries = vec![
            ("ns-a/item1".to_string(), 1, Some("10".to_string())),
            ("ns-a/item2".to_string(), 2, Some("11".to_string())),
        ];
        let result = paginate_entries(entries, None, None).expect("should succeed");
        assert_eq!(result.items, vec![1, 2]);
        assert!(result.next_cursor.is_none());
        assert_eq!(result.remaining, 0);
    }

    #[test]
    fn paginate_entries_with_limit_sets_cursor() {
        let entries = vec![
            ("a/1".to_string(), 1, Some("1".to_string())),
            ("a/2".to_string(), 2, Some("2".to_string())),
            ("a/3".to_string(), 3, Some("3".to_string())),
        ];
        let result = paginate_entries(entries, None, Some(2)).expect("should succeed");
        assert_eq!(result.items, vec![1, 2]);
        assert_eq!(result.remaining, 1);
        let cursor = result.next_cursor.expect("cursor expected");
        assert_eq!(cursor.key, "a/2");
        assert_eq!(cursor.resource_version.as_deref(), Some("2"));
    }

    #[test]
    fn zero_limit_is_rejected() {
        let entries = vec![("a/1".to_string(), 1, None)];
        let err = paginate_entries(entries, None, Some(0)).unwrap_err();
        match err {
            PaginationError::InvalidLimit(msg) => assert!(msg.contains("greater than 0")),
            _ => panic!("expected InvalidLimit"),
        }
    }

    #[test]
    fn decode_invalid_token_errors() {
        let err = decode_continue_token("not-base64", "pods").unwrap_err();
        match err {
            PaginationError::InvalidContinue(msg) => {
                assert!(msg.contains("base64"));
            }
            PaginationError::InvalidLimit(_) => panic!("expected invalid continue error"),
        }
    }
}
