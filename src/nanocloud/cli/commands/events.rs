use std::error::Error;
use std::io;
use std::io::ErrorKind;
use std::time::Duration;

use chrono::DateTime;
use futures_util::StreamExt;
use humantime::parse_duration;
use serde_json;
use tokio::time::sleep;

use crate::nanocloud::api::client::{EventLevel, EventQuery, NanocloudClient};
use crate::nanocloud::cli::args::{EventLevelArg, EventsArgs};
use crate::nanocloud::cli::Terminal;
use crate::nanocloud::k8s::event::{Event, EventWatchEvent};

const WATCH_TIMEOUT_SECONDS: u64 = 30;

pub async fn handle_events(
    client: &NanocloudClient,
    args: &EventsArgs,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let since = args
        .since
        .as_deref()
        .map(normalize_since)
        .transpose()?
        .map(|value| value.to_string());
    let level = args.level.map(convert_level);
    let reasons = args
        .reasons
        .iter()
        .map(|value| value.trim())
        .filter(|value| !value.is_empty())
        .map(|value| value.to_string())
        .collect();
    let query = EventQuery {
        bundle: args.bundle.clone(),
        limit: args.limit,
        since,
        timeout_seconds: Some(WATCH_TIMEOUT_SECONDS),
        level,
        reasons,
        ..Default::default()
    };

    let namespace = args.namespace.as_deref();
    let list = client.list_events(namespace, &query).await?;
    let mut header_printed = false;
    let mut cursor = list.metadata.resource_version.clone();
    if !list.items.is_empty() {
        render_event_header();
        header_printed = true;
        for event in &list.items {
            render_event_row(event);
            if let Some(rv) = event.metadata.resource_version.as_ref() {
                cursor = Some(rv.clone());
            }
        }
    } else if args.follow {
        Terminal::stdout(format_args!("Waiting for events..."));
    } else {
        Terminal::stdout(format_args!("No events found."));
    }

    if args.follow {
        if !header_printed {
            render_event_header();
        }
        follow_events(client, namespace, &query, cursor).await?;
    }

    Ok(())
}

fn render_event_header() {
    Terminal::stdout(format_args!(
        "{:<24} {:<8} {:<28} {:<24} {}",
        "TIME", "TYPE", "REASON", "OBJECT", "MESSAGE"
    ));
}

fn render_event_row(event: &Event) {
    let timestamp = event_timestamp(event);
    let level = event.event_type.as_deref().unwrap_or("-");
    let reason = event.reason.as_deref().unwrap_or("-");
    let subject = event_subject(event);
    let message = event.message.as_deref().unwrap_or("-");
    Terminal::stdout(format_args!(
        "{:<24} {:<8} {:<28} {:<24} {}",
        timestamp, level, reason, subject, message
    ));
}

fn event_timestamp(event: &Event) -> &str {
    event
        .event_time
        .as_deref()
        .or(event.last_timestamp.as_deref())
        .or(event.first_timestamp.as_deref())
        .unwrap_or("-")
}

fn event_subject(event: &Event) -> String {
    let namespace = event
        .involved_object
        .namespace
        .as_deref()
        .or(event.metadata.namespace.as_deref())
        .unwrap_or("default");
    let name = event
        .involved_object
        .name
        .as_deref()
        .or(event.metadata.name.as_deref())
        .unwrap_or("-");
    format!("{}/{}", namespace, name)
}

fn convert_level(level: EventLevelArg) -> EventLevel {
    match level {
        EventLevelArg::Normal => EventLevel::Normal,
        EventLevelArg::Warning => EventLevel::Warning,
    }
}

fn normalize_since(value: &str) -> Result<String, io::Error> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err(io::Error::new(
            ErrorKind::InvalidInput,
            "--since must not be empty",
        ));
    }

    if DateTime::parse_from_rfc3339(trimmed).is_ok() {
        return Ok(trimmed.to_string());
    }

    parse_duration(trimmed).map_err(|_| {
        io::Error::new(
            ErrorKind::InvalidInput,
            "Invalid --since value. Use RFC3339 timestamps or durations like 30m, 6h, 2d",
        )
    })?;
    Ok(trimmed.to_string())
}

async fn follow_events(
    client: &NanocloudClient,
    namespace: Option<&str>,
    base_query: &EventQuery,
    mut cursor: Option<String>,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let mut attempts: u32 = 0;
    loop {
        let mut watch_query = base_query.clone();
        watch_query.resource_version = cursor.clone();
        let response = match client.watch_events(namespace, &watch_query).await {
            Ok(resp) => {
                attempts = 0;
                resp
            }
            Err(err) => {
                attempts = attempts.saturating_add(1);
                Terminal::error(format_args!("events watch failed: {}", err));
                sleep(backoff_duration(attempts)).await;
                continue;
            }
        };

        match consume_watch_stream(response, &mut cursor).await {
            Ok(()) => attempts = 0,
            Err(err) => {
                attempts = attempts.saturating_add(1);
                Terminal::error(format_args!("events stream error: {}", err));
            }
        }

        if attempts > 0 {
            sleep(backoff_duration(attempts)).await;
        }
    }
}

async fn consume_watch_stream(
    response: reqwest::Response,
    cursor: &mut Option<String>,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let mut stream = response.bytes_stream();
    let mut buffer: Vec<u8> = Vec::new();

    while let Some(chunk) = stream.next().await {
        let bytes = chunk?;
        buffer.extend_from_slice(&bytes);
        while let Some(pos) = buffer.iter().position(|b| *b == b'\n') {
            let line = buffer.drain(..=pos).collect::<Vec<u8>>();
            process_watch_line(&line, cursor)?;
        }
    }

    if !buffer.is_empty() {
        process_watch_line(&buffer, cursor)?;
    }

    Ok(())
}

fn process_watch_line(
    line: &[u8],
    cursor: &mut Option<String>,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let text = std::str::from_utf8(line)?.trim();
    if text.is_empty() {
        return Ok(());
    }
    let event: EventWatchEvent = serde_json::from_str(text)?;
    match event.event_type.as_str() {
        "BOOKMARK" => {
            if let Some(rv) = event.object.metadata.resource_version {
                *cursor = Some(rv);
            }
        }
        _ => {
            render_event_row(&event.object);
            if let Some(rv) = event.object.metadata.resource_version {
                *cursor = Some(rv);
            }
        }
    }
    Ok(())
}

fn backoff_duration(attempts: u32) -> Duration {
    let capped = attempts.min(6);
    let millis = 500u64.saturating_mul(1u64 << capped);
    Duration::from_millis(millis.min(10_000))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::nanocloud::k8s::event::{EventSource, ObjectReference};
    use crate::nanocloud::k8s::pod::ObjectMeta;

    fn sample_event(rv: &str, reason: &str, level: &str) -> Event {
        Event {
            api_version: "v1".to_string(),
            kind: "Event".to_string(),
            metadata: ObjectMeta {
                name: Some("event-1".to_string()),
                namespace: Some("default".to_string()),
                resource_version: Some(rv.to_string()),
                ..Default::default()
            },
            involved_object: ObjectReference {
                api_version: Some("nanocloud.io/v1".to_string()),
                kind: Some("Bundle".to_string()),
                name: Some("postgres".to_string()),
                namespace: Some("default".to_string()),
                uid: None,
                resource_version: None,
                field_path: None,
            },
            reason: Some(reason.to_string()),
            message: Some("bundle message".to_string()),
            event_type: Some(level.to_string()),
            first_timestamp: Some("2025-01-14T11:00:00Z".to_string()),
            last_timestamp: Some("2025-01-14T11:00:10Z".to_string()),
            event_time: Some("2025-01-14T11:00:10Z".to_string()),
            count: Some(1),
            reporting_component: Some("tests".to_string()),
            reporting_instance: Some("tests".to_string()),
            action: None,
            related: None,
            series: None,
            source: Some(EventSource {
                component: Some("tests".to_string()),
                host: None,
            }),
            deprecated_source: None,
            deprecated_first_timestamp: None,
            deprecated_last_timestamp: None,
            deprecated_count: None,
        }
    }

    #[test]
    fn normalize_since_accepts_rfc3339() {
        let value = normalize_since("2025-01-14T11:22:33Z").expect("rfc3339");
        assert_eq!(value, "2025-01-14T11:22:33Z");
    }

    #[test]
    fn normalize_since_accepts_duration() {
        let value = normalize_since("15m").expect("duration");
        assert_eq!(value, "15m");
    }

    #[test]
    fn normalize_since_rejects_invalid() {
        let error = normalize_since("nonsense").expect_err("invalid");
        assert_eq!(error.kind(), ErrorKind::InvalidInput);
    }

    #[test]
    fn process_watch_line_updates_cursor_for_events() {
        let event = EventWatchEvent {
            event_type: "ADDED".to_string(),
            object: sample_event("42", "BundleReconciled", "Normal"),
        };
        let json = serde_json::to_string(&event).expect("json");
        let mut cursor = None;
        process_watch_line(json.as_bytes(), &mut cursor).expect("process");
        assert_eq!(cursor.as_deref(), Some("42"));
    }

    #[test]
    fn process_watch_line_updates_cursor_for_bookmarks() {
        let bookmark = EventWatchEvent {
            event_type: "BOOKMARK".to_string(),
            object: Event {
                metadata: ObjectMeta {
                    resource_version: Some("99".to_string()),
                    ..Default::default()
                },
                ..sample_event("0", "BundleReconciled", "Normal")
            },
        };
        let json = serde_json::to_string(&bookmark).expect("json");
        let mut cursor = Some("1".to_string());
        process_watch_line(json.as_bytes(), &mut cursor).expect("process");
        assert_eq!(cursor.as_deref(), Some("99"));
    }

    #[test]
    fn event_subject_prefers_involved_object() {
        let event = sample_event("10", "BundleReconciled", "Normal");
        assert_eq!(event_subject(&event), "default/postgres");
    }

    #[test]
    fn convert_level_maps_cli_enum() {
        assert!(matches!(
            convert_level(EventLevelArg::Normal),
            EventLevel::Normal
        ));
        assert!(matches!(
            convert_level(EventLevelArg::Warning),
            EventLevel::Warning
        ));
    }
}
