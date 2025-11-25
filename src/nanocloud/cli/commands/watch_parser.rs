use std::fmt;
use std::ops::ControlFlow;

use bytes::BytesMut;
use futures_util::StreamExt;
use serde::de::DeserializeOwned;
use tokio_util::sync::CancellationToken;

#[derive(Debug)]
pub(crate) enum WatchParseError {
    BufferExceeded { limit: usize },
    Utf8(std::str::Utf8Error),
    Json(serde_json::Error),
    Stream(reqwest::Error),
}

impl fmt::Display for WatchParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            WatchParseError::BufferExceeded { limit } => {
                write!(f, "watch line exceeded {} bytes", limit)
            }
            WatchParseError::Utf8(err) => write!(f, "watch stream contained invalid UTF-8: {err}"),
            WatchParseError::Json(err) => write!(f, "failed to parse watch event: {err}"),
            WatchParseError::Stream(err) => write!(f, "watch stream error: {err}"),
        }
    }
}

impl std::error::Error for WatchParseError {}

/// Parse newline-delimited JSON payloads from a streaming response with a bounded buffer.
/// Cancels early when the optional token fires and surfaces malformed UTF-8/JSON as typed errors.
pub(crate) async fn parse_json_lines<S, T, F>(
    mut stream: S,
    max_buffer: usize,
    cancel: Option<&CancellationToken>,
    mut on_item: F,
) -> Result<(), WatchParseError>
where
    S: futures_core::Stream<Item = Result<bytes::Bytes, reqwest::Error>> + Unpin,
    T: DeserializeOwned,
    F: FnMut(T) -> ControlFlow<()>,
{
    let mut buffer = BytesMut::new();

    loop {
        let next = if let Some(token) = cancel {
            tokio::select! {
                biased;
                _ = token.cancelled() => None,
                item = stream.next() => item,
            }
        } else {
            stream.next().await
        };
        let Some(chunk) = next else {
            break;
        };
        let chunk = chunk.map_err(WatchParseError::Stream)?;
        if buffer.len() + chunk.len() > max_buffer {
            return Err(WatchParseError::BufferExceeded { limit: max_buffer });
        }
        buffer.extend_from_slice(&chunk);
        while let Some(pos) = buffer.iter().position(|b| *b == b'\n') {
            let line = buffer.split_to(pos + 1);
            process_line(line.as_ref(), &mut on_item, max_buffer)?;
        }
    }

    if !buffer.is_empty() {
        process_line(buffer.as_ref(), &mut on_item, max_buffer)?;
    }

    Ok(())
}

fn process_line<T, F>(
    line: &[u8],
    on_item: &mut F,
    max_buffer: usize,
) -> Result<(), WatchParseError>
where
    T: DeserializeOwned,
    F: FnMut(T) -> ControlFlow<()>,
{
    if line.len() > max_buffer {
        return Err(WatchParseError::BufferExceeded { limit: max_buffer });
    }
    let mut data = line;
    if let Some(stripped) = line.strip_suffix(b"\n") {
        data = stripped;
    }
    if let Some(stripped) = data.strip_suffix(b"\r") {
        data = stripped;
    }
    if data.is_empty() {
        return Ok(());
    }
    let text = std::str::from_utf8(data).map_err(WatchParseError::Utf8)?;
    let parsed: T = serde_json::from_str(text.trim()).map_err(WatchParseError::Json)?;
    if let ControlFlow::Break(()) = on_item(parsed) {
        return Ok(());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use futures_util::stream;
    use serde::Deserialize;
    use tokio_util::sync::CancellationToken;

    #[derive(Deserialize, Debug, PartialEq)]
    struct Event {
        value: u32,
    }

    #[tokio::test]
    async fn parses_multiple_lines() {
        let stream = stream::iter(vec![
            Ok(Bytes::from(r#"{"value":1}"#)),
            Ok(Bytes::from("\n")),
            Ok(Bytes::from(r#"{"value":2}"#)),
            Ok(Bytes::from("\n")),
        ]);
        let mut seen = Vec::new();
        parse_json_lines::<_, Event, _>(stream, 1024, None, |event| {
            seen.push(event.value);
            ControlFlow::Continue(())
        })
        .await
        .expect("parse");
        assert_eq!(seen, vec![1, 2]);
    }

    #[tokio::test]
    async fn stops_on_cancellation() {
        let stream = stream::pending::<Result<Bytes, reqwest::Error>>();
        let token = CancellationToken::new();
        token.cancel();
        parse_json_lines::<_, Event, _>(stream, 1024, Some(&token), |_event| {
            ControlFlow::Continue(())
        })
        .await
        .expect("cancel graceful");
    }

    #[tokio::test]
    async fn errors_on_buffer_overflow() {
        let payload = "a".repeat(10);
        let stream = stream::iter(vec![Ok(Bytes::from(payload))]);
        let result =
            parse_json_lines::<_, Event, _>(stream, 5, None, |_| ControlFlow::Continue(()))
                .await
                .expect_err("overflow");
        assert!(matches!(result, WatchParseError::BufferExceeded { .. }));
    }

    #[tokio::test]
    async fn errors_on_invalid_utf8() {
        let stream = stream::iter(vec![Ok(Bytes::from(vec![0xf0, 0x28, 0x8c, 0x28]))]);
        let result =
            parse_json_lines::<_, Event, _>(stream, 1024, None, |_| ControlFlow::Continue(()))
                .await
                .expect_err("utf8");
        assert!(matches!(result, WatchParseError::Utf8(_)));
    }

    #[tokio::test]
    async fn errors_on_invalid_json() {
        let stream = stream::iter(vec![Ok(Bytes::from("not-json\n"))]);
        let result =
            parse_json_lines::<_, Event, _>(stream, 1024, None, |_| ControlFlow::Continue(()))
                .await
                .expect_err("json");
        assert!(matches!(result, WatchParseError::Json(_)));
    }
}
