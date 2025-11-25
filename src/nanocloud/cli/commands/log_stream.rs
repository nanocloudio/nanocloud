use std::error::Error;
use std::time::{Duration, Instant};

use bytes::Bytes;
use futures_util::future::BoxFuture;
use futures_util::stream::Stream;
use futures_util::StreamExt;
use std::pin::Pin;
use tokio::io::{self as tokio_io, AsyncWriteExt, BufWriter};
use tokio::time::{sleep, timeout, MissedTickBehavior};
use tokio_util::sync::CancellationToken;

use crate::nanocloud::api::client::{HttpError, NanocloudClient};
use crate::nanocloud::cli::Terminal;

/// Tunable settings for log/event streaming loops.
/// `flush_interval` controls how often buffered stdout is flushed; `drain_timeout` sets how long to read remaining bytes after cancellation (set to zero to skip draining).
#[derive(Clone, Copy, Debug)]
pub(crate) struct LogStreamConfig {
    pub retry_backoff: Duration,
    pub max_backoff: Duration,
    pub drain_timeout: Duration,
    pub max_retries: Option<u32>,
    pub flush_interval: Duration,
}

type LogByteStream = Pin<Box<dyn Stream<Item = Result<Bytes, reqwest::Error>> + Send + 'static>>;

pub(crate) trait LogStreamClient: Clone + Send + Sync + 'static {
    fn logs_stream(
        &self,
        namespace: Option<&str>,
        service: &str,
        follow: bool,
    ) -> BoxFuture<'static, Result<LogByteStream, Box<dyn Error + Send + Sync>>>;
}

impl LogStreamClient for NanocloudClient {
    fn logs_stream(
        &self,
        namespace: Option<&str>,
        service: &str,
        follow: bool,
    ) -> BoxFuture<'static, Result<LogByteStream, Box<dyn Error + Send + Sync>>> {
        let ns = namespace.map(|value| value.to_string());
        let svc = service.to_string();
        let client = self.clone();
        Box::pin(async move {
            let response = client.logs_stream(ns.as_deref(), &svc, follow).await?;
            Ok(Box::pin(response.bytes_stream()) as LogByteStream)
        })
    }
}

impl Default for LogStreamConfig {
    fn default() -> Self {
        Self {
            retry_backoff: Duration::from_millis(500),
            max_backoff: Duration::from_secs(5),
            drain_timeout: Duration::from_millis(200),
            max_retries: None,
            flush_interval: Duration::from_millis(200),
        }
    }
}

/// Stream logs to stdout with retry/backoff for transient errors and cooperative cancellation.
/// Buffers stdout and flushes on newlines or at the configured flush interval to balance throughput and responsiveness.
pub(crate) async fn stream_logs_to_terminal<C: LogStreamClient>(
    client: C,
    namespace: Option<String>,
    service: String,
    follow: bool,
    cancel: CancellationToken,
    config: LogStreamConfig,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let mut retry_backoff = config.retry_backoff;
    let mut attempts: u32 = 0;

    loop {
        if cancel.is_cancelled() {
            return Ok(());
        }
        let stream_result = tokio::select! {
            res = client.logs_stream(namespace.as_deref(), &service, follow) => res,
            _ = cancel.cancelled() => return Ok(()),
        };
        match stream_result {
            Ok(mut stream) => {
                let mut stdout = BufWriter::new(tokio_io::stdout());
                let mut flush_tick = tokio::time::interval(config.flush_interval);
                flush_tick.set_missed_tick_behavior(MissedTickBehavior::Delay);
                let mut needs_flush = false;
                loop {
                    tokio::select! {
                        _ = cancel.cancelled() => {
                            if needs_flush {
                                stdout.flush().await?;
                            }
                            if config.drain_timeout.is_zero() {
                                return Ok(());
                            }
                            let drain_deadline = Instant::now() + config.drain_timeout;
                            loop {
                                let now = Instant::now();
                                if now >= drain_deadline {
                                    break;
                                }
                                let remaining = drain_deadline.saturating_duration_since(now);
                                match timeout(remaining, stream.next()).await {
                                    Ok(Some(chunk)) => {
                                        let bytes = chunk?;
                                        stdout.write_all(&bytes).await?;
                                        stdout.flush().await?;
                                    }
                                    Ok(None) => break,
                                    Err(_) => break,
                                }
                            }
                            return Ok(());
                        }
                        chunk = stream.next() => {
                            match chunk {
                                Some(data) => {
                                    let bytes = data?;
                                    stdout.write_all(&bytes).await?;
                                    needs_flush = true;
                                    if bytes.contains(&b'\n') {
                                        stdout.flush().await?;
                                        needs_flush = false;
                                    }
                                }
                                None => {
                                    if needs_flush {
                                        stdout.flush().await?;
                                    }
                                    return Ok(());
                                },
                            }
                        }
                        _ = flush_tick.tick(), if needs_flush => {
                            stdout.flush().await?;
                            needs_flush = false;
                        }
                    }
                }
            }
            Err(err) => {
                let backoff = retry_backoff.min(config.max_backoff);
                let should_retry = match err.downcast::<HttpError>() {
                    Ok(http_err) => {
                        if http_err.status == reqwest::StatusCode::NOT_FOUND && follow {
                            Terminal::stderr(format_args!(
                                "Service '{}' not ready yet; retrying log stream in {:?}",
                                service, backoff
                            ));
                            true
                        } else {
                            return Err(Box::new(http_err));
                        }
                    }
                    Err(err) => match err.downcast::<reqwest::Error>() {
                        Ok(req_err) => {
                            if follow && (req_err.is_connect() || req_err.is_timeout()) {
                                Terminal::stderr(format_args!(
                                    "Log stream error ({}); retrying in {:?}",
                                    req_err, backoff
                                ));
                                true
                            } else {
                                return Err(Box::new(req_err));
                            }
                        }
                        Err(other) => return Err(other),
                    },
                };

                if should_retry {
                    attempts = attempts.saturating_add(1);
                    if let Some(limit) = config.max_retries {
                        if attempts > limit || cancel.is_cancelled() {
                            return Err(format!("log stream failed after {} retries", limit).into());
                        }
                    }
                    tokio::select! {
                        _ = sleep(backoff) => {},
                        _ = cancel.cancelled() => return Ok(()),
                    }
                    retry_backoff = (retry_backoff * 2).min(config.max_backoff);
                    continue;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use futures_util::stream;
    use std::collections::VecDeque;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};
    use tokio::time::Duration;

    #[derive(Clone)]
    struct StubLogClient {
        attempts: Arc<AtomicUsize>,
        responses: Arc<Mutex<VecDeque<StubResult>>>,
    }

    #[derive(Clone)]
    enum StubResult {
        NotFound,
        Stream(Vec<&'static [u8]>),
        Pending,
    }

    impl StubLogClient {
        fn new(responses: Vec<StubResult>) -> Self {
            Self {
                attempts: Arc::new(AtomicUsize::new(0)),
                responses: Arc::new(Mutex::new(responses.into_iter().collect())),
            }
        }

        fn attempt_count(&self) -> usize {
            self.attempts.load(Ordering::SeqCst)
        }
    }

    impl LogStreamClient for StubLogClient {
        fn logs_stream(
            &self,
            _namespace: Option<&str>,
            _service: &str,
            _follow: bool,
        ) -> BoxFuture<'static, Result<LogByteStream, Box<dyn Error + Send + Sync>>> {
            let attempts = self.attempts.clone();
            let responses = self.responses.clone();
            Box::pin(async move {
                attempts.fetch_add(1, Ordering::SeqCst);
                let mut guard = responses.lock().unwrap();
                let result = guard.pop_front().unwrap_or(StubResult::NotFound);
                drop(guard);
                match result {
                    StubResult::NotFound => Err(Box::new(HttpError {
                        status: reqwest::StatusCode::NOT_FOUND,
                        message: "not found".into(),
                        conflicts: None,
                    })
                        as Box<dyn Error + Send + Sync>),
                    StubResult::Pending => Ok(Box::pin(stream::pending::<
                        Result<Bytes, reqwest::Error>,
                    >()) as LogByteStream),
                    StubResult::Stream(chunks) => {
                        Ok(Box::pin(stream::iter(chunks.into_iter().map(|chunk| {
                            Ok::<Bytes, reqwest::Error>(Bytes::from(chunk.to_vec()))
                        }))) as LogByteStream)
                    }
                }
            })
        }
    }

    #[tokio::test]
    async fn retries_not_found_and_succeeds() {
        let client = StubLogClient::new(vec![
            StubResult::NotFound,
            StubResult::NotFound,
            StubResult::Stream(vec![b"hello\n", b"world\n"]),
        ]);

        let config = LogStreamConfig {
            retry_backoff: Duration::from_millis(1),
            max_backoff: Duration::from_millis(2),
            drain_timeout: Duration::from_millis(10),
            max_retries: Some(5),
            flush_interval: Duration::from_millis(1),
        };

        stream_logs_to_terminal(
            client.clone(),
            None,
            "service".into(),
            true,
            CancellationToken::new(),
            config,
        )
        .await
        .expect("log stream eventually succeeds");

        assert_eq!(client.attempt_count(), 3);
    }

    #[tokio::test]
    async fn cancels_pending_stream() {
        let client = StubLogClient::new(vec![StubResult::Pending]);
        let config = LogStreamConfig::default();
        let cancel = CancellationToken::new();
        cancel.cancel();

        stream_logs_to_terminal(
            client,
            Some("ns".into()),
            "svc".into(),
            true,
            cancel,
            config,
        )
        .await
        .expect("cancels cleanly");
    }
}
