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

use std::collections::VecDeque;
use std::error::Error;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use chrono::{DateTime, Utc};
use libc;
use serde::Deserialize;
use tokio::fs::{self, File, OpenOptions};
use tokio::io::{
    self, AsyncBufReadExt, AsyncRead, AsyncSeekExt, AsyncWriteExt, BufReader, BufWriter,
};
use tokio::sync::{mpsc, watch};
use tokio::task::{self, JoinHandle};
use tokio::time::{sleep, Instant};
use tokio_stream::wrappers::ReceiverStream;

use crate::nanocloud::logger::log_error;
use crate::nanocloud::oci::runtime::container_root_path;
use crate::nanocloud::util::error::with_context;

pub type LogError = Box<dyn Error + Send + Sync>;
pub type LogResult<T> = Result<T, LogError>;

const DEFAULT_POLL_INTERVAL: Duration = Duration::from_millis(30);
const DEFAULT_DRAIN_TIMEOUT: Duration = Duration::from_millis(750);

#[derive(Clone, Debug)]
pub struct LogEntry {
    pub message: String,
    pub timestamp: Option<SystemTime>,
}

#[derive(Copy, Clone, Debug, Default)]
pub enum TailStart {
    #[default]
    Beginning,
    End,
}

#[derive(Clone, Debug)]
pub struct TailOptions {
    pub follow: bool,
    pub start: TailStart,
    pub poll_interval: Duration,
    pub drain_timeout: Duration,
    pub tail_lines: Option<usize>,
    pub since: Option<SystemTime>,
}

impl Default for TailOptions {
    fn default() -> Self {
        TailOptions {
            follow: false,
            start: TailStart::Beginning,
            poll_interval: DEFAULT_POLL_INTERVAL,
            drain_timeout: DEFAULT_DRAIN_TIMEOUT,
            tail_lines: None,
            since: None,
        }
    }
}

#[derive(Clone, Debug)]
enum StopSignal {
    Run,
    Drain,
}

pub struct LogTailHandle {
    stop_tx: Option<watch::Sender<StopSignal>>,
    join: Option<JoinHandle<()>>,
    follow: bool,
}

impl LogTailHandle {
    pub fn request_stop(&self) {
        if let Some(tx) = &self.stop_tx {
            let _ = tx.send(StopSignal::Drain);
        }
    }

    pub async fn wait(mut self) -> Result<(), tokio::task::JoinError> {
        match self.join.take() {
            Some(join) => join.await,
            None => Ok(()),
        }
    }

    pub async fn shutdown(self) -> Result<(), tokio::task::JoinError> {
        if self.follow {
            self.request_stop();
        }
        self.wait().await
    }
}

impl Drop for LogTailHandle {
    fn drop(&mut self) {
        if self.follow {
            if let Some(tx) = &self.stop_tx {
                let _ = tx.send(StopSignal::Drain);
            }
        }
        if let Some(join) = self.join.take() {
            task::spawn(async move {
                let _ = join.await;
            });
        }
    }
}

pub fn spawn_log_stream<P: AsRef<Path>>(
    log_path: P,
    options: TailOptions,
) -> (LogTailHandle, ReceiverStream<LogResult<LogEntry>>) {
    let (tx, rx) = mpsc::channel::<LogResult<LogEntry>>(256);
    let path = log_path.as_ref().to_path_buf();
    let follow = options.follow;

    let (stop_tx, stop_rx) = if follow {
        let (tx, rx) = watch::channel(StopSignal::Run);
        (Some(tx), Some(rx))
    } else {
        (None, None)
    };

    let mut stop_rx_opt = stop_rx;
    let join = task::spawn(async move {
        tail_task(path, tx, options, stop_rx_opt.take()).await;
    });

    let handle = LogTailHandle {
        stop_tx,
        join: Some(join),
        follow,
    };

    (handle, ReceiverStream::new(rx))
}

// Which stream a captured line came from while writing.
#[derive(Copy, Clone)]
enum CaptureStreamKind {
    Stdout,
    Stderr,
}

struct CapturedLine {
    when: SystemTime,
    stream: CaptureStreamKind,
    line: String,
    arrival: Instant,
}

pub async fn write_docker_json_logs<R1, R2, P>(stdout: R1, stderr: R2, log_path: P) -> LogResult<()>
where
    R1: AsyncRead + Unpin + Send + 'static,
    R2: AsyncRead + Unpin + Send + 'static,
    P: AsRef<Path>,
{
    let log_path = log_path.as_ref().to_path_buf();
    if let Some(parent) = log_path.parent() {
        fs::create_dir_all(parent).await.map_err(|e| {
            with_context(
                e,
                format!("Failed to create log directory {}", parent.display()),
            )
        })?;
    }

    let file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&log_path)
        .await
        .map_err(|e| with_context(e, format!("Failed to open log file {}", log_path.display())))?;
    let mut writer = BufWriter::new(file);
    let log_path_display = log_path.display().to_string();

    let (tx, mut rx) = mpsc::channel::<CapturedLine>(1024);

    {
        let reader = BufReader::new(stdout);
        let tx = tx.clone();
        let path_context = log_path_display.clone();
        task::spawn(async move {
            if let Err(e) = read_stream_lines(reader, CaptureStreamKind::Stdout, tx).await {
                let error_text = e.to_string();
                let error_fields = [
                    ("stream", "stdout"),
                    ("path", path_context.as_str()),
                    ("error", error_text.as_str()),
                ];
                log_error(
                    "log",
                    "Failed to read from captured stdout stream",
                    &error_fields,
                );
            }
        });
    }

    {
        let reader = BufReader::new(stderr);
        let tx = tx.clone();
        let path_context = log_path_display.clone();
        task::spawn(async move {
            if let Err(e) = read_stream_lines(reader, CaptureStreamKind::Stderr, tx).await {
                let error_text = e.to_string();
                let error_fields = [
                    ("stream", "stderr"),
                    ("path", path_context.as_str()),
                    ("error", error_text.as_str()),
                ];
                log_error(
                    "log",
                    "Failed to read from captured stderr stream",
                    &error_fields,
                );
            }
        });
    }

    drop(tx);

    while let Some(item) = rx.recv().await {
        let ts = format_rfc3339_nanos(item.when);
        let stream = match item.stream {
            CaptureStreamKind::Stdout => "stdout",
            CaptureStreamKind::Stderr => "stderr",
        };
        let escaped = escape_json(&item.line);
        writer
            .write_all(
                format!(
                    r#"{{"log":"{}","stream":"{}","time":"{}"}}"#,
                    escaped, stream, ts
                )
                .as_bytes(),
            )
            .await
            .map_err(|e| {
                with_context(
                    e,
                    format!("Failed to write log entry to {}", log_path_display),
                )
            })?;
        writer.write_all(b"\n").await.map_err(|e| {
            with_context(
                e,
                format!("Failed to terminate log entry in {}", log_path_display),
            )
        })?;

        if matches!(item.stream, CaptureStreamKind::Stderr)
            || item.arrival.elapsed().as_millis() < 2
        {
            writer.flush().await.map_err(|e| {
                with_context(
                    e,
                    format!("Failed to flush log writer for {}", log_path_display),
                )
            })?;
        }
    }

    writer.flush().await.map_err(|e| {
        with_context(
            e,
            format!("Failed to finalize log writer for {}", log_path_display),
        )
    })?;
    Ok(())
}

#[derive(Deserialize)]
struct DockerJsonLogLine {
    #[serde(default)]
    log: String,
    #[serde(default)]
    time: Option<String>,
}

struct FileTail {
    path: PathBuf,
    reader: Option<BufReader<File>>,
    buffer: String,
    start: TailStart,
    positioned: bool,
    prefetched: VecDeque<LogEntry>,
}

impl FileTail {
    async fn build(path: PathBuf, options: &TailOptions) -> LogResult<Self> {
        let mut start = options.start;
        let mut prefetched = VecDeque::new();

        if options.tail_lines.is_some() || options.since.is_some() {
            let mut entries = read_log_entries(&path).await?;

            if let Some(since) = options.since {
                entries.retain(|entry| entry.timestamp.map(|ts| ts >= since).unwrap_or(true));
            }

            if let Some(limit) = options.tail_lines {
                if limit == 0 {
                    entries.clear();
                } else if entries.len() > limit {
                    entries.drain(0..entries.len().saturating_sub(limit));
                }
            }

            prefetched = entries.into_iter().collect();
            start = TailStart::End;
        }

        Ok(FileTail {
            path,
            reader: None,
            buffer: String::new(),
            start,
            positioned: false,
            prefetched,
        })
    }

    async fn next_entry(&mut self) -> LogResult<Option<LogEntry>> {
        if let Some(entry) = self.prefetched.pop_front() {
            return Ok(Some(entry));
        }

        self.ensure_reader().await?;

        let reader = match self.reader.as_mut() {
            Some(reader) => reader,
            None => return Ok(None),
        };

        self.buffer.clear();
        let path_display = self.path.display().to_string();
        match reader.read_line(&mut self.buffer).await {
            Ok(0) => Ok(None),
            Ok(_) => {
                let raw = self.buffer.trim_end_matches(['\n', '\r']);
                match parse_log_line(raw) {
                    Ok(entry) => Ok(Some(entry)),
                    Err(err) => Err(err),
                }
            }
            Err(err) => {
                self.reader = None;
                Err(with_context(
                    err,
                    format!("Failed to read log file {}", path_display),
                ))
            }
        }
    }

    async fn ensure_reader(&mut self) -> LogResult<()> {
        if self.reader.is_some() {
            return Ok(());
        }

        match OpenOptions::new().read(true).open(&self.path).await {
            Ok(mut file) => {
                if !self.positioned {
                    if matches!(self.start, TailStart::End) {
                        file.seek(io::SeekFrom::End(0)).await.map_err(|e| {
                            with_context(
                                e,
                                format!(
                                    "Failed to seek to end of log file {}",
                                    self.path.display()
                                ),
                            )
                        })?;
                    }
                    self.positioned = true;
                }
                self.reader = Some(BufReader::new(file));
                Ok(())
            }
            Err(err) if err.kind() == io::ErrorKind::NotFound => {
                self.reader = None;
                self.positioned = false;
                Ok(())
            }
            Err(err) => Err(with_context(
                err,
                format!("Failed to open log file {}", self.path.display()),
            )),
        }
    }
}

async fn read_log_entries(path: &Path) -> LogResult<Vec<LogEntry>> {
    let file = match File::open(path).await {
        Ok(file) => file,
        Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(err) => {
            return Err(with_context(
                err,
                format!("Failed to open log file {}", path.display()),
            ))
        }
    };

    let mut reader = BufReader::new(file);
    let mut buffer = String::new();
    let mut entries = Vec::new();

    loop {
        buffer.clear();
        match reader.read_line(&mut buffer).await {
            Ok(0) => break,
            Ok(_) => {
                let raw = buffer.trim_end_matches(['\n', '\r']);
                if raw.is_empty() {
                    continue;
                }
                let entry = parse_log_line(raw)?;
                entries.push(entry);
            }
            Err(err) => {
                return Err(with_context(
                    err,
                    format!("Failed to read log file {}", path.display()),
                ));
            }
        }
    }

    Ok(entries)
}

async fn tail_task(
    path: PathBuf,
    tx: mpsc::Sender<LogResult<LogEntry>>,
    options: TailOptions,
    mut stop_rx: Option<watch::Receiver<StopSignal>>,
) {
    let mut reader = match FileTail::build(path, &options).await {
        Ok(reader) => reader,
        Err(err) => {
            let _ = tx.send(Err(err)).await;
            return;
        }
    };
    let mut stop_requested = !options.follow;
    let mut idle_since = Instant::now();

    loop {
        match reader.next_entry().await {
            Ok(Some(entry)) => {
                if let Some(since) = options.since {
                    if entry.timestamp.map(|ts| ts < since).unwrap_or(false) {
                        continue;
                    }
                }
                idle_since = Instant::now();
                if tx.send(Ok(entry)).await.is_err() {
                    break;
                }
                continue;
            }
            Ok(None) => {
                if !options.follow {
                    break;
                }
                if stop_requested && idle_since.elapsed() >= options.drain_timeout {
                    break;
                }
            }
            Err(err) => {
                if tx.send(Err(err)).await.is_err() {
                    break;
                }
            }
        }

        let sleeper = sleep(options.poll_interval);
        tokio::pin!(sleeper);

        match stop_rx {
            Some(ref mut rx) if !stop_requested => {
                tokio::select! {
                    _ = &mut sleeper => {}
                    changed = rx.changed() => {
                        if changed.is_ok() && matches!(*rx.borrow(), StopSignal::Drain) {
                            stop_requested = true;
                            idle_since = Instant::now();
                        }
                    }
                }
            }
            _ => {
                sleeper.await;
            }
        }
    }
}

async fn read_stream_lines<R>(
    mut reader: BufReader<R>,
    kind: CaptureStreamKind,
    tx: mpsc::Sender<CapturedLine>,
) -> LogResult<()>
where
    R: AsyncRead + Unpin,
{
    let mut buf = Vec::with_capacity(4096);

    loop {
        buf.clear();
        let n = reader.read_until(b'\n', &mut buf).await.map_err(|e| {
            let stream = match kind {
                CaptureStreamKind::Stdout => "stdout",
                CaptureStreamKind::Stderr => "stderr",
            };
            with_context(e, format!("Failed to read {stream} log data"))
        })?;
        if n == 0 {
            break;
        }

        if let Some(&b'\n') = buf.last() {
            buf.pop();
            if let Some(&b'\r') = buf.last() {
                buf.pop();
            }
        }

        let line = String::from_utf8_lossy(&buf).into_owned();
        let when = SystemTime::now();
        let arrival = Instant::now();

        if tx
            .send(CapturedLine {
                when,
                stream: kind,
                line,
                arrival,
            })
            .await
            .is_err()
        {
            break;
        }
    }

    Ok(())
}

pub fn parse_log_line(raw: &str) -> LogResult<LogEntry> {
    let entry: DockerJsonLogLine = serde_json::from_str(raw)
        .map_err(|e| with_context(e, "Failed to parse Docker JSON log line"))?;
    let timestamp = entry.time.as_deref().and_then(parse_rfc3339_optional);

    Ok(LogEntry {
        message: entry.log,
        timestamp,
    })
}

fn parse_rfc3339_optional(input: &str) -> Option<SystemTime> {
    DateTime::parse_from_rfc3339(input)
        .map(|dt| dt.with_timezone(&Utc).into())
        .ok()
}

fn escape_json(input: &str) -> String {
    let mut out = String::with_capacity(input.len() + 16);
    for ch in input.chars() {
        match ch {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            '\u{08}' => out.push_str("\\b"),
            '\u{0C}' => out.push_str("\\f"),
            c if (c as u32) < 0x20 => {
                use std::fmt::Write as _;
                let _ = write!(out, "\\u{:04X}", c as u32);
            }
            c => out.push(c),
        }
    }
    out
}

fn format_rfc3339_nanos(t: SystemTime) -> String {
    let dur = t
        .duration_since(UNIX_EPOCH)
        .unwrap_or_else(|e| Duration::from_secs(0) + e.duration());

    let secs = dur.as_secs() as i64;
    let nanos = dur.subsec_nanos();

    unsafe {
        let mut tm: libc::tm = std::mem::zeroed();
        let mut ts = secs as libc::time_t;
        let _ = libc::gmtime_r(&mut ts as *mut libc::time_t, &mut tm as *mut libc::tm);

        let mut buf = [0u8; 32];
        let len = {
            use std::fmt::Write as _;
            let mut s = String::new();
            let _ = write!(
                s,
                "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}",
                tm.tm_year + 1900,
                tm.tm_mon + 1,
                tm.tm_mday,
                tm.tm_hour,
                tm.tm_min,
                tm.tm_sec
            );
            let bytes = s.as_bytes();
            buf[..bytes.len()].copy_from_slice(bytes);
            bytes.len()
        };

        let frac = {
            let mut s = String::new();
            use std::fmt::Write as _;
            let _ = write!(s, ".{:09}Z", nanos);
            s
        };

        let mut out = String::from_utf8_lossy(&buf[..len]).into_owned();
        out.push_str(&frac);
        out
    }
}

pub fn container_log_dir(container_id: &str) -> PathBuf {
    container_root_path(container_id).join("logs")
}

pub fn container_log_path(container_id: &str) -> PathBuf {
    container_log_dir(container_id).join(format!("{}-json.log", container_id))
}

pub fn container_previous_log_path(container_id: &str) -> PathBuf {
    container_log_dir(container_id).join(format!("{}-json.log.1", container_id))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::tempdir;

    fn write_log(path: &Path, entries: &[(&str, &str)]) {
        let mut payload = String::new();
        for (timestamp, message) in entries {
            payload.push_str(&format!(
                "{{\"log\":\"{}\\n\",\"time\":\"{}\",\"stream\":\"stdout\"}}\n",
                message, timestamp
            ));
        }
        fs::write(path, payload).expect("write log file");
    }

    #[tokio::test]
    async fn tail_options_limit_last_lines() {
        let dir = tempdir().expect("tempdir");
        let path = dir.path().join("container-json.log");
        write_log(
            &path,
            &[
                ("1970-01-01T00:00:01Z", "first"),
                ("1970-01-01T00:00:02Z", "second"),
                ("1970-01-01T00:00:03Z", "third"),
            ],
        );

        let options = TailOptions {
            tail_lines: Some(2),
            ..Default::default()
        };

        let mut tail = FileTail::build(path.clone(), &options)
            .await
            .expect("tail build");

        let entry = tail.next_entry().await.expect("entry").expect("log entry");
        assert_eq!(entry.message.trim_end_matches('\n'), "second");

        let entry = tail.next_entry().await.expect("entry").expect("log entry");
        assert_eq!(entry.message.trim_end_matches('\n'), "third");

        assert!(tail.next_entry().await.expect("entry").is_none());
    }

    #[tokio::test]
    async fn tail_options_filters_since_timestamp() {
        let dir = tempdir().expect("tempdir");
        let path = dir.path().join("container-json.log");
        write_log(
            &path,
            &[
                ("1970-01-01T00:00:10Z", "old"),
                ("1970-01-01T00:01:00Z", "recent"),
            ],
        );

        let options = TailOptions {
            since: Some(UNIX_EPOCH + Duration::from_secs(30)),
            ..Default::default()
        };

        let mut tail = FileTail::build(path.clone(), &options)
            .await
            .expect("tail build");

        let entry = tail.next_entry().await.expect("entry").expect("log entry");
        assert_eq!(entry.message.trim_end_matches('\n'), "recent");

        assert!(tail.next_entry().await.expect("entry").is_none());
    }
}
