use std::collections::HashMap;
use std::env;
use std::error::Error;
use std::fmt;
use std::fs;
use std::io;
use std::os::fd::{AsRawFd, BorrowedFd, FromRawFd, OwnedFd, RawFd};
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use base64::engine::general_purpose::STANDARD as BASE64;
use base64::Engine as _;
use chrono::{DateTime, Utc};
use futures_util::stream::SelectAll;
use futures_util::{SinkExt, StreamExt};
use libc;
use openssl::nid::Nid;
use openssl::pkcs12::Pkcs12;
use openssl::pkey::PKey;
use openssl::ssl::{SslConnector, SslMethod, SslVerifyMode};
use openssl::x509::X509;
use reqwest::header::CONTENT_TYPE;
use reqwest::tls::{Certificate, Identity};
use reqwest::{Client, StatusCode, Url};
use serde::de::DeserializeOwned;
use serde::Deserialize;
use serde_json;
use tokio::io::unix::AsyncFd;
use tokio::io::DuplexStream;
use tokio::io::Interest;
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::signal::unix::{signal, SignalKind};
use tokio::sync::{mpsc, oneshot, Notify};
use tokio::task::JoinHandle;
use tokio::time::sleep;
use tokio_openssl::SslStream;
use tokio_stream::wrappers::ReceiverStream;
use tokio_tungstenite::client_async;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::http::header::{HeaderValue, SEC_WEBSOCKET_PROTOCOL};
use tokio_tungstenite::tungstenite::http::HeaderMap;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::WebSocketStream;

use crate::nanocloud::api::types::{
    ApplyConflict, Bundle, BundleList, BundleSnapshotSource, BundleSpec, CaRequest,
    CertificateRequest, CertificateResponse, CertificateSpec, Device, DeviceList, DeviceSpec,
    NetworkPolicyDebugResponse, PodTable, ServiceActionResponse,
};
use crate::nanocloud::k8s::event::EventList;
use crate::nanocloud::k8s::pod::{ObjectMeta, Pod};
use crate::nanocloud::util::security::JsonTlsInfo;
use nix::unistd::isatty;

/// Canonical environment variable for overriding the Nanocloud API endpoint.
const SERVER_ENV: &str = "NANOCLOUD_SERVER";
const DEFAULT_SERVER_ENDPOINT: &str = "https://127.0.0.1:6443";
const POD_TABLE_ACCEPT: &str = "application/json;as=Table;g=meta.k8s.io;v=v1";
const RETRY_ATTEMPTS: usize = 3;
const RETRY_BACKOFF: Duration = Duration::from_millis(200);
const MAX_BACKOFF: Duration = Duration::from_secs(2);
const CERTIFICATE_API_VERSION: &str = "nanocloud.io/v1";
const CERTIFICATE_KIND: &str = "Certificate";
const BUNDLE_API_VERSION: &str = "nanocloud.io/v1";
const BUNDLE_KIND: &str = "Bundle";
const EXEC_PROTOCOL_PREFERENCE: [&str; 2] = ["v5.channel.k8s.io", "v4.channel.k8s.io"];
const EXEC_PROTOCOL_HEADER_VALUE: &str = "v5.channel.k8s.io, v4.channel.k8s.io";
const CHANNEL_STDIN: u8 = 0;
const CHANNEL_STDOUT: u8 = 1;
const CHANNEL_STDERR: u8 = 2;
const CHANNEL_STATUS: u8 = 3;
const CHANNEL_RESIZE: u8 = 4;
const CHANNEL_CLOSE: u8 = 255;

/// Options that tweak bundle profile export behavior.
#[derive(Clone, Copy, Debug, Default)]
pub struct BundleExportOptions {
    /// Include encrypted secret payloads in the export artifact when supported.
    pub include_secrets: bool,
}

pub struct ApplyBundleOptions<'a> {
    pub payload: &'a [u8],
    pub content_type: &'a str,
    pub field_manager: &'a str,
    pub force: bool,
    pub dry_run: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ExecProtocol {
    V5,
    V4,
}

#[derive(Clone, Debug)]
pub struct ExecRequest {
    pub namespace: String,
    pub pod: String,
    pub container: Option<String>,
    pub command: Vec<String>,
    pub stdin: bool,
    pub stdout: bool,
    pub stderr: bool,
    pub tty: bool,
}

#[derive(Debug)]
pub struct HttpError {
    pub status: StatusCode,
    pub message: String,
    pub conflicts: Option<Vec<ApplyConflict>>,
}

fn should_retry_status(status: StatusCode) -> bool {
    status == StatusCode::TOO_MANY_REQUESTS || status.is_server_error()
}

fn is_retryable_reqwest(err: &reqwest::Error) -> bool {
    err.is_timeout() || err.is_connect()
}

fn next_backoff(current: Duration) -> Duration {
    current
        .checked_mul(2)
        .unwrap_or(MAX_BACKOFF)
        .min(MAX_BACKOFF)
}

fn parse_timestamp(value: &str) -> Result<SystemTime, chrono::ParseError> {
    DateTime::parse_from_rfc3339(value).map(|dt| dt.with_timezone(&Utc).into())
}

fn sanitize_pem(pem: &str, label: &str) -> Result<Vec<u8>, io::Error> {
    let trimmed = pem.trim();
    if trimmed.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("{label} payload is empty"),
        ));
    }
    if !trimmed.starts_with("-----BEGIN") {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("{label} is not PEM encoded"),
        ));
    }
    Ok(trimmed.as_bytes().to_vec())
}

fn build_reqwest_identity(cert_pem: &[u8], key_pem: &[u8]) -> Result<Identity, String> {
    let cert = X509::from_pem(cert_pem)
        .map_err(|err| format!("failed to parse client certificate: {err}"))?;
    let key = PKey::private_key_from_pem(key_pem)
        .map_err(|err| format!("failed to parse client key: {err}"))?;
    let pkcs12 = Pkcs12::builder()
        .name("nanocloud-client")
        .pkey(&key)
        .cert(&cert)
        .build2("")
        .map_err(|err| format!("failed to build client PKCS#12 bundle: {err}"))?;
    let pkcs12_der = pkcs12
        .to_der()
        .map_err(|err| format!("failed to encode client PKCS#12 bundle: {err}"))?;
    Identity::from_pkcs12_der(&pkcs12_der, "")
        .map_err(|err| format!("failed to load client identity: {err}"))
}

fn convert_certificate_response(
    response: CertificateResponse,
) -> Result<EphemeralCertificate, Box<dyn Error + Send + Sync>> {
    let certificate = sanitize_pem(&response.status.certificate_pem, "certificate")
        .map_err(|err| Box::new(err) as Box<dyn Error + Send + Sync>)?;
    let ca_bundle = sanitize_pem(&response.status.ca_bundle_pem, "CA bundle")
        .map_err(|err| Box::new(err) as Box<dyn Error + Send + Sync>)?;
    let expiration = match response.status.expiration_timestamp.as_deref() {
        Some(ts) if !ts.trim().is_empty() => {
            Some(parse_timestamp(ts).map_err(|err| Box::new(err) as Box<dyn Error + Send + Sync>)?)
        }
        _ => None,
    };
    Ok(EphemeralCertificate {
        certificate,
        ca_bundle,
        expiration,
    })
}

impl HttpError {
    fn new(status: StatusCode, message: impl Into<String>) -> Self {
        HttpError {
            status,
            message: message.into(),
            conflicts: None,
        }
    }

    fn with_conflicts(
        status: StatusCode,
        message: impl Into<String>,
        conflicts: Vec<ApplyConflict>,
    ) -> Self {
        HttpError {
            status,
            message: message.into(),
            conflicts: Some(conflicts),
        }
    }

    pub fn conflicts(&self) -> Option<&[ApplyConflict]> {
        self.conflicts.as_deref()
    }
}

#[derive(Debug)]
enum WriterCommand {
    Raw(Message),
}

struct ExecChannelMultiplexer {
    stdin_tx: mpsc::Sender<Vec<u8>>,
    resize_tx: mpsc::Sender<Vec<u8>>,
    stdout_rx: Option<mpsc::Receiver<Vec<u8>>>,
    stderr_rx: Option<mpsc::Receiver<Vec<u8>>>,
    status_rx: Option<mpsc::Receiver<Vec<u8>>>,
    close_rx: Option<mpsc::Receiver<()>>,
    control_tx: mpsc::Sender<WriterCommand>,
    writer_handle: JoinHandle<()>,
    reader_handle: JoinHandle<()>,
}

impl ExecChannelMultiplexer {
    fn new(stream: ExecWebSocketStream) -> Self {
        let (sink, stream) = stream.split();

        let (stdin_tx, stdin_rx) = mpsc::channel::<Vec<u8>>(16);
        let (resize_tx, resize_rx) = mpsc::channel::<Vec<u8>>(16);
        let (control_tx, control_rx) = mpsc::channel::<WriterCommand>(8);
        let control_for_reader = control_tx.clone();

        let (stdout_tx, stdout_rx) = mpsc::channel::<Vec<u8>>(32);
        let (stderr_tx, stderr_rx) = mpsc::channel::<Vec<u8>>(32);
        let (status_tx, status_rx) = mpsc::channel::<Vec<u8>>(8);
        let (close_tx, close_rx) = mpsc::channel::<()>(1);

        let writer_handle = tokio::spawn(async move {
            Self::run_writer(sink, stdin_rx, resize_rx, control_rx).await;
        });

        let reader_handle = tokio::spawn(async move {
            Self::run_reader(
                stream,
                stdout_tx,
                stderr_tx,
                status_tx,
                close_tx,
                control_for_reader,
            )
            .await;
        });

        ExecChannelMultiplexer {
            stdin_tx,
            resize_tx,
            stdout_rx: Some(stdout_rx),
            stderr_rx: Some(stderr_rx),
            status_rx: Some(status_rx),
            close_rx: Some(close_rx),
            control_tx,
            writer_handle,
            reader_handle,
        }
    }

    fn new_for_tests(stream: WebSocketStream<DuplexStream>) -> Self {
        let (sink, stream) = stream.split();

        let (stdin_tx, stdin_rx) = mpsc::channel::<Vec<u8>>(16);
        let (resize_tx, resize_rx) = mpsc::channel::<Vec<u8>>(16);
        let (control_tx, control_rx) = mpsc::channel::<WriterCommand>(8);
        let control_for_reader = control_tx.clone();

        let (stdout_tx, stdout_rx) = mpsc::channel::<Vec<u8>>(32);
        let (stderr_tx, stderr_rx) = mpsc::channel::<Vec<u8>>(32);
        let (status_tx, status_rx) = mpsc::channel::<Vec<u8>>(8);
        let (close_tx, close_rx) = mpsc::channel::<()>(1);

        let writer_handle = tokio::spawn(async move {
            Self::run_writer(sink, stdin_rx, resize_rx, control_rx).await;
        });

        let reader_handle = tokio::spawn(async move {
            Self::run_reader(
                stream,
                stdout_tx,
                stderr_tx,
                status_tx,
                close_tx,
                control_for_reader,
            )
            .await;
        });

        ExecChannelMultiplexer {
            stdin_tx,
            resize_tx,
            stdout_rx: Some(stdout_rx),
            stderr_rx: Some(stderr_rx),
            status_rx: Some(status_rx),
            close_rx: Some(close_rx),
            control_tx,
            writer_handle,
            reader_handle,
        }
    }

    fn stdin_sender(&self) -> mpsc::Sender<Vec<u8>> {
        self.stdin_tx.clone()
    }

    fn resize_sender(&self) -> mpsc::Sender<Vec<u8>> {
        self.resize_tx.clone()
    }

    fn take_stdout(&mut self) -> Option<mpsc::Receiver<Vec<u8>>> {
        self.stdout_rx.take()
    }

    fn take_stderr(&mut self) -> Option<mpsc::Receiver<Vec<u8>>> {
        self.stderr_rx.take()
    }

    fn take_status(&mut self) -> Option<mpsc::Receiver<Vec<u8>>> {
        self.status_rx.take()
    }

    fn take_close(&mut self) -> Option<mpsc::Receiver<()>> {
        self.close_rx.take()
    }

    async fn shutdown(self) {
        let ExecChannelMultiplexer {
            stdin_tx,
            resize_tx,
            control_tx,
            writer_handle,
            reader_handle,
            ..
        } = self;

        drop(stdin_tx);
        drop(resize_tx);
        drop(control_tx);

        let _ = writer_handle.await;
        let _ = reader_handle.await;
    }

    async fn run_writer<S>(
        mut sink: futures_util::stream::SplitSink<WebSocketStream<S>, Message>,
        mut stdin_rx: mpsc::Receiver<Vec<u8>>,
        mut resize_rx: mpsc::Receiver<Vec<u8>>,
        mut control_rx: mpsc::Receiver<WriterCommand>,
    ) where
        S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
        let mut stdin_open = true;
        let mut resize_open = true;
        let mut control_open = true;

        loop {
            tokio::select! {
                result = stdin_rx.recv(), if stdin_open => {
                    match result {
                        Some(data) => {
                            if send_channel_frame(&mut sink, CHANNEL_STDIN, data).await.is_err() {
                                break;
                            }
                        }
                        None => {
                            stdin_open = false;
                        }
                    }
                }
                result = resize_rx.recv(), if resize_open => {
                    match result {
                        Some(data) => {
                            if send_channel_frame(&mut sink, CHANNEL_RESIZE, data).await.is_err() {
                                break;
                            }
                        }
                        None => {
                            resize_open = false;
                        }
                    }
                }
                result = control_rx.recv(), if control_open => {
                    match result {
                        Some(WriterCommand::Raw(message)) => {
                            if sink.send(message).await.is_err() {
                                break;
                            }
                        }
                        None => {
                            control_open = false;
                        }
                    }
                }
                else => {
                    break;
                }
            }
        }

        let _ = sink.close().await;
    }

    async fn run_reader<S>(
        mut stream: futures_util::stream::SplitStream<WebSocketStream<S>>,
        stdout_tx: mpsc::Sender<Vec<u8>>,
        stderr_tx: mpsc::Sender<Vec<u8>>,
        status_tx: mpsc::Sender<Vec<u8>>,
        close_tx: mpsc::Sender<()>,
        control_tx: mpsc::Sender<WriterCommand>,
    ) where
        S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
        while let Some(message) = stream.next().await {
            match message {
                Ok(Message::Binary(payload)) => {
                    if payload.is_empty() {
                        continue;
                    }
                    let channel = payload[0];
                    let data = payload[1..].to_vec();
                    match channel {
                        CHANNEL_STDOUT => {
                            if stdout_tx.send(data).await.is_err() {
                                break;
                            }
                        }
                        CHANNEL_STDERR => {
                            if stderr_tx.send(data).await.is_err() {
                                break;
                            }
                        }
                        CHANNEL_STATUS => {
                            if status_tx.send(data).await.is_err() {
                                break;
                            }
                        }
                        CHANNEL_CLOSE => {
                            let _ = close_tx.send(()).await;
                            break;
                        }
                        _ => {}
                    }
                }
                Ok(Message::Close(frame)) => {
                    let _ = control_tx
                        .send(WriterCommand::Raw(Message::Close(frame)))
                        .await;
                    let _ = close_tx.send(()).await;
                    break;
                }
                Ok(Message::Ping(payload)) => {
                    let _ = control_tx
                        .send(WriterCommand::Raw(Message::Pong(payload)))
                        .await;
                }
                Ok(Message::Pong(_)) => {}
                Ok(Message::Text(_)) => {}
                Ok(Message::Frame(_)) => {}
                Err(_) => {
                    break;
                }
            }
        }
    }
}

async fn send_channel_frame<S>(
    sink: &mut futures_util::stream::SplitSink<WebSocketStream<S>, Message>,
    channel: u8,
    data: Vec<u8>,
) -> Result<(), tokio_tungstenite::tungstenite::Error>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let mut frame = Vec::with_capacity(data.len() + 1);
    frame.push(channel);
    frame.extend_from_slice(&data);
    sink.send(Message::Binary(frame.into())).await
}

async fn forward_stream<W>(mut receiver: mpsc::Receiver<Vec<u8>>, mut writer: W) -> io::Result<()>
where
    W: AsyncWrite + Unpin,
{
    while let Some(chunk) = receiver.recv().await {
        if chunk.is_empty() {
            continue;
        }
        writer.write_all(&chunk).await?;
    }
    writer.flush().await?;
    Ok(())
}

async fn forward_tty_streams(
    stdout_rx: Option<mpsc::Receiver<Vec<u8>>>,
    stderr_rx: Option<mpsc::Receiver<Vec<u8>>>,
) -> io::Result<()> {
    let mut combined = SelectAll::new();

    if let Some(receiver) = stdout_rx {
        combined.push(ReceiverStream::new(receiver));
    }
    if let Some(receiver) = stderr_rx {
        combined.push(ReceiverStream::new(receiver));
    }

    if combined.is_empty() {
        return Ok(());
    }

    let mut combined = combined;
    let mut writer = tokio::io::stdout();
    let mut filter = CursorReportFilter::new();

    while let Some(chunk) = combined.next().await {
        if chunk.is_empty() {
            continue;
        }
        let filtered = filter.consume(&chunk);
        if filtered.is_empty() {
            continue;
        }
        writer.write_all(&filtered).await?;
        writer.flush().await?;
    }

    let remaining = filter.finish();
    if !remaining.is_empty() {
        writer.write_all(&remaining).await?;
    }

    writer.flush().await?;
    Ok(())
}

struct CursorReportFilter {
    buffer: Vec<u8>,
}

impl CursorReportFilter {
    fn new() -> Self {
        CursorReportFilter { buffer: Vec::new() }
    }

    fn consume(&mut self, chunk: &[u8]) -> Vec<u8> {
        self.buffer.extend_from_slice(chunk);

        let mut output = Vec::new();
        let mut index = 0;
        let len = self.buffer.len();

        while index < len {
            let byte = self.buffer[index];
            if byte != 0x1b {
                output.push(byte);
                index += 1;
                continue;
            }

            if index + 1 >= len {
                break;
            }

            if self.buffer[index + 1] != b'[' {
                output.push(byte);
                index += 1;
                continue;
            }

            let mut cursor = index + 2;
            if cursor >= len {
                break;
            }

            let row_start = cursor;
            while cursor < len && self.buffer[cursor].is_ascii_digit() {
                cursor += 1;
            }
            if cursor == row_start {
                output.push(byte);
                index += 1;
                continue;
            }
            if cursor >= len {
                break;
            }

            if self.buffer[cursor] != b';' {
                output.push(byte);
                index += 1;
                continue;
            }
            cursor += 1;
            if cursor >= len {
                break;
            }

            let col_start = cursor;
            while cursor < len && self.buffer[cursor].is_ascii_digit() {
                cursor += 1;
            }
            if cursor == col_start {
                output.push(byte);
                index += 1;
                continue;
            }
            if cursor >= len {
                break;
            }

            if self.buffer[cursor] != b'R' {
                output.push(byte);
                index += 1;
                continue;
            }

            cursor += 1;
            index = cursor;
        }

        self.buffer.drain(..index);
        output
    }

    fn finish(&mut self) -> Vec<u8> {
        self.buffer.drain(..).collect()
    }
}

#[derive(Debug, Default)]
struct ExecExitInfo {
    code: Option<i32>,
    message: Option<String>,
    reason: Option<String>,
}

fn failure_summary(exit_code: i32, exit_info: &ExecExitInfo) -> String {
    let mut text = format!("exec failed with exit code {exit_code}");
    if let Some(reason) = exit_info
        .reason
        .as_deref()
        .filter(|value| !value.is_empty())
    {
        text.push_str(&format!(" ({reason})"));
    }
    if let Some(message) = exit_info
        .message
        .as_deref()
        .filter(|value| !value.is_empty())
    {
        text.push_str(": ");
        text.push_str(message);
    }
    text
}

fn exit_code_and_summary(options: &ExecRequest, exit_info: &ExecExitInfo) -> (i32, Option<String>) {
    match exit_info.code {
        Some(0) => {
            if options.tty {
                (0, None)
            } else {
                (0, Some("exec completed with exit code 0".to_string()))
            }
        }
        Some(code) => {
            if options.tty {
                (
                    code,
                    Some(format!("command terminated with exit code {code}")),
                )
            } else {
                (code, Some(failure_summary(code, exit_info)))
            }
        }
        None => (
            0,
            Some(
                "exec session finished without reporting an exit status; assuming exit code 0"
                    .to_string(),
            ),
        ),
    }
}

fn is_terminal(fd: RawFd) -> io::Result<bool> {
    unsafe { isatty(BorrowedFd::borrow_raw(fd)) }.map_err(io::Error::from)
}

fn current_terminal_size(fd: RawFd) -> io::Result<Option<(u16, u16)>> {
    unsafe {
        let mut size: libc::winsize = std::mem::zeroed();
        if libc::ioctl(fd, libc::TIOCGWINSZ, &mut size) == -1 {
            let err = io::Error::last_os_error();
            if err.raw_os_error() == Some(libc::ENOTTY) {
                return Ok(None);
            }
            return Err(err);
        }
        if size.ws_col == 0 || size.ws_row == 0 {
            return Ok(None);
        }
        Ok(Some((size.ws_col, size.ws_row)))
    }
}

fn encode_resize_payload(width: u16, height: u16) -> io::Result<Vec<u8>> {
    serde_json::to_vec(&serde_json::json!({
        "Width": width,
        "Height": height,
    }))
    .map_err(|err| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("failed to encode resize payload: {err}"),
        )
    })
}

async fn pump_resize_events(
    sender: mpsc::Sender<Vec<u8>>,
    fd: RawFd,
    shutdown: Arc<Notify>,
) -> io::Result<()> {
    if !is_terminal(fd)? {
        return Ok(());
    }

    let mut last_dimensions = None;

    if let Some((width, height)) = current_terminal_size(fd)? {
        let payload = encode_resize_payload(width, height)?;
        if sender.send(payload).await.is_err() {
            return Ok(());
        }
        last_dimensions = Some((width, height));
    }

    let mut signal = signal(SignalKind::window_change())
        .map_err(|err| io::Error::other(format!("failed to watch SIGWINCH: {err}")))?;

    loop {
        tokio::select! {
            _ = shutdown.notified() => {
                break;
            }
            _ = sender.closed() => {
                break;
            }
            maybe_event = signal.recv() => {
                let Some(_) = maybe_event else {
                    break;
                };
                let Some((width, height)) = current_terminal_size(fd)? else {
                    continue;
                };
                if last_dimensions == Some((width, height)) {
                    continue;
                }
                let payload = encode_resize_payload(width, height)?;
                if sender.send(payload).await.is_err() {
                    break;
                }
                last_dimensions = Some((width, height));
            }
        }
    }

    Ok(())
}

fn parse_status_payload(payload: &[u8]) -> io::Result<ExecExitInfo> {
    #[derive(Deserialize)]
    struct StatusBody {
        #[serde(default)]
        message: Option<String>,
        #[serde(default)]
        reason: Option<String>,
        #[serde(default)]
        code: Option<i32>,
    }

    let body: StatusBody = serde_json::from_slice(payload).map_err(|err| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("failed to parse status payload: {err}"),
        )
    })?;
    Ok(ExecExitInfo {
        code: body.code,
        message: body.message,
        reason: body.reason,
    })
}

async fn pipe_stdin_to_channel(
    sender: mpsc::Sender<Vec<u8>>,
    shutdown: Arc<Notify>,
) -> io::Result<()> {
    let stdin_fd = duplicate_stdin()?;
    set_nonblocking(stdin_fd.as_raw_fd())?;
    let async_fd = AsyncFd::with_interest(stdin_fd, Interest::READABLE)?;
    let mut buffer = [0u8; 4096];

    loop {
        tokio::select! {
            _ = shutdown.notified() => {
                break;
            }
            _ = sender.closed() => {
                break;
            }
            readiness = async_fd.readable() => {
                let mut guard = readiness?;
                loop {
                    match read_nonblocking(guard.get_ref().as_raw_fd(), &mut buffer) {
                        Ok(0) => {
                            let _ = sender.send(Vec::new()).await;
                            return Ok(());
                        }
                        Ok(bytes_read) => {
                            if sender.send(buffer[..bytes_read].to_vec()).await.is_err() {
                                return Ok(());
                            }
                        }
                        Err(err) if err.kind() == io::ErrorKind::Interrupted => {
                            continue;
                        }
                        Err(err) if err.kind() == io::ErrorKind::WouldBlock => {
                            guard.clear_ready();
                            break;
                        }
                        Err(err) => {
                            return Err(err);
                        }
                    }
                }
            }
        }
    }

    Ok(())
}

fn duplicate_stdin() -> io::Result<OwnedFd> {
    let fd = unsafe { libc::fcntl(libc::STDIN_FILENO, libc::F_DUPFD_CLOEXEC, 0) };
    if fd == -1 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: fcntl returns a new owned file descriptor on success.
    Ok(unsafe { OwnedFd::from_raw_fd(fd) })
}

fn set_nonblocking(fd: RawFd) -> io::Result<()> {
    unsafe {
        let flags = libc::fcntl(fd, libc::F_GETFL);
        if flags == -1 {
            return Err(io::Error::last_os_error());
        }
        if libc::fcntl(fd, libc::F_SETFL, flags | libc::O_NONBLOCK) == -1 {
            return Err(io::Error::last_os_error());
        }
    }
    Ok(())
}

fn read_nonblocking(fd: RawFd, buffer: &mut [u8]) -> io::Result<usize> {
    loop {
        let result =
            unsafe { libc::read(fd, buffer.as_mut_ptr() as *mut libc::c_void, buffer.len()) };
        if result == -1 {
            let err = io::Error::last_os_error();
            if err.kind() == io::ErrorKind::Interrupted {
                continue;
            }
            return Err(err);
        }
        return Ok(result as usize);
    }
}

#[doc(hidden)]
#[allow(dead_code)]
pub mod test_support {
    use super::*;
    use tokio::io::DuplexStream;
    use tokio::sync::mpsc;
    use tokio_tungstenite::tungstenite::http::HeaderMap;
    use tokio_tungstenite::WebSocketStream;

    pub fn test_client() -> NanocloudClient {
        NanocloudClient::with_bearer("https://nanocloud.test", "test-token".into())
            .expect("construct test Nanocloud client")
    }

    pub fn build_exec_request(container: Option<&str>) -> ExecRequest {
        ExecRequest {
            namespace: "demo".to_string(),
            pod: "web-0".to_string(),
            container: container.map(|value| value.to_string()),
            command: vec!["sh".into(), "-c".into(), "echo hi".into()],
            stdin: true,
            stdout: true,
            stderr: false,
            tty: true,
        }
    }

    pub const CHANNEL_STDIN: u8 = super::CHANNEL_STDIN;
    pub const CHANNEL_STDOUT: u8 = super::CHANNEL_STDOUT;
    pub const CHANNEL_STDERR: u8 = super::CHANNEL_STDERR;
    pub const CHANNEL_STATUS: u8 = super::CHANNEL_STATUS;
    pub const CHANNEL_RESIZE: u8 = super::CHANNEL_RESIZE;
    pub const CHANNEL_CLOSE: u8 = super::CHANNEL_CLOSE;

    pub fn parse_status_code(payload: &[u8]) -> io::Result<Option<i32>> {
        super::parse_status_payload(payload).map(|info| info.code)
    }

    pub fn extract_exec_protocol_label(
        headers: &HeaderMap,
    ) -> Result<&'static str, Box<dyn Error + Send + Sync>> {
        match NanocloudClient::extract_exec_protocol(headers)? {
            ExecProtocol::V5 => Ok("v5"),
            ExecProtocol::V4 => Ok("v4"),
        }
    }

    pub struct MultiplexerHarness {
        multiplexer: ExecChannelMultiplexer,
    }

    impl MultiplexerHarness {
        pub fn new(stream: WebSocketStream<DuplexStream>) -> Self {
            Self {
                multiplexer: ExecChannelMultiplexer::new_for_tests(stream),
            }
        }

        pub fn stdin_sender(&self) -> mpsc::Sender<Vec<u8>> {
            self.multiplexer.stdin_sender()
        }

        pub fn resize_sender(&self) -> mpsc::Sender<Vec<u8>> {
            self.multiplexer.resize_sender()
        }

        pub fn take_stdout(&mut self) -> Option<mpsc::Receiver<Vec<u8>>> {
            self.multiplexer.take_stdout()
        }

        pub fn take_stderr(&mut self) -> Option<mpsc::Receiver<Vec<u8>>> {
            self.multiplexer.take_stderr()
        }

        pub fn take_status(&mut self) -> Option<mpsc::Receiver<Vec<u8>>> {
            self.multiplexer.take_status()
        }

        pub fn take_close(&mut self) -> Option<mpsc::Receiver<()>> {
            self.multiplexer.take_close()
        }

        pub async fn shutdown(self) {
            self.multiplexer.shutdown().await;
        }
    }
}

impl fmt::Display for HttpError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} (status {})", self.message, self.status)
    }
}

impl Error for HttpError {}

#[derive(Clone, Copy, Debug)]
pub enum KubeFieldSource {
    InlineData,
    FilePath,
}

#[derive(Clone)]
pub struct CurlAuthData {
    pub kubeconfig_path: PathBuf,
    pub kubeconfig_dir: PathBuf,
    pub cluster_name: String,
    pub user_name: String,
    pub ca_source: Option<KubeFieldSource>,
    pub cert_source: KubeFieldSource,
    pub key_source: KubeFieldSource,
}

#[derive(Clone, Debug)]
pub struct EphemeralCertificate {
    pub certificate: Vec<u8>,
    pub ca_bundle: Vec<u8>,
    pub expiration: Option<SystemTime>,
}

#[derive(Clone)]
struct ResolvedData {
    bytes: Vec<u8>,
    from_data_field: bool,
}

#[derive(Deserialize)]
struct KubeConfig {
    #[serde(default)]
    clusters: Vec<NamedCluster>,
    #[serde(default)]
    users: Vec<NamedUser>,
    #[serde(default)]
    contexts: Vec<NamedContext>,
    #[serde(rename = "current-context")]
    current_context: Option<String>,
}

#[derive(Deserialize)]
struct NamedCluster {
    name: String,
    cluster: Cluster,
}

#[derive(Deserialize)]
struct Cluster {
    server: String,
    #[serde(rename = "certificate-authority-data")]
    certificate_authority_data: Option<String>,
    #[serde(rename = "certificate-authority")]
    certificate_authority: Option<String>,
}

#[derive(Deserialize)]
struct NamedUser {
    name: String,
    user: UserEntry,
}

#[derive(Deserialize)]
struct UserEntry {
    #[serde(rename = "client-certificate-data")]
    client_certificate_data: Option<String>,
    #[serde(rename = "client-certificate")]
    client_certificate: Option<String>,
    #[serde(rename = "client-key-data")]
    client_key_data: Option<String>,
    #[serde(rename = "client-key")]
    client_key: Option<String>,
}

#[derive(Deserialize)]
struct NamedContext {
    name: String,
    context: ContextEntry,
}

#[derive(Deserialize)]
struct ContextEntry {
    cluster: String,
    user: String,
}

struct KubeAuth {
    kubeconfig_path: PathBuf,
    kubeconfig_dir: PathBuf,
    cluster_name: String,
    user_name: String,
    server: String,
    ca: Option<ResolvedData>,
    cert: ResolvedData,
    key: ResolvedData,
}

#[derive(Clone)]
struct ClientTls {
    client_certificate: Vec<u8>,
    client_key: Vec<u8>,
    ca_bundle: Option<Vec<u8>>,
}

#[derive(Clone)]
struct CertificateAuth {
    tls: ClientTls,
    owner: String,
    curl: CurlAuthData,
}

#[derive(Clone)]
enum AuthContext {
    ClientCertificate(CertificateAuth),
    BearerToken { token: String },
}

fn extract_certificate_owner(pem: &[u8]) -> Result<String, Box<dyn Error + Send + Sync>> {
    let certificates = X509::stack_from_pem(pem).map_err(|err| {
        Box::new(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("invalid client certificate PEM: {err}"),
        )) as Box<dyn Error + Send + Sync>
    })?;
    let leaf = certificates.first().ok_or_else(|| {
        Box::new(io::Error::new(
            io::ErrorKind::InvalidData,
            "client certificate bundle is empty",
        )) as Box<dyn Error + Send + Sync>
    })?;
    let common_name = leaf
        .subject_name()
        .entries_by_nid(Nid::COMMONNAME)
        .next()
        .and_then(|entry| entry.data().as_utf8().ok().map(|data| data.to_string()))
        .ok_or_else(|| {
            Box::new(io::Error::new(
                io::ErrorKind::InvalidData,
                "client certificate is missing a common name",
            )) as Box<dyn Error + Send + Sync>
        })?;
    Ok(common_name)
}

type ExecWebSocketStream = WebSocketStream<SslStream<TcpStream>>;

impl ExecProtocol {
    fn parse(raw: &str) -> Option<Self> {
        if raw.eq_ignore_ascii_case(EXEC_PROTOCOL_PREFERENCE[0]) {
            Some(ExecProtocol::V5)
        } else if raw.eq_ignore_ascii_case(EXEC_PROTOCOL_PREFERENCE[1]) {
            Some(ExecProtocol::V4)
        } else {
            None
        }
    }
}

fn kubeconfig_path() -> Result<PathBuf, Box<dyn Error + Send + Sync>> {
    if let Ok(path) = env::var("KUBECONFIG") {
        let trimmed = path.trim();
        if !trimmed.is_empty() {
            return Ok(PathBuf::from(trimmed));
        }
    }

    let home = env::var("HOME").map_err(|_| {
        io::Error::new(
            io::ErrorKind::NotFound,
            "HOME environment variable is not set",
        )
    })?;
    Ok(PathBuf::from(home).join(".kube").join("config"))
}

fn resolve_path(path: &str, base_dir: &Path) -> Result<PathBuf, Box<dyn Error + Send + Sync>> {
    let trimmed = path.trim();
    let expanded = if let Some(stripped) = trimmed.strip_prefix("~/") {
        let home = env::var("HOME").map_err(|_| {
            io::Error::new(
                io::ErrorKind::NotFound,
                "HOME environment variable is not set",
            )
        })?;
        PathBuf::from(home).join(stripped)
    } else if trimmed == "~" {
        let home = env::var("HOME").map_err(|_| {
            io::Error::new(
                io::ErrorKind::NotFound,
                "HOME environment variable is not set",
            )
        })?;
        PathBuf::from(home)
    } else {
        PathBuf::from(trimmed)
    };

    if expanded.is_absolute() {
        Ok(expanded)
    } else {
        Ok(base_dir.join(expanded))
    }
}

fn read_resolved_data(
    data_field: Option<&String>,
    path_field: Option<&String>,
    config_dir: &Path,
    field_name: &str,
    required: bool,
) -> Result<Option<ResolvedData>, Box<dyn Error + Send + Sync>> {
    if let Some(raw) = data_field {
        let trimmed = raw.trim();
        if trimmed.is_empty() {
            if required {
                return Err(Box::new(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("kubeconfig contains empty {}", field_name),
                )));
            }
            return Ok(None);
        }
        let bytes = BASE64.decode(trimmed).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("failed to decode base64 for {}", field_name),
            )
        })?;
        return Ok(Some(ResolvedData {
            bytes,
            from_data_field: true,
        }));
    }

    if let Some(path) = path_field {
        let resolved = resolve_path(path, config_dir)?;
        let bytes = fs::read(&resolved).map_err(|err| {
            io::Error::new(
                err.kind(),
                format!(
                    "failed to read {} from {}: {}",
                    field_name,
                    resolved.display(),
                    err
                ),
            )
        })?;
        return Ok(Some(ResolvedData {
            bytes,
            from_data_field: false,
        }));
    }

    if required {
        Err(Box::new(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("kubeconfig missing required {}", field_name),
        )))
    } else {
        Ok(None)
    }
}

fn kube_default_pem_path(file_name: &str) -> Result<PathBuf, Box<dyn Error + Send + Sync>> {
    let home = env::var("HOME").map_err(|_| {
        Box::new(io::Error::new(
            io::ErrorKind::NotFound,
            "HOME environment variable is not set",
        )) as Box<dyn Error + Send + Sync>
    })?;
    Ok(PathBuf::from(home).join(".kube").join(file_name))
}

fn read_default_pem_optional(
    file_name: &str,
    description: &str,
) -> Result<(PathBuf, Option<ResolvedData>), Box<dyn Error + Send + Sync>> {
    let path = kube_default_pem_path(file_name)?;
    match fs::read(&path) {
        Ok(bytes) => Ok((
            path,
            Some(ResolvedData {
                bytes,
                from_data_field: false,
            }),
        )),
        Err(err) if err.kind() == io::ErrorKind::NotFound => Ok((path, None)),
        Err(err) => Err(Box::new(io::Error::new(
            err.kind(),
            format!(
                "failed to read default {description} from {}: {err}",
                path.display()
            ),
        ))),
    }
}

fn read_default_pem_required(
    file_name: &str,
    description: &str,
) -> Result<ResolvedData, Box<dyn Error + Send + Sync>> {
    let (path, data) = read_default_pem_optional(file_name, description)?;
    data.ok_or_else(|| {
        Box::new(io::Error::new(
            io::ErrorKind::NotFound,
            format!("default {description} is missing at {}", path.display()),
        )) as Box<dyn Error + Send + Sync>
    })
}

fn load_kube_auth() -> Result<Option<KubeAuth>, Box<dyn Error + Send + Sync>> {
    let path = match kubeconfig_path() {
        Ok(path) => path,
        Err(_) => return Ok(None),
    };
    let raw = match fs::read_to_string(&path) {
        Ok(contents) => contents,
        Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(err) => {
            return Err(Box::new(io::Error::new(
                err.kind(),
                format!("failed to read kubeconfig {}: {}", path.display(), err),
            )))
        }
    };
    let config: KubeConfig = serde_yaml::from_str(&raw).map_err(|err| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("failed to parse kubeconfig {}: {}", path.display(), err),
        )
    })?;

    let context_name = config
        .current_context
        .or_else(|| config.contexts.first().map(|ctx| ctx.name.clone()))
        .ok_or_else(|| {
            Box::new(io::Error::new(
                io::ErrorKind::InvalidData,
                "kubeconfig does not define any contexts",
            )) as Box<dyn Error + Send + Sync>
        })?;

    let context = config
        .contexts
        .iter()
        .find(|ctx| ctx.name == context_name)
        .ok_or_else(|| {
            Box::new(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("kubeconfig missing context '{}'", context_name),
            )) as Box<dyn Error + Send + Sync>
        })?;

    let cluster = config
        .clusters
        .iter()
        .find(|cl| cl.name == context.context.cluster)
        .ok_or_else(|| {
            Box::new(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "kubeconfig missing cluster '{}' referenced by context '{}'",
                    context.context.cluster, context_name
                ),
            )) as Box<dyn Error + Send + Sync>
        })?;

    let user = config
        .users
        .iter()
        .find(|usr| usr.name == context.context.user)
        .ok_or_else(|| {
            Box::new(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "kubeconfig missing user '{}' referenced by context '{}'",
                    context.context.user, context_name
                ),
            )) as Box<dyn Error + Send + Sync>
        })?;

    let config_dir = path
        .parent()
        .map(|dir| dir.to_path_buf())
        .unwrap_or_else(|| PathBuf::from("."));

    let explicit_ca = cluster.cluster.certificate_authority_data.is_some()
        || cluster.cluster.certificate_authority.is_some();
    let explicit_cert =
        user.user.client_certificate_data.is_some() || user.user.client_certificate.is_some();
    let explicit_key = user.user.client_key_data.is_some() || user.user.client_key.is_some();

    let ca = if explicit_ca {
        match read_resolved_data(
            cluster.cluster.certificate_authority_data.as_ref(),
            cluster.cluster.certificate_authority.as_ref(),
            &config_dir,
            "certificate authority",
            false,
        )? {
            Some(data) => Some(data),
            None => Some(read_default_pem_required(
                "ca.pem",
                "certificate authority",
            )?),
        }
    } else {
        Some(read_default_pem_required(
            "ca.pem",
            "certificate authority",
        )?)
    };

    let cert = if explicit_cert {
        read_resolved_data(
            user.user.client_certificate_data.as_ref(),
            user.user.client_certificate.as_ref(),
            &config_dir,
            "client certificate",
            true,
        )?
        .ok_or_else(|| {
            Box::new(io::Error::new(
                io::ErrorKind::InvalidData,
                "client certificate is required in kubeconfig",
            )) as Box<dyn Error + Send + Sync>
        })?
    } else {
        read_default_pem_required("admin_cert.pem", "client certificate")?
    };

    let key = if explicit_key {
        read_resolved_data(
            user.user.client_key_data.as_ref(),
            user.user.client_key.as_ref(),
            &config_dir,
            "client key",
            true,
        )?
        .ok_or_else(|| {
            Box::new(io::Error::new(
                io::ErrorKind::InvalidData,
                "client key is required in kubeconfig",
            )) as Box<dyn Error + Send + Sync>
        })?
    } else {
        read_default_pem_required("admin_key.pem", "client key")?
    };

    Ok(Some(KubeAuth {
        kubeconfig_path: path,
        kubeconfig_dir: config_dir,
        cluster_name: cluster.name.clone(),
        user_name: user.name.clone(),
        server: cluster.cluster.server.clone(),
        ca,
        cert,
        key,
    }))
}

#[derive(Clone)]
pub struct NanocloudClient {
    client: Client,
    base_url: Url,
    auth: AuthContext,
}

impl NanocloudClient {
    pub fn new() -> Result<Self, Box<dyn Error + Send + Sync>> {
        Self::from_kube_auth(load_kube_auth()?)
    }

    pub fn with_bearer(host: &str, token: String) -> Result<Self, Box<dyn Error + Send + Sync>> {
        let token_trimmed = token.trim();
        if token_trimmed.is_empty() {
            return Err(Box::new(io::Error::new(
                io::ErrorKind::InvalidInput,
                "bootstrap token must not be empty",
            )) as Box<dyn Error + Send + Sync>);
        }
        let endpoint = select_endpoint(Some(host))?;
        let base_url = Url::parse(&endpoint)?;
        let builder = Client::builder()
            .timeout(Duration::from_secs(30))
            .http1_only()
            .danger_accept_invalid_certs(true);

        let client = builder.build().map_err(|err| {
            io::Error::other(format!("failed to construct Nanocloud HTTP client: {err}"))
        })?;

        Ok(NanocloudClient {
            client,
            base_url,
            auth: AuthContext::BearerToken {
                token: token_trimmed.to_string(),
            },
        })
    }

    fn from_kube_auth(kube_auth: Option<KubeAuth>) -> Result<Self, Box<dyn Error + Send + Sync>> {
        let kube_auth = kube_auth.ok_or_else(|| {
            Box::new(io::Error::new(
                io::ErrorKind::NotFound,
                "kubeconfig is required for this command",
            )) as Box<dyn Error + Send + Sync>
        })?;

        let endpoint = env::var(SERVER_ENV).unwrap_or_else(|_| kube_auth.server.clone());
        let base_url = Url::parse(&endpoint)?;

        let identity = build_reqwest_identity(&kube_auth.cert.bytes, &kube_auth.key.bytes)
            .map_err(|err| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("kubeconfig contains malformed client identity: {err}"),
                )
            })?;

        let ca_bytes = kube_auth.ca.as_ref().map(|data| data.bytes.clone());

        let mut client_builder = Client::builder()
            .identity(identity)
            .timeout(Duration::from_secs(30))
            .http1_only();

        if let Some(bytes) = ca_bytes.as_ref() {
            let ca_certificate = Certificate::from_pem(bytes)?;
            client_builder = client_builder.add_root_certificate(ca_certificate);
        }

        let client = client_builder.build().map_err(|err| {
            io::Error::other(format!("failed to construct Nanocloud HTTP client: {err}"))
        })?;

        let tls = ClientTls {
            client_certificate: kube_auth.cert.bytes.clone(),
            client_key: kube_auth.key.bytes.clone(),
            ca_bundle: ca_bytes.clone(),
        };

        let owner = extract_certificate_owner(&kube_auth.cert.bytes).map_err(|err| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("failed to derive owner from client certificate: {err}"),
            )
        })?;

        let curl_auth = CurlAuthData {
            kubeconfig_path: kube_auth.kubeconfig_path.clone(),
            kubeconfig_dir: kube_auth.kubeconfig_dir.clone(),
            cluster_name: kube_auth.cluster_name.clone(),
            user_name: kube_auth.user_name.clone(),
            ca_source: kube_auth.ca.as_ref().map(|ca| {
                if ca.from_data_field {
                    KubeFieldSource::InlineData
                } else {
                    KubeFieldSource::FilePath
                }
            }),
            cert_source: if kube_auth.cert.from_data_field {
                KubeFieldSource::InlineData
            } else {
                KubeFieldSource::FilePath
            },
            key_source: if kube_auth.key.from_data_field {
                KubeFieldSource::InlineData
            } else {
                KubeFieldSource::FilePath
            },
        };

        Ok(NanocloudClient {
            client,
            base_url,
            auth: AuthContext::ClientCertificate(CertificateAuth {
                tls,
                owner,
                curl: curl_auth,
            }),
        })
    }

    pub fn curl_identity(&self) -> Option<&CurlAuthData> {
        match &self.auth {
            AuthContext::ClientCertificate(ctx) => Some(&ctx.curl),
            _ => None,
        }
    }

    pub fn owner(&self) -> Option<&str> {
        match &self.auth {
            AuthContext::ClientCertificate(ctx) => Some(ctx.owner.as_str()),
            _ => None,
        }
    }

    pub fn bearer_token(&self) -> Option<&str> {
        match &self.auth {
            AuthContext::BearerToken { token } => Some(token.as_str()),
            _ => None,
        }
    }

    fn apply_auth(&self, request: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        match &self.auth {
            AuthContext::ClientCertificate(ctx) => request.header("X-Nanocloud-Owner", &ctx.owner),
            AuthContext::BearerToken { token } => request.bearer_auth(token),
        }
    }

    fn certificate_auth(&self) -> Result<&CertificateAuth, Box<dyn Error + Send + Sync>> {
        match &self.auth {
            AuthContext::ClientCertificate(ctx) => Ok(ctx),
            _ => Err(Box::new(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "client certificate is required for this operation",
            )) as Box<dyn Error + Send + Sync>),
        }
    }

    pub fn url_from_segments(
        &self,
        segments: &[&str],
    ) -> Result<Url, Box<dyn Error + Send + Sync>> {
        let mut url = self.base_url.clone();
        {
            let mut parts = url
                .path_segments_mut()
                .map_err(|_| "base URL cannot be base for segments")?;
            parts.clear();
            for segment in segments {
                if !segment.is_empty() {
                    parts.push(segment);
                }
            }
        }
        Ok(url)
    }

    pub fn logs_segments<'a>(namespace: Option<&'a str>, service: &'a str) -> Vec<&'a str> {
        let mut segments = vec!["api", "v1"];
        if let Some(ns) = namespace.filter(|s| !s.is_empty()) {
            segments.push("namespaces");
            segments.push(ns);
            segments.push("pods");
        } else {
            segments.push("pods");
        }
        segments.push(service);
        segments.push("log");
        segments
    }

    pub fn pod_collection_segments(namespace: Option<&str>) -> Vec<&str> {
        let mut segments = vec!["api", "v1"];
        if let Some(ns) = namespace.filter(|s| !s.is_empty()) {
            segments.push("namespaces");
            segments.push(ns);
            segments.push("pods");
        } else {
            segments.push("pods");
        }
        segments
    }

    pub fn event_collection_segments(namespace: Option<&str>) -> Vec<&str> {
        let mut segments = vec!["api", "v1"];
        if let Some(ns) = namespace.filter(|s| !s.is_empty()) {
            segments.push("namespaces");
            segments.push(ns);
            segments.push("events");
        } else {
            segments.push("events");
        }
        segments
    }

    pub async fn list_pods_table(
        &self,
        namespace: Option<&str>,
    ) -> Result<PodTable, Box<dyn Error + Send + Sync>> {
        let segments = Self::pod_collection_segments(namespace);
        let url = self.url_from_segments(&segments)?;
        let request = self
            .client
            .get(url)
            .query(&[("format", "table")])
            .header(reqwest::header::ACCEPT, POD_TABLE_ACCEPT);
        self.send_json(request).await
    }

    async fn open_exec_websocket(
        &self,
        url: &Url,
    ) -> Result<(ExecWebSocketStream, ExecProtocol), Box<dyn Error + Send + Sync>> {
        let mut ws_url = url.clone();
        match ws_url.scheme() {
            "https" => {
                ws_url.set_scheme("wss").map_err(|_| {
                    Box::new(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "failed to convert exec URL to wss scheme",
                    )) as Box<dyn Error + Send + Sync>
                })?;
            }
            "http" => {
                ws_url.set_scheme("ws").map_err(|_| {
                    Box::new(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "failed to convert exec URL to ws scheme",
                    )) as Box<dyn Error + Send + Sync>
                })?;
                return Err(Box::new(io::Error::new(
                    io::ErrorKind::Unsupported,
                    "exec over plain WebSocket is not supported; use HTTPS/WSS endpoint",
                )));
            }
            "wss" => {}
            "ws" => {
                return Err(Box::new(io::Error::new(
                    io::ErrorKind::Unsupported,
                    "exec over plain WebSocket is not supported; use HTTPS/WSS endpoint",
                )));
            }
            other => {
                return Err(Box::new(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("unsupported exec URL scheme: {}", other),
                )));
            }
        }

        let host = ws_url.host_str().ok_or_else(|| {
            Box::new(io::Error::new(
                io::ErrorKind::InvalidInput,
                "exec URL is missing a host",
            )) as Box<dyn Error + Send + Sync>
        })?;
        let port = ws_url.port_or_known_default().ok_or_else(|| {
            Box::new(io::Error::new(
                io::ErrorKind::InvalidInput,
                "exec URL is missing a port",
            )) as Box<dyn Error + Send + Sync>
        })?;

        let mut address = host.to_string();
        if address.contains(':') && !address.starts_with('[') {
            address = format!("[{}]", address);
        }
        address.push(':');
        address.push_str(&port.to_string());

        let tcp_stream = TcpStream::connect(address).await.map_err(|err| {
            Box::new(io::Error::new(
                err.kind(),
                format!("failed to connect to exec endpoint: {err}"),
            )) as Box<dyn Error + Send + Sync>
        })?;

        let connector = self.build_exec_ssl_connector()?;
        let config = connector
            .configure()
            .map_err(|err| Box::new(err) as Box<dyn Error + Send + Sync>)?;
        let ssl = config
            .into_ssl(host)
            .map_err(|err| Box::new(err) as Box<dyn Error + Send + Sync>)?;
        let mut tls_stream = SslStream::new(ssl, tcp_stream)
            .map_err(|err| Box::new(err) as Box<dyn Error + Send + Sync>)?;
        Pin::new(&mut tls_stream)
            .connect()
            .await
            .map_err(|err| Box::new(err) as Box<dyn Error + Send + Sync>)?;

        let mut request = ws_url
            .into_client_request()
            .map_err(|err| Box::new(err) as Box<dyn Error + Send + Sync>)?;
        request.headers_mut().insert(
            SEC_WEBSOCKET_PROTOCOL,
            HeaderValue::from_static(EXEC_PROTOCOL_HEADER_VALUE),
        );

        let (stream, response) = client_async(request, tls_stream)
            .await
            .map_err(|err| Box::new(err) as Box<dyn Error + Send + Sync>)?;
        let protocol = Self::extract_exec_protocol(response.headers())?;
        Ok((stream, protocol))
    }

    fn build_exec_ssl_connector(&self) -> Result<SslConnector, Box<dyn Error + Send + Sync>> {
        let certificate_auth = self.certificate_auth()?;
        let tls = &certificate_auth.tls;
        let mut builder = SslConnector::builder(SslMethod::tls_client())
            .map_err(|err| Box::new(err) as Box<dyn Error + Send + Sync>)?;

        let certificates = X509::stack_from_pem(&tls.client_certificate).map_err(|err| {
            Box::new(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("invalid client certificate PEM: {err}"),
            )) as Box<dyn Error + Send + Sync>
        })?;
        let (leaf, chain) = certificates.split_first().ok_or_else(|| {
            Box::new(io::Error::new(
                io::ErrorKind::InvalidData,
                "client certificate PEM did not contain any certificates",
            )) as Box<dyn Error + Send + Sync>
        })?;
        builder
            .set_certificate(leaf)
            .map_err(|err| Box::new(err) as Box<dyn Error + Send + Sync>)?;
        for cert in chain {
            builder
                .add_extra_chain_cert(cert.to_owned())
                .map_err(|err| Box::new(err) as Box<dyn Error + Send + Sync>)?;
        }

        let private_key = PKey::private_key_from_pem(&tls.client_key).map_err(|err| {
            Box::new(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("invalid client key PEM: {err}"),
            )) as Box<dyn Error + Send + Sync>
        })?;
        builder
            .set_private_key(&private_key)
            .map_err(|err| Box::new(err) as Box<dyn Error + Send + Sync>)?;
        builder
            .check_private_key()
            .map_err(|err| Box::new(err) as Box<dyn Error + Send + Sync>)?;

        if let Some(ca_bundle) = &tls.ca_bundle {
            let store = builder.cert_store_mut();
            let ca_chain = X509::stack_from_pem(ca_bundle).map_err(|err| {
                Box::new(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("invalid CA bundle PEM: {err}"),
                )) as Box<dyn Error + Send + Sync>
            })?;
            for cert in ca_chain {
                store
                    .add_cert(cert)
                    .map_err(|err| Box::new(err) as Box<dyn Error + Send + Sync>)?;
            }
        }

        builder.set_verify(SslVerifyMode::PEER);
        Ok(builder.build())
    }

    fn extract_exec_protocol(
        headers: &HeaderMap,
    ) -> Result<ExecProtocol, Box<dyn Error + Send + Sync>> {
        let header = match headers.get(SEC_WEBSOCKET_PROTOCOL) {
            Some(value) => value,
            None => {
                return Err(Box::new(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "exec server did not select a channel.k8s.io subprotocol",
                )))
            }
        };

        let value = header.to_str().map_err(|err| {
            Box::new(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("invalid Sec-WebSocket-Protocol header: {err}"),
            )) as Box<dyn Error + Send + Sync>
        })?;

        let tokens: Vec<&str> = value
            .split(',')
            .map(|token| token.trim())
            .filter(|token| !token.is_empty())
            .collect();

        if tokens.is_empty() {
            return Err(Box::new(io::Error::new(
                io::ErrorKind::InvalidData,
                "exec server returned empty Sec-WebSocket-Protocol header",
            )) as Box<dyn Error + Send + Sync>);
        }

        for preferred in EXEC_PROTOCOL_PREFERENCE {
            if tokens.contains(&preferred) {
                return Ok(
                    ExecProtocol::parse(preferred).expect("preferred protocol must be supported")
                );
            }
        }

        if let Some(first_supported) = tokens.iter().find_map(|token| ExecProtocol::parse(token)) {
            return Ok(first_supported);
        }

        Err(Box::new(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "exec server selected unsupported subprotocol '{}'; expected one of {}",
                value, EXEC_PROTOCOL_HEADER_VALUE
            ),
        )) as Box<dyn Error + Send + Sync>)
    }

    pub async fn exec(&self, options: &ExecRequest) -> Result<i32, Box<dyn Error + Send + Sync>> {
        let url = self.build_exec_url(options)?;
        let (websocket, _) = self.open_exec_websocket(&url).await?;
        let mut multiplexer = ExecChannelMultiplexer::new(websocket);

        let stdin_sender = if options.stdin {
            Some(multiplexer.stdin_sender())
        } else {
            None
        };
        let mut stdout_rx = multiplexer.take_stdout();
        let mut stderr_rx = multiplexer.take_stderr();
        let status_rx = multiplexer.take_status();
        let tty_requested = options.tty;
        let resize_sender = if tty_requested {
            Some(multiplexer.resize_sender())
        } else {
            None
        };
        let close_rx = multiplexer.take_close();
        let (exit_tx, exit_rx) = oneshot::channel();
        let session_done = Arc::new(Notify::new());

        let stdout_stream = stdout_rx.take();
        let stderr_for_stdout = if tty_requested {
            stderr_rx.take()
        } else {
            None
        };

        let stdin_task = {
            let session_done = Arc::clone(&session_done);
            async move {
                if let Some(sender) = stdin_sender {
                    pipe_stdin_to_channel(sender, session_done).await?;
                }
                Ok::<(), io::Error>(())
            }
        };

        let stdout_task = async move {
            if tty_requested {
                forward_tty_streams(stdout_stream, stderr_for_stdout).await?;
            } else if let Some(receiver) = stdout_stream {
                forward_stream(receiver, tokio::io::stdout()).await?;
            }
            Ok::<(), io::Error>(())
        };

        let stderr_remaining = stderr_rx;
        let stderr_task = async move {
            if !tty_requested {
                if let Some(receiver) = stderr_remaining {
                    forward_stream(receiver, tokio::io::stderr()).await?;
                }
            }
            Ok::<(), io::Error>(())
        };

        let close_task = {
            let session_done = Arc::clone(&session_done);
            async move {
                let result = async {
                    if let Some(mut receiver) = close_rx {
                        let _ = receiver.recv().await;
                    }
                    Ok::<(), io::Error>(())
                }
                .await;
                session_done.notify_waiters();
                result
            }
        };

        let resize_task = {
            let session_done = Arc::clone(&session_done);
            async move {
                if tty_requested {
                    if let Some(sender) = resize_sender {
                        pump_resize_events(sender, libc::STDIN_FILENO, session_done).await?;
                    }
                }
                Ok::<(), io::Error>(())
            }
        };

        let status_task = {
            let session_done = Arc::clone(&session_done);
            async move {
                let result = async {
                    if let Some(mut receiver) = status_rx {
                        if let Some(payload) = receiver.recv().await {
                            let result = parse_status_payload(&payload)?;
                            let _ = exit_tx.send(result);
                        } else {
                            let _ = exit_tx.send(ExecExitInfo::default());
                        }
                    } else {
                        let _ = exit_tx.send(ExecExitInfo::default());
                    }
                    Ok::<(), io::Error>(())
                }
                .await;
                session_done.notify_waiters();
                result
            }
        };

        tokio::try_join!(
            stdin_task,
            stdout_task,
            stderr_task,
            close_task,
            resize_task,
            status_task
        )
        .map_err(|err| Box::new(err) as Box<dyn Error + Send + Sync>)?;

        let exit_info: ExecExitInfo = exit_rx.await.unwrap_or_default();
        let (exit_code, summary) = exit_code_and_summary(options, &exit_info);
        if let Some(text) = summary {
            if options.tty {
                eprint!("\r{}\r\n", text);
            } else {
                eprintln!("{text}");
            }
        }

        multiplexer.shutdown().await;
        Ok(exit_code)
    }

    fn build_exec_url(&self, options: &ExecRequest) -> Result<Url, Box<dyn Error + Send + Sync>> {
        let segments = vec![
            "api",
            "v1",
            "namespaces",
            options.namespace.as_str(),
            "pods",
            options.pod.as_str(),
            "exec",
        ];
        let mut url = self.url_from_segments(&segments)?;

        {
            let mut query = url.query_pairs_mut();
            if let Some(container) = options
                .container
                .as_deref()
                .filter(|value| !value.is_empty())
            {
                query.append_pair("container", container);
            }
            for command in &options.command {
                query.append_pair("command", command);
            }
            query.append_pair("stdin", Self::bool_query_value(options.stdin));
            query.append_pair("stdout", Self::bool_query_value(options.stdout));
            query.append_pair("stderr", Self::bool_query_value(options.stderr));
            query.append_pair("tty", Self::bool_query_value(options.tty));
        }

        Ok(url)
    }

    #[inline]
    fn bool_query_value(value: bool) -> &'static str {
        if value {
            "true"
        } else {
            "false"
        }
    }

    pub fn pod_segments<'a>(namespace: Option<&'a str>, pod: &'a str) -> Vec<&'a str> {
        let mut segments = vec!["api", "v1"];
        if let Some(ns) = namespace.filter(|s| !s.is_empty()) {
            segments.push("namespaces");
            segments.push(ns);
            segments.push("pods");
        } else {
            segments.push("pods");
        }
        segments.push(pod);
        segments
    }

    #[allow(clippy::needless_lifetimes)]
    pub fn bundle_collection_segments<'a>(namespace: Option<&'a str>) -> Vec<&'a str> {
        let ns = namespace.filter(|s| !s.is_empty()).unwrap_or("default");
        vec!["apis", "nanocloud.io", "v1", "namespaces", ns, "bundles"]
    }

    #[allow(clippy::needless_lifetimes)]
    pub fn bundle_segments<'a>(namespace: Option<&'a str>, name: &'a str) -> Vec<&'a str> {
        let mut segments = Self::bundle_collection_segments(namespace);
        segments.push(name);
        segments
    }

    pub fn bundle_export_segments(namespace: Option<&str>, name: &str) -> Vec<String> {
        let mut segments = Self::bundle_collection_segments(namespace)
            .into_iter()
            .map(|segment| segment.to_string())
            .collect::<Vec<_>>();
        segments.push(name.to_string());
        segments.push("exportProfile".to_string());
        segments
    }

    async fn handle_json<T>(
        &self,
        response: reqwest::Response,
    ) -> Result<T, Box<dyn Error + Send + Sync>>
    where
        T: DeserializeOwned,
    {
        let status = response.status();
        if status.is_success() {
            let body = response.json::<T>().await?;
            return Ok(body);
        }

        let text = response.text().await.unwrap_or_default();
        if let Ok(parsed) = serde_json::from_str::<crate::nanocloud::api::types::ErrorBody>(&text) {
            let err = match parsed.conflicts {
                Some(conflicts) if !conflicts.is_empty() => {
                    HttpError::with_conflicts(status, parsed.error, conflicts)
                }
                _ => HttpError::new(status, parsed.error),
            };
            return Err(Box::new(err));
        }

        let message = if text.is_empty() {
            status
                .canonical_reason()
                .unwrap_or("request failed")
                .to_string()
        } else {
            text
        };

        Err(Box::new(HttpError::new(status, message)))
    }

    async fn send_json<T>(
        &self,
        request: reqwest::RequestBuilder,
    ) -> Result<T, Box<dyn Error + Send + Sync>>
    where
        T: DeserializeOwned,
    {
        let response = self.apply_auth(request).send().await?;
        self.handle_json(response).await
    }

    pub async fn issue_certificate(
        &self,
        common_name: &str,
        additional: Option<Vec<String>>,
    ) -> Result<JsonTlsInfo, Box<dyn Error + Send + Sync>> {
        let url = self.url_from_segments(&["v1", "ca"])?;
        let payload = CaRequest {
            common_name: common_name.to_string(),
            additional,
        };
        let response = self.client.post(url).json(&payload);
        self.send_json(response).await
    }

    pub async fn request_ephemeral_certificate(
        &self,
        csr_pem: &str,
    ) -> Result<EphemeralCertificate, Box<dyn Error + Send + Sync>> {
        if self.bearer_token().is_none() {
            return Err(Box::new(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "bootstrap token authentication is required to request an ephemeral certificate",
            )) as Box<dyn Error + Send + Sync>);
        }

        let csr = csr_pem.trim();
        if csr.is_empty() {
            return Err(Box::new(io::Error::new(
                io::ErrorKind::InvalidInput,
                "certificate signing request must not be empty",
            )));
        }

        let url = self.url_from_segments(&["apis", "nanocloud.io", "v1", "certificates"])?;
        let payload = CertificateRequest {
            api_version: CERTIFICATE_API_VERSION.to_string(),
            kind: CERTIFICATE_KIND.to_string(),
            spec: CertificateSpec {
                csr_pem: csr.to_string(),
            },
        };

        let mut backoff = RETRY_BACKOFF;
        let mut last_err: Option<Box<dyn Error + Send + Sync>> = None;

        for attempt in 0..RETRY_ATTEMPTS {
            let request = self.client.post(url.clone()).json(&payload);
            let response = self.apply_auth(request).send().await;

            match response {
                Ok(resp) => match self.handle_json::<CertificateResponse>(resp).await {
                    Ok(body) => return convert_certificate_response(body),
                    Err(err) => match err.downcast::<HttpError>() {
                        Ok(http_err) => {
                            let status = http_err.status;
                            let boxed: Box<dyn Error + Send + Sync> = http_err;
                            if should_retry_status(status) && attempt + 1 < RETRY_ATTEMPTS {
                                last_err = Some(boxed);
                            } else {
                                return Err(boxed);
                            }
                        }
                        Err(other) => {
                            if attempt + 1 < RETRY_ATTEMPTS {
                                last_err = Some(other);
                            } else {
                                return Err(other);
                            }
                        }
                    },
                },
                Err(err) => {
                    if is_retryable_reqwest(&err) && attempt + 1 < RETRY_ATTEMPTS {
                        last_err = Some(Box::new(err));
                    } else {
                        return Err(Box::new(err));
                    }
                }
            }

            if attempt + 1 < RETRY_ATTEMPTS {
                sleep(backoff).await;
                backoff = next_backoff(backoff);
            }
        }

        Err(last_err.unwrap_or_else(|| {
            Box::new(HttpError::new(
                StatusCode::SERVICE_UNAVAILABLE,
                "failed to request certificate",
            ))
        }))
    }

    #[allow(dead_code)] // TODO(api-devices): wire into device management CLI (see docs/refactor.md).
    pub async fn list_devices(
        &self,
        namespace: Option<&str>,
    ) -> Result<DeviceList, Box<dyn Error + Send + Sync>> {
        let ns = namespace.filter(|s| !s.is_empty()).unwrap_or("default");
        let url =
            self.url_from_segments(&["apis", "nanocloud.io", "v1", "namespaces", ns, "devices"])?;
        let request = self.client.get(url);
        self.send_json(request).await
    }

    #[allow(dead_code)] // TODO(api-devices): wire into device management CLI (see docs/refactor.md).
    pub async fn get_device(
        &self,
        namespace: &str,
        name: &str,
    ) -> Result<Device, Box<dyn Error + Send + Sync>> {
        let ns = if namespace.is_empty() {
            "default"
        } else {
            namespace
        };
        let url = self.url_from_segments(&[
            "apis",
            "nanocloud.io",
            "v1",
            "namespaces",
            ns,
            "devices",
            name,
        ])?;
        let request = self.client.get(url);
        self.send_json(request).await
    }

    #[allow(dead_code)] // TODO(api-devices): wire into device management CLI (see docs/refactor.md).
    pub async fn create_device(
        &self,
        namespace: Option<&str>,
        hash: &str,
        description: Option<&str>,
    ) -> Result<Device, Box<dyn Error + Send + Sync>> {
        let trimmed = hash.trim();
        if trimmed.is_empty() {
            return Err(Box::new(io::Error::new(
                io::ErrorKind::InvalidInput,
                "device hash must not be empty",
            )) as Box<dyn Error + Send + Sync>);
        }

        let ns = namespace.filter(|s| !s.is_empty()).unwrap_or("default");
        let metadata = ObjectMeta {
            name: Some(format!("device-{trimmed}")),
            namespace: Some(ns.to_string()),
            ..Default::default()
        };
        let spec = DeviceSpec {
            hash: trimmed.to_string(),
            certificate_subject: format!("device:{trimmed}"),
            description: description.map(|value| value.to_string()),
        };
        let payload = Device {
            api_version: "nanocloud.io/v1".to_string(),
            kind: "Device".to_string(),
            metadata,
            spec,
            status: None,
        };

        let url =
            self.url_from_segments(&["apis", "nanocloud.io", "v1", "namespaces", ns, "devices"])?;
        let request = self.client.post(url).json(&payload);
        self.send_json(request).await
    }

    #[allow(dead_code)] // TODO(api-devices): wire into device management CLI (see docs/refactor.md).
    pub async fn delete_device(
        &self,
        namespace: &str,
        name: &str,
    ) -> Result<Device, Box<dyn Error + Send + Sync>> {
        let ns = if namespace.is_empty() {
            "default"
        } else {
            namespace
        };
        let url = self.url_from_segments(&[
            "apis",
            "nanocloud.io",
            "v1",
            "namespaces",
            ns,
            "devices",
            name,
        ])?;
        let request = self.client.delete(url);
        self.send_json(request).await
    }

    #[allow(dead_code)] // TODO(api-devices): wire into device management CLI (see docs/refactor.md).
    pub async fn issue_device_certificate(
        &self,
        namespace: &str,
        csr_pem: &str,
    ) -> Result<CertificateResponse, Box<dyn Error + Send + Sync>> {
        let csr = csr_pem.trim();
        if csr.is_empty() {
            return Err(Box::new(io::Error::new(
                io::ErrorKind::InvalidInput,
                "certificate signing request must not be empty",
            )) as Box<dyn Error + Send + Sync>);
        }

        let ns = if namespace.is_empty() {
            "default"
        } else {
            namespace
        };
        let url = self.url_from_segments(&[
            "apis",
            "nanocloud.io",
            "v1",
            "namespaces",
            ns,
            "devices",
            "certificates",
        ])?;

        let payload = CertificateRequest {
            api_version: CERTIFICATE_API_VERSION.to_string(),
            kind: CERTIFICATE_KIND.to_string(),
            spec: CertificateSpec {
                csr_pem: csr.to_string(),
            },
        };

        let request = self.client.post(url).json(&payload);
        self.send_json(request).await
    }

    pub async fn create_bundle(
        &self,
        namespace: Option<&str>,
        service: &str,
        options: HashMap<String, String>,
        snapshot: Option<&str>,
        start: bool,
        update: bool,
    ) -> Result<Bundle, Box<dyn Error + Send + Sync>> {
        let namespace_value = namespace.filter(|s| !s.is_empty());
        let path_namespace = namespace_value.unwrap_or("default");
        let metadata = ObjectMeta {
            name: Some(service.to_string()),
            namespace: Some(path_namespace.to_string()),
            ..Default::default()
        };

        let spec = BundleSpec {
            service: service.to_string(),
            namespace: namespace_value.map(|ns| ns.to_string()),
            options,
            profile_key: None,
            snapshot: snapshot.map(|path| BundleSnapshotSource {
                source: path.to_string(),
                media_type: None,
            }),
            start,
            update,
            security: None,
        };

        let payload = Bundle {
            api_version: BUNDLE_API_VERSION.to_string(),
            kind: BUNDLE_KIND.to_string(),
            metadata,
            spec,
            status: None,
        };

        let url = self.url_from_segments(&[
            "apis",
            "nanocloud.io",
            "v1",
            "namespaces",
            path_namespace,
            "bundles",
        ])?;
        let request = self.client.post(url).json(&payload);
        self.send_json(request).await
    }

    pub async fn apply_bundle(
        &self,
        namespace: Option<&str>,
        service: &str,
        options: ApplyBundleOptions<'_>,
    ) -> Result<Bundle, Box<dyn Error + Send + Sync>> {
        let trimmed_manager = options.field_manager.trim();
        if trimmed_manager.is_empty() {
            return Err(Box::new(io::Error::new(
                io::ErrorKind::InvalidInput,
                "field manager is required for apply operations",
            )));
        }

        let segments = Self::bundle_segments(namespace, service);
        let mut url = self.url_from_segments(&segments)?;
        {
            let mut pairs = url.query_pairs_mut();
            pairs.append_pair("fieldManager", trimmed_manager);
            if options.force {
                pairs.append_pair("force", "true");
            }
            if options.dry_run {
                pairs.append_pair("dryRun", "true");
            }
        }

        let request = self
            .client
            .patch(url)
            .header(CONTENT_TYPE, options.content_type)
            .body(options.payload.to_vec());
        self.send_json(request).await
    }

    pub async fn list_bundles(
        &self,
        namespace: Option<&str>,
    ) -> Result<BundleList, Box<dyn Error + Send + Sync>> {
        let segments = Self::bundle_collection_segments(namespace);
        let url = self.url_from_segments(&segments)?;
        let request = self.client.get(url);
        self.send_json(request).await
    }

    pub async fn get_bundle(
        &self,
        namespace: Option<&str>,
        service: &str,
    ) -> Result<Option<Bundle>, Box<dyn Error + Send + Sync>> {
        let segments = Self::bundle_segments(namespace, service);
        let url = self.url_from_segments(&segments)?;
        let response = self.apply_auth(self.client.get(url)).send().await?;
        match response.status() {
            StatusCode::NOT_FOUND => Ok(None),
            status if status.is_success() => {
                let bundle = response.json::<Bundle>().await?;
                Ok(Some(bundle))
            }
            status => {
                let text = response.text().await.unwrap_or_default();
                let message = if text.is_empty() {
                    status
                        .canonical_reason()
                        .unwrap_or("request failed")
                        .to_string()
                } else {
                    text
                };
                Err(Box::new(HttpError::new(status, message)))
            }
        }
    }

    pub async fn uninstall_bundle(
        &self,
        namespace: Option<&str>,
        service: &str,
    ) -> Result<ServiceActionResponse, Box<dyn Error + Send + Sync>> {
        self.bundle_action(namespace, service, "uninstall").await
    }

    pub async fn delete_bundle(
        &self,
        namespace: Option<&str>,
        service: &str,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        let ns = namespace.filter(|s| !s.is_empty()).unwrap_or("default");
        let url = self.url_from_segments(&[
            "apis",
            "nanocloud.io",
            "v1",
            "namespaces",
            ns,
            "bundles",
            service,
        ])?;

        let response = self.apply_auth(self.client.delete(url)).send().await?;
        if response.status() == StatusCode::NOT_FOUND {
            return Ok(());
        }
        if response.status().is_success() {
            return Ok(());
        }

        let status = response.status();
        let text = response.text().await.unwrap_or_default();
        let message = if text.is_empty() {
            status
                .canonical_reason()
                .unwrap_or("request failed")
                .to_string()
        } else {
            text
        };

        Err(Box::new(HttpError::new(status, message)))
    }

    pub async fn start_bundle(
        &self,
        namespace: Option<&str>,
        service: &str,
    ) -> Result<ServiceActionResponse, Box<dyn Error + Send + Sync>> {
        self.bundle_action(namespace, service, "start").await
    }

    pub async fn stop_bundle(
        &self,
        namespace: Option<&str>,
        service: &str,
    ) -> Result<ServiceActionResponse, Box<dyn Error + Send + Sync>> {
        self.bundle_action(namespace, service, "stop").await
    }

    pub async fn restart_bundle(
        &self,
        namespace: Option<&str>,
        service: &str,
    ) -> Result<ServiceActionResponse, Box<dyn Error + Send + Sync>> {
        self.bundle_action(namespace, service, "restart").await
    }

    async fn bundle_action(
        &self,
        namespace: Option<&str>,
        service: &str,
        action: &str,
    ) -> Result<ServiceActionResponse, Box<dyn Error + Send + Sync>> {
        let ns = namespace.filter(|s| !s.is_empty()).unwrap_or("default");
        let action_segment = action.to_string();
        let url = self.url_from_segments(&[
            "apis",
            "nanocloud.io",
            "v1",
            "namespaces",
            ns,
            "bundles",
            service,
            "actions",
            action_segment.as_str(),
        ])?;
        let request = self.client.post(url);
        self.send_json(request).await
    }

    pub async fn latest_bundle_backup(
        &self,
        namespace: Option<&str>,
        service: &str,
    ) -> Result<reqwest::Response, Box<dyn Error + Send + Sync>> {
        let ns = namespace.filter(|s| !s.is_empty()).unwrap_or("default");
        let url = self.url_from_segments(&[
            "apis",
            "nanocloud.io",
            "v1",
            "namespaces",
            ns,
            "bundles",
            service,
            "backups",
            "latest",
        ])?;
        let response = self.apply_auth(self.client.get(url)).send().await?;
        if response.status().is_success() {
            Ok(response)
        } else {
            let status = response.status();
            let text = response.text().await.unwrap_or_default();
            let message = if text.is_empty() {
                status
                    .canonical_reason()
                    .unwrap_or("request failed")
                    .to_string()
            } else {
                text
            };
            Err(Box::new(HttpError::new(status, message)))
        }
    }

    pub async fn export_bundle_profile(
        &self,
        namespace: Option<&str>,
        service: &str,
        options: BundleExportOptions,
    ) -> Result<Vec<u8>, Box<dyn Error + Send + Sync>> {
        let owned_segments = Self::bundle_export_segments(namespace, service);
        let segment_refs: Vec<&str> = owned_segments
            .iter()
            .map(|segment| segment.as_str())
            .collect();
        let mut url = self.url_from_segments(&segment_refs)?;
        if options.include_secrets {
            url.query_pairs_mut()
                .append_pair("includeSecrets", Self::bool_query_value(true));
        }
        let response = self.apply_auth(self.client.post(url)).send().await?;
        if response.status().is_success() {
            let body = response.bytes().await?;
            return Ok(body.to_vec());
        }
        let status = response.status();
        let message = response.text().await.unwrap_or_else(|_| "".to_string());
        if message.is_empty() {
            Err(Box::new(HttpError::new(
                status,
                "failed to export bundle profile",
            )))
        } else {
            Err(Box::new(HttpError::new(status, message)))
        }
    }

    pub async fn network_policy_debug(
        &self,
    ) -> Result<NetworkPolicyDebugResponse, Box<dyn Error + Send + Sync>> {
        let url = self.url_from_segments(&["v1", "networkpolicies", "debug"])?;
        let request = self.client.get(url);
        self.send_json(request).await
    }

    pub async fn logs_stream(
        &self,
        namespace: Option<&str>,
        service: &str,
        follow: bool,
    ) -> Result<reqwest::Response, Box<dyn Error + Send + Sync>> {
        let segments = Self::logs_segments(namespace, service);
        let url = self.url_from_segments(&segments)?;
        let mut request = self.client.get(url);
        if follow {
            request = request
                .query(&[("follow", follow)])
                .timeout(Duration::from_secs(60 * 60 * 24 * 365));
        }
        let response = self.apply_auth(request).send().await?;
        if response.status().is_success() {
            Ok(response)
        } else {
            let status = response.status();
            let text = response.text().await.unwrap_or_default();
            let message = if text.is_empty() {
                status
                    .canonical_reason()
                    .unwrap_or("request failed")
                    .to_string()
            } else {
                text
            };
            Err(Box::new(HttpError::new(status, message)))
        }
    }

    pub async fn get_pod(
        &self,
        namespace: Option<&str>,
        pod: &str,
    ) -> Result<Option<Pod>, Box<dyn Error + Send + Sync>> {
        let segments = Self::pod_segments(namespace, pod);
        let url = self.url_from_segments(&segments)?;
        let response = self.apply_auth(self.client.get(url)).send().await?;
        if response.status() == StatusCode::NOT_FOUND {
            return Ok(None);
        }
        let pod = self.handle_json(response).await?;
        Ok(Some(pod))
    }

    pub async fn watch_pod(
        &self,
        namespace: Option<&str>,
        pod: &str,
        timeout_seconds: Option<u64>,
        resource_version: Option<&str>,
    ) -> Result<reqwest::Response, Box<dyn Error + Send + Sync>> {
        let segments = Self::pod_collection_segments(namespace);
        let url = self.url_from_segments(&segments)?;

        let mut query: Vec<(&str, String)> = Vec::new();
        query.push(("watch", "true".to_string()));
        query.push(("fieldSelector", format!("metadata.name={pod}")));
        if let Some(timeout) = timeout_seconds {
            query.push(("timeoutSeconds", timeout.to_string()));
        }
        if let Some(rv) = resource_version {
            query.push(("resourceVersion", rv.to_string()));
        }

        let query_pairs: Vec<(&str, &str)> = query.iter().map(|(k, v)| (*k, v.as_str())).collect();
        let request = self.client.get(url).query(&query_pairs);
        let response = self.apply_auth(request).send().await?;
        if response.status().is_success() {
            Ok(response)
        } else {
            let status = response.status();
            let text = response.text().await.unwrap_or_default();
            let message = if text.is_empty() {
                status
                    .canonical_reason()
                    .unwrap_or("request failed")
                    .to_string()
            } else {
                text
            };
            Err(Box::new(HttpError::new(status, message)))
        }
    }

    pub async fn list_events(
        &self,
        namespace: Option<&str>,
        query: &EventQuery,
    ) -> Result<EventList, Box<dyn Error + Send + Sync>> {
        let segments = Self::event_collection_segments(namespace);
        let url = self.url_from_segments(&segments)?;
        let mut params: Vec<(String, String)> = Vec::new();

        if let Some(limit) = query.limit {
            if limit > 0 {
                params.push(("limit".to_string(), limit.to_string()));
            }
        }

        if let Some(since) = query
            .since
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
        {
            params.push(("since".to_string(), since.to_string()));
        }

        if let Some(level) = query.level {
            params.push(("level".to_string(), level.as_str().to_string()));
        }

        if !query.reasons.is_empty() {
            let joined = query
                .reasons
                .iter()
                .map(|value| value.trim())
                .filter(|value| !value.is_empty())
                .collect::<Vec<_>>()
                .join(",");
            if !joined.is_empty() {
                params.push(("reason".to_string(), joined));
            }
        }

        if let Some(selector) = query.build_field_selector() {
            params.push(("fieldSelector".to_string(), selector));
        }

        let request = if params.is_empty() {
            self.client.get(url)
        } else {
            let pairs: Vec<(&str, &str)> = params
                .iter()
                .map(|(k, v)| (k.as_str(), v.as_str()))
                .collect();
            self.client.get(url).query(&pairs)
        };

        let response = self.apply_auth(request).send().await?;
        self.handle_json(response).await
    }

    pub async fn watch_events(
        &self,
        namespace: Option<&str>,
        query: &EventQuery,
    ) -> Result<reqwest::Response, Box<dyn Error + Send + Sync>> {
        let segments = Self::event_collection_segments(namespace);
        let url = self.url_from_segments(&segments)?;

        let mut params: Vec<(String, String)> = Vec::new();
        params.push(("watch".to_string(), "true".to_string()));
        if let Some(selector) = query.build_field_selector() {
            params.push(("fieldSelector".to_string(), selector));
        }

        if let Some(limit) = query.limit {
            if limit > 0 {
                params.push(("limit".to_string(), limit.to_string()));
            }
        }

        if let Some(since) = query
            .since
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
        {
            params.push(("since".to_string(), since.to_string()));
        }

        if let Some(rv) = query.resource_version.as_deref() {
            params.push(("resourceVersion".to_string(), rv.to_string()));
            params.push((
                "resourceVersionMatch".to_string(),
                "NotOlderThan".to_string(),
            ));
        }

        params.push(("allowWatchBookmarks".to_string(), "true".to_string()));
        let timeout = query.timeout_seconds.unwrap_or(30);
        params.push(("timeoutSeconds".to_string(), timeout.to_string()));

        let pairs: Vec<(&str, &str)> = params
            .iter()
            .map(|(k, v)| (k.as_str(), v.as_str()))
            .collect();
        let request = self.client.get(url).query(&pairs);
        let response = self.apply_auth(request).send().await?;
        if response.status().is_success() {
            Ok(response)
        } else {
            let status = response.status();
            let text = response.text().await.unwrap_or_default();
            let message = if text.is_empty() {
                status
                    .canonical_reason()
                    .unwrap_or("watch request failed")
                    .to_string()
            } else {
                text
            };
            Err(Box::new(HttpError::new(status, message)))
        }
    }
}

fn select_endpoint(host: Option<&str>) -> Result<String, Box<dyn Error + Send + Sync>> {
    if let Ok(value) = env::var(SERVER_ENV) {
        let trimmed = value.trim();
        if !trimmed.is_empty() {
            return Ok(trimmed.to_string());
        }
    }

    if let Some(host) = host.map(str::trim) {
        if !host.is_empty() {
            return Ok(normalize_host_to_url(host));
        }
    }

    Ok(DEFAULT_SERVER_ENDPOINT.to_string())
}

fn normalize_host_to_url(host: &str) -> String {
    if host.starts_with("http://") || host.starts_with("https://") {
        host.to_string()
    } else {
        format!("https://{}", host)
    }
}

#[derive(Clone, Debug, Default)]
pub struct EventQuery {
    pub bundle: Option<String>,
    pub limit: Option<u32>,
    pub since: Option<String>,
    pub resource_version: Option<String>,
    pub timeout_seconds: Option<u64>,
    pub level: Option<EventLevel>,
    pub reasons: Vec<String>,
}

impl EventQuery {
    fn build_field_selector(&self) -> Option<String> {
        let mut selectors: Vec<String> = Vec::new();
        if let Some(bundle) = self
            .bundle
            .as_ref()
            .map(|value| value.trim())
            .filter(|v| !v.is_empty())
        {
            selectors.push(format!("involvedObject.name={bundle}"));
        }
        if selectors.is_empty() {
            None
        } else {
            Some(selectors.join(","))
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub enum EventLevel {
    Normal,
    Warning,
}

impl EventLevel {
    fn as_str(&self) -> &'static str {
        match self {
            EventLevel::Normal => "Normal",
            EventLevel::Warning => "Warning",
        }
    }
}
