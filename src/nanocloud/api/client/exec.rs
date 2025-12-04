//! Exec transport and terminal helpers for `NanocloudClient`.

use super::NanocloudClient;
use futures_util::stream::SelectAll;
use futures_util::{SinkExt, StreamExt};
use libc;
use nix::unistd::isatty;
use std::error::Error;
use std::io;
use std::os::fd::{AsRawFd, BorrowedFd, FromRawFd, OwnedFd, RawFd};
use std::pin::Pin;
use std::sync::Arc;
use tokio::io::unix::AsyncFd;
use tokio::io::DuplexStream;
use tokio::io::Interest;
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::signal::unix::{signal, SignalKind};
use tokio::sync::{mpsc, oneshot, Notify};
use tokio::task::JoinHandle;
use tokio_openssl::SslStream;
use tokio_stream::wrappers::ReceiverStream;
use tokio_tungstenite::client_async;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::error::ProtocolError;
use tokio_tungstenite::tungstenite::http::header::{HeaderValue, SEC_WEBSOCKET_PROTOCOL};
use tokio_tungstenite::tungstenite::http::HeaderMap;
use tokio_tungstenite::tungstenite::Error as WsError;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::WebSocketStream;

const EXEC_PROTOCOL_PREFERENCE: [&str; 2] = ["v5.channel.k8s.io", "v4.channel.k8s.io"];
const EXEC_PROTOCOL_HEADER_VALUE: &str = "v5.channel.k8s.io, v4.channel.k8s.io";
const STDIN_BUFFER: usize = 16;
const RESIZE_BUFFER: usize = 16;
const CONTROL_BUFFER: usize = 8;
const STDOUT_BUFFER: usize = 32;
const STDERR_BUFFER: usize = 32;
const STATUS_BUFFER: usize = 8;
const CLOSE_BUFFER: usize = 1;
pub const CHANNEL_STDIN: u8 = 0;
pub const CHANNEL_STDOUT: u8 = 1;
pub const CHANNEL_STDERR: u8 = 2;
pub const CHANNEL_STATUS: u8 = 3;
pub const CHANNEL_RESIZE: u8 = 4;
pub const CHANNEL_CLOSE: u8 = 255;

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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ExecProtocol {
    V5,
    V4,
}

#[derive(Debug, Default)]
struct ExecExitInfo {
    code: Option<i32>,
    message: Option<String>,
    reason: Option<String>,
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

impl NanocloudClient {
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

        let session_done_for_error = Arc::clone(&session_done);
        let error_task = {
            let errors_rx = multiplexer.take_errors();
            async move {
                if let Some(mut rx) = errors_rx {
                    if let Some(err) = rx.recv().await {
                        session_done_for_error.notify_waiters();
                        return Err(err);
                    }
                }
                Ok::<(), io::Error>(())
            }
        };

        tokio::try_join!(
            stdin_task,
            stdout_task,
            stderr_task,
            close_task,
            resize_task,
            status_task,
            error_task
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

    fn build_exec_url(
        &self,
        options: &ExecRequest,
    ) -> Result<reqwest::Url, Box<dyn Error + Send + Sync>> {
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

    async fn open_exec_websocket(
        &self,
        url: &reqwest::Url,
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

        let connector = self.exec_tls_connector()?;
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
}

#[allow(dead_code)]
pub(super) fn extract_exec_protocol_label(
    headers: &HeaderMap,
) -> Result<&'static str, Box<dyn Error + Send + Sync>> {
    match NanocloudClient::extract_exec_protocol(headers)? {
        ExecProtocol::V5 => Ok("v5"),
        ExecProtocol::V4 => Ok("v4"),
    }
}

#[derive(Debug)]
enum WriterCommand {
    Raw(Message),
}

pub(super) struct ExecChannelMultiplexer {
    stdin_tx: mpsc::Sender<Vec<u8>>,
    resize_tx: mpsc::Sender<Vec<u8>>,
    stdout_rx: Option<mpsc::Receiver<Vec<u8>>>,
    stderr_rx: Option<mpsc::Receiver<Vec<u8>>>,
    status_rx: Option<mpsc::Receiver<Vec<u8>>>,
    close_rx: Option<mpsc::Receiver<()>>,
    errors_rx: Option<mpsc::Receiver<io::Error>>,
    control_tx: mpsc::Sender<WriterCommand>,
    writer_handle: JoinHandle<()>,
    reader_handle: JoinHandle<()>,
}

impl ExecChannelMultiplexer {
    fn new(stream: ExecWebSocketStream) -> Self {
        let (sink, stream) = stream.split();

        let (stdin_tx, stdin_rx) = mpsc::channel::<Vec<u8>>(STDIN_BUFFER);
        let (resize_tx, resize_rx) = mpsc::channel::<Vec<u8>>(RESIZE_BUFFER);
        let (control_tx, control_rx) = mpsc::channel::<WriterCommand>(CONTROL_BUFFER);
        let control_for_reader = control_tx.clone();

        let (stdout_tx, stdout_rx) = mpsc::channel::<Vec<u8>>(STDOUT_BUFFER);
        let (stderr_tx, stderr_rx) = mpsc::channel::<Vec<u8>>(STDERR_BUFFER);
        let (status_tx, status_rx) = mpsc::channel::<Vec<u8>>(STATUS_BUFFER);
        let (close_tx, close_rx) = mpsc::channel::<()>(CLOSE_BUFFER);
        let (error_tx, errors_rx) = mpsc::channel::<io::Error>(4);

        let error_tx_for_writer = error_tx.clone();
        let writer_handle = tokio::spawn(async move {
            Self::run_writer(sink, stdin_rx, resize_rx, control_rx, error_tx_for_writer).await;
        });

        let reader_handle = tokio::spawn(async move {
            Self::run_reader(
                stream,
                stdout_tx,
                stderr_tx,
                status_tx,
                close_tx,
                control_for_reader,
                error_tx,
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
            errors_rx: Some(errors_rx),
            control_tx,
            writer_handle,
            reader_handle,
        }
    }

    #[allow(dead_code)]
    pub(super) fn new_for_tests(stream: WebSocketStream<DuplexStream>) -> Self {
        let (sink, stream) = stream.split();

        let (stdin_tx, stdin_rx) = mpsc::channel::<Vec<u8>>(STDIN_BUFFER);
        let (resize_tx, resize_rx) = mpsc::channel::<Vec<u8>>(RESIZE_BUFFER);
        let (control_tx, control_rx) = mpsc::channel::<WriterCommand>(CONTROL_BUFFER);
        let control_for_reader = control_tx.clone();

        let (stdout_tx, stdout_rx) = mpsc::channel::<Vec<u8>>(STDOUT_BUFFER);
        let (stderr_tx, stderr_rx) = mpsc::channel::<Vec<u8>>(STDERR_BUFFER);
        let (status_tx, status_rx) = mpsc::channel::<Vec<u8>>(STATUS_BUFFER);
        let (close_tx, close_rx) = mpsc::channel::<()>(CLOSE_BUFFER);
        let (error_tx, errors_rx) = mpsc::channel::<io::Error>(4);

        let error_tx_for_writer = error_tx.clone();
        let writer_handle = tokio::spawn(async move {
            Self::run_writer(sink, stdin_rx, resize_rx, control_rx, error_tx_for_writer).await;
        });

        let reader_handle = tokio::spawn(async move {
            Self::run_reader(
                stream,
                stdout_tx,
                stderr_tx,
                status_tx,
                close_tx,
                control_for_reader,
                error_tx,
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
            errors_rx: Some(errors_rx),
            control_tx,
            writer_handle,
            reader_handle,
        }
    }

    pub(super) fn stdin_sender(&self) -> mpsc::Sender<Vec<u8>> {
        self.stdin_tx.clone()
    }

    pub(super) fn resize_sender(&self) -> mpsc::Sender<Vec<u8>> {
        self.resize_tx.clone()
    }

    pub(super) fn take_stdout(&mut self) -> Option<mpsc::Receiver<Vec<u8>>> {
        self.stdout_rx.take()
    }

    pub(super) fn take_stderr(&mut self) -> Option<mpsc::Receiver<Vec<u8>>> {
        self.stderr_rx.take()
    }

    pub(super) fn take_status(&mut self) -> Option<mpsc::Receiver<Vec<u8>>> {
        self.status_rx.take()
    }

    pub(super) fn take_close(&mut self) -> Option<mpsc::Receiver<()>> {
        self.close_rx.take()
    }

    pub(super) fn take_errors(&mut self) -> Option<mpsc::Receiver<io::Error>> {
        self.errors_rx.take()
    }

    pub(super) async fn shutdown(self) {
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
        error_tx: mpsc::Sender<io::Error>,
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
                            let result = send_channel_frame(&mut sink, CHANNEL_STDIN, data).await;
                            if handle_send_result("stdin", result, &error_tx) {
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
                            let result = send_channel_frame(&mut sink, CHANNEL_RESIZE, data).await;
                            if handle_send_result("resize", result, &error_tx) {
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
                            let result = sink.send(message).await;
                            if handle_send_result("control", result, &error_tx) {
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
        error_tx: mpsc::Sender<io::Error>,
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
                        unexpected => {
                            record_protocol_error(
                                &error_tx,
                                format!("received unexpected exec channel {unexpected}"),
                            );
                            break;
                        }
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
                Ok(Message::Text(_)) => {
                    record_protocol_error(&error_tx, "received unexpected text exec frame");
                    break;
                }
                Ok(Message::Frame(_)) => {}
                Err(err) => {
                    record_protocol_error(&error_tx, format!("exec websocket error: {err}"));
                    break;
                }
            }
        }
    }
}

fn is_send_after_close_error(err: &WsError) -> bool {
    matches!(err, WsError::Protocol(ProtocolError::SendAfterClosing))
}

/// Returns `true` when the writer loop should stop after attempting a send.
fn handle_send_result(
    channel: &str,
    result: Result<(), WsError>,
    error_tx: &mpsc::Sender<io::Error>,
) -> bool {
    match result {
        Ok(()) => false,
        Err(err) if is_send_after_close_error(&err) => true,
        Err(err) => {
            record_channel_error(error_tx, channel, err);
            true
        }
    }
}

fn record_channel_error<E: std::fmt::Display>(tx: &mpsc::Sender<io::Error>, channel: &str, err: E) {
    let _ = tx.try_send(io::Error::other(format!(
        "exec {channel} channel error: {err}"
    )));
}

fn record_protocol_error(tx: &mpsc::Sender<io::Error>, message: impl Into<String>) {
    let _ = tx.try_send(io::Error::other(message.into()));
}

async fn send_channel_frame<S>(
    sink: &mut futures_util::stream::SplitSink<WebSocketStream<S>, Message>,
    channel: u8,
    data: Vec<u8>,
) -> Result<(), WsError>
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

/// Returns whether the provided raw file descriptor refers to a terminal.
/// Caller must ensure `fd` is a valid, open descriptor.
fn is_terminal(fd: RawFd) -> io::Result<bool> {
    unsafe { isatty(BorrowedFd::borrow_raw(fd)) }.map_err(io::Error::from)
}

/// Attempts to read the current terminal size for the given descriptor.
/// Returns `Ok(None)` when the descriptor is not a TTY.
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
    #[derive(serde::Deserialize)]
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

#[allow(dead_code)]
pub(super) fn parse_status_code(payload: &[u8]) -> io::Result<Option<i32>> {
    parse_status_payload(payload).map(|info| info.code)
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

/// Duplicates stdin as a new owned file descriptor with CLOEXEC set.
fn duplicate_stdin() -> io::Result<OwnedFd> {
    let fd = unsafe { libc::fcntl(libc::STDIN_FILENO, libc::F_DUPFD_CLOEXEC, 0) };
    if fd == -1 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: fcntl returns a new owned file descriptor on success.
    Ok(unsafe { OwnedFd::from_raw_fd(fd) })
}

/// Enables non-blocking mode for the provided descriptor.
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

/// Reads from `fd` in non-blocking mode into `buffer`, retrying on EINTR.
/// Caller must ensure `fd` is valid and already set non-blocking.
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::nanocloud::api::client::auth::{
        exec_ssl_build_count, reset_exec_ssl_build_count, AuthContext, CertificateAuth, ClientTls,
        CurlAuthData, KubeFieldSource,
    };
    use crate::nanocloud::api::client::NanocloudClient;
    use openssl::asn1::Asn1Time;
    use openssl::hash::MessageDigest;
    use openssl::pkey::PKey;
    use openssl::rsa::Rsa;
    use openssl::x509::X509NameBuilder;
    use reqwest::Url;
    use std::path::PathBuf;
    use tokio::io::duplex;
    use tokio::time::{timeout, Duration};
    use tokio_tungstenite::tungstenite::protocol::Role;
    use tokio_tungstenite::WebSocketStream;

    async fn websocket_pair() -> (WebSocketStream<DuplexStream>, WebSocketStream<DuplexStream>) {
        let (a, b) = duplex(1024);
        let client = WebSocketStream::from_raw_socket(a, Role::Client, None).await;
        let server = WebSocketStream::from_raw_socket(b, Role::Server, None).await;
        (client, server)
    }

    fn status_payload(code: i32) -> Vec<u8> {
        let mut frame = vec![CHANNEL_STATUS];
        frame.extend_from_slice(
            serde_json::to_string(&serde_json::json!({ "code": code }))
                .unwrap()
                .as_bytes(),
        );
        frame
    }

    fn build_test_tls() -> ClientTls {
        let rsa = Rsa::generate(2048).expect("rsa");
        let pkey = PKey::from_rsa(rsa).unwrap();

        let mut name = X509NameBuilder::new().unwrap();
        name.append_entry_by_text("CN", "owner").unwrap();
        let name = name.build();

        let mut builder = openssl::x509::X509Builder::new().unwrap();
        builder.set_version(2).unwrap();
        builder.set_subject_name(&name).unwrap();
        builder.set_issuer_name(&name).unwrap();
        builder
            .set_not_before(&Asn1Time::days_from_now(0).unwrap())
            .unwrap();
        builder
            .set_not_after(&Asn1Time::days_from_now(1).unwrap())
            .unwrap();
        builder.set_pubkey(&pkey).unwrap();
        builder.sign(&pkey, MessageDigest::sha256()).unwrap();

        ClientTls {
            client_certificate: builder.build().to_pem().unwrap(),
            client_key: pkey.private_key_to_pem_pkcs8().unwrap(),
            ca_bundle: None,
        }
    }

    fn test_client_with_tls() -> NanocloudClient {
        let tls = build_test_tls();
        let curl = CurlAuthData {
            kubeconfig_path: PathBuf::new(),
            kubeconfig_dir: PathBuf::new(),
            cluster_name: "demo".into(),
            user_name: "demo".into(),
            ca_source: None,
            ca_path: None,
            ca_data: None,
            cert_source: KubeFieldSource::InlineData,
            cert_path: None,
            cert_data: tls.client_certificate.clone(),
            key_source: KubeFieldSource::InlineData,
            key_path: None,
            key_data: tls.client_key.clone(),
        };
        NanocloudClient {
            client: reqwest::Client::new(),
            base_url: Url::parse("https://example.test").unwrap(),
            auth: AuthContext::ClientCertificate(Box::new(CertificateAuth {
                tls,
                owner: "owner".into(),
                curl,
            })),
            exec_connector: Arc::new(std::sync::Mutex::new(None)),
        }
    }

    #[tokio::test]
    async fn multiplexer_routes_stdout_status_and_close() {
        let (client_ws, mut server_ws) = websocket_pair().await;
        let mut mux = ExecChannelMultiplexer::new_for_tests(client_ws);

        let mut stdout = mux.take_stdout().expect("stdout receiver");
        let mut status = mux.take_status().expect("status receiver");
        let mut close = mux.take_close().expect("close receiver");

        server_ws
            .send(Message::Binary(vec![CHANNEL_STDOUT, b'h', b'i'].into()))
            .await
            .unwrap();
        server_ws
            .send(Message::Binary(status_payload(5).into()))
            .await
            .unwrap();
        server_ws.send(Message::Close(None)).await.unwrap();

        let out = stdout.recv().await.expect("stdout data");
        assert_eq!(out, b"hi");
        let payload = status.recv().await.expect("status data");
        let info = parse_status_payload(&payload).unwrap();
        assert_eq!(info.code, Some(5));
        assert!(close.recv().await.is_some());

        mux.shutdown().await;
    }

    #[tokio::test]
    async fn multiplexer_reports_unexpected_channel_error() {
        let (client_ws, mut server_ws) = websocket_pair().await;
        let mut mux = ExecChannelMultiplexer::new_for_tests(client_ws);
        let mut errors = mux.take_errors().expect("errors receiver");

        server_ws
            .send(Message::Binary(vec![99, 0x01, 0x02].into()))
            .await
            .unwrap();

        let err = errors.recv().await.expect("error message");
        assert!(
            err.to_string().contains("unexpected exec channel"),
            "unexpected error text: {err}"
        );

        mux.shutdown().await;
    }

    #[tokio::test]
    async fn multiplexer_treats_close_frame_as_clean_shutdown() {
        let (client_ws, mut server_ws) = websocket_pair().await;
        let mut mux = ExecChannelMultiplexer::new_for_tests(client_ws);

        let mut close = mux.take_close().expect("close receiver");
        let mut errors = mux.take_errors().expect("errors receiver");

        server_ws.send(Message::Close(None)).await.unwrap();

        timeout(Duration::from_secs(1), close.recv())
            .await
            .expect("close signal") // timeout
            .expect("close message");

        if let Ok(Some(err)) = timeout(Duration::from_millis(100), errors.recv()).await {
            panic!("unexpected exec error after websocket close: {err}");
        }

        mux.shutdown().await;
    }

    #[test]
    fn exec_tls_connector_is_cached() {
        reset_exec_ssl_build_count();
        assert_eq!(exec_ssl_build_count(), 0);
        let client = test_client_with_tls();
        let first = client.exec_tls_connector().expect("first connector");
        let second = client.exec_tls_connector().expect("second connector");
        assert!(Arc::ptr_eq(&first, &second));
        assert_eq!(exec_ssl_build_count(), 1);
        let _ = client.exec_tls_connector().unwrap();
        assert_eq!(exec_ssl_build_count(), 1);
    }

    #[test]
    fn cursor_report_filter_strips_reports() {
        let mut filter = CursorReportFilter::new();
        let filtered = filter.consume(b"abc\x1b[12;34Rdef");
        assert_eq!(filtered, b"abcdef");
        let remaining = filter.finish();
        assert!(remaining.is_empty());
    }
}
