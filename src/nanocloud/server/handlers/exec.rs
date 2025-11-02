use std::convert::TryFrom;
use std::fs::File;
use std::io::{self, ErrorKind, Read, Write};
use std::os::fd::{FromRawFd, IntoRawFd, OwnedFd};
use std::os::unix::process::ExitStatusExt;
use std::process::ExitStatus;
use std::sync::Arc;
use std::thread;

use axum::body::Body;
use axum::extract::ws::{
    rejection::WebSocketUpgradeRejection, CloseFrame, Message, Utf8Bytes, WebSocket,
    WebSocketUpgrade,
};
use axum::extract::{FromRequestParts, Path, Query};
use axum::http::header::{HeaderValue, SEC_WEBSOCKET_PROTOCOL};
use axum::http::{HeaderMap, Method, Request, Response, StatusCode, Version};
use nix::pty::{openpty, Winsize};
use nix::unistd;
use serde_json::{json, Map, Value};
use tokio::runtime::Handle;
use tokio::sync::mpsc::{self, UnboundedReceiver, UnboundedSender};
use tokio::task;

use crate::nanocloud::engine::container::resolve_container_id;
use crate::nanocloud::logger::{log_debug, log_error, log_info, log_warn};
use crate::nanocloud::observability::metrics::{
    record_exec_handshake_failure, ExecHandshakeFailure, ExecTransport,
};
use crate::nanocloud::oci::container_runtime;
use crate::nanocloud::oci::runtime::{ExecRequest, ExecResult};

#[cfg(feature = "openapi")]
use crate::nanocloud::api::types::ErrorBody;

use super::error::ApiError;
use super::exec_common::{
    parse_resize_payload, validate_query, ExecOptions, RawExecQuery, ResizeEvent, CHANNEL_CLOSE,
    CHANNEL_RESIZE, CHANNEL_STATUS, CHANNEL_STDERR, CHANNEL_STDIN, CHANNEL_STDOUT,
};

const COMPONENT: &str = "exec";
const SUPPORTED_SUBPROTOCOLS: &[&str] =
    &["v5.channel.k8s.io", "v4.channel.k8s.io", "channel.k8s.io"];

#[derive(Clone)]
struct SessionContext {
    namespace: Arc<String>,
    pod: Arc<String>,
    container: Arc<String>,
    protocol: &'static str,
    command: Arc<String>,
}

impl SessionContext {
    fn new(options: &ExecOptions, protocol: &'static str) -> Self {
        let container = options
            .container
            .clone()
            .unwrap_or_else(|| "(pod default)".to_string());
        let command = if options.command.is_empty() {
            "<none>".to_string()
        } else {
            options.command.join(" ")
        };
        SessionContext {
            namespace: Arc::new(options.namespace.clone()),
            pod: Arc::new(options.pod.clone()),
            container: Arc::new(container),
            protocol,
            command: Arc::new(command),
        }
    }

    fn metadata<'a>(&'a self, extra: &[(&'a str, &'a str)]) -> Vec<(&'a str, &'a str)> {
        let mut fields = Vec::with_capacity(4 + extra.len());
        fields.push(("namespace", self.namespace.as_str()));
        fields.push(("pod", self.pod.as_str()));
        fields.push(("container", self.container.as_str()));
        fields.push(("protocol", self.protocol));
        fields.extend_from_slice(extra);
        fields
    }

    fn command(&self) -> &str {
        self.command.as_str()
    }
}

struct ExecSession {
    options: ExecOptions,
    context: Arc<SessionContext>,
}

#[cfg_attr(feature = "openapi", utoipa::path(
    get,
    path = "/api/v1/namespaces/{namespace}/pods/{name}/exec",
    params(
        ("namespace" = String, Path, description = "Namespace of the target pod"),
        ("name" = String, Path, description = "Pod name"),
        ("container" = Option<String>, Query, description = "Target container in the pod"),
        ("stdin" = Option<bool>, Query, description = "Enable stdin stream"),
        ("stdout" = Option<bool>, Query, description = "Include stdout stream"),
        ("stderr" = Option<bool>, Query, description = "Include stderr stream"),
        ("tty" = Option<bool>, Query, description = "Request a pseudo-terminal")
    ),
    responses(
        (status = 101, description = "Switching protocols"),
        (status = 400, description = "Invalid exec request", body = ErrorBody),
        (status = 404, description = "Pod not found", body = ErrorBody),
        (status = 500, description = "Internal error", body = ErrorBody)
    ),
    security(
        ("NanocloudCertificate" = []),
        ("NanocloudBearer" = ["pods.exec"])
    ),
    tag = "nanocloud"
))]
pub(crate) async fn exec_ws_namespaced(
    Path((namespace, pod)): Path<(String, String)>,
    Query(mut raw): Query<RawExecQuery>,
    ws: WebSocketUpgrade,
    headers: HeaderMap,
) -> Result<Response<Body>, ApiError> {
    raw.namespace = namespace;
    raw.pod = pod;
    exec_websocket_impl(raw, ws, headers).await
}

#[cfg_attr(feature = "openapi", utoipa::path(
    get,
    path = "/api/v1/pods/{name}/exec",
    params(
        ("name" = String, Path, description = "Pod name"),
        ("namespace" = Option<String>, Query, description = "Namespace of the target pod (defaults to \"default\")"),
        ("container" = Option<String>, Query, description = "Target container in the pod"),
        ("stdin" = Option<bool>, Query, description = "Enable stdin stream"),
        ("stdout" = Option<bool>, Query, description = "Include stdout stream"),
        ("stderr" = Option<bool>, Query, description = "Include stderr stream"),
        ("tty" = Option<bool>, Query, description = "Request a pseudo-terminal")
    ),
    responses(
        (status = 101, description = "Switching protocols"),
        (status = 400, description = "Invalid exec request", body = ErrorBody),
        (status = 404, description = "Pod not found", body = ErrorBody),
        (status = 500, description = "Internal error", body = ErrorBody)
    ),
    security(
        ("NanocloudCertificate" = []),
        ("NanocloudBearer" = ["pods.exec"])
    ),
    tag = "nanocloud"
))]
pub(crate) async fn exec_ws_cluster(
    Path(pod): Path<String>,
    Query(mut raw): Query<RawExecQuery>,
    ws: WebSocketUpgrade,
    headers: HeaderMap,
) -> Result<Response<Body>, ApiError> {
    if raw.namespace.trim().is_empty() {
        raw.namespace = "default".into();
    }
    raw.pod = pod;
    exec_websocket_impl(raw, ws, headers).await
}

#[cfg_attr(feature = "openapi", utoipa::path(
    post,
    path = "/api/v1/namespaces/{namespace}/pods/{name}/exec",
    params(
        ("namespace" = String, Path, description = "Namespace of the target pod"),
        ("name" = String, Path, description = "Pod name"),
        ("container" = Option<String>, Query, description = "Target container in the pod"),
        ("stdin" = Option<bool>, Query, description = "Enable stdin stream"),
        ("stdout" = Option<bool>, Query, description = "Include stdout stream"),
        ("stderr" = Option<bool>, Query, description = "Include stderr stream"),
        ("tty" = Option<bool>, Query, description = "Request a pseudo-terminal")
    ),
    responses(
        (status = 101, description = "Switching protocols"),
        (status = 400, description = "Invalid exec request", body = ErrorBody),
        (status = 404, description = "Pod not found", body = ErrorBody),
        (status = 500, description = "Internal error", body = ErrorBody)
    ),
    security(
        ("NanocloudCertificate" = []),
        ("NanocloudBearer" = ["pods.exec"])
    ),
    tag = "nanocloud"
))]
pub(crate) async fn exec_http_post_namespaced(
    Path((namespace, pod)): Path<(String, String)>,
    Query(mut raw): Query<RawExecQuery>,
    req: Request<Body>,
) -> Result<Response<Body>, ApiError> {
    raw.namespace = namespace;
    raw.pod = pod;
    exec_http_post_impl(raw, req).await
}

#[cfg_attr(feature = "openapi", utoipa::path(
    post,
    path = "/api/v1/pods/{name}/exec",
    params(
        ("name" = String, Path, description = "Pod name"),
        ("namespace" = Option<String>, Query, description = "Namespace of the target pod (defaults to \"default\")"),
        ("container" = Option<String>, Query, description = "Target container in the pod"),
        ("stdin" = Option<bool>, Query, description = "Enable stdin stream"),
        ("stdout" = Option<bool>, Query, description = "Include stdout stream"),
        ("stderr" = Option<bool>, Query, description = "Include stderr stream"),
        ("tty" = Option<bool>, Query, description = "Request a pseudo-terminal")
    ),
    responses(
        (status = 101, description = "Switching protocols"),
        (status = 400, description = "Invalid exec request", body = ErrorBody),
        (status = 404, description = "Pod not found", body = ErrorBody),
        (status = 500, description = "Internal error", body = ErrorBody)
    ),
    security(
        ("NanocloudCertificate" = []),
        ("NanocloudBearer" = ["pods.exec"])
    ),
    tag = "nanocloud"
))]
pub(crate) async fn exec_http_post_cluster(
    Path(pod): Path<String>,
    Query(mut raw): Query<RawExecQuery>,
    req: Request<Body>,
) -> Result<Response<Body>, ApiError> {
    if raw.namespace.trim().is_empty() {
        raw.namespace = "default".into();
    }
    raw.pod = pod;
    exec_http_post_impl(raw, req).await
}

async fn exec_websocket_impl(
    raw: RawExecQuery,
    ws: WebSocketUpgrade,
    headers: HeaderMap,
) -> Result<Response<Body>, ApiError> {
    let options = validate_query(raw)?;
    let protocol = select_subprotocol(&headers)?;
    let context = Arc::new(SessionContext::new(&options, protocol));
    let metadata = context.metadata(&[("command", context.command())]);
    log_info(COMPONENT, "accepted exec WebSocket upgrade", &metadata);

    let mut response = ws.protocols([protocol]).on_upgrade(move |socket| {
        let session = ExecSession {
            options,
            context: Arc::clone(&context),
        };
        handle_exec_session(socket, session)
    });

    apply_stream_protocol_header(response.headers_mut(), protocol);

    Ok(response)
}

async fn exec_http_post_impl(
    raw: RawExecQuery,
    req: Request<Body>,
) -> Result<Response<Body>, ApiError> {
    if req.version() != Version::HTTP_11 {
        record_exec_handshake_failure(ExecTransport::WebSocket, ExecHandshakeFailure::HttpVersion);
        return Err(ApiError::new(
            StatusCode::HTTP_VERSION_NOT_SUPPORTED,
            "exec WebSocket upgrade requires HTTP/1.1",
        ));
    }

    let options = validate_query(raw)?;
    let (mut parts, _) = req.into_parts();
    let protocol = select_subprotocol(&parts.headers)?;
    let context = Arc::new(SessionContext::new(&options, protocol));
    let metadata = context.metadata(&[("command", context.command())]);
    log_info(COMPONENT, "accepted exec HTTP/1.1 upgrade", &metadata);
    let original_method = parts.method.clone();
    parts.method = axum::http::Method::GET;

    let upgrade = match WebSocketUpgrade::from_request_parts(&mut parts, &()).await {
        Ok(upgrade) => upgrade,
        Err(err) => return Err(map_websocket_rejection(err, original_method)),
    };

    let mut response = upgrade.protocols([protocol]).on_upgrade(move |socket| {
        let session = ExecSession {
            options,
            context: Arc::clone(&context),
        };
        handle_exec_session(socket, session)
    });

    apply_stream_protocol_header(response.headers_mut(), protocol);

    Ok(response)
}

fn apply_stream_protocol_header(headers: &mut HeaderMap, protocol: &str) {
    if let Ok(value) = HeaderValue::from_str(protocol) {
        headers.insert("X-Stream-Protocol-Version", value);
    }
}

fn select_subprotocol(headers: &HeaderMap) -> Result<&'static str, ApiError> {
    let raw = headers
        .get(SEC_WEBSOCKET_PROTOCOL)
        .and_then(|value| value.to_str().ok())
        .map(|value| value.trim())
        .filter(|value| !value.is_empty());
    let Some(raw) = raw else {
        record_exec_handshake_failure(
            ExecTransport::WebSocket,
            ExecHandshakeFailure::MissingProtocol,
        );
        return Err(ApiError::new(
            StatusCode::BAD_REQUEST,
            "client must include Sec-WebSocket-Protocol header with a supported channel.k8s.io protocol",
        ));
    };

    let offered: Vec<&str> = raw
        .split(',')
        .map(|token| token.trim())
        .filter(|token| !token.is_empty())
        .collect();
    for candidate in SUPPORTED_SUBPROTOCOLS {
        if offered
            .iter()
            .any(|token| token.eq_ignore_ascii_case(candidate))
        {
            return Ok(candidate);
        }
    }

    record_exec_handshake_failure(
        ExecTransport::WebSocket,
        ExecHandshakeFailure::MissingProtocol,
    );
    let requested = if offered.is_empty() {
        "none".to_string()
    } else {
        offered.join(", ")
    };
    Err(ApiError::new(
        StatusCode::BAD_REQUEST,
        format!(
            "none of the requested Sec-WebSocket-Protocol values are supported: {}",
            requested
        ),
    ))
}

fn map_websocket_rejection(
    rejection: WebSocketUpgradeRejection,
    original_method: Method,
) -> ApiError {
    match rejection {
        WebSocketUpgradeRejection::MethodNotGet(_) => {
            record_exec_handshake_failure(
                ExecTransport::WebSocket,
                ExecHandshakeFailure::MissingProtocol,
            );
            ApiError::new(
                StatusCode::METHOD_NOT_ALLOWED,
                format!(
                    "exec WebSocket upgrade requires GET; received {}",
                    original_method
                ),
            )
        }
        WebSocketUpgradeRejection::InvalidConnectionHeader(_) => {
            record_exec_handshake_failure(
                ExecTransport::WebSocket,
                ExecHandshakeFailure::MissingProtocol,
            );
            ApiError::new(
                StatusCode::UPGRADE_REQUIRED,
                "exec requires Connection: Upgrade",
            )
        }
        WebSocketUpgradeRejection::InvalidUpgradeHeader(_) => {
            record_exec_handshake_failure(
                ExecTransport::WebSocket,
                ExecHandshakeFailure::MissingProtocol,
            );
            ApiError::new(
                StatusCode::UPGRADE_REQUIRED,
                "exec requires Upgrade: websocket",
            )
        }
        WebSocketUpgradeRejection::InvalidWebSocketVersionHeader(_) => {
            record_exec_handshake_failure(
                ExecTransport::WebSocket,
                ExecHandshakeFailure::MissingProtocol,
            );
            ApiError::new(
                StatusCode::UPGRADE_REQUIRED,
                "exec requires Sec-WebSocket-Version: 13",
            )
        }
        WebSocketUpgradeRejection::WebSocketKeyHeaderMissing(_) => {
            record_exec_handshake_failure(
                ExecTransport::WebSocket,
                ExecHandshakeFailure::MissingProtocol,
            );
            ApiError::new(StatusCode::BAD_REQUEST, "missing Sec-WebSocket-Key header")
        }
        WebSocketUpgradeRejection::ConnectionNotUpgradable(_) => {
            record_exec_handshake_failure(
                ExecTransport::WebSocket,
                ExecHandshakeFailure::HttpVersion,
            );
            ApiError::new(
                StatusCode::UPGRADE_REQUIRED,
                "connection is not upgradable to WebSocket",
            )
        }
        _ => {
            record_exec_handshake_failure(
                ExecTransport::WebSocket,
                ExecHandshakeFailure::MissingProtocol,
            );
            ApiError::internal_message("websocket upgrade failed")
        }
    }
}

async fn handle_exec_session(socket: WebSocket, session: ExecSession) {
    let ExecSession { options, context } = session;
    let (command_tx, command_rx) = mpsc::unbounded_channel();
    let (event_tx, mut event_rx) = mpsc::unbounded_channel();

    let process_options = options.clone();
    let process_context = Arc::clone(&context);
    let runtime = Handle::current();
    let process_task = task::spawn_blocking(move || {
        run_exec_process(
            process_options,
            command_rx,
            event_tx,
            process_context,
            runtime,
        )
    });

    let mut ws = socket;
    let mut status_sent = false;
    let session_metadata = context.metadata(&[("command", context.command())]);
    log_debug(COMPONENT, "exec session started", &session_metadata);

    loop {
        tokio::select! {
            biased;

            Some(event) = event_rx.recv(), if !status_sent => {
                match event {
                    ProcessEvent::Stdout(data) => {
                        if data.is_empty() {
                            continue;
                        }
                        let frame = build_channel_frame(CHANNEL_STDOUT, &data);
                        if ws.send(Message::Binary(frame.into())).await.is_err() {
                            let metadata = context.metadata(&[("stream", "stdout")]);
                            log_warn(
                                COMPONENT,
                                "failed to forward stdout to client",
                                &metadata,
                            );
                            break;
                        }
                    }
                    ProcessEvent::Stderr(data) => {
                        if data.is_empty() {
                            continue;
                        }
                        let channel = if options.tty {
                            CHANNEL_STDOUT
                        } else {
                            CHANNEL_STDERR
                        };
                        let frame = build_channel_frame(channel, &data);
                        let stream = if options.tty { "stdout" } else { "stderr" };
                        if ws.send(Message::Binary(frame.into())).await.is_err() {
                            let metadata = context.metadata(&[("stream", stream)]);
                            log_warn(
                                COMPONENT,
                                "failed to forward stderr to client",
                                &metadata,
                            );
                            break;
                        }
                    }
                    ProcessEvent::Status { code, message, reason } => {
                        let payload = build_status_payload(code, message.clone(), reason.clone());
                        let exit_code = code.to_string();
                        let mut extra: Vec<(&str, &str)> = Vec::new();
                        extra.push(("exit_code", exit_code.as_str()));
                        extra.push(("command", context.command()));
                        if let Some(ref reason_value) = reason {
                            extra.push(("reason", reason_value.as_str()));
                        }
                        if let Some(ref msg) = message {
                            if !msg.is_empty() {
                                extra.push(("message", msg.as_str()));
                            }
                        }
                        let metadata = context.metadata(&extra);
                        if code == 0 {
                            log_debug(COMPONENT, "exec completed successfully", &metadata);
                        } else {
                            log_error(COMPONENT, "exec completed with failure", &metadata);
                        }
                        let send_result = ws.send(Message::Binary(payload.into())).await;
                        status_sent = true;
                        if send_result.is_ok() {
                            send_normal_close(&mut ws).await;
                        } else {
                            log_warn(
                                COMPONENT,
                                "failed to deliver status frame to client",
                                &metadata,
                            );
                        }
                        break;
                    }
                }
            }
            msg = ws.recv() => {
                match msg {
                    Some(Ok(Message::Binary(frame))) => {
                        match decode_client_frame(&options, &frame) {
                            Ok(Some(ProcessCommand::Close)) => {
                                let _ = command_tx.send(ProcessCommand::Close);
                                break;
                            }
                            Ok(Some(cmd)) => {
                                if command_tx.send(cmd).is_err() {
                                    let metadata = context.metadata(&[("event", "command_tx_closed")]);
                                    log_warn(
                                        COMPONENT,
                                        "exec worker channel closed while forwarding command",
                                        &metadata,
                                    );
                                    break;
                                }
                            }
                            Ok(None) => {}
                            Err(err) => {
                                let err_str = err;
                                let metadata = context.metadata(&[("error", err_str.as_str())]);
                                log_warn(
                                    COMPONENT,
                                    "discarding invalid exec frame",
                                    &metadata,
                                );
                            }
                        }
                    }
                    Some(Ok(Message::Close(_))) => {
                        let _ = command_tx.send(ProcessCommand::Close);
                        let metadata = context.metadata(&[("event", "client_close")]);
                        log_debug(COMPONENT, "client closed exec websocket", &metadata);
                        break;
                    }
                    Some(Ok(Message::Ping(payload))) => {
                        if ws.send(Message::Pong(payload)).await.is_err() {
                            let metadata = context.metadata(&[("event", "pong_failed")]);
                            log_warn(
                                COMPONENT,
                                "failed to respond to client ping",
                                &metadata,
                            );
                            break;
                        }
                    }
                    Some(Ok(Message::Pong(_))) => {}
                    Some(Ok(Message::Text(_))) => {}
                    Some(Err(err)) => {
                        let error_string = err.to_string();
                        let metadata = context.metadata(&[("error", error_string.as_str())]);
                        log_warn(
                            COMPONENT,
                            "websocket error while reading frame",
                            &metadata,
                        );
                        let _ = command_tx.send(ProcessCommand::Close);
                        break;
                    }
                    None => {
                        let metadata = context.metadata(&[("event", "client_disconnected")]);
                        log_debug(COMPONENT, "client disconnected", &metadata);
                        let _ = command_tx.send(ProcessCommand::Close);
                        break;
                    }
                }
            }
            else => break,
        }
    }

    let _ = command_tx.send(ProcessCommand::Close);
    drop(command_tx);

    match process_task.await {
        Ok(Ok(())) => {
            if !status_sent {
                let payload = build_status_payload(0, None, None);
                let metadata = context.metadata(&[("exit_code", "0")]);
                if ws.send(Message::Binary(payload.into())).await.is_ok() {
                    log_debug(
                        COMPONENT,
                        "sent implicit success status after process completion",
                        &metadata,
                    );
                    send_normal_close(&mut ws).await;
                } else {
                    log_warn(
                        COMPONENT,
                        "failed to deliver implicit success status",
                        &metadata,
                    );
                }
            }
        }
        Ok(Err(err)) => {
            if !status_sent {
                let reason = err
                    .reason()
                    .map(|value| value.to_string())
                    .unwrap_or_else(|| "InternalError".to_string());
                let message = err.to_string();
                let metadata = context.metadata(&[
                    ("exit_code", "1"),
                    ("reason", reason.as_str()),
                    ("message", message.as_str()),
                ]);
                log_error(COMPONENT, "exec process completed with error", &metadata);
                let payload = build_status_payload(1, Some(message.clone()), Some(reason.clone()));
                if ws.send(Message::Binary(payload.into())).await.is_ok() {
                    send_normal_close(&mut ws).await;
                } else {
                    log_warn(
                        COMPONENT,
                        "failed to deliver exec error status to client",
                        &metadata,
                    );
                }
            }
        }
        Err(err) => {
            if !status_sent {
                let err_message = err.to_string();
                let metadata = context.metadata(&[
                    ("exit_code", "1"),
                    ("reason", "InternalError"),
                    ("message", err_message.as_str()),
                ]);
                log_error(COMPONENT, "exec worker task failed to join", &metadata);
                let payload = build_status_payload(
                    1,
                    Some(err_message.clone()),
                    Some("InternalError".to_string()),
                );
                if ws.send(Message::Binary(payload.into())).await.is_ok() {
                    send_normal_close(&mut ws).await;
                } else {
                    log_warn(
                        COMPONENT,
                        "failed to deliver exec worker panic status to client",
                        &metadata,
                    );
                }
            }
        }
    }
}

async fn send_normal_close(ws: &mut WebSocket) {
    let frame = CloseFrame {
        code: 1000,
        reason: Utf8Bytes::from_static(""),
    };
    let _ = ws.send(Message::Close(Some(frame))).await;
}

fn build_channel_frame(channel: u8, payload: &[u8]) -> Vec<u8> {
    let mut frame = Vec::with_capacity(1 + payload.len());
    frame.push(channel);
    frame.extend_from_slice(payload);
    frame
}

fn build_status_payload(code: i32, message: Option<String>, reason: Option<String>) -> Vec<u8> {
    let status = if code == 0 { "Success" } else { "Failure" };
    let default_message = if code == 0 {
        "".to_string()
    } else {
        "exec failed".to_string()
    };
    let msg = message.unwrap_or(default_message);

    let mut body = Map::new();
    body.insert("kind".to_string(), json!("Status"));
    body.insert("apiVersion".to_string(), json!("v1"));
    body.insert("metadata".to_string(), json!({}));
    body.insert("status".to_string(), json!(status));
    body.insert("message".to_string(), json!(msg));
    let reason_value = if code == 0 {
        None
    } else {
        reason.or_else(|| Some("NonZeroExitCode".to_string()))
    };
    if let Some(value) = reason_value {
        body.insert("reason".to_string(), json!(value));
    }
    let details_value = if code == 0 {
        Value::Null
    } else {
        json!({
            "causes": [
                {
                    "reason": "ExitCode",
                    "message": code.to_string(),
                }
            ]
        })
    };
    body.insert("details".to_string(), details_value);
    body.insert("code".to_string(), json!(code));

    let mut frame = Vec::new();
    frame.push(CHANNEL_STATUS);
    match serde_json::to_vec(&Value::Object(body)) {
        Ok(mut payload) => frame.append(&mut payload),
        Err(_) => {
            frame.extend_from_slice(br#"{"status":"Failure","message":"encoding error","code":1}"#)
        }
    }
    frame
}

fn decode_client_frame(
    options: &ExecOptions,
    frame: &[u8],
) -> Result<Option<ProcessCommand>, String> {
    if frame.is_empty() {
        return Ok(None);
    }
    let channel = frame[0];
    let payload = &frame[1..];

    match channel {
        CHANNEL_STDIN => {
            if !options.stdin && !options.tty {
                return Ok(None);
            }
            if payload.is_empty() {
                return Ok(Some(ProcessCommand::Close));
            }
            Ok(Some(ProcessCommand::Stdin(payload.to_vec())))
        }
        CHANNEL_RESIZE => {
            let event = parse_resize_payload(payload)?;
            Ok(Some(ProcessCommand::Resize(event)))
        }
        CHANNEL_CLOSE => Ok(Some(ProcessCommand::Close)),
        _ => Ok(None),
    }
}

enum ChildIo {
    Pty {
        fd: OwnedFd,
    },
    Pipes {
        stdin: Option<OwnedFd>,
        stdout: Option<OwnedFd>,
        stderr: Option<OwnedFd>,
    },
}

enum StreamKind {
    Stdout,
    Stderr,
}

impl StreamKind {
    fn as_str(&self) -> &'static str {
        match self {
            StreamKind::Stdout => "stdout",
            StreamKind::Stderr => "stderr",
        }
    }
}

struct StatusPacket {
    code: i32,
    message: Option<String>,
    reason: Option<String>,
}

fn run_exec_process(
    options: ExecOptions,
    mut command_rx: UnboundedReceiver<ProcessCommand>,
    event_tx: UnboundedSender<ProcessEvent>,
    context: Arc<SessionContext>,
    runtime: Handle,
) -> Result<(), ExecProcessError> {
    if options.command.is_empty() {
        let err = ExecProcessError::with_reason("no command provided", "BadRequest");
        let reason = err.reason().unwrap_or("InternalError").to_string();
        let message = err.to_string();
        enqueue_status_error(&event_tx, context.as_ref(), message.clone(), reason);
        return Err(err);
    }

    let container_id = match resolve_exec_container(&options) {
        Ok(id) => {
            let metadata = context.metadata(&[
                ("resolved_container", id.as_str()),
                ("command", context.command()),
            ]);
            log_debug(COMPONENT, "resolved exec container", &metadata);
            id
        }
        Err(err) => {
            let reason = err.reason().unwrap_or("InternalError").to_string();
            let message = err.to_string();
            let metadata = context.metadata(&[
                ("stage", "resolve_container"),
                ("reason", reason.as_str()),
                ("message", message.as_str()),
                ("command", context.command()),
            ]);
            log_error(COMPONENT, "failed to resolve exec container", &metadata);
            enqueue_status_error(&event_tx, context.as_ref(), message.clone(), reason);
            return Err(err);
        }
    };
    let command = options.command.clone();
    let tty = options.tty;
    let stdin_enabled = options.stdin || options.tty;
    let stdout_enabled = options.stdout || options.tty;
    let stderr_enabled = options.stderr && !options.tty;

    let io_container_id = container_id.clone();
    let io_task = runtime.spawn_blocking({
        let context = Arc::clone(&context);
        move || {
            setup_exec_io(
                tty,
                stdin_enabled,
                stdout_enabled,
                stderr_enabled,
                context,
                io_container_id,
            )
        }
    });

    let io_resources = match runtime.block_on(io_task) {
        Ok(Ok(resources)) => resources,
        Ok(Err(err)) => return Err(err),
        Err(join_err) => {
            let error_kind = if join_err.is_panic() {
                "panic"
            } else {
                "cancelled"
            };
            let metadata = context.metadata(&[
                ("stage", "io_setup"),
                ("error", error_kind),
                ("command", context.command()),
            ]);
            log_error(COMPONENT, "exec IO setup task failed", &metadata);
            return Err(ExecProcessError::new(format!(
                "exec IO setup task {error_kind}"
            )));
        }
    };

    let ExecIoResources {
        mut stdin_writer,
        resize_fd,
        stdout_reader,
        stderr_reader,
        status_reader,
        status_writer,
        child_io,
    } = io_resources;

    let mut stdout_reader_handle: Option<thread::JoinHandle<()>> = stdout_reader.map(|reader| {
        let tx = event_tx.clone();
        let stream_context = Arc::clone(&context);
        thread::spawn(move || forward_stream(reader, tx, StreamKind::Stdout, stream_context))
    });

    let mut stderr_reader_handle: Option<thread::JoinHandle<()>> = stderr_reader.map(|reader| {
        let tx = event_tx.clone();
        let stream_context = Arc::clone(&context);
        thread::spawn(move || forward_stream(reader, tx, StreamKind::Stderr, stream_context))
    });

    let status_tx = event_tx.clone();
    let status_context = Arc::clone(&context);
    let status_handle =
        runtime.spawn_blocking(move || listen_for_status(status_reader, status_tx, status_context));

    let exec_thread =
        thread::spawn(move || run_container_exec(container_id, command, child_io, status_writer));

    let mut fatal_error: Option<ExecProcessError> = None;
    let mut resize_warning_emitted = false;
    while let Some(cmd) = command_rx.blocking_recv() {
        match cmd {
            ProcessCommand::Stdin(data) => {
                if let Some(writer) = stdin_writer.as_mut() {
                    if let Err(err) = writer.write_all(&data) {
                        if err.kind() == io::ErrorKind::BrokenPipe {
                            let metadata = context.metadata(&[
                                ("stage", "stdin_write"),
                                ("error", "broken_pipe"),
                                ("command", context.command()),
                            ]);
                            log_debug(COMPONENT, "stdin pipe closed by exec process", &metadata);
                            break;
                        }
                        let error_string = err.to_string();
                        let metadata = context.metadata(&[
                            ("stage", "stdin_write"),
                            ("error", error_string.as_str()),
                            ("command", context.command()),
                        ]);
                        log_error(COMPONENT, "failed to write to exec stdin", &metadata);
                        let reason = "InternalError".to_string();
                        let status_message = format!("stdin stream error: {}", error_string);
                        enqueue_status_error(
                            &event_tx,
                            context.as_ref(),
                            status_message.clone(),
                            reason.clone(),
                        );
                        if fatal_error.is_none() {
                            fatal_error =
                                Some(ExecProcessError::with_reason(status_message, reason));
                        }
                        break;
                    }
                }
            }
            ProcessCommand::Close => break,
            ProcessCommand::Resize(event) => {
                let width = event.width.to_string();
                let height = event.height.to_string();
                if tty {
                    if let Some(handle) = resize_fd.as_ref() {
                        if let Err(err) = apply_resize(handle.fd(), &event) {
                            let err_string = err.to_string();
                            let metadata = context.metadata(&[
                                ("stage", "resize"),
                                ("width", width.as_str()),
                                ("height", height.as_str()),
                                ("error", err_string.as_str()),
                                ("command", context.command()),
                            ]);
                            log_warn(COMPONENT, "failed to apply terminal resize", &metadata);
                        } else {
                            let metadata = context.metadata(&[
                                ("stage", "resize"),
                                ("width", width.as_str()),
                                ("height", height.as_str()),
                                ("command", context.command()),
                            ]);
                            log_debug(COMPONENT, "applied terminal resize", &metadata);
                        }
                    }
                } else {
                    let metadata = context.metadata(&[
                        ("stage", "resize"),
                        ("width", width.as_str()),
                        ("height", height.as_str()),
                        ("command", context.command()),
                    ]);
                    if !resize_warning_emitted {
                        resize_warning_emitted = true;
                        log_warn(
                            COMPONENT,
                            "received terminal resize without tty support; ignoring",
                            &metadata,
                        );
                    } else {
                        log_debug(
                            COMPONENT,
                            "ignored terminal resize because tty is disabled",
                            &metadata,
                        );
                    }
                }
            }
        }
    }

    drop(stdin_writer);
    drop(resize_fd);

    match exec_thread.join() {
        Ok(Ok(())) => {}
        Ok(Err(err)) => {
            let message = err.to_string();
            let metadata = context.metadata(&[
                ("stage", "exec_worker"),
                ("error", message.as_str()),
                ("command", context.command()),
            ]);
            log_error(COMPONENT, "exec runtime returned error", &metadata);
            if fatal_error.is_none() {
                fatal_error = Some(err);
            }
        }
        Err(_) => {
            let metadata = context.metadata(&[
                ("stage", "exec_worker"),
                ("error", "panic"),
                ("command", context.command()),
            ]);
            log_error(COMPONENT, "exec worker panicked", &metadata);
            if fatal_error.is_none() {
                fatal_error = Some(ExecProcessError::new("exec worker panicked"));
            }
        }
    }

    match runtime.block_on(status_handle) {
        Ok(Ok(())) => {}
        Ok(Err(err)) => {
            let message = err.to_string();
            let metadata = context.metadata(&[
                ("stage", "status_reader"),
                ("error", message.as_str()),
                ("command", context.command()),
            ]);
            log_error(COMPONENT, "status reader failed to complete", &metadata);
            if fatal_error.is_none() {
                fatal_error = Some(err);
            }
        }
        Err(join_err) => {
            let error_kind = if join_err.is_panic() {
                "panic"
            } else {
                "cancelled"
            };
            let metadata = context.metadata(&[
                ("stage", "status_reader"),
                ("error", error_kind),
                ("command", context.command()),
            ]);
            log_error(COMPONENT, "status reader task failed", &metadata);
            if fatal_error.is_none() {
                fatal_error = Some(ExecProcessError::new(format!(
                    "status reader task {error_kind}"
                )));
            }
        }
    }

    if let Some(handle) = stdout_reader_handle.take() {
        if handle.join().is_err() {
            let metadata =
                context.metadata(&[("stream", "stdout"), ("command", context.command())]);
            log_warn(COMPONENT, "stdout forwarding thread panicked", &metadata);
            if fatal_error.is_none() {
                fatal_error = Some(ExecProcessError::new("stdout forwarding thread panicked"));
            }
        }
    }
    if let Some(handle) = stderr_reader_handle.take() {
        if handle.join().is_err() {
            let metadata =
                context.metadata(&[("stream", "stderr"), ("command", context.command())]);
            log_warn(COMPONENT, "stderr forwarding thread panicked", &metadata);
            if fatal_error.is_none() {
                fatal_error = Some(ExecProcessError::new("stderr forwarding thread panicked"));
            }
        }
    }

    if let Some(err) = fatal_error {
        return Err(err);
    }

    Ok(())
}

struct ExecIoResources {
    stdin_writer: Option<File>,
    resize_fd: Option<FdGuard>,
    stdout_reader: Option<File>,
    stderr_reader: Option<File>,
    status_reader: File,
    status_writer: OwnedFd,
    child_io: ChildIo,
}

fn setup_exec_io(
    tty: bool,
    stdin_enabled: bool,
    stdout_enabled: bool,
    stderr_enabled: bool,
    context: Arc<SessionContext>,
    container_id: String,
) -> Result<ExecIoResources, ExecProcessError> {
    let io_metadata = context.metadata(&[
        ("container_id", container_id.as_str()),
        ("stdin", if stdin_enabled { "true" } else { "false" }),
        ("stdout", if stdout_enabled { "true" } else { "false" }),
        ("stderr", if stderr_enabled { "true" } else { "false" }),
        ("tty", if tty { "true" } else { "false" }),
        ("command", context.command()),
    ]);
    log_debug(COMPONENT, "configuring exec IO", &io_metadata);

    let (status_read_fd, status_write_fd) =
        unistd::pipe().map_err(|err| ExecProcessError::new(err.to_string()))?;
    let status_reader = unsafe { File::from_raw_fd(status_read_fd.into_raw_fd()) };
    let status_writer = status_write_fd;

    let mut stdin_writer: Option<File> = None;
    let mut resize_fd: Option<FdGuard> = None;
    let mut stdout_reader: Option<File> = None;
    let mut stderr_reader: Option<File> = None;
    let child_io: ChildIo;

    if tty {
        let pty = openpty(None, None).map_err(|err| ExecProcessError::new(err.to_string()))?;
        let master = File::from(pty.master);
        let reader = master
            .try_clone()
            .map_err(|err| ExecProcessError::new(err.to_string()))?;
        stdin_writer = Some(master);
        stdout_reader = Some(reader);
        let resize_dup =
            unistd::dup(&pty.slave).map_err(|err| ExecProcessError::new(err.to_string()))?;
        resize_fd = Some(FdGuard::new(resize_dup.into_raw_fd()));
        child_io = ChildIo::Pty { fd: pty.slave };
    } else {
        let mut child_stdin: Option<OwnedFd> = None;
        let mut child_stdout: Option<OwnedFd> = None;
        let mut child_stderr: Option<OwnedFd> = None;

        if stdin_enabled {
            let (read_fd, write_fd) =
                unistd::pipe().map_err(|err| ExecProcessError::new(err.to_string()))?;
            child_stdin = Some(read_fd);
            let writer = unsafe { File::from_raw_fd(write_fd.into_raw_fd()) };
            stdin_writer = Some(writer);
        }

        if stdout_enabled {
            let (read_fd, write_fd) =
                unistd::pipe().map_err(|err| ExecProcessError::new(err.to_string()))?;
            stdout_reader = Some(unsafe { File::from_raw_fd(read_fd.into_raw_fd()) });
            child_stdout = Some(write_fd);
        }

        if stderr_enabled {
            let (read_fd, write_fd) =
                unistd::pipe().map_err(|err| ExecProcessError::new(err.to_string()))?;
            stderr_reader = Some(unsafe { File::from_raw_fd(read_fd.into_raw_fd()) });
            child_stderr = Some(write_fd);
        }

        child_io = ChildIo::Pipes {
            stdin: child_stdin,
            stdout: child_stdout,
            stderr: child_stderr,
        };
    }

    Ok(ExecIoResources {
        stdin_writer,
        resize_fd,
        stdout_reader,
        stderr_reader,
        status_reader,
        status_writer,
        child_io,
    })
}

fn enqueue_status_error(
    tx: &UnboundedSender<ProcessEvent>,
    context: &SessionContext,
    message: String,
    reason: String,
) {
    if tx
        .send(ProcessEvent::Status {
            code: 1,
            message: Some(message.clone()),
            reason: Some(reason.clone()),
        })
        .is_err()
    {
        let metadata = context.metadata(&[
            ("stage", "status_channel"),
            ("reason", reason.as_str()),
            ("message", message.as_str()),
            ("command", context.command()),
        ]);
        log_warn(
            COMPONENT,
            "failed to enqueue exec failure status",
            &metadata,
        );
    }
}

fn run_container_exec(
    container_id: String,
    command: Vec<String>,
    io: ChildIo,
    status_writer_fd: OwnedFd,
) -> Result<(), ExecProcessError> {
    let runtime = container_runtime();
    let exec_result = runtime
        .exec(
            &container_id,
            Box::new(move || {
                prepare_exec_request(command, io)
                    .map_err(|err| Box::new(err) as Box<dyn std::error::Error + Send + Sync>)
            }),
        )
        .map_err(|err| ExecProcessError::new(err.to_string()))?;

    write_exec_status(status_writer_fd, exec_result)
}

fn prepare_exec_request(
    command: Vec<String>,
    io: ChildIo,
) -> Result<ExecRequest, ExecProcessError> {
    let mut parts = command.into_iter();
    let program = parts
        .next()
        .ok_or_else(|| ExecProcessError::new("no command provided"))?;
    let args: Vec<String> = parts.collect();

    match io {
        ChildIo::Pty { fd } => configure_pty_io(fd)?,
        ChildIo::Pipes {
            stdin,
            stdout,
            stderr,
        } => {
            configure_pipe_io(libc::STDIN_FILENO, stdin, true)?;
            configure_pipe_io(libc::STDOUT_FILENO, stdout, false)?;
            configure_pipe_io(libc::STDERR_FILENO, stderr, false)?;
        }
    }

    Ok(ExecRequest {
        program,
        args,
        env: None,
    })
}

fn configure_pty_io(fd: OwnedFd) -> Result<(), ExecProcessError> {
    let tty_fd = fd.into_raw_fd();
    for target in [libc::STDIN_FILENO, libc::STDOUT_FILENO, libc::STDERR_FILENO] {
        if unsafe { libc::dup2(tty_fd, target) } == -1 {
            let err = io::Error::last_os_error();
            unsafe {
                libc::close(tty_fd);
            }
            return Err(ExecProcessError::new(err.to_string()));
        }
    }

    unsafe {
        if libc::setsid() == -1 {
            let err = io::Error::last_os_error();
            let _ = libc::close(tty_fd);
            return Err(ExecProcessError::new(err.to_string()));
        }
        if libc::ioctl(tty_fd, libc::TIOCSCTTY, 0) == -1 {
            let err = io::Error::last_os_error();
            let _ = libc::close(tty_fd);
            return Err(ExecProcessError::new(err.to_string()));
        }
    }

    if unsafe { libc::close(tty_fd) } == -1 {
        let err = io::Error::last_os_error();
        return Err(ExecProcessError::new(err.to_string()));
    }
    Ok(())
}

fn configure_pipe_io(
    target: i32,
    fd: Option<OwnedFd>,
    read_only: bool,
) -> Result<(), ExecProcessError> {
    let raw_fd = match fd {
        Some(handle) => handle.into_raw_fd(),
        None => {
            let flags = if read_only {
                nix::fcntl::OFlag::O_RDONLY
            } else {
                nix::fcntl::OFlag::O_WRONLY
            };
            nix::fcntl::open("/dev/null", flags, nix::sys::stat::Mode::empty())
                .map_err(|err| ExecProcessError::new(err.to_string()))?
                .into_raw_fd()
        }
    };

    if unsafe { libc::dup2(raw_fd, target) } == -1 {
        let err = io::Error::last_os_error();
        unsafe {
            libc::close(raw_fd);
        }
        return Err(ExecProcessError::new(err.to_string()));
    }
    if unsafe { libc::close(raw_fd) } == -1 {
        let err = io::Error::last_os_error();
        return Err(ExecProcessError::new(err.to_string()));
    }
    Ok(())
}

fn write_exec_status(
    status_writer_fd: OwnedFd,
    exec_result: ExecResult,
) -> Result<(), ExecProcessError> {
    let mut status_writer = File::from(status_writer_fd);
    let exit_status = wait_status_to_exit_status(exec_result.wait_status);
    let (code, message) = summarize_exit(exit_status);
    let reason = if code == 0 {
        None
    } else {
        Some("NonZeroExitCode".to_string())
    };
    let packet = StatusPacket {
        code,
        message,
        reason,
    };

    write_status_packet(&mut status_writer, &packet)
        .map_err(|err| ExecProcessError::new(err.to_string()))
}

fn wait_status_to_exit_status(wait_status: nix::sys::wait::WaitStatus) -> ExitStatus {
    use nix::sys::wait::WaitStatus;
    match wait_status {
        WaitStatus::Exited(_, code) => ExitStatus::from_raw((code & 0xff) << 8),
        WaitStatus::Signaled(_, signal, _) => {
            let raw = (signal as i32 & 0x7f) | 0x80;
            ExitStatus::from_raw(raw)
        }
        WaitStatus::Stopped(_, signal) => {
            let raw = ((signal as i32 & 0x7f) << 8) | 0x7f;
            ExitStatus::from_raw(raw)
        }
        _ => ExitStatus::from_raw(1 << 8),
    }
}

fn forward_stream(
    mut reader: File,
    tx: UnboundedSender<ProcessEvent>,
    kind: StreamKind,
    context: Arc<SessionContext>,
) {
    let mut buf = [0u8; 4096];
    loop {
        match reader.read(&mut buf) {
            Ok(0) => break,
            Ok(n) => {
                let data = buf[..n].to_vec();
                let event = match kind {
                    StreamKind::Stdout => ProcessEvent::Stdout(data),
                    StreamKind::Stderr => ProcessEvent::Stderr(data),
                };
                if tx.send(event).is_err() {
                    let metadata = context.metadata(&[("stream", kind.as_str())]);
                    log_debug(
                        COMPONENT,
                        "dropping exec stream chunk because receiver closed",
                        &metadata,
                    );
                    break;
                }
            }
            Err(err) => {
                if err.kind() == io::ErrorKind::Interrupted {
                    continue;
                }
                let error_string = err.to_string();
                let stream_name = kind.as_str();
                let metadata =
                    context.metadata(&[("stream", stream_name), ("error", error_string.as_str())]);
                log_error(COMPONENT, "failed to read exec stream", &metadata);
                let message = format!("{} stream error: {}", stream_name, error_string);
                let _ = tx.send(ProcessEvent::Status {
                    code: 1,
                    message: Some(message),
                    reason: Some("InternalError".to_string()),
                });
                break;
            }
        }
    }
}

fn listen_for_status(
    mut reader: File,
    tx: UnboundedSender<ProcessEvent>,
    context: Arc<SessionContext>,
) -> Result<(), ExecProcessError> {
    match read_status_packet(&mut reader) {
        Ok(packet) => {
            let exit_code = packet.code.to_string();
            let mut extra: Vec<(&str, &str)> = Vec::new();
            extra.push(("exit_code", exit_code.as_str()));
            if let Some(ref reason) = packet.reason {
                extra.push(("reason", reason.as_str()));
            }
            if let Some(ref message) = packet.message {
                if !message.is_empty() {
                    extra.push(("message", message.as_str()));
                }
            }
            let metadata = context.metadata(&extra);
            log_debug(COMPONENT, "received exec status packet", &metadata);
            let _ = tx.send(ProcessEvent::Status {
                code: packet.code,
                message: packet.message,
                reason: packet.reason,
            });
            Ok(())
        }
        Err(err) => {
            let error_string = err.to_string();
            let metadata =
                context.metadata(&[("stage", "status_reader"), ("error", error_string.as_str())]);
            log_error(COMPONENT, "failed to read exec status packet", &metadata);
            let _ = tx.send(ProcessEvent::Status {
                code: 1,
                message: Some(error_string),
                reason: Some("InternalError".to_string()),
            });
            Err(err)
        }
    }
}

fn read_status_packet(reader: &mut File) -> Result<StatusPacket, ExecProcessError> {
    let mut header = [0u8; 12];
    reader
        .read_exact(&mut header)
        .map_err(|err| ExecProcessError::new(err.to_string()))?;
    let code = i32::from_be_bytes(
        header[0..4]
            .try_into()
            .map_err(|_| ExecProcessError::new("invalid status header"))?,
    );
    let reason_len = u32::from_be_bytes(
        header[4..8]
            .try_into()
            .map_err(|_| ExecProcessError::new("invalid status header"))?,
    ) as usize;
    let message_len = u32::from_be_bytes(
        header[8..12]
            .try_into()
            .map_err(|_| ExecProcessError::new("invalid status header"))?,
    ) as usize;

    let mut reason_bytes = vec![0u8; reason_len];
    if reason_len > 0 {
        reader
            .read_exact(&mut reason_bytes)
            .map_err(|err| ExecProcessError::new(err.to_string()))?;
    }

    let mut message_bytes = vec![0u8; message_len];
    if message_len > 0 {
        reader
            .read_exact(&mut message_bytes)
            .map_err(|err| ExecProcessError::new(err.to_string()))?;
    }

    let reason = if reason_len > 0 {
        Some(
            String::from_utf8(reason_bytes)
                .map_err(|err| ExecProcessError::new(err.to_string()))?,
        )
    } else {
        None
    };
    let message = if message_len > 0 {
        Some(
            String::from_utf8(message_bytes)
                .map_err(|err| ExecProcessError::new(err.to_string()))?,
        )
    } else {
        None
    };

    Ok(StatusPacket {
        code,
        message,
        reason,
    })
}

fn write_status_packet(writer: &mut File, packet: &StatusPacket) -> io::Result<()> {
    let reason_bytes = packet
        .reason
        .as_ref()
        .map(|value| value.as_bytes().to_vec())
        .unwrap_or_default();
    let message_bytes = packet
        .message
        .as_ref()
        .map(|value| value.as_bytes().to_vec())
        .unwrap_or_default();
    let reason_len = u32::try_from(reason_bytes.len())
        .map_err(|_| io::Error::new(ErrorKind::InvalidInput, "status reason too long"))?;
    let message_len = u32::try_from(message_bytes.len())
        .map_err(|_| io::Error::new(ErrorKind::InvalidInput, "status message too long"))?;

    let mut buffer = Vec::with_capacity(12 + reason_bytes.len() + message_bytes.len());
    buffer.extend_from_slice(&packet.code.to_be_bytes());
    buffer.extend_from_slice(&reason_len.to_be_bytes());
    buffer.extend_from_slice(&message_len.to_be_bytes());
    buffer.extend_from_slice(&reason_bytes);
    buffer.extend_from_slice(&message_bytes);
    writer.write_all(&buffer)
}

fn apply_resize(fd: i32, event: &ResizeEvent) -> io::Result<()> {
    let winsize = Winsize {
        ws_row: event.height,
        ws_col: event.width,
        ws_xpixel: 0,
        ws_ypixel: 0,
    };
    let result = unsafe { libc::ioctl(fd, libc::TIOCSWINSZ, &winsize) };
    if result == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}

fn summarize_exit(status: ExitStatus) -> (i32, Option<String>) {
    if let Some(code) = status.code() {
        if code == 0 {
            (0, None)
        } else {
            (code, Some(format!("command exited with status {}", code)))
        }
    } else if let Some(signal) = status.signal() {
        let message = format!("command terminated by signal {}", signal);
        (128 + signal, Some(message))
    } else {
        (1, Some("command terminated abnormally".to_string()))
    }
}

#[derive(Debug)]
struct ExecProcessError {
    message: String,
    reason: Option<String>,
}

impl ExecProcessError {
    fn new(message: impl Into<String>) -> Self {
        ExecProcessError {
            message: message.into(),
            reason: None,
        }
    }

    fn with_reason(message: impl Into<String>, reason: impl Into<String>) -> Self {
        ExecProcessError {
            message: message.into(),
            reason: Some(reason.into()),
        }
    }

    fn reason(&self) -> Option<&str> {
        self.reason.as_deref()
    }
}

impl std::fmt::Display for ExecProcessError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for ExecProcessError {}

fn resolve_exec_container(options: &ExecOptions) -> Result<String, ExecProcessError> {
    fn classify_reason(message: &str) -> &'static str {
        if message.to_ascii_lowercase().contains("not found") {
            "NotFound"
        } else {
            "InternalError"
        }
    }

    let namespace_trimmed = options.namespace.trim();
    let namespace = if namespace_trimmed.is_empty() {
        None
    } else {
        Some(namespace_trimmed)
    };
    let mut candidates: Vec<(Option<&str>, &str)> = Vec::new();

    if let Some(ns) = namespace {
        candidates.push((Some(ns), options.pod.as_str()));
    }
    candidates.push((None, options.pod.as_str()));

    if let Some(container_name) = options
        .container
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        if let Some(ns) = namespace {
            candidates.push((Some(ns), container_name));
        }
        candidates.push((None, container_name));
    }

    let mut not_found_error: Option<ExecProcessError> = None;

    for (ns, lookup) in candidates {
        match resolve_container_id(ns, lookup) {
            Ok(id) => return Ok(id),
            Err(err) => {
                let message = err.to_string();
                let reason = classify_reason(&message);
                if reason != "NotFound" {
                    return Err(ExecProcessError::with_reason(message, reason));
                }
                if not_found_error.is_none() {
                    not_found_error = Some(ExecProcessError::with_reason(message, reason));
                }
            }
        }
    }

    Err(not_found_error
        .unwrap_or_else(|| ExecProcessError::with_reason("container lookup failed", "NotFound")))
}

#[derive(Debug)]
enum ProcessCommand {
    Stdin(Vec<u8>),
    Close,
    Resize(ResizeEvent),
}

enum ProcessEvent {
    Stdout(Vec<u8>),
    Stderr(Vec<u8>),
    Status {
        code: i32,
        message: Option<String>,
        reason: Option<String>,
    },
}

struct FdGuard(i32);

impl FdGuard {
    fn new(fd: i32) -> Self {
        FdGuard(fd)
    }

    fn fd(&self) -> i32 {
        self.0
    }
}

impl Drop for FdGuard {
    fn drop(&mut self) {
        unsafe {
            libc::close(self.0);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::header::SEC_WEBSOCKET_PROTOCOL;
    use axum::http::{HeaderMap, HeaderName, HeaderValue, Request, StatusCode, Version};
    use axum::response::IntoResponse;
    use nix::unistd;
    use serde_json::{json, Value};
    use std::convert::TryFrom;
    use std::process::Command;

    fn sample_options() -> ExecOptions {
        ExecOptions {
            namespace: "default".into(),
            pod: "demo".into(),
            container: None,
            command: vec!["/bin/true".into()],
            stdin: false,
            stdout: true,
            stderr: false,
            tty: false,
        }
    }

    #[test]
    fn selects_highest_supported_subprotocol() {
        let mut headers = HeaderMap::new();
        headers.insert(
            SEC_WEBSOCKET_PROTOCOL,
            HeaderValue::from_static("channel.k8s.io, V5.CHANNEL.K8S.IO, v4.channel.k8s.io"),
        );
        let selected = select_subprotocol(&headers).expect("protocol should be selected");
        assert_eq!(selected, "v5.channel.k8s.io");
    }

    #[test]
    fn applies_stream_protocol_header() {
        let mut headers = HeaderMap::new();
        apply_stream_protocol_header(&mut headers, "v5.channel.k8s.io");
        let header = headers
            .get("X-Stream-Protocol-Version")
            .expect("expected X-Stream-Protocol-Version header");
        assert_eq!(header, "v5.channel.k8s.io");
    }

    #[test]
    fn channel_frame_includes_leading_identifier() {
        let payload = b"hello world";
        let frame = build_channel_frame(CHANNEL_STDOUT, payload);
        assert_eq!(frame[0], CHANNEL_STDOUT);
        assert_eq!(&frame[1..], payload);
    }

    #[test]
    fn decode_resize_command_returns_dimensions() {
        let mut data = serde_json::to_vec(&json!({"Width": 120, "Height": 40}))
            .expect("failed to serialize resize payload");
        let mut frame = vec![CHANNEL_RESIZE];
        frame.append(&mut data);
        let options = sample_options();
        let command = decode_client_frame(&options, &frame)
            .expect("frame decoding should succeed")
            .expect("expected resize command");
        match command {
            ProcessCommand::Resize(event) => {
                assert_eq!(event.width, 120);
                assert_eq!(event.height, 40);
            }
            other => panic!("unexpected command variant: {:?}", other),
        }
    }

    #[test]
    fn status_payload_wraps_failure_exit_details() {
        let payload =
            build_status_payload(3, Some("boom".to_string()), Some("BadRequest".to_string()));
        assert_eq!(payload[0], CHANNEL_STATUS);
        let body: Value = serde_json::from_slice(&payload[1..]).expect("expected JSON payload");
        assert_eq!(body["status"], "Failure");
        assert_eq!(body["code"], 3);
        assert_eq!(body["message"], "boom");
        assert_eq!(body["reason"], "BadRequest");
    }

    #[tokio::test]
    async fn rejects_http2_upgrade_requests() {
        let raw = super::super::exec_common::RawExecQuery {
            namespace: "default".into(),
            pod: "demo".into(),
            container: None,
            command: vec!["/bin/true".into()],
            stdin: Some(true),
            stdout: Some(false),
            stderr: Some(false),
            tty: Some(false),
        };

        let request = Request::builder()
            .method("POST")
            .uri("/api/v1/namespaces/default/pods/demo/exec")
            .version(Version::HTTP_2)
            .body(super::Body::empty())
            .expect("failed to build request");

        let err = exec_http_post_impl(raw, request)
            .await
            .expect_err("HTTP/2 request must be rejected");
        let response = err.into_response();
        assert_eq!(response.status(), StatusCode::HTTP_VERSION_NOT_SUPPORTED);
    }

    fn run_child_process(
        command: Vec<String>,
        io: ChildIo,
        status_writer: OwnedFd,
    ) -> Result<(), ExecProcessError> {
        // The unit test only exercises the pipe-based path with no extra descriptors.
        if !matches!(
            io,
            ChildIo::Pipes {
                stdin: None,
                stdout: None,
                stderr: None
            }
        ) {
            return Err(ExecProcessError::new(
                "run_child_process test helper only supports simple pipe IO",
            ));
        }

        let mut parts = command.into_iter();
        let program = parts
            .next()
            .ok_or_else(|| ExecProcessError::new("no command provided"))?;
        let args: Vec<String> = parts.collect();

        let status = Command::new(&program)
            .args(&args)
            .status()
            .map_err(|err| ExecProcessError::new(err.to_string()))?;

        let wait_status = if let Some(code) = status.code() {
            nix::sys::wait::WaitStatus::Exited(nix::unistd::Pid::this(), code)
        } else if let Some(signal) = status.signal() {
            let signal = nix::sys::signal::Signal::try_from(signal)
                .unwrap_or(nix::sys::signal::Signal::SIGKILL);
            nix::sys::wait::WaitStatus::Signaled(nix::unistd::Pid::this(), signal, false)
        } else {
            nix::sys::wait::WaitStatus::Exited(nix::unistd::Pid::this(), 1)
        };

        write_exec_status(status_writer, ExecResult { wait_status })
    }

    #[test]
    fn rejects_when_no_supported_protocols() {
        let mut headers = HeaderMap::new();
        headers.insert(
            SEC_WEBSOCKET_PROTOCOL,
            HeaderValue::from_static("v3.channel.k8s.io, v2.channel.k8s.io"),
        );

        let err =
            select_subprotocol(&headers).expect_err("unsupported protocols should return an error");
        let response = err.into_response();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[test]
    fn run_child_process_writes_success_packet() {
        let (reader_fd, writer_fd) = unistd::pipe().expect("failed to create pipe");
        let child_io = ChildIo::Pipes {
            stdin: None,
            stdout: None,
            stderr: None,
        };

        run_child_process(vec!["/bin/true".into()], child_io, writer_fd)
            .expect("command should succeed");

        let mut reader = File::from(reader_fd);
        let packet = read_status_packet(&mut reader).expect("status packet should decode");
        assert_eq!(packet.code, 0);
        assert!(packet.reason.is_none());
        assert!(packet
            .message
            .as_deref()
            .map(|s| s.is_empty())
            .unwrap_or(true));
    }

    #[test]
    fn sets_stream_protocol_header() {
        let mut headers = HeaderMap::new();
        apply_stream_protocol_header(&mut headers, "v4.channel.k8s.io");
        let header_name = HeaderName::from_static("x-stream-protocol-version");
        let value = headers
            .get(&header_name)
            .expect("expected stream protocol header");
        assert_eq!(value, "v4.channel.k8s.io");
    }

    #[test]
    fn decode_client_frame_returns_resize_event() {
        let options = sample_options();
        let payload = serde_json::json!({"Width": 120, "Height": 40});
        let mut frame = vec![CHANNEL_RESIZE];
        frame.extend_from_slice(serde_json::to_string(&payload).unwrap().as_bytes());

        let command = decode_client_frame(&options, &frame)
            .expect("frame should decode")
            .expect("resize command expected");
        if let ProcessCommand::Resize(event) = command {
            assert_eq!(event.width, 120);
            assert_eq!(event.height, 40);
        } else {
            panic!("expected resize command");
        }
    }

    #[test]
    fn decode_client_frame_respects_stdin_flag() {
        let mut options = sample_options();
        options.stdin = false;
        options.tty = false;
        let mut frame = vec![CHANNEL_STDIN];
        frame.extend_from_slice(b"hello");

        let command = decode_client_frame(&options, &frame).expect("frame should decode");
        assert!(command.is_none(), "stdin should be ignored when disabled");
    }

    #[test]
    fn build_status_payload_encodes_exit_details() {
        let payload = build_status_payload(
            17,
            Some("boom".to_string()),
            Some("InternalError".to_string()),
        );
        assert_eq!(payload[0], CHANNEL_STATUS);
        let value: Value =
            serde_json::from_slice(&payload[1..]).expect("expected valid status json");
        assert_eq!(value["code"], 17);
        assert_eq!(value["message"], "boom");
        assert_eq!(value["reason"], "InternalError");
        assert_eq!(value["status"], "Failure");
        let causes = value["details"]["causes"]
            .as_array()
            .expect("expected causes array");
        assert_eq!(causes.len(), 1);
        assert_eq!(causes[0]["reason"], "ExitCode");
        assert_eq!(causes[0]["message"], "17");

        let success = build_status_payload(0, None, None);
        assert_eq!(success[0], CHANNEL_STATUS);
        let value: Value =
            serde_json::from_slice(&success[1..]).expect("expected valid status json");
        assert_eq!(value["code"], 0);
        assert_eq!(value["status"], "Success");
        assert_eq!(value["message"], "");
        assert!(value.get("reason").is_none(), "success should omit reason");
        assert!(
            value["details"].is_null(),
            "success should produce null details"
        );
    }
}
