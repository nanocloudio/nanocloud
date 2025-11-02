use std::collections::HashMap;

use futures_util::{SinkExt, StreamExt};
use nanocloud::nanocloud::api::client::test_support::{
    build_exec_request,
    extract_exec_protocol_label,
    parse_status_code,
    test_client,
    MultiplexerHarness,
    CHANNEL_CLOSE,
    CHANNEL_RESIZE,
    CHANNEL_STDIN,
    CHANNEL_STDERR,
    CHANNEL_STATUS,
    CHANNEL_STDOUT,
};
use tokio::io::duplex;
use tokio::time::{timeout, Duration};
use tokio_tungstenite::tungstenite::http::{
    header::HeaderValue,
    header::SEC_WEBSOCKET_PROTOCOL,
    HeaderMap,
};
use tokio_tungstenite::tungstenite::protocol::Role;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::WebSocketStream;

#[test]
fn build_exec_url_sets_flags_and_commands() {
    let client = test_client();
    let options = build_exec_request(Some("main"));

    let url = client.build_exec_url(&options).expect("exec url");
    assert_eq!(url.path(), "/api/v1/namespaces/demo/pods/web-0/exec");

    let mut commands: Vec<String> = Vec::new();
    let mut params: HashMap<String, String> = HashMap::new();
    for (key, value) in url.query_pairs() {
        if key == "command" {
            commands.push(value.into_owned());
        } else {
            params.insert(key.into_owned(), value.into_owned());
        }
    }

    assert_eq!(commands, vec!["sh", "-c", "echo hi"]);
    assert_eq!(params.get("container").map(String::as_str), Some("main"));
    assert_eq!(params.get("stdin").map(String::as_str), Some("true"));
    assert_eq!(params.get("stdout").map(String::as_str), Some("true"));
    assert_eq!(params.get("stderr").map(String::as_str), Some("false"));
    assert_eq!(params.get("tty").map(String::as_str), Some("true"));
}

#[test]
fn build_exec_url_omits_container_when_none() {
    let client = test_client();
    let options = build_exec_request(None);

    let url = client.build_exec_url(&options).expect("exec url");
    let has_container = url.query_pairs().any(|(key, _)| key == "container");
    assert!(!has_container);
}

#[test]
fn extract_exec_protocol_prefers_highest_supported_protocol() {
    let mut headers = HeaderMap::new();
    headers.insert(
        SEC_WEBSOCKET_PROTOCOL,
        HeaderValue::from_static("v4.channel.k8s.io, v5.channel.k8s.io"),
    );

    let protocol = extract_exec_protocol_label(&headers).expect("protocol selection");
    assert_eq!(protocol, "v5");
}

#[tokio::test]
async fn multiplexer_handles_streams_and_resize() {
    let (client_stream, server_stream) = duplex(4096);
    let ws_client = WebSocketStream::from_raw_socket(client_stream, Role::Client, None).await;
    let mut ws_server =
        WebSocketStream::from_raw_socket(server_stream, Role::Server, None).await;

    let mut harness = MultiplexerHarness::new(ws_client);

    let mut stdout_rx = harness.take_stdout().expect("stdout receiver");
    let mut stderr_rx = harness.take_stderr().expect("stderr receiver");
    let mut status_rx = harness.take_status().expect("status receiver");
    let mut close_rx = harness.take_close().expect("close receiver");
    let stdin_tx = harness.stdin_sender();
    let resize_tx = harness.resize_sender();

    ws_server
        .send(Message::Binary(
            [vec![CHANNEL_STDOUT], b"hello".to_vec()].concat().into(),
        ))
        .await
        .expect("send stdout frame");
    assert_eq!(
        stdout_rx.recv().await.expect("stdout data").as_slice(),
        b"hello"
    );

    ws_server
        .send(Message::Binary(
            [vec![CHANNEL_STDERR], b"errors".to_vec()].concat().into(),
        ))
        .await
        .expect("send stderr frame");
    assert_eq!(
        stderr_rx.recv().await.expect("stderr data").as_slice(),
        b"errors"
    );

    let status_payload = serde_json::json!({
        "status": "Success",
        "message": "ok",
        "reason": serde_json::Value::Null,
        "code": 7
    });
    let mut status_frame = vec![CHANNEL_STATUS];
    status_frame.extend_from_slice(
        serde_json::to_vec(&status_payload)
            .expect("status json")
            .as_slice(),
    );
    ws_server
        .send(Message::Binary(status_frame.into()))
        .await
        .expect("send status frame");
    ws_server
        .send(Message::Binary(vec![CHANNEL_CLOSE].into()))
        .await
        .expect("send close frame");

    let status_bytes = status_rx.recv().await.expect("status bytes");
    let exit_code = parse_status_code(&status_bytes).expect("parse status");
    assert_eq!(exit_code, Some(7));

    close_rx.recv().await.expect("close signal");

    stdin_tx
        .send(b"input".to_vec())
        .await
        .expect("send stdin payload");
    resize_tx
        .send(
            serde_json::to_vec(&serde_json::json!({"Width": 120, "Height": 40}))
                .expect("resize json"),
        )
        .await
        .expect("send resize payload");

    let mut stdin_payload = None;
    let mut resize_payload = None;
    while stdin_payload.is_none() || resize_payload.is_none() {
        let maybe_frame = timeout(Duration::from_secs(1), ws_server.next())
            .await
            .expect("outbound frame timed out");
        match maybe_frame {
            Some(Ok(Message::Binary(frame))) => match frame.first().copied() {
                Some(channel) if channel == CHANNEL_STDIN => {
                    stdin_payload = Some(frame[1..].to_vec());
                }
                Some(channel) if channel == CHANNEL_RESIZE => {
                    resize_payload = Some(frame[1..].to_vec());
                }
                other => panic!("unexpected outbound channel {:?}", other),
            },
            Some(Ok(Message::Close(_))) => break,
            Some(Ok(_)) => {}
            Some(Err(err)) => panic!("websocket error: {err}"),
            None => break,
        }
    }

    assert_eq!(stdin_payload.as_deref(), Some(b"input".as_ref()));

    let resize_value: serde_json::Value =
        serde_json::from_slice(resize_payload.as_ref().expect("resize payload").as_slice())
            .expect("decode resize");
    assert_eq!(
        resize_value,
        serde_json::json!({"Width": 120, "Height": 40})
    );

    drop(stdin_tx);
    drop(resize_tx);
    ws_server.close(None).await.expect("close mock server");
    drop(ws_server);
    harness.shutdown().await;
}

#[test]
fn extract_exec_protocol_errors_without_header() {
    let headers = HeaderMap::new();
    let error = extract_exec_protocol_label(&headers)
        .expect_err("missing protocol header should error");
    assert!(error
        .to_string()
        .contains("exec server did not select a channel.k8s.io subprotocol"));
}

#[test]
fn extract_exec_protocol_rejects_unsupported_protocol() {
    let mut headers = HeaderMap::new();
    headers.insert(
        SEC_WEBSOCKET_PROTOCOL,
        HeaderValue::from_static("channel.v9.example.io"),
    );
    let error = extract_exec_protocol_label(&headers)
        .expect_err("unsupported protocol should error");
    assert!(error
        .to_string()
        .contains("exec server selected unsupported subprotocol"));
}

#[test]
fn extract_exec_protocol_skips_empty_tokens() {
    let mut headers = HeaderMap::new();
    headers.insert(
        SEC_WEBSOCKET_PROTOCOL,
        HeaderValue::from_static("  , , v4.channel.k8s.io ,"),
    );

    let protocol = extract_exec_protocol_label(&headers).expect("protocol selection");
    assert_eq!(protocol, "v4");
}
