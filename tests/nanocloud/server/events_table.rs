use axum::http::{HeaderMap, HeaderValue, StatusCode};
use chrono::{Duration as ChronoDuration, Utc};
use futures_util::StreamExt;
use nanocloud::nanocloud::k8s::event::{Event, EventRegistry, EventSource, ObjectReference};
use nanocloud::nanocloud::k8s::pod::ObjectMeta;
use nanocloud::nanocloud::k8s::table::Table;
use nanocloud::nanocloud::server::handlers::events::{handle_request, EventWatchParams};
use nanocloud::nanocloud::server::handlers::pods::TABLE_CONTENT_TYPE;
use std::sync::OnceLock;

fn registry_guard() -> &'static tokio::sync::Mutex<()> {
    static GUARD: OnceLock<tokio::sync::Mutex<()>> = OnceLock::new();
    GUARD.get_or_init(|| tokio::sync::Mutex::new(()))
}

fn sample_event(namespace: &str, name: &str, reason: &str, event_type: &str) -> Event {
    let timestamp = Utc::now().to_rfc3339();
    Event {
        api_version: "v1".to_string(),
        kind: "Event".to_string(),
        metadata: ObjectMeta {
            name: Some(name.to_string()),
            namespace: Some(namespace.to_string()),
            ..Default::default()
        },
        involved_object: ObjectReference {
            api_version: Some("nanocloud.io/v1".to_string()),
            kind: Some("Bundle".to_string()),
            name: Some(name.to_string()),
            namespace: Some(namespace.to_string()),
            uid: Some(format!("bundle:{}/{}", namespace, name)),
            resource_version: None,
            field_path: None,
        },
        reason: Some(reason.to_string()),
        message: Some(format!("Event for {}", name)),
        event_type: Some(event_type.to_string()),
        first_timestamp: Some(timestamp.clone()),
        last_timestamp: Some(timestamp.clone()),
        event_time: Some(timestamp.clone()),
        count: Some(1),
        reporting_component: Some("tests".to_string()),
        reporting_instance: Some("tests".to_string()),
        action: Some("Reconcile".to_string()),
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

fn set_event_timestamp(event: &mut Event, timestamp: &str) {
    event.event_time = Some(timestamp.to_string());
    event.first_timestamp = Some(timestamp.to_string());
    event.last_timestamp = Some(timestamp.to_string());
}

#[tokio::test]
async fn list_returns_events() {
    let _guard = registry_guard().lock().await;
    let registry = EventRegistry::shared();
    registry.clear().await;
    registry
        .record(sample_event(
            "default",
            "bundle-a",
            "BundleReconciled",
            "Normal",
        ))
        .await;

    let response = handle_request(None, EventWatchParams::default(), HeaderMap::new())
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::OK);
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body bytes");
    let list: nanocloud::nanocloud::k8s::event::EventList =
        serde_json::from_slice(&body).expect("list json");
    assert_eq!(list.items.len(), 1);
    let event = &list.items[0];
    assert_eq!(event.metadata.name.as_deref(), Some("event-1"));
    assert_eq!(event.reason.as_deref(), Some("BundleReconciled"));
}

#[tokio::test]
async fn list_returns_table_when_requested() {
    let _guard = registry_guard().lock().await;
    let registry = EventRegistry::shared();
    registry.clear().await;
    registry
        .record(sample_event(
            "default",
            "bundle-a",
            "BundleReconciled",
            "Normal",
        ))
        .await;

    let mut headers = HeaderMap::new();
    headers.insert(
        axum::http::header::ACCEPT,
        HeaderValue::from_static(TABLE_CONTENT_TYPE),
    );
    let response = handle_request(None, EventWatchParams::default(), headers)
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::OK);
    let content_type = response
        .headers()
        .get(axum::http::header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .unwrap_or("");
    assert_eq!(content_type, TABLE_CONTENT_TYPE);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body bytes");
    let table: Table = serde_json::from_slice(&body).expect("table json");
    assert_eq!(table.rows.len(), 1);
    assert_eq!(table.column_definitions.len(), 5);
    assert_eq!(table.column_definitions[0].name, "LAST SEEN");
}

#[tokio::test]
async fn watch_stream_includes_initial_events() {
    let _guard = registry_guard().lock().await;
    let registry = EventRegistry::shared();
    registry.clear().await;
    registry
        .record(sample_event(
            "default",
            "bundle-b",
            "BundleReconciled",
            "Normal",
        ))
        .await;

    let params: EventWatchParams = serde_urlencoded::from_str("watch=true").expect("watch params");
    let response = handle_request(None, params, HeaderMap::new())
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::OK);
    let mut stream = response.into_body().into_data_stream();
    let first_chunk = stream
        .next()
        .await
        .expect("first chunk")
        .expect("chunk result");
    let text = String::from_utf8(first_chunk.to_vec()).expect("utf8");
    assert!(text.contains("\"type\":\"ADDED\""));
    assert!(text.contains("\"BundleReconciled\""));
}

#[tokio::test]
async fn field_selector_filters_events() {
    let _guard = registry_guard().lock().await;
    let registry = EventRegistry::shared();
    registry.clear().await;
    registry
        .record(sample_event(
            "default",
            "bundle-a",
            "BundleReconciled",
            "Normal",
        ))
        .await;
    registry
        .record(sample_event(
            "default",
            "bundle-b",
            "BundleReconcileFailed",
            "Warning",
        ))
        .await;

    let params: EventWatchParams =
        serde_urlencoded::from_str("fieldSelector=reason%3DBundleReconciled")
            .expect("field selector params");
    let response = handle_request(None, params, HeaderMap::new())
        .await
        .expect("response");
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body bytes");
    let list: nanocloud::nanocloud::k8s::event::EventList =
        serde_json::from_slice(&body).expect("list json");
    assert_eq!(list.items.len(), 1);
    let event = &list.items[0];
    assert_eq!(event.involved_object.name.as_deref(), Some("bundle-a"));
}

#[tokio::test]
async fn since_query_filters_old_events() {
    let _guard = registry_guard().lock().await;
    let registry = EventRegistry::shared();
    registry.clear().await;

    let now = Utc::now();
    let fresh_ts = now.to_rfc3339();
    let old_ts = (now - ChronoDuration::minutes(30)).to_rfc3339();

    let mut old_event = sample_event("default", "bundle-old", "BundleReconciled", "Normal");
    set_event_timestamp(&mut old_event, &old_ts);
    registry.record(old_event).await;

    let mut fresh_event = sample_event("default", "bundle-new", "BundleReconciled", "Normal");
    set_event_timestamp(&mut fresh_event, &fresh_ts);
    registry.record(fresh_event).await;

    let params: EventWatchParams = serde_urlencoded::from_str("since=5m").unwrap();
    let response = handle_request(None, params, HeaderMap::new())
        .await
        .expect("response");
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body bytes");
    let list: nanocloud::nanocloud::k8s::event::EventList =
        serde_json::from_slice(&body).expect("list json");
    assert_eq!(list.items.len(), 1);
    assert_eq!(
        list.items[0].involved_object.name.as_deref(),
        Some("bundle-new")
    );
}

#[tokio::test]
async fn level_query_filters_events() {
    let _guard = registry_guard().lock().await;
    let registry = EventRegistry::shared();
    registry.clear().await;
    registry
        .record(sample_event(
            "default",
            "bundle-a",
            "BundleReconciled",
            "Normal",
        ))
        .await;
    registry
        .record(sample_event(
            "default",
            "bundle-b",
            "BundleReconcileFailed",
            "Warning",
        ))
        .await;

    let params: EventWatchParams =
        serde_urlencoded::from_str("level=Warning").expect("level params");
    let response = handle_request(None, params, HeaderMap::new())
        .await
        .expect("response");
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body bytes");
    let list: nanocloud::nanocloud::k8s::event::EventList =
        serde_json::from_slice(&body).expect("list json");
    assert_eq!(list.items.len(), 1);
    assert_eq!(
        list.items[0].involved_object.name.as_deref(),
        Some("bundle-b")
    );
}

#[tokio::test]
async fn reason_query_supports_multiple_values() {
    let _guard = registry_guard().lock().await;
    let registry = EventRegistry::shared();
    registry.clear().await;
    registry
        .record(sample_event(
            "default",
            "bundle-a",
            "BundleReconciled",
            "Normal",
        ))
        .await;
    registry
        .record(sample_event(
            "default",
            "bundle-b",
            "SecurityPolicyViolation",
            "Warning",
        ))
        .await;

    let params: EventWatchParams =
        serde_urlencoded::from_str("reason=BundleReconciled,PrivilegeEscalationDenied")
            .expect("reason params");
    let response = handle_events_request(None, params, HeaderMap::new())
        .await
        .expect("response");
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body bytes");
    let list: nanocloud::nanocloud::k8s::event::EventList =
        serde_json::from_slice(&body).expect("list json");
    assert_eq!(list.items.len(), 1);
    assert_eq!(
        list.items[0].involved_object.name.as_deref(),
        Some("bundle-a")
    );
}
