use axum::body::to_bytes;
use axum::http::{header, HeaderMap, HeaderValue, StatusCode};
use chrono::{Duration as ChronoDuration, Utc};
use serde_json::json;
use std::collections::HashMap;
use std::sync::OnceLock;
use tokio::sync::Mutex;

use nanocloud::nanocloud::api::types::PodTable;
use nanocloud::nanocloud::k8s::pod::{
    ContainerSpec, ContainerStatus, ObjectMeta, Pod, PodSpec, PodStatus,
};
use nanocloud::nanocloud::k8s::table::TableRow;
use nanocloud::nanocloud::server::handlers::{
    list_pods_all, PodTableRow, WatchParams, TABLE_CONTENT_TYPE,
};

fn running_status(name: &str, ready: bool, restart_count: u32) -> ContainerStatus {
    ContainerStatus {
        name: name.to_string(),
        ready,
        restart_count,
        ..Default::default()
    }
}

fn waiting_status(name: &str, reason: &str, restart_count: u32) -> ContainerStatus {
    let mut state = HashMap::new();
    state.insert("waiting".to_string(), json!({ "reason": reason }));
    ContainerStatus {
        name: name.to_string(),
        ready: false,
        restart_count,
        state: Some(state),
        ..Default::default()
    }
}

fn terminated_status(name: &str, reason: &str, restart_count: u32) -> ContainerStatus {
    let mut state = HashMap::new();
    state.insert(
        "terminated".to_string(),
        json!({ "reason": reason, "exitCode": 143 }),
    );
    ContainerStatus {
        name: name.to_string(),
        ready: false,
        restart_count,
        state: Some(state),
        ..Default::default()
    }
}

fn pod_with_statuses(
    name: &str,
    namespace: Option<&str>,
    statuses: Vec<ContainerStatus>,
    start_offset: ChronoDuration,
    phase: Option<&str>,
) -> Pod {
    let metadata = ObjectMeta {
        name: Some(name.to_string()),
        namespace: namespace.map(|value| value.to_string()),
        resource_version: Some("99".to_string()),
        ..Default::default()
    };

    let containers: Vec<ContainerSpec> = statuses
        .iter()
        .map(|status| ContainerSpec {
            name: status.name.clone(),
            ..Default::default()
        })
        .collect();

    let status = PodStatus {
        phase: phase.map(|value| value.to_string()),
        start_time: Some((Utc::now() - start_offset).to_rfc3339()),
        container_statuses: statuses,
        ..Default::default()
    };

    let spec = PodSpec {
        containers,
        ..Default::default()
    };

    Pod {
        api_version: "v1".to_string(),
        kind: "Pod".to_string(),
        metadata,
        spec,
        status: Some(status),
    }
}

#[test]
fn row_counts_ready_containers_multi_state() {
    let pod = pod_with_statuses(
        "multi-ready",
        Some("demo"),
        vec![
            running_status("app", true, 0),
            running_status("sidecar", true, 0),
            waiting_status("debug", "ContainerCreating", 0),
        ],
        ChronoDuration::seconds(420),
        Some("Running"),
    );
    let now = Utc::now();
    let row: TableRow = PodTableRow::from_pod(&pod, now).into();
    let values: Vec<String> = row
        .cells
        .iter()
        .map(|value| value.as_str().unwrap_or("").to_string())
        .collect();
    assert_eq!(values[0], "demo");
    assert_eq!(values[1], "multi-ready");
    assert_eq!(values[2], "2/3");
    assert_eq!(values[3], "ContainerCreating");
    assert_eq!(values[4], "0");
}

#[test]
fn row_sums_restart_counts_across_containers() {
    let pod = pod_with_statuses(
        "restart-heavy",
        Some("ops"),
        vec![
            running_status("app", true, 1),
            terminated_status("worker", "Error", 4),
            waiting_status("helper", "CrashLoopBackOff", 2),
        ],
        ChronoDuration::seconds(900),
        Some("Running"),
    );
    let now = Utc::now();
    let row: TableRow = PodTableRow::from_pod(&pod, now).into();
    let values: Vec<String> = row
        .cells
        .iter()
        .map(|value| value.as_str().unwrap_or("").to_string())
        .collect();
    assert_eq!(values[2], "1/3");
    assert_eq!(values[3], "CrashLoopBackOff");
    assert_eq!(values[4], "7");
}

#[test]
fn row_defaults_namespace_to_default() {
    let pod = pod_with_statuses(
        "cluster-pod",
        None,
        vec![running_status("app", true, 0)],
        ChronoDuration::seconds(120),
        Some("Running"),
    );
    let now = Utc::now();
    let row: TableRow = PodTableRow::from_pod(&pod, now).into();
    let namespace = row
        .cells
        .first()
        .and_then(|value| value.as_str())
        .unwrap_or("");
    assert_eq!(namespace, "default");
}

fn kubelet_guard() -> &'static Mutex<()> {
    static GUARD: OnceLock<Mutex<()>> = OnceLock::new();
    GUARD.get_or_init(|| Mutex::new(()))
}

#[tokio::test]
async fn api_smoke_negotiates_table_output() {
    let _lock = kubelet_guard().lock().await;
    let mut headers = HeaderMap::new();
    headers.insert(header::ACCEPT, HeaderValue::from_static(TABLE_CONTENT_TYPE));

    let response = list_pods_all(axum::extract::Query(WatchParams::default()), headers)
        .await
        .expect("list pods request succeeds");

    assert_eq!(response.status(), StatusCode::OK);
    let content_type = response
        .headers()
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default()
        .to_string();
    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body bytes");
    let table: PodTable = serde_json::from_slice(&body).expect("table json");
    assert_eq!(content_type, TABLE_CONTENT_TYPE);
    assert_eq!(table.kind, "Table");
}
