use futures_util::FutureExt;
use nanocloud::nanocloud::observability::health::{
    readiness_report_with, HealthDependencies, HealthStatus,
};

#[tokio::test]
async fn readiness_reports_ready_when_dependencies_succeed() {
    let dependencies = HealthDependencies::default()
        .with_bridge_check(Box::new(|| async { Ok(None) }.boxed()))
        .with_proxy_check(Box::new(|| Ok(())))
        .with_runtime_check(Box::new(|| Ok(())))
        .with_kubelet_check(Box::new(|| async { Ok(()) }.boxed()));

    let report = readiness_report_with(&dependencies).await;
    assert_eq!(report.status, HealthStatus::Ready);
}

#[tokio::test]
async fn readiness_reports_degraded_when_dependency_fails() {
    let dependencies = HealthDependencies::default()
        .with_bridge_check(Box::new(|| async { Ok(None) }.boxed()))
        .with_proxy_check(Box::new(|| Err("proxy unavailable".to_string())))
        .with_runtime_check(Box::new(|| Ok(())))
        .with_kubelet_check(Box::new(|| async { Ok(()) }.boxed()));

    let report = readiness_report_with(&dependencies).await;
    assert_eq!(report.status, HealthStatus::Degraded);
    assert!(report
        .components
        .iter()
        .any(|component| component.name == "service_proxy" && !component.healthy));
}
