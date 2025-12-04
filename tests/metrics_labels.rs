use nanocloud::nanocloud::observability::metrics;

#[test]
fn metrics_apply_standard_labels_and_units() {
    metrics::record_policy_error_classification(None, "", "boom");
    metrics::record_proxy_error_classification(None, "", "boom");
    metrics::record_restart(None, "", "manual");
    let _ = metrics::observe_policy_operation(None, "", metrics::PolicyOperation::Sync, || {
        Ok::<_, ()>(())
    });

    let body = metrics::gather().expect("metrics encoded");
    let text = String::from_utf8(body).expect("utf8 metrics");

    assert!(
        text.contains("nanocloud_policy_operation_duration_seconds"),
        "duration histograms should use _seconds units"
    );
    assert!(text.contains("namespace=\"default\""));
    assert!(text.contains("pod=\"unknown\"") || text.contains("service=\"unknown\""));
    assert!(text.contains("restarts_total"));
    assert!(text.contains("service=\"unknown\""));
}
