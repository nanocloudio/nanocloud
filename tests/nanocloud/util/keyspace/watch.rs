use nanocloud::nanocloud::test_support::keyspace_lock;
use nanocloud::nanocloud::util::{Keyspace, KeyspaceEventType};
use std::env;
use std::fs;
use std::sync::Once;

fn ensure_test_keyspace() {
    static INIT: Once = Once::new();
    INIT.call_once(|| {
        let dir = tempfile::tempdir().expect("tempdir");
        let base = dir.path().to_path_buf();
        let keyspace_dir = base.join("keyspace");
        let lock_dir = base.join("lock");
        fs::create_dir_all(&keyspace_dir).expect("keyspace dir");
        fs::create_dir_all(&lock_dir).expect("lock dir");
        let lock_file = lock_dir.join("nanocloud.lock");
        env::set_var(
            "NANOCLOUD_KEYSPACE",
            keyspace_dir.to_str().expect("utf-8 keyspace"),
        );
        env::set_var(
            "NANOCLOUD_LOCK_FILE",
            lock_file.to_str().expect("utf-8 lock"),
        );
        // Leak the directory to keep the temporary paths alive for the duration of the test run.
        Box::leak(Box::new(dir));
    });
}

#[tokio::test]
async fn watch_emits_events_in_order() {
    {
        let _lock = keyspace_lock().lock();
        ensure_test_keyspace();
    }
    let keyspace = Keyspace::new("watch-events");

    let mut watch = keyspace.watch("/", None);

    {
        let _lock = keyspace_lock().lock();
        keyspace.put("/alpha", "one").expect("put alpha");
    }
    let added = watch.next().await.expect("added event");
    assert_eq!(added.event_type, KeyspaceEventType::Added);
    assert_eq!(added.key, "/alpha");
    assert_eq!(added.value.as_deref(), Some("one"));

    {
        let _lock = keyspace_lock().lock();
        keyspace.put("/alpha", "uno").expect("update alpha");
    }
    let modified = watch.next().await.expect("modified event");
    assert_eq!(modified.event_type, KeyspaceEventType::Modified);
    assert_eq!(modified.key, "/alpha");
    assert_eq!(modified.value.as_deref(), Some("uno"));
    assert!(modified.resource_version > added.resource_version);

    {
        let _lock = keyspace_lock().lock();
        keyspace.delete("/alpha").expect("delete alpha");
    }
    let deleted = watch.next().await.expect("deleted event");
    assert_eq!(deleted.event_type, KeyspaceEventType::Deleted);
    assert_eq!(deleted.key, "/alpha");
    assert!(deleted.value.is_none());
    assert!(deleted.resource_version > modified.resource_version);

}

#[tokio::test]
async fn watch_resumes_from_resource_version() {
    {
        let _lock = keyspace_lock().lock();
        ensure_test_keyspace();
    }
    let keyspace = Keyspace::new("watch-resume");

    {
        let _lock = keyspace_lock().lock();
        keyspace.put("/foo/item", "one").expect("put foo/item");
        keyspace.put("/foo/item", "two").expect("update foo/item");
        keyspace.put("/bar/other", "else").expect("put bar/other");
    }

    let mut initial = keyspace.watch("/", None);
    let first = initial.next().await.expect("first event");
    let second = initial.next().await.expect("second event");
    assert_eq!(first.key, "/foo/item");
    assert_eq!(first.event_type, KeyspaceEventType::Added);
    assert_eq!(second.event_type, KeyspaceEventType::Modified);

    let resume_version = second.resource_version;

    let mut resumed = keyspace.watch("/", Some(resume_version));
    let resumed_event = resumed.next().await.expect("resumed event");
    assert_eq!(resumed_event.key, "/bar/other");
    assert_eq!(resumed_event.event_type, KeyspaceEventType::Added);
    assert!(resumed_event.resource_version > resume_version);

    let mut foo_watch = keyspace.watch("/foo", None);
    let foo_first = foo_watch.next().await.expect("foo added");
    let foo_second = foo_watch.next().await.expect("foo modified");
    assert_eq!(foo_first.key, "/foo/item");
    assert_eq!(foo_first.event_type, KeyspaceEventType::Added);
    assert_eq!(foo_second.event_type, KeyspaceEventType::Modified);
}
