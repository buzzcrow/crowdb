// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_protocol::port::alloc;
use crowdb_protocol::port::namespace::RuntimeNamespace;
use crowdb_protocol::ServicePort;

#[test]
fn namespaces_have_disjoint_ports_and_paths() {
    let mut first = RuntimeNamespace::ephemeral("protocol-namespace").expect("create first namespace");
    let mut second = RuntimeNamespace::ephemeral("protocol-namespace").expect("create second namespace");

    let first_port = first
        .assign_port(ServicePort::ChunkKvRpc, 0)
        .expect("assign first port");
    let second_port = second
        .assign_port(ServicePort::ChunkKvRpc, 0)
        .expect("assign second port");
    assert_ne!(first.id(), second.id());
    assert_ne!(first.root(), second.root());
    assert_ne!(first_port, second_port);
}

#[test]
fn logical_assignment_is_stable_within_namespace() {
    let mut namespace = RuntimeNamespace::ephemeral("stable-assignment").expect("create namespace");
    let first = namespace
        .assign_port(ServicePort::DiskdbRpc, 7)
        .expect("assign port");
    let reopened = namespace
        .assign_port(ServicePort::DiskdbRpc, 7)
        .expect("reopen assignment");
    assert_eq!(first, reopened);

    let service = namespace
        .service_dir("diskdb", "owner-7")
        .expect("create service directory");
    assert!(service.join("data").is_dir());
    assert!(service.join("log").is_dir());
}

#[test]
fn access_listener_allocations_skip_claimed_and_occupied_ports() {
    for service in [
        ServicePort::AccessServerHttp,
        ServicePort::AccessServerIcebergHttp,
        ServicePort::AccessServerDatasetHttp,
    ] {
        let mut first = RuntimeNamespace::ephemeral("access-claimed").unwrap();
        let claimed = first.assign_port(service, 0).unwrap();
        let mut second = RuntimeNamespace::ephemeral("access-next").unwrap();
        let next = second.assign_port(service, 0).unwrap();
        assert_ne!(claimed, next, "an unbound claimed port must be skipped");

        let occupied = std::net::TcpListener::bind(("127.0.0.1", claimed)).unwrap();
        drop(first);
        let mut third = RuntimeNamespace::ephemeral("access-occupied").unwrap();
        let port = third.assign_port(service, 0).unwrap();
        assert_ne!(port, occupied.local_addr().unwrap().port());
        assert_ne!(port, next);
        assert!((service.base()..service.base() + service.range_size()).contains(&port));
    }
}

#[test]
fn stopped_listener_namespaces_release_ports_before_process_exit() {
    let service = ServicePort::AccessServerIcebergHttp;
    let mut seen = std::collections::HashSet::new();
    for _ in 0..=service.range_size() {
        let mut listener = RuntimeNamespace::ephemeral("short-lived-access").unwrap();
        let port = listener.assign_port(service, 0).unwrap();
        seen.insert(port);
    }
    assert!(seen.len() <= usize::from(service.range_size()));
}

#[test]
fn persistent_namespace_reopens_saved_assignments() {
    let ephemeral = RuntimeNamespace::ephemeral("persistent-parent").expect("create parent namespace");
    let root = ephemeral.root().join("durable");
    let first_port = {
        let mut persistent =
            RuntimeNamespace::persistent(&root, "mini-cluster").expect("create persistent namespace");
        persistent
            .assign_port(ServicePort::KvServerMgmt, 0)
            .expect("assign persistent port")
    };

    let mut reopened =
        RuntimeNamespace::persistent(&root, "mini-cluster").expect("reopen persistent namespace");
    assert_eq!(
        reopened
            .assign_port(ServicePort::KvServerMgmt, 0)
            .expect("reuse persistent port"),
        first_port
    );
    reopened.delete().expect("delete persistent namespace");
}

#[test]
fn compatibility_allocator_shares_the_namespace_registry() {
    alloc::reset_test_claims();
    let legacy = alloc::alloc_test_port(ServicePort::Web);
    let mut namespace = RuntimeNamespace::ephemeral("allocator-compatibility").expect("create namespace");
    let namespaced = namespace
        .assign_port(ServicePort::Web, 0)
        .expect("assign namespaced port");
    assert_ne!(legacy, namespaced);
    alloc::reset_test_claims();
}

#[test]
#[cfg(target_os = "linux")]
fn descendant_launchers_record_processes_in_the_parent_namespace() {
    use crowdb_protocol::port::namespace::record_workspace_process;
    if let Some(workspace) = std::env::var_os("CROWDB_NAMESPACE_DESCENDANT_TEST_ROOT") {
        let workspace = std::path::Path::new(&workspace);
        let mut cluster = RuntimeNamespace::persistent(workspace, "descendant-cluster").unwrap();
        cluster.assign_port(ServicePort::ChunkKvRpc, 0).unwrap();
        record_workspace_process(workspace, std::process::id()).unwrap();
        return;
    }
    let namespace = RuntimeNamespace::ephemeral("descendant-owner").unwrap();
    let workspace = namespace.data_dir();
    let child = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "descendant_launchers_record_processes_in_the_parent_namespace",
            "--nocapture",
        ])
        .env("CROWDB_NAMESPACE_DESCENDANT_TEST_ROOT", workspace)
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .unwrap();
    let pid = child.id();
    let output = child.wait_with_output().unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let record = namespace
        .root()
        .join(format!("processes/{pid}/process-owner.json"));
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
    let value = loop {
        if let Ok(bytes) = std::fs::read(&record) {
            if let Ok(value) = serde_json::from_slice::<serde_json::Value>(&bytes) {
                break value;
            }
        }
        assert!(
            std::time::Instant::now() < deadline,
            "descendant process ownership record becomes readable"
        );
        std::thread::sleep(std::time::Duration::from_millis(10));
    };
    assert_eq!(value["processes"][0]["pid"], pid);
    let cluster = RuntimeNamespace::persistent(namespace.data_dir(), "descendant-cluster").unwrap();
    let claims: serde_json::Value = serde_json::from_slice(
        &std::fs::read(crowdb_protocol::port::namespace::runtime_root().join("ports/claims.json")).unwrap(),
    )
    .unwrap();
    let claim = claims
        .as_array()
        .unwrap()
        .iter()
        .find(|claim| claim["namespace_id"] == cluster.id())
        .expect("child launcher claim survives until parent cleanup");
    assert_eq!(claim["mode"], "ephemeral");
    assert_eq!(claim["owner_pid"], std::process::id());
}

#[test]
fn workspace_process_records_are_isolated_and_never_enroll_persistent_namespaces() {
    use crowdb_protocol::port::namespace::record_workspace_process;
    let namespace = RuntimeNamespace::ephemeral("workspace-owner").unwrap();
    let workspace = namespace.root().join("N-1/services");
    std::fs::create_dir_all(&workspace).unwrap();
    let pid = std::process::id();
    record_workspace_process(&workspace, pid).unwrap();
    let record = namespace
        .root()
        .join(format!("processes/{pid}/process-owner.json"));
    let value: serde_json::Value = serde_json::from_slice(&std::fs::read(record).unwrap()).unwrap();
    assert_eq!(value["processes"][0]["pid"], pid);
    assert!(value["processes"][0]["start"].as_str().is_some());
    let mut manifest: serde_json::Value =
        serde_json::from_slice(&std::fs::read(namespace.root().join("namespace.json")).unwrap()).unwrap();
    manifest["mode"] = serde_json::json!("persistent");
    std::fs::write(
        namespace.root().join("namespace.json"),
        serde_json::to_vec(&manifest).unwrap(),
    )
    .unwrap();
    std::fs::remove_dir_all(namespace.root().join("processes")).unwrap();
    record_workspace_process(&workspace, pid).unwrap();
    assert!(!namespace.root().join("processes").exists());
}
