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
