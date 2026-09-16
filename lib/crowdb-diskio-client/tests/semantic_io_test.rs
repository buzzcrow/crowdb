// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use crowdb_diskio_client::{
    DiskId, DiskioClient, DiskioClientConfig, DiskioError, Durability, SegmentTarget,
};
use crowdb_test_harness::cluster::KvCluster;
use crowdb_test_harness::diskio::{check_binaries, connect_to_diskio, DiskioProcess, DiskioStartOpts};
use crowdb_test_harness::hardware::{seed_hardware, standard_disk_ids_4, UNIT_SIZE_BYTES};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[allow(clippy::too_many_lines)]
async fn semantic_client_owns_route_transport_payload_and_durability() {
    if !check_binaries() {
        return;
    }
    let cluster = KvCluster::start().await;
    let hardware = cluster.make_hardware_client();
    seed_hardware(&hardware, &standard_disk_ids_4()).await;
    let diskio = DiskioProcess::start(&DiskioStartOpts {
        dummy_disk: "mem",
        kv_seeds: &cluster.mgmt_endpoints,
        disks: &[],
        fault_error_rate: 0.0,
        fault_latency_ms: Some((200, 200)),
        no_o_direct: false,
    });
    let (wire_server, wire_connection, wire_client) = connect_to_diskio(&diskio);
    diskio
        .wait_for_disks(&wire_client, &wire_server, &wire_connection)
        .await;
    let disk_id = DiskId::new(0, 1);
    let client = Arc::new(
        DiskioClient::connect_with_clients(
            cluster.make_service_registry_client(),
            hardware,
            DiskioClientConfig {
                normal_connections_per_endpoint: 2,
                priority_connections_per_endpoint: 1,
                max_pending_calls: 1,
                retry_attempts: 20,
                default_timeout: Duration::from_secs(5),
                ..DiskioClientConfig::default()
            },
        )
        .await
        .expect("connect semantic client"),
    );
    let target = SegmentTarget::new(disk_id, 0, 0, 4, UNIT_SIZE_BYTES).expect("target");
    let buffered_payload = Bytes::from_static(b"buffered-semantic-payload");
    client
        .write(
            target,
            0,
            buffered_payload.clone(),
            Durability::Buffered,
            client.normal_options(),
        )
        .await
        .expect("buffered semantic write");
    assert_eq!(client.status().fsync_operations, 0);

    let payload = Bytes::from_static(b"durable-semantic-payload");

    client
        .write(
            target,
            4096,
            payload.clone(),
            Durability::Fsync,
            client.normal_options(),
        )
        .await
        .expect("durable semantic write");
    let read = client
        .read(
            target,
            4096,
            u32::try_from(payload.len()).expect("payload length"),
            client.normal_options(),
        )
        .await
        .expect("semantic read");
    assert_eq!(read, payload);

    let view_payload = [
        Bytes::from_static(b"bounded-"),
        Bytes::from_static(b"scatter-"),
        Bytes::from_static(b"gather"),
    ];
    client
        .write_views(
            target,
            8192,
            view_payload.to_vec(),
            Durability::Buffered,
            client.normal_options(),
        )
        .await
        .expect("view-chain semantic write");
    let view_read = client
        .read(target, 8192, 22, client.normal_options())
        .await
        .expect("view-chain semantic read");
    assert_eq!(view_read, Bytes::from_static(b"bounded-scatter-gather"));

    let priority_read = client
        .read(
            target,
            0,
            u32::try_from(buffered_payload.len()).expect("payload length"),
            client.normal_options().priority(),
        )
        .await
        .expect("priority semantic read");
    assert_eq!(priority_read, buffered_payload);

    let reads_before_invalid = client.status().read_operations;
    let invalid = client
        .read(target, target.capacity(), 1, client.normal_options())
        .await;
    assert!(matches!(invalid, Err(DiskioError::InvalidInput(_))));
    assert_eq!(client.status().read_operations, reads_before_invalid);

    let bad_zone = SegmentTarget::new(disk_id, 99, 0, 1, UNIT_SIZE_BYTES).expect("bad-zone target");
    let permanent = client
        .write(
            bad_zone,
            0,
            Bytes::from_static(b"permanent"),
            Durability::Buffered,
            client.normal_options(),
        )
        .await;
    assert!(matches!(permanent, Err(DiskioError::DiskFailure(_))));

    let status = client.status();
    assert_eq!(status.disks, 4);
    assert_eq!(status.endpoints, 1);
    assert_eq!(status.normal_connections, 2);
    assert_eq!(status.priority_connections, 1);
    assert_eq!(status.inflight, 0);
    assert_eq!(status.write_operations, 4);
    assert_eq!(status.fsync_operations, 1);
    assert_eq!(status.retries, 0);
    assert!(status.read_average_us > 0);
    assert!(status.write_average_us > 0);
    assert!(status.fsync_average_us > 0);

    let saturated_client = Arc::clone(&client);
    let saturated_normal = tokio::spawn(async move {
        saturated_client
            .read(target, 12_288, 16, saturated_client.normal_options())
            .await
    });
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert!(matches!(
        client.read(target, 12_288, 16, client.normal_options()).await,
        Err(DiskioError::Backpressure(_))
    ));
    assert_eq!(client.status().admission_rejections, 1);
    client
        .read(target, 12_288, 16, client.normal_options().priority())
        .await
        .expect("priority admission remains independent");
    saturated_normal
        .await
        .expect("saturated normal task")
        .expect("first normal request completes");

    let old_generation = status.route_generation;
    let delayed_client = Arc::clone(&client);
    let delayed_read = tokio::spawn(async move {
        delayed_client
            .read(target, 16_384, 16, delayed_client.normal_options())
            .await
    });
    tokio::time::sleep(Duration::from_millis(20)).await;
    drop(diskio);
    wire_server.stop();

    let replacement = DiskioProcess::start(&DiskioStartOpts {
        dummy_disk: "mem",
        kv_seeds: &cluster.mgmt_endpoints,
        disks: &[],
        fault_error_rate: 0.0,
        fault_latency_ms: None,
        no_o_direct: false,
    });
    let (replacement_wire_server, replacement_connection, replacement_wire_client) =
        connect_to_diskio(&replacement);
    replacement
        .wait_for_disks(
            &replacement_wire_client,
            &replacement_wire_server,
            &replacement_connection,
        )
        .await;
    let service = cluster.make_service_registry_client();
    let heartbeat_deadline = std::time::Instant::now() + Duration::from_secs(5);
    loop {
        let current = service
            .read_instance("diskio", crowdb_test_harness::hardware::INSTANCE_ID)
            .await
            .expect("read replacement DiskIO heartbeat");
        if current.is_some_and(|instance| instance.rpc_endpoint.ends_with(&format!(":{}", replacement.port)))
        {
            break;
        }
        assert!(
            std::time::Instant::now() < heartbeat_deadline,
            "replacement DiskIO did not publish its endpoint"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    client.refresh().await.expect("publish replacement endpoint");
    assert!(client.status().route_generation > old_generation);
    assert_eq!(client.status().normal_connections, 2);
    assert_eq!(client.status().priority_connections, 1);
    assert!(matches!(
        delayed_read.await.expect("delayed read task"),
        Err(DiskioError::DeadlineExceeded | DiskioError::TransportUnavailable(_))
    ));
    let replacement_read = client
        .read(target, 16_384, 16, client.normal_options())
        .await
        .expect("new generation remains usable after old failure");
    assert_eq!(replacement_read, Bytes::from_static(&[0; 16]));
    drop(client);
    drop(replacement);
    replacement_wire_server.stop();

    let faulty_diskio = DiskioProcess::start(&DiskioStartOpts {
        dummy_disk: "mem",
        kv_seeds: &cluster.mgmt_endpoints,
        disks: &[],
        fault_error_rate: 1.0,
        fault_latency_ms: None,
        no_o_direct: false,
    });
    let (fault_wire_server, fault_connection, fault_wire_client) = connect_to_diskio(&faulty_diskio);
    faulty_diskio
        .wait_for_disks(&fault_wire_client, &fault_wire_server, &fault_connection)
        .await;
    let service = cluster.make_service_registry_client();
    let heartbeat_deadline = std::time::Instant::now() + Duration::from_secs(5);
    loop {
        let current = service
            .read_instance("diskio", crowdb_test_harness::hardware::INSTANCE_ID)
            .await
            .expect("read faulty DiskIO heartbeat");
        if current.is_some_and(|instance| {
            instance
                .rpc_endpoint
                .ends_with(&format!(":{}", faulty_diskio.port))
        }) {
            break;
        }
        assert!(
            std::time::Instant::now() < heartbeat_deadline,
            "faulty DiskIO did not publish its endpoint"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let faulty = DiskioClient::connect_with_clients(
        service,
        cluster.make_hardware_client(),
        DiskioClientConfig::default(),
    )
    .await
    .expect("connect fault-injected semantic client");
    let failure_target = SegmentTarget::new(disk_id, 0, 0, 1, UNIT_SIZE_BYTES).expect("failure target");
    assert!(matches!(
        faulty
            .write(
                failure_target,
                0,
                Bytes::from_static(b"fault"),
                Durability::Buffered,
                faulty.normal_options(),
            )
            .await,
        Err(DiskioError::DiskFailure(_))
    ));
    assert!(matches!(
        faulty.read(failure_target, 0, 5, faulty.normal_options()).await,
        Err(DiskioError::DiskFailure(_))
    ));
    assert!(matches!(
        faulty.fsync(disk_id, faulty.normal_options()).await,
        Err(DiskioError::DiskFailure(_))
    ));
    assert_eq!(faulty.status().retries, 0);
    fault_wire_server.stop();
}
