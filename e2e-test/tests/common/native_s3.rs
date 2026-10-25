// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Native S3 consumer using only live management, metadata and storage services.

pub mod multipart;
pub mod signed_copy;
pub mod transport;

use std::num::NonZeroU64;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use crowdb_access_s3::metadata::TenantId;
use crowdb_access_s3::storage::{S3LargeWriteSettings, S3WriteSettings};
use crowdb_chunk_client::SmallWritePolicy;
use crowdb_protocol::chunkdb::rpc::ChunkType;
use crowdb_protocol::frame::{MAX_FRAME_BYTES, MAX_FRAME_V2_PAYLOAD_BYTES};
use crowdb_sdk_native::s3::{
    GetOptions, ObjectMetadata, PutObjectInfo, PutOptions, S3Client, S3ClientConfig, S3WritePolicy,
};
use crowdb_sdk_native::{ClientRuntime, OperationContext, RuntimeConfig};

pub fn context() -> OperationContext {
    static NEXT: AtomicU64 = AtomicU64::new(1);
    OperationContext::new(
        NonZeroU64::new(NEXT.fetch_add(1, Ordering::Relaxed)).unwrap(),
        Instant::now() + Duration::from_secs(30),
    )
}

pub fn payload(length: usize) -> Vec<u8> {
    (0..length)
        .map(|offset| u8::try_from((offset * 13 + length) % 251).unwrap())
        .collect()
}

pub struct TestNativeS3 {
    pub runtime: ClientRuntime,
    pub client: S3Client,
    pub bucket: &'static [u8],
}

impl TestNativeS3 {
    pub async fn connect(addresses: &[String], tenant: &[u8]) -> Self {
        let artifacts = std::env::var("CROWDB_E2E_ARTIFACTS").unwrap();
        crowdb_rpc_ffi::init_logging(&artifacts, "debug", 30, 5, "native-client");
        crowdb_rpc_ffi::add_log_stderr("error");
        let policies = S3WriteSettings {
            small: SmallWritePolicy {
                mirror_copies: 2,
                conversion_enabled: false,
                memory_budget: 64 << 20,
                max_pipelines: 4,
                ..SmallWritePolicy::new(ChunkType::S3)
            },
            threshold_ratio: 0.5,
            disk_block_bytes: 1 << 20,
            ec_data: 2,
            ec_code: 1,
            large: S3LargeWriteSettings {
                max_chunk_size: Some(4 << 20),
                memory_budget_bytes: Some(64 << 20),
                ..S3LargeWriteSettings::default()
            },
        }
        .policies()
        .unwrap();
        let mut config = RuntimeConfig::new(
            addresses[..3]
                .iter()
                .map(|ip| super::management::origin(ip))
                .collect(),
        );
        config.small_write = policies.small;
        config.limits.held_bytes = 8 << 20;
        config.limits.producer_bytes = 8 << 20;
        config.data_rdma = Some(transport::configuration());
        let runtime = ClientRuntime::connect(config).await.unwrap();
        let mut config = S3ClientConfig::from(TenantId::new(tenant.to_vec()).unwrap());
        config.write_policy = Some(S3WritePolicy {
            small_object_limit: policies.small_threshold,
            large_write: policies.large,
        });
        let client = S3Client::new(runtime.clone(), config).unwrap();
        Self {
            runtime,
            client,
            bucket: if transport::enabled()
                && std::env::var("CROWDB_NATIVE_S3_EXECUTION").as_deref() == Ok("oci")
            {
                b"native-corpus-oci"
            } else {
                b"native-corpus"
            },
        }
    }

    pub async fn put(&self, key: &[u8], data: Vec<u8>) -> ObjectMetadata {
        self.client
            .put_vec(context(), self.bucket, key, data, PutOptions::default())
            .await
            .unwrap()
    }

    pub async fn put_framed(&self, key: &[u8], length: usize) -> ObjectMetadata {
        let before = transport::data_status(&self.runtime);
        let mut writer = self
            .client
            .begin_put(
                context(),
                self.bucket,
                key,
                PutOptions {
                    object: PutObjectInfo {
                        content_length: Some(length as u64),
                        ..PutObjectInfo::default()
                    },
                    ..PutOptions::default()
                },
            )
            .await
            .unwrap();
        let full = 16 * MAX_FRAME_V2_PAYLOAD_BYTES;
        for start in (0..length).step_by(full) {
            let count = (length - start).min(full);
            let frozen = self
                .runtime
                .allocate_producer(
                    u32::try_from(count.div_ceil(MAX_FRAME_V2_PAYLOAD_BYTES) * MAX_FRAME_BYTES).unwrap(),
                )
                .unwrap()
                .initialize_framed(count, 0, |offset, bytes| {
                    for (index, byte) in bytes.iter_mut().enumerate() {
                        *byte = u8::try_from(((start + offset + index) * 13 + length) % 251).unwrap();
                    }
                })
                .unwrap();
            assert_eq!(frozen.registration_id() != 0, transport::enabled());
            writer.write_framed(frozen).await.unwrap();
        }
        let result = writer.finish().await.unwrap();
        if transport::enabled() {
            let after = transport::data_status(&self.runtime);
            let direct = after["rdma"]["direct_payload_bytes"].as_u64().unwrap()
                - before["rdma"]["direct_payload_bytes"].as_u64().unwrap();
            assert!(
                direct >= length as u64,
                "registered producer did not send directly: {after}"
            );
            if length == 513 {
                assert_eq!(
                    after["rdma"]["copied_payload_bytes"],
                    before["rdma"]["copied_payload_bytes"]
                );
            }
        }
        result
    }

    pub async fn read(&self, key: &[u8], options: GetOptions) -> (ObjectMetadata, Vec<u8>) {
        let mut get = self
            .client
            .get_object(context(), self.bucket, key, &options)
            .await
            .unwrap_or_else(|error| panic!("native GET {} failed: {error}", String::from_utf8_lossy(key)));
        let mut data = Vec::new();
        let mut sequence = 0;
        let start = options.range.map_or(0, |range| range.start);
        while let Some(lease) = get.body.next().await.unwrap() {
            let metadata = lease.metadata();
            assert_eq!(metadata.sequence, sequence);
            assert_eq!(metadata.logical_offset, start + data.len() as u64);
            assert_eq!(metadata.length, lease.as_slice().len());
            assert!(metadata.allocation_capacity >= metadata.length);
            if transport::enabled() && metadata.registration_id != 0 {
                assert!(matches!(
                    metadata.allocation_capacity,
                    65_536 | 262_144 | 1_048_576
                ));
            }
            data.extend_from_slice(lease.as_slice());
            sequence += 1;
        }
        assert!(get.body.is_complete());
        assert!(get.body.next().await.unwrap().is_none());
        (get.metadata, data)
    }

    pub async fn assert_pressure_and_shutdown(&self) {
        let resources = self.runtime.resources().clone();
        let held = resources.retain_vec(vec![0; 8 << 20], 1, 1, 0, 0).unwrap();
        let mut get = self
            .client
            .get_object(context(), self.bucket, b"framed-small", &GetOptions::default())
            .await
            .unwrap();
        assert!(tokio::time::timeout(Duration::from_millis(20), get.body.next())
            .await
            .is_err());
        assert_eq!(resources.status().held_bytes, 8 << 20);
        drop(held);
        let lease = get.body.next().await.unwrap().unwrap();
        assert_eq!(lease.as_slice(), payload(513));
        assert_eq!(lease.metadata().registration_id != 0, transport::enabled());
        assert!(get.body.next().await.unwrap().is_none());
        let (release, wait) = tokio::sync::oneshot::channel();
        let copy = tokio::spawn(async move {
            wait.await.unwrap();
            lease.as_slice().to_vec()
        });
        self.runtime.shutdown().await.unwrap();
        assert!(resources.status().held_bytes > 0);
        release.send(()).unwrap();
        assert_eq!(copy.await.unwrap(), payload(513));
        assert_eq!(resources.status().held_bytes, 0);
        assert_eq!(resources.status().requests, 0);
    }
}
