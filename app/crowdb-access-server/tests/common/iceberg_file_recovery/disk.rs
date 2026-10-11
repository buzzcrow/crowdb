// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use arc_swap::ArcSwap;
use async_trait::async_trait;
use crowdb_chunk_client::{DiskWriter, IoError, Result};
use crowdb_protocol::diskdb::rpc::Segment;
use hyper::body::Bytes;
use std::{collections::BTreeMap, sync::Arc};

type TestSegments = BTreeMap<(u64, u64, u32, u64), Arc<Vec<u8>>>;

#[derive(Default)]
pub struct TestDisk {
    segments: ArcSwap<TestSegments>,
}

fn key(segment: &Segment) -> (u64, u64, u32, u64) {
    let disk = segment.disk_id.unwrap();
    (disk.high, disk.low, segment.zone_index, segment.unit_offset)
}

#[async_trait]
impl DiskWriter for TestDisk {
    async fn write(&self, segment: &Segment, unit_bytes: u64, data: Bytes) -> Result<()> {
        self.write_at_byte_offset(segment, unit_bytes, 0, data).await
    }

    async fn write_at_byte_offset(
        &self,
        segment: &Segment,
        unit_bytes: u64,
        offset: u64,
        data: Bytes,
    ) -> Result<()> {
        let start = usize::try_from(offset).unwrap();
        let capacity = usize::try_from(u64::from(segment.unit_count) * unit_bytes).unwrap();
        let end = start.checked_add(data.len()).unwrap();
        assert!(end <= capacity, "component write exceeds its allocated segment");
        self.segments.rcu(|current| {
            let mut next = (**current).clone();
            let bytes = next
                .entry(key(segment))
                .or_insert_with(|| Arc::new(vec![0; capacity]));
            Arc::make_mut(bytes)[start..end].copy_from_slice(&data);
            next
        });
        Ok(())
    }

    async fn write_views_at_byte_offset(
        &self,
        segment: &Segment,
        unit_bytes: u64,
        offset: u64,
        data: Vec<Bytes>,
    ) -> Result<()> {
        let bytes: Vec<_> = data.iter().flat_map(|view| view.iter().copied()).collect();
        self.write_at_byte_offset(segment, unit_bytes, offset, Bytes::from(bytes))
            .await
    }

    async fn read(&self, segment: &Segment, _unit_bytes: u64, offset: u64, length: u32) -> Result<Bytes> {
        let snapshot = self.segments.load();
        let bytes = snapshot
            .get(&key(segment))
            .ok_or_else(|| IoError::ReadFailed("segment absent".into()))?;
        let start = usize::try_from(offset).unwrap();
        let end = start.checked_add(length as usize).unwrap();
        let range = bytes
            .get(start..end)
            .ok_or_else(|| IoError::ReadFailed("segment range absent".into()))?;
        Ok(Bytes::copy_from_slice(range))
    }
}
