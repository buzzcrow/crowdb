// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::args::{Operation, PaxosKvArgs, ReadMode as Mode};
use crowdb_paxos_kv_client::{GetOutcome, PaxosKvClient, ReadMode};
use futures::{stream, StreamExt, TryStreamExt};
use std::sync::Arc;

pub(super) fn key(id: u64) -> String {
    format!("k{id:020}")
}
pub(super) fn size(args: &PaxosKvArgs, id: u64) -> usize {
    if args.mixed_values {
        match id % 10 {
            0..=6 => 64,
            7..=8 => 1024,
            _ => 16384,
        }
    } else {
        args.value_size
    }
}
fn value(args: &PaxosKvArgs, id: u64) -> Vec<u8> {
    (0..size(args, id))
        .map(|offset| id.wrapping_add(offset as u64).to_le_bytes()[0])
        .collect()
}
fn verify_value(args: &PaxosKvArgs, id: u64, actual: &[u8]) -> Result<(), String> {
    if actual.len() != size(args, id)
        || actual
            .iter()
            .enumerate()
            .any(|(offset, b)| *b != id.wrapping_add(offset as u64).to_le_bytes()[0])
    {
        return Err(format!("incorrect bytes for key {}", key(id)));
    }
    Ok(())
}

pub(super) async fn prepare(client: Arc<PaxosKvClient>, args: Arc<PaxosKvArgs>) -> Result<(), String> {
    stream::iter(0..args.key_space)
        .map(|id| {
            let client = client.clone();
            let args = args.clone();
            async move {
                client
                    .put(
                        args.store,
                        args.group,
                        key(id).as_bytes(),
                        &value(&args, id),
                        None,
                    )
                    .await
                    .map(|_| ())
                    .map_err(|e| e.to_string())
            }
        })
        .buffer_unordered(args.load.loader_num)
        .try_collect::<Vec<_>>()
        .await?;
    verify(client, args).await
}
pub(super) async fn verify(client: Arc<PaxosKvClient>, args: Arc<PaxosKvArgs>) -> Result<(), String> {
    stream::iter(0..args.key_space)
        .map(|id| {
            let client = client.clone();
            let args = args.clone();
            async move {
                match client
                    .get(
                        args.store,
                        args.group,
                        key(id).as_bytes(),
                        ReadMode::Linearizable,
                        None,
                    )
                    .await
                    .map_err(|e| e.to_string())?
                {
                    GetOutcome::Found { value, .. } => verify_value(&args, id, &value),
                    GetOutcome::NotFound => Err(format!("missing key {}", key(id))),
                }
            }
        })
        .buffer_unordered(args.load.loader_num.max(16))
        .try_collect::<Vec<_>>()
        .await?;
    Ok(())
}

pub(super) async fn operation(client: &PaxosKvClient, args: &PaxosKvArgs, id: u64) -> Result<u64, String> {
    let mode = match args.read_mode {
        Mode::Linearizable => ReadMode::Linearizable,
        Mode::Minslot => ReadMode::MinSlot,
    };
    match args.operation {
        Operation::Write => {
            client
                .put(args.store, args.group, key(id).as_bytes(), &value(args, id), None)
                .await
                .map_err(|e| e.to_string())?;
            Ok(size(args, id) as u64)
        }
        Operation::Read => match client
            .get(args.store, args.group, key(id).as_bytes(), mode, Some(0))
            .await
            .map_err(|e| e.to_string())?
        {
            GetOutcome::Found { value, .. } if value.len() == size(args, id) => Ok(value.len() as u64),
            GetOutcome::Found { .. } => Err("incorrect value length".into()),
            GetOutcome::NotFound => Err(format!("missing key {}", key(id))),
        },
        Operation::Scan => scan(client, args, mode, false).await,
        Operation::Prepare => Err("prepare cannot be timed".into()),
    }
}

pub(super) async fn scan(
    client: &PaxosKvClient,
    args: &PaxosKvArgs,
    mode: ReadMode,
    verify: bool,
) -> Result<u64, String> {
    // Keep scans inside this bounded dataset even after a larger earlier run.
    let upper = key(args.key_space);
    let prefix = if args.scan_prefix.is_empty() {
        "k"
    } else {
        &args.scan_prefix
    };
    let start = if args.reverse && (args.scan_start_after.is_empty() || args.scan_start_after > upper) {
        &upper
    } else {
        &args.scan_start_after
    };
    let end = if args.reverse { "" } else { &upper };
    let result = if args.reverse {
        client
            .scan_reverse(
                args.store,
                args.group,
                prefix.as_bytes(),
                start.as_bytes(),
                end.as_bytes(),
                args.scan_limit,
                mode,
                Some(0),
                false,
                None,
            )
            .await
    } else {
        client
            .scan(
                args.store,
                args.group,
                prefix.as_bytes(),
                start.as_bytes(),
                end.as_bytes(),
                args.scan_limit,
                mode,
                Some(0),
                false,
                None,
            )
            .await
    }
    .map_err(|e| e.to_string())?;
    if result.items.is_empty() {
        return Err("scan returned no items; choose a nonempty dataset range".into());
    }
    if result.timed_out {
        return Err("partial timed-out scan".into());
    }
    if verify {
        let mut ids: Vec<_> = (0..args.key_space)
            .filter(|id| {
                let k = key(*id);
                k.starts_with(&args.scan_prefix)
                    && (args.scan_start_after.is_empty()
                        || if args.reverse {
                            k < args.scan_start_after
                        } else {
                            k > args.scan_start_after
                        })
            })
            .collect();
        if args.reverse {
            ids.reverse();
        }
        let expected_len = ids.len().min(args.scan_limit as usize);
        if result.items.len() != expected_len || result.truncated != (ids.len() > expected_len) {
            return Err("scan count/truncated mismatch".into());
        }
        for ((k, v), id) in result.items.iter().zip(ids) {
            if k.as_ref() != key(id).as_bytes() {
                return Err("scan key ordering/bound mismatch".into());
            }
            verify_value(args, id, v)?;
        }
    }
    Ok(result.items.iter().map(|(k, v)| (k.len() + v.len()) as u64).sum())
}
