// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
use bytes::{Bytes, OwnerAllocation};
use crowdb_rpc_ffi::{Buffer, BufferChain, BufferChainError};
use std::{
    alloc::Layout,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};
#[derive(Default)]
struct TestLedger {
    admitted: AtomicUsize,
    allocated: AtomicUsize,
    claims: AtomicUsize,
    refuse: AtomicUsize,
}
struct TestAccount(Arc<TestLedger>);
impl OwnerAllocation for TestAccount {
    type Error = ();
    fn claim(&self, layout: Layout) -> Result<(), ()> {
        let ordinal = self.0.claims.fetch_add(1, Ordering::SeqCst) + 1;
        if ordinal == self.0.refuse.load(Ordering::SeqCst) {
            return Err(());
        }
        self.0.admitted.fetch_add(layout.size(), Ordering::SeqCst);
        Ok(())
    }
    fn record(&self, layout: Layout, acquired: bool) {
        if acquired {
            self.0.allocated.fetch_add(layout.size(), Ordering::SeqCst);
        } else {
            assert!(self.0.allocated.fetch_sub(layout.size(), Ordering::SeqCst) >= layout.size());
        }
    }
    fn release(&self, layout: Layout) {
        assert!(self.0.admitted.fetch_sub(layout.size(), Ordering::SeqCst) >= layout.size());
    }
}
fn owner(ledger: &Arc<TestLedger>) -> Bytes {
    Bytes::try_from_owner_accounted([7u8; 64], TestAccount(ledger.clone())).unwrap()
}
fn released(ledger: &TestLedger) {
    assert_eq!(ledger.allocated.load(Ordering::SeqCst), 0);
    assert_eq!(ledger.admitted.load(Ordering::SeqCst), 0);
}
#[test]
fn accounted_bytes_pay_all_rpc_controls_and_survive_original_owner_release() {
    let ledger = Arc::new(TestLedger::default());
    let bytes = owner(&ledger);
    let base = ledger.allocated.load(Ordering::SeqCst);
    let original = bytes.as_ptr();
    let buffer = Buffer::try_from_owned_bytes(bytes.clone()).unwrap();
    assert_eq!(buffer.as_slice().as_ptr(), original);
    assert_eq!(ledger.claims.load(Ordering::SeqCst), 5); // Bytes, Rust Box, Buffer, ref, handle.
    assert!(ledger.allocated.load(Ordering::SeqCst) > base);
    assert_eq!(
        ledger.allocated.load(Ordering::SeqCst),
        ledger.admitted.load(Ordering::SeqCst)
    );
    drop(bytes);
    assert_eq!(buffer.as_slice(), &[7; 64]);
    drop(buffer);
    released(&ledger);
}
#[test]
fn every_rpc_control_admission_failure_unwinds_the_partial_owner() {
    for refused in 2..=5 {
        let ledger = Arc::new(TestLedger::default());
        let bytes = owner(&ledger);
        ledger.refuse.store(refused, Ordering::SeqCst);
        assert!(Buffer::try_from_owned_bytes(bytes).is_err(), "claim {refused}");
        released(&ledger);
    }
}
#[test]
fn slices_share_bytes_capacity_but_each_rpc_view_pays_its_controls() {
    let ledger = Arc::new(TestLedger::default());
    let bytes = owner(&ledger);
    let first = Buffer::try_from_owned_bytes(bytes.slice(1..9)).unwrap();
    let after_first = ledger.allocated.load(Ordering::SeqCst);
    let second = Buffer::try_from_owned_bytes(bytes.slice(15..19)).unwrap();
    assert!(ledger.allocated.load(Ordering::SeqCst) > after_first);
    drop(bytes);
    drop(first);
    assert_eq!(second.as_slice(), &[7; 4]);
    drop(second);
    released(&ledger);
}
#[test]
fn chain_reports_owner_admission_failure_without_submitting_empty_payload() {
    let ledger = Arc::new(TestLedger::default());
    let bytes = owner(&ledger);
    ledger.refuse.store(2, Ordering::SeqCst);
    assert!(matches!(
        BufferChain::from_owned_bytes([bytes]),
        Err(BufferChainError::OwnerAllocation(_))
    ));
    released(&ledger);
}
