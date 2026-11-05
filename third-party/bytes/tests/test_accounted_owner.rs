use bytes::{Bytes, OwnerAllocation};
use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::ptr;
use std::sync::atomic::{AtomicBool, AtomicPtr, AtomicUsize, Ordering};
use std::sync::Arc;

thread_local! { static CLAIMED: Cell<*const State> = const { Cell::new(ptr::null()) }; }
#[global_allocator]
static TRACKED: Tracked = Tracked;
static ALLOCATION: AtomicPtr<u8> = AtomicPtr::new(ptr::null_mut());
static OBSERVER: AtomicPtr<State> = AtomicPtr::new(ptr::null_mut());
struct Tracked;
unsafe impl GlobalAlloc for Tracked {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let result = System.alloc(layout);
        let _ = CLAIMED.try_with(|state| {
            let state = state.replace(ptr::null());
            if !state.is_null() {
                assert_eq!((*state).bytes.load(Ordering::SeqCst), layout.size());
                OBSERVER.store(state.cast_mut(), Ordering::SeqCst);
                ALLOCATION.store(result, Ordering::SeqCst);
            }
        });
        result
    }
    unsafe fn dealloc(&self, data: *mut u8, layout: Layout) {
        let tracked = ALLOCATION
            .compare_exchange(data, ptr::null_mut(), Ordering::SeqCst, Ordering::SeqCst)
            .is_ok();
        System.dealloc(data, layout);
        if tracked {
            (*OBSERVER.swap(ptr::null_mut(), Ordering::SeqCst))
                .freed
                .store(true, Ordering::SeqCst);
        }
    }
}

#[derive(Default)]
struct State {
    bytes: AtomicUsize,
    claims: AtomicUsize,
    releases: AtomicUsize,
    owner_drops: AtomicUsize,
    freed: AtomicBool,
    refuse: bool,
}
struct Account(Arc<State>);
impl OwnerAllocation for Account {
    type Error = ();
    fn claim(&self, layout: Layout) -> Result<(), ()> {
        if self.0.refuse {
            return Err(());
        }
        self.0.claims.fetch_add(1, Ordering::SeqCst);
        self.0.bytes.store(layout.size(), Ordering::SeqCst);
        CLAIMED.with(|state| state.set(Arc::as_ptr(&self.0)));
        Ok(())
    }
    fn release(&self, layout: Layout) {
        assert!(
            self.0.freed.load(Ordering::SeqCst),
            "refund ran before actual deallocation"
        );
        assert_eq!(self.0.owner_drops.load(Ordering::SeqCst), 1);
        assert_eq!(self.0.bytes.swap(0, Ordering::SeqCst), layout.size());
        self.0.releases.fetch_add(1, Ordering::SeqCst);
    }
}
struct Owner {
    data: [u8; 64],
    state: Arc<State>,
    panic_read: bool,
    panic_drop: bool,
}
impl AsRef<[u8]> for Owner {
    fn as_ref(&self) -> &[u8] {
        assert!(!self.panic_read, "expected AsRef panic");
        &self.data
    }
}
impl Drop for Owner {
    fn drop(&mut self) {
        self.state.owner_drops.fetch_add(1, Ordering::SeqCst);
        assert!(!self.panic_drop, "expected owner Drop panic");
    }
}
fn make(state: &Arc<State>, panic_read: bool, panic_drop: bool) -> Result<Bytes, ()> {
    Bytes::try_from_owner_accounted(
        Owner {
            data: [7; 64],
            state: state.clone(),
            panic_read,
            panic_drop,
        },
        Account(state.clone()),
    )
}
fn released(state: &State) {
    assert_eq!(state.claims.load(Ordering::SeqCst), 1);
    assert_eq!(state.releases.load(Ordering::SeqCst), 1);
    assert_eq!(state.bytes.load(Ordering::SeqCst), 0);
}

// One test owns the allocator observation slot; all paths, including a final
// clone on another thread, verify the free-before-refund ordering directly.
#[test]
fn exact_layout_and_free_before_refund_cover_every_owner_exit() {
    let state = Arc::new(State::default());
    let bytes = make(&state, false, false).unwrap();
    let slice = bytes.slice(8..32);
    drop(bytes);
    assert_eq!(state.releases.load(Ordering::SeqCst), 0);
    std::thread::spawn(move || assert_eq!(slice.as_ref(), &[7; 24]))
        .join()
        .unwrap();
    released(&state);

    let state = Arc::new(State::default());
    let bytes = make(&state, false, false).unwrap();
    assert_eq!(Vec::from(bytes), vec![7; 64]);
    released(&state);

    let state = Arc::new(State::default());
    let result = std::panic::catch_unwind(|| make(&state, true, false));
    assert!(result.is_err());
    released(&state);

    let state = Arc::new(State::default());
    let bytes = make(&state, false, true).unwrap();
    let result = std::panic::catch_unwind(|| drop(bytes));
    assert!(result.is_err());
    released(&state);

    let state = Arc::new(State {
        refuse: true,
        ..Default::default()
    });
    assert!(make(&state, false, false).is_err());
    assert_eq!(state.owner_drops.load(Ordering::SeqCst), 1);
    assert_eq!(state.claims.load(Ordering::SeqCst), 0);
    assert_eq!(state.releases.load(Ordering::SeqCst), 0);
    external_control_leases_retain_the_same_account_until_last_refund();
}

#[derive(Default)]
struct Ledger {
    admitted: AtomicUsize,
    allocated: AtomicUsize,
    refuse: AtomicBool,
}
struct SharedAccount(Arc<Ledger>);
impl OwnerAllocation for SharedAccount {
    type Error = ();
    fn claim(&self, layout: Layout) -> Result<(), ()> {
        if self.0.refuse.load(Ordering::SeqCst) {
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
fn external_control_leases_retain_the_same_account_until_last_refund() {
    let ledger = Arc::new(Ledger::default());
    let bytes = Bytes::try_from_owner_accounted([1u8; 32], SharedAccount(ledger.clone())).unwrap();
    let owner_bytes = ledger.allocated.load(Ordering::SeqCst);
    assert_eq!(ledger.admitted.load(Ordering::SeqCst), owner_bytes);
    let layout = Layout::new::<[u8; 17]>();
    let refused_storage = bytes.try_owner_allocation(layout).unwrap().unwrap();
    assert_eq!(ledger.admitted.load(Ordering::SeqCst), owner_bytes + 17);
    assert_eq!(ledger.allocated.load(Ordering::SeqCst), owner_bytes);
    drop(refused_storage);
    assert_eq!(ledger.admitted.load(Ordering::SeqCst), owner_bytes);
    let mut lease = bytes
        .slice(1..)
        .try_owner_allocation(layout)
        .unwrap()
        .unwrap();
    let storage = Box::new([0u8; 17]);
    lease.allocated();
    lease.allocated();
    assert_eq!(ledger.allocated.load(Ordering::SeqCst), owner_bytes + 17);
    let callbacks = bytes.owner_allocation().unwrap();
    callbacks.claim(Layout::new::<u64>()).unwrap();
    let native = Box::new(3u64);
    callbacks.record(Layout::new::<u64>(), true);
    drop(native);
    callbacks.record(Layout::new::<u64>(), false);
    callbacks.release(Layout::new::<u64>());
    ledger.refuse.store(true, Ordering::SeqCst);
    assert!(bytes.try_owner_allocation(layout).is_err());
    assert_eq!(ledger.admitted.load(Ordering::SeqCst), owner_bytes + 17);
    drop(bytes);
    assert_eq!(ledger.allocated.load(Ordering::SeqCst), owner_bytes + 17);
    drop(storage);
    drop(lease);
    assert_eq!(ledger.allocated.load(Ordering::SeqCst), 0);
    assert_eq!(ledger.admitted.load(Ordering::SeqCst), 0);
    assert!(Bytes::from_static(b"ordinary")
        .try_owner_allocation(layout)
        .unwrap()
        .is_none());
}
