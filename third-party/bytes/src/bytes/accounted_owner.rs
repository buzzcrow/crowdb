// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the MIT license, like the bytes crate.

use super::{Bytes, Vtable};
use crate::loom::sync::atomic::{AtomicPtr, AtomicUsize, Ordering};
use crate::BytesMut;
use alloc::boxed::Box;
use alloc::vec::Vec;
use core::alloc::Layout;
use core::mem::ManuallyDrop;
use core::ptr::NonNull;

/// Allocation admission for the control block created by
/// [`Bytes::try_from_owner_accounted`]. The exact allocator layout is supplied;
/// cloning and slicing never create another control block or another claim.
/// Methods must not panic. Claims must fail without acquiring any charge.
pub trait OwnerAllocation: Send + Sync + 'static {
    /// Admission refusal returned before allocating the control block.
    type Error;
    /// Claim the exact control-block size before its allocation.
    fn claim(&self, layout: Layout) -> Result<(), Self::Error>;
    /// Observe the exact allocation after creation or after deallocation.
    fn record(&self, _layout: Layout, _acquired: bool) {}
    /// Release only after the owner has dropped and the block was deallocated.
    fn release(&self, layout: Layout);
}

#[repr(C)]
struct Accounted<T, A> {
    references: AtomicUsize,
    owner: ManuallyDrop<T>,
    allocation: ManuallyDrop<A>,
}

impl<T, A: OwnerAllocation> Accounted<T, A> {
    const VTABLE: Vtable = Vtable {
        owner_allocation: Some(owner_allocation::<T, A>),
        clone: clone::<T, A>,
        into_vec: into_vec::<T, A>,
        into_mut: into_mut::<T, A>,
        is_unique: is_unique,
        drop: drop_bytes::<T, A>,
    };
}

impl Bytes {
    /// Transfer a pinned immutable owner and admit the exact control allocation.
    /// The accounting object is stored inline in that block. Admission failure
    /// drops both inputs. Final release runs after the block is deallocated,
    /// including when owner destruction or `AsRef` unwinds.
    ///
    /// Like [`Bytes::from_owner`], conversion into mutable bytes copies the data.
    pub fn try_from_owner_accounted<T, A>(owner: T, allocation: A) -> Result<Self, A::Error>
    where
        T: AsRef<[u8]> + Send + 'static,
        A: OwnerAllocation,
    {
        allocation.claim(Layout::new::<Accounted<T, A>>())?;
        let storage = Box::into_raw(Box::new(Accounted {
            references: AtomicUsize::new(1),
            owner: ManuallyDrop::new(owner),
            allocation: ManuallyDrop::new(allocation),
        }));
        unsafe { &*storage }
            .allocation
            .record(Layout::new::<Accounted<T, A>>(), true);
        let mut bytes = Bytes {
            ptr: NonNull::dangling().as_ptr(),
            len: 0,
            data: AtomicPtr::new(storage.cast()),
            vtable: &Accounted::<T, A>::VTABLE,
        };
        // Store ownership in Bytes before calling AsRef so an unwind releases
        // both the owner and the fully constructed control block.
        let value = unsafe { &*storage }.owner.as_ref();
        bytes.ptr = value.as_ptr();
        bytes.len = value.len();
        Ok(bytes)
    }
}

unsafe fn clone<T, A: OwnerAllocation>(data: &AtomicPtr<()>, ptr: *const u8, len: usize) -> Bytes {
    let storage = data.load(Ordering::Relaxed);
    let references = &(*storage.cast::<Accounted<T, A>>()).references;
    if references.fetch_add(1, Ordering::Relaxed) > usize::MAX >> 1 {
        crate::abort();
    }
    Bytes {
        ptr,
        len,
        data: AtomicPtr::new(storage),
        vtable: &Accounted::<T, A>::VTABLE,
    }
}

// Dropping this stack guard refunds even if the owner's destructor unwinds.
// It is declared before Box so unwinding deallocates Box before the refund.
struct Refund<A: OwnerAllocation> {
    allocation: A,
    layout: Layout,
}
impl<A: OwnerAllocation> Drop for Refund<A> {
    fn drop(&mut self) {
        self.allocation.record(self.layout, false);
        self.allocation.release(self.layout);
    }
}

unsafe fn release<T, A: OwnerAllocation>(storage: *mut ()) {
    let storage = storage.cast::<Accounted<T, A>>();
    let references = &(*storage).references;
    let previous = references.fetch_sub(1, Ordering::Release);
    debug_assert!(previous > 0 && previous <= usize::MAX >> 1);
    if previous != 1 {
        return;
    }
    references.load(Ordering::Acquire);
    let refund = Refund {
        allocation: ManuallyDrop::take(&mut (*storage).allocation),
        layout: Layout::new::<Accounted<T, A>>(),
    };
    let mut storage = Box::from_raw(storage);
    // The owner stays at its pinned address through destruction. Moving only
    // the accounting value is safe: it was never exposed through AsRef.
    ManuallyDrop::drop(&mut storage.owner);
    drop(storage);
    drop(refund);
}

unsafe fn drop_bytes<T, A: OwnerAllocation>(
    data: &mut AtomicPtr<()>,
    _ptr: *const u8,
    _len: usize,
) {
    release::<T, A>(data.load(Ordering::Relaxed));
}
unsafe fn into_vec<T, A: OwnerAllocation>(
    data: &AtomicPtr<()>,
    ptr: *const u8,
    len: usize,
) -> Vec<u8> {
    let result = core::slice::from_raw_parts(ptr, len).to_vec();
    release::<T, A>(data.load(Ordering::Relaxed));
    result
}
unsafe fn into_mut<T, A: OwnerAllocation>(
    data: &AtomicPtr<()>,
    ptr: *const u8,
    len: usize,
) -> BytesMut {
    BytesMut::from_vec(into_vec::<T, A>(data, ptr, len))
}
unsafe fn is_unique(_data: &AtomicPtr<()>) -> bool {
    false
}

unsafe fn owner_allocation<T, A: OwnerAllocation>(
    data: &AtomicPtr<()>,
    action: super::owner_lease::Action,
    layout: Layout,
) -> bool {
    let storage = data.load(Ordering::Relaxed).cast::<Accounted<T, A>>();
    let allocation = &(*storage).allocation;
    match action {
        super::owner_lease::Action::Claim => return allocation.claim(layout).is_ok(),
        super::owner_lease::Action::Allocated => allocation.record(layout, true),
        super::owner_lease::Action::Freed => allocation.record(layout, false),
        super::owner_lease::Action::Release => allocation.release(layout),
    }
    true
}
