// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the MIT license, like the bytes crate.

use super::Bytes;
use core::alloc::Layout;
use core::fmt;

pub(crate) enum Action {
    Claim,
    Allocated,
    Freed,
    Release,
}

/// The immutable owner's budget could not admit another control allocation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OwnerAllocationError;
impl fmt::Display for OwnerAllocationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("owner allocation budget exhausted")
    }
}

/// An admitted control allocation sharing the byte owner's accounting domain.
/// The lease retains the owner without allocating another control block. Drop
/// it only after deallocating the associated storage. Clones and slices of the
/// original bytes do not create leases implicitly.
pub struct OwnerAllocationLease {
    owner: Bytes,
    layout: Layout,
    allocated: bool,
}
impl fmt::Debug for OwnerAllocationLease {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OwnerAllocationLease")
            .field("layout", &self.layout)
            .field("allocated", &self.allocated)
            .finish()
    }
}
impl OwnerAllocationLease {
    /// Mark the capacity as physically allocated after allocation succeeds.
    /// This operation is idempotent. Dropping an unmarked lease refunds only
    /// its admission claim, such as when the external allocator refuses it.
    pub fn allocated(&mut self) {
        if !self.allocated {
            self.invoke(Action::Allocated);
            self.allocated = true;
        }
    }

    fn invoke(&self, action: Action) {
        let operation = self.owner.vtable.owner_allocation.unwrap();
        unsafe { operation(&self.owner.data, action, self.layout) };
    }
}
impl Drop for OwnerAllocationLease {
    fn drop(&mut self) {
        if self.allocated {
            self.invoke(Action::Freed);
        }
        self.invoke(Action::Release);
    }
}

impl Bytes {
    /// Admit an external control allocation using this immutable owner's
    /// budget. `None` identifies bytes created through ordinary constructors;
    /// their external allocation policy remains the caller's responsibility.
    /// `Err` means an accounted owner refused the claim before allocation.
    ///
    /// The returned lease must be marked with `allocated` after allocation and
    /// destroyed only after that storage is deallocated. It retains the byte
    /// owner and works after other byte clones have been dropped.
    pub fn try_owner_allocation(
        &self,
        layout: Layout,
    ) -> Result<Option<OwnerAllocationLease>, OwnerAllocationError> {
        let operation = match self.vtable.owner_allocation {
            Some(operation) => operation,
            None => return Ok(None),
        };
        if !unsafe { operation(&self.data, Action::Claim, layout) } {
            return Err(OwnerAllocationError);
        }
        Ok(Some(OwnerAllocationLease {
            owner: self.clone(),
            layout,
            allocated: false,
        }))
    }
}

/// Borrowed accounting callbacks for an external allocator. The backing Bytes
/// must remain live until every successful claim has been released. Claims and
/// physical records must pair exactly, with free records after deallocation.
#[derive(Debug, Clone, Copy)]
pub struct OwnerAllocationContext<'a> {
    owner: &'a Bytes,
}
impl OwnerAllocationContext<'_> {
    /// Claim one exact allocation layout before invoking its allocator.
    pub fn claim(self, layout: Layout) -> Result<(), OwnerAllocationError> {
        if self.invoke(Action::Claim, layout) {
            Ok(())
        } else {
            Err(OwnerAllocationError)
        }
    }
    /// Record actual creation or completed deallocation of the claimed layout.
    pub fn record(self, layout: Layout, acquired: bool) {
        self.invoke(
            if acquired {
                Action::Allocated
            } else {
                Action::Freed
            },
            layout,
        );
    }
    /// Refund the corresponding claim after actual deallocation or refusal by
    /// the underlying allocator. Does not emit a physical allocation record.
    pub fn release(self, layout: Layout) {
        self.invoke(Action::Release, layout);
    }
    fn invoke(self, action: Action, layout: Layout) -> bool {
        let operation = self.owner.vtable.owner_allocation.unwrap();
        unsafe { operation(&self.owner.data, action, layout) }
    }
}
impl Bytes {
    /// Borrow this immutable owner's allocator callbacks without allocating.
    /// Ordinary Bytes constructors return None; the caller owns their policy.
    pub fn owner_allocation(&self) -> Option<OwnerAllocationContext<'_>> {
        self.vtable
            .owner_allocation
            .map(|_| OwnerAllocationContext { owner: self })
    }
}
