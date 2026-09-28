//! Stable hash-slot selection with exact-key overflow routing.

use xxhash_rust::xxh64::xxh64;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Placement {
    Slot,
    Overflow,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct HashSlotFallback {
    mask: u16,
}

impl HashSlotFallback {
    #[must_use]
    pub const fn new(slots: u16) -> Option<Self> {
        if slots == 0 || !slots.is_power_of_two() {
            return None;
        }
        Some(Self { mask: slots - 1 })
    }

    #[must_use]
    /// # Panics
    /// Panics only if the internal mask exceeds the width of a `u16`.
    pub fn slot(self, identity: &[u8]) -> u16 {
        let slot = xxh64(identity, 0) & u64::from(self.mask);
        u16::try_from(slot).expect("masked slot fits in u16")
    }

    #[must_use]
    pub fn placement(occupant: Option<&[u8]>, identity: &[u8]) -> Placement {
        match occupant {
            None => Placement::Slot,
            Some(existing) if existing == identity => Placement::Slot,
            Some(_) => Placement::Overflow,
        }
    }
}
