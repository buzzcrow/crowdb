use crowdb_common::hash_slot::{HashSlotFallback, Placement};

#[test]
fn stable_slots_and_exact_key_overflow() {
    let index = HashSlotFallback::new(4096).unwrap();
    assert_eq!(index.slot(b"same operation"), index.slot(b"same operation"));
    assert!(index.slot(b"same operation") < 4096);
    assert_eq!(HashSlotFallback::placement(None, b"one"), Placement::Slot);
    assert_eq!(HashSlotFallback::placement(Some(b"one"), b"one"), Placement::Slot);
    assert_eq!(
        HashSlotFallback::placement(Some(b"two"), b"one"),
        Placement::Overflow
    );
    assert!(HashSlotFallback::new(0).is_none());
    assert!(HashSlotFallback::new(4095).is_none());
}
