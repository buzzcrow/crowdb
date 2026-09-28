// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_access_multipart::{
    reserve_part_accounting, validate_selected_parts, PartAccounting, SelectedPart, StateError,
};

fn selected(number: u16, revision: u64) -> SelectedPart {
    SelectedPart {
        number,
        revision,
        digest: [1; 32],
    }
}

#[test]
fn selected_parts_require_strict_order_and_stable_revisions() {
    assert_eq!(validate_selected_parts(&[], 10), Err(StateError::EmptySelection));
    assert_eq!(
        validate_selected_parts(&[selected(2, 1), selected(2, 2)], 10),
        Err(StateError::InvalidPartNumber)
    );
    assert_eq!(
        validate_selected_parts(&[selected(2, 1), selected(1, 2)], 10),
        Err(StateError::InvalidPartNumber)
    );
    assert_eq!(
        validate_selected_parts(&[selected(1, 0)], 10),
        Err(StateError::InvalidRevision)
    );
    assert_eq!(
        validate_selected_parts(&[selected(2, 1), selected(9, 4)], 10),
        Ok(())
    );
}

#[test]
fn replacing_a_part_preserves_count_and_reclaims_its_old_credit() {
    let current = PartAccounting {
        count: 2,
        staged_bytes: 15,
    };
    let next = reserve_part_accounting(current, Some(10), 8, 3, 20).unwrap();
    assert_eq!(
        next,
        PartAccounting {
            count: 2,
            staged_bytes: 13
        }
    );
    assert_eq!(
        reserve_part_accounting(current, None, 6, 3, 20),
        Err(StateError::StagedLimit)
    );
    assert_eq!(
        reserve_part_accounting(current, Some(16), 1, 3, 20),
        Err(StateError::InvalidAccounting)
    );
}
