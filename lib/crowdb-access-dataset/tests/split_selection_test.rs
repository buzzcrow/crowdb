// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
use crowdb_access_dataset::split::*;

fn weights() -> Vec<PartitionWeight> {
    [("train", 80), ("validation", 10), ("test", 10)]
        .into_iter()
        .map(|(name, weight)| PartitionWeight {
            name: SplitName::new(name).unwrap(),
            weight,
        })
        .collect()
}
#[test]
fn partition_vectors_match_independent_python_sha256_integer_arithmetic() {
    // Also computed with Python hashlib and arbitrary precision multiplication.
    for (seed, id, bucket, wide) in [
        (0, b"a".as_slice(), 49, 2_137_339_266),
        (1, b"a".as_slice(), 94, 4_077_257_936),
        (0, &[0, 255, 0], 89, 3_832_633_626),
        (u64::MAX, b"sample-42", 24, 1_070_817_225),
    ] {
        assert_eq!(partition_bucket(seed, id, 100).unwrap(), bucket);
        assert_eq!(partition_bucket(seed, id, u32::MAX).unwrap(), wide);
    }
    assert_eq!(
        SamplePartition::new(0, weights())
            .unwrap()
            .assign(b"a")
            .unwrap()
            .as_str(),
        "train"
    );
    assert_eq!(
        SamplePartition::new(1, weights())
            .unwrap()
            .assign(b"a")
            .unwrap()
            .as_str(),
        "test"
    );
    assert_eq!(
        SamplePartition::new(0, weights())
            .unwrap()
            .assign(&[0, 255, 0])
            .unwrap()
            .as_str(),
        "validation"
    );
}
#[test]
fn partition_is_order_independent_and_rejects_invalid_intervals() {
    let first = SamplePartition::new(42, weights()).unwrap();
    let restarted = SamplePartition::new(42, weights()).unwrap();
    let ids: Vec<_> = (0u32..1000).map(u32::to_be_bytes).collect();
    let assignments: Vec<_> = ids.iter().map(|id| first.assign(id).unwrap().clone()).collect();
    for (id, expected) in ids.iter().zip(assignments).rev() {
        assert_eq!(restarted.assign(id).unwrap(), &expected);
    }
    assert!(SamplePartition::new(0, vec![]).is_err());
    let mut zero = weights();
    zero[0].weight = 0;
    assert!(SamplePartition::new(0, zero).is_err());
    let mut overflow = weights();
    overflow[0].weight = u32::MAX;
    assert!(SamplePartition::new(0, overflow).is_err());
    let mut duplicate = weights();
    duplicate[1].name = duplicate[0].name.clone();
    assert!(SamplePartition::new(0, duplicate).is_err());
    assert!(partition_bucket(0, b"", 100).is_err());
    assert!(partition_bucket(0, b"a", 0).is_err());
}
#[test]
fn policy_tracks_coverage_and_overlap_independently_with_bounded_examples() {
    let mut report = SplitValidation::default();
    report.observe(b"assigned", 1).unwrap();
    for id in 0u32..30 {
        report.observe(&id.to_be_bytes(), 0).unwrap();
    }
    for id in 30u32..60 {
        report.observe(&id.to_be_bytes(), 2).unwrap();
    }
    assert_eq!(
        (
            report.scanned,
            report.assigned,
            report.unassigned,
            report.overlapping
        ),
        (61, 31, 30, 30)
    );
    assert_eq!(report.unassigned_examples.len(), 20);
    assert_eq!(report.overlap_examples.len(), 20);
    assert!(!report.satisfies(SplitPolicy::default()));
    assert!(report.satisfies(SplitPolicy {
        overlap: OverlapPolicy::AllowOverlap,
        coverage: CoveragePolicy::AllowUnassigned
    }));
    assert!(!report.satisfies(SplitPolicy {
        overlap: OverlapPolicy::AllowOverlap,
        coverage: CoveragePolicy::RequireComplete
    }));
    let mut large = SplitValidation::default();
    for _ in 0..10 {
        large.observe(&vec![255; 16 * 1024], 0).unwrap();
    }
    assert_eq!(large.unassigned_examples.len(), 2);
    let mut complete = SplitValidation::default();
    complete.observe(b"a", 1).unwrap();
    assert!(complete.satisfies(SplitPolicy {
        overlap: OverlapPolicy::Disjoint,
        coverage: CoveragePolicy::RequireComplete
    }));
}
