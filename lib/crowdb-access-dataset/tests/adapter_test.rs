use crowdb_access_dataset::{BoundedPrefetch, WorkerPartition};

#[test]
fn worker_partition_is_stable_across_ranks_and_workers() {
    let ids: Vec<_> = (0..12).collect();
    let first = WorkerPartition {
        worker: 0,
        workers: 2,
        rank: 0,
        world_size: 2,
    }
    .select(ids.clone())
    .unwrap();
    let second = WorkerPartition {
        worker: 1,
        workers: 2,
        rank: 0,
        world_size: 2,
    }
    .select(ids.clone())
    .unwrap();
    let third = WorkerPartition {
        worker: 0,
        workers: 2,
        rank: 1,
        world_size: 2,
    }
    .select(ids.clone())
    .unwrap();
    let fourth = WorkerPartition {
        worker: 1,
        workers: 2,
        rank: 1,
        world_size: 2,
    }
    .select(ids)
    .unwrap();
    assert_eq!(first, vec![0, 4, 8]);
    assert_eq!(second, vec![1, 5, 9]);
    assert_eq!(third, vec![2, 6, 10]);
    assert_eq!(fourth, vec![3, 7, 11]);
}

#[test]
fn bounded_prefetch_rejects_overflow_until_consumer_drains() {
    let mut queue = BoundedPrefetch::new(2).unwrap();
    assert!(queue.push(1).is_ok());
    assert!(queue.push(2).is_ok());
    assert_eq!(queue.push(3), Err(3));
    assert_eq!(queue.pop(), Some(1));
    assert!(queue.push(3).is_ok());
}
