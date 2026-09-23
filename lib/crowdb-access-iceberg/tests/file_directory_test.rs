use crowdb_access_iceberg::file::{
    ChunkDirectory, ChunkEntry, ChunkRoot, FileIdentity, TableLocation, MAX_DIRECTORY_ENTRIES,
};
use crowdb_access_iceberg::key::{CatalogId, FileId, TableId};
use crowdb_protocol::common::ChunkId;

fn directory(count: usize) -> ChunkDirectory {
    ChunkDirectory {
        owner: FileIdentity {
            table: TableLocation {
                catalog: CatalogId::random(),
                table: TableId::random(),
            },
            file: FileId::random(),
        },
        height: 1,
        entries: (0..count)
            .map(|index| ChunkEntry {
                length: 100,
                root: ChunkRoot {
                    chunk: ChunkId {
                        high: 1,
                        low: index as u64,
                    },
                    offset: 0,
                    physical_length: 256,
                    logical_offset: 0,
                    logical_length: 100,
                    height: 0,
                    digest: [3; 32],
                },
            })
            .collect(),
    }
}

#[test]
fn directories_bind_every_child_to_a_bounded_height_owner_and_span() {
    for count in [1, 2, MAX_DIRECTORY_ENTRIES] {
        let node = directory(count);
        let bytes = node.encode().unwrap();
        assert!(bytes.len() <= 32 * 1024);
        assert_eq!(
            ChunkDirectory::decode(&bytes, node.owner, node.height, node.length().unwrap()).unwrap(),
            node
        );
        let foreign = FileIdentity {
            file: FileId::random(),
            ..node.owner
        };
        assert!(ChunkDirectory::decode(&bytes, foreign, node.height, node.length().unwrap()).is_err());
        assert!(ChunkDirectory::decode(&bytes, node.owner, node.height + 1, node.length().unwrap()).is_err());
        assert!(ChunkDirectory::decode(&bytes, node.owner, node.height, node.length().unwrap() + 1).is_err());
        for length in 0..bytes.len() {
            assert!(ChunkDirectory::decode(
                &bytes[..length],
                node.owner,
                node.height,
                node.length().unwrap()
            )
            .is_err());
        }
    }
    assert!(directory(0).encode().is_err());
    assert!(directory(MAX_DIRECTORY_ENTRIES + 1).encode().is_err());
}

#[test]
fn directories_reject_invalid_addresses_versions_children_and_overflow() {
    let node = directory(2);
    for corruption in 0..7 {
        let mut broken = node.clone();
        match corruption {
            0 => broken.height = 0,
            1 => broken.height = 9,
            2 => broken.entries[0].length = 0,
            3 => broken.entries[0].root.height = 1,
            4 => broken.entries[0].root.chunk = ChunkId::default(),
            5 => broken.entries[0].root.offset = u64::MAX,
            _ => {
                broken.entries[0].length = u64::MAX;
                broken.entries[0].root.logical_length = u64::MAX;
            }
        }
        assert!(broken.encode().is_err());
    }
    let mut bytes = node.encode().unwrap();
    bytes[4] = 2;
    assert!(ChunkDirectory::decode(&bytes, node.owner, 1, 200).is_err());
    bytes = node.encode().unwrap();
    bytes.push(0);
    assert!(ChunkDirectory::decode(&bytes, node.owner, 1, 200).is_err());
    assert!(ChunkDirectory::decode(&vec![0; 32 * 1024 + 1], node.owner, 1, 200).is_err());
}
