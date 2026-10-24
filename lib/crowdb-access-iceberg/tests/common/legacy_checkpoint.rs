use std::sync::Arc;

use crowdb_access_iceberg::file::{
    ChunkDirectory, ChunkEntry, FileBlockStore, FileDigest, FileIdentity, FileTree, FileTreeWriter,
    FileWriterCheckpoint,
};

pub struct TestLegacyCheckpoint;

impl TestLegacyCheckpoint {
    pub async fn create(
        store: Arc<dyn FileBlockStore>,
        owner: FileIdentity,
        bytes: &[u8],
        block_bytes: usize,
    ) -> (FileWriterCheckpoint, FileTree) {
        let mut writer = FileTreeWriter::new(store.clone(), owner, block_bytes).unwrap();
        writer.push(bytes).await.unwrap();
        let tree = writer.finish().await.unwrap();
        let mut digest = FileDigest::new(owner);
        digest.update(bytes).unwrap();
        let digest = digest.checkpoint();
        let mut encoded = b"ICFW\x01".to_vec();
        encoded.extend_from_slice(&u16::try_from(digest.len()).unwrap().to_be_bytes());
        encoded.extend_from_slice(&digest);
        // The persisted v1 layout contains nine frontier slots.
        for level in 0..=8 {
            let frontier = tree
                .root
                .as_ref()
                .filter(|root| root.height == level)
                .map(|root| {
                    ChunkDirectory {
                        owner,
                        height: level + 1,
                        entries: vec![ChunkEntry {
                            length: tree.length,
                            root: root.clone(),
                        }],
                    }
                    .encode()
                    .unwrap()
                })
                .unwrap_or_default();
            encoded.extend_from_slice(&u32::try_from(frontier.len()).unwrap().to_be_bytes());
            encoded.extend_from_slice(&frontier);
        }
        let root = store.put(owner, 0, &encoded).await.unwrap();
        (FileWriterCheckpoint { root }, tree)
    }
}
