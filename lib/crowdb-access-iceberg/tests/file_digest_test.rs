use crowdb_access_iceberg::file::{FileDigest, FileIdentity, TableLocation};
use crowdb_access_iceberg::key::{CatalogId, FileId, TableId};
use sha2::{Digest, Sha256};

fn owner() -> FileIdentity {
    FileIdentity {
        table: TableLocation {
            catalog: CatalogId::random(),
            table: TableId::random(),
        },
        file: FileId::random(),
    }
}

#[test]
fn resumable_digest_matches_rustcrypto_at_every_padding_and_update_boundary() {
    let owner = owner();
    for length in [0, 1, 55, 56, 57, 63, 64, 65, 127, 128, 129, 4096, 65536] {
        let bytes: Vec<_> = (0..length)
            .map(|index| u8::try_from(index % 251).unwrap())
            .collect();
        for step in [1, 7, 55, 64, 65, 1024] {
            let mut digest = FileDigest::new(owner);
            for chunk in bytes.chunks(step) {
                digest.update(chunk).unwrap();
                let checkpoint = digest.checkpoint();
                assert_eq!(checkpoint.len(), 189);
                digest = FileDigest::restore(owner, &checkpoint).unwrap();
            }
            assert_eq!(digest.length(), bytes.len() as u64);
            assert_eq!(digest.finish(), <[u8; 32]>::from(Sha256::digest(&bytes)));
        }
    }
}

#[test]
fn digest_checkpoints_bind_file_identity_and_detect_every_changed_byte() {
    let owner = owner();
    let mut digest = FileDigest::new(owner);
    digest.update(&[42; 100]).unwrap();
    let checkpoint = digest.checkpoint();
    for offset in 0..checkpoint.len() {
        let mut corrupt = checkpoint.clone();
        corrupt[offset] ^= 1;
        assert!(FileDigest::restore(owner, &corrupt).is_err());
    }
    assert!(FileDigest::restore(
        FileIdentity {
            file: FileId::random(),
            ..owner
        },
        &checkpoint
    )
    .is_err());
    assert!(FileDigest::restore(owner, &checkpoint[..188]).is_err());
    let mut extra = checkpoint.clone();
    extra.push(0);
    assert!(FileDigest::restore(owner, &extra).is_err());
    let mut corrupt = checkpoint;
    corrupt[156] = 1;
    let checksum = Sha256::digest(&corrupt[..157]);
    corrupt[157..].copy_from_slice(&checksum);
    assert!(FileDigest::restore(owner, &corrupt).is_err());
}

#[test]
fn digest_empty_updates_and_million_byte_vector_preserve_exact_hash() {
    let owner = owner();
    let mut digest = FileDigest::new(owner);
    for _ in 0..1000 {
        digest.update(&[b'a'; 1000]).unwrap();
        digest.update(&[]).unwrap();
        digest = FileDigest::restore(owner, &digest.checkpoint()).unwrap();
    }
    assert_eq!(
        digest.finish(),
        <[u8; 32]>::from(Sha256::digest(vec![b'a'; 1_000_000]))
    );
}
