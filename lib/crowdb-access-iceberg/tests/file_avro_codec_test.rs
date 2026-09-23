use std::io::Write;

use crowdb_access_iceberg::file::{AvroBlock, AvroCodec, AvroContainerError, FormatHint};
use flate2::{write::DeflateEncoder, Compression};

fn block(encoded: Vec<u8>) -> AvroBlock {
    AvroBlock {
        records: 1,
        payload: FormatHint {
            offset: 0,
            length: encoded.len() as u64,
        },
        encoded,
    }
}

#[test]
fn avro_required_codecs_preserve_bytes_and_enforce_independent_expansion_limits() {
    assert_eq!(AvroCodec::parse("null").unwrap(), AvroCodec::Null);
    assert_eq!(AvroCodec::parse("deflate").unwrap(), AvroCodec::Deflate);
    assert!(AvroCodec::parse("gzip").is_err());
    assert!(AvroCodec::parse("DEFLATE").is_err());
    let hello = vec![0xcb, 0x48, 0xcd, 0xc9, 0xc9, 0x07, 0x00];
    assert_eq!(block(hello).decode(AvroCodec::Deflate, 5).unwrap(), b"hello");
    for plain in [vec![], vec![1], vec![42; 65536]] {
        let mut compressor = DeflateEncoder::new(Vec::new(), Compression::default());
        compressor.write_all(&plain).unwrap();
        let encoded = compressor.finish().unwrap();
        let limit = plain.len().max(1);
        assert_eq!(
            block(encoded.clone()).decode(AvroCodec::Deflate, limit).unwrap(),
            plain
        );
        assert_eq!(
            block(plain.clone()).decode(AvroCodec::Null, limit).unwrap(),
            plain
        );
        if plain.len() > 1 {
            assert!(matches!(
                block(encoded).decode(AvroCodec::Deflate, limit - 1),
                Err(AvroContainerError::Bounds)
            ));
            assert!(matches!(
                block(plain).decode(AvroCodec::Null, limit - 1),
                Err(AvroContainerError::Bounds)
            ));
        }
    }
}

#[test]
fn avro_deflate_rejects_truncation_trailing_streams_and_invalid_limits() {
    let encoded = vec![0xcb, 0x48, 0xcd, 0xc9, 0xc9, 0x07, 0x00];
    for end in 0..encoded.len() {
        assert!(block(encoded[..end].to_vec())
            .decode(AvroCodec::Deflate, 100)
            .is_err());
    }
    for extra in [vec![0], encoded.clone()] {
        let mut bytes = encoded.clone();
        bytes.extend(extra);
        assert!(block(bytes).decode(AvroCodec::Deflate, 100).is_err());
    }
    for limit in [0, 8 * 1024 * 1024 + 1, usize::MAX] {
        assert!(matches!(
            block(encoded.clone()).decode(AvroCodec::Deflate, limit),
            Err(AvroContainerError::Bounds)
        ));
    }
    assert!(block(vec![255; 10]).decode(AvroCodec::Deflate, 100).is_err());
}
