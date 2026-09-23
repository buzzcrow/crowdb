use std::sync::Arc;

use crowdb_access_iceberg::file::{
    ContentFormat, FileContent, FileIdentity, FileKind, FileRecord, FileTreeWriter, ParquetMetadataLimits,
    TableLocation,
};
use crowdb_access_iceberg::key::{CatalogId, FileId, TableId};

use super::blocks::TestBlocks;

pub type TestFields = Vec<(i16, u8, Vec<u8>)>;

pub fn limits() -> ParquetMetadataLimits {
    ParquetMetadataLimits {
        footer_bytes: 8192,
        values: 2000,
        depth: 32,
        schema_elements: 32,
        row_groups: 32,
    }
}

pub fn number(value: i64) -> Vec<u8> {
    unsigned((value.unsigned_abs() << 1).wrapping_sub(u64::from(value < 0)))
}

pub fn unsigned(mut value: u64) -> Vec<u8> {
    let mut bytes = Vec::new();
    while value > 127 {
        bytes.push(u8::try_from(value & 127).unwrap() | 128);
        value >>= 7;
    }
    bytes.push(u8::try_from(value).unwrap());
    bytes
}

pub fn binary(value: &[u8]) -> Vec<u8> {
    let mut bytes = unsigned(value.len() as u64);
    bytes.extend(value);
    bytes
}

pub fn structure(fields: &TestFields) -> Vec<u8> {
    let mut bytes = Vec::new();
    for (id, kind, value) in fields {
        bytes.push(*kind);
        bytes.extend(number(i64::from(*id)));
        bytes.extend(value);
    }
    bytes.push(0);
    bytes
}

pub fn list(kind: u8, values: &[Vec<u8>]) -> Vec<u8> {
    let mut bytes = if values.len() < 15 {
        vec![(u8::try_from(values.len()).unwrap() << 4) | kind]
    } else {
        let mut bytes = vec![0xf0 | kind];
        bytes.extend(unsigned(values.len() as u64));
        bytes
    };
    for value in values {
        bytes.extend(value);
    }
    bytes
}

pub fn schema() -> Vec<Vec<u8>> {
    vec![
        structure(&vec![(4, 8, binary(b"schema")), (5, 5, number(1))]),
        structure(&vec![
            (1, 5, number(2)),
            (3, 5, number(1)),
            (4, 8, binary(b"value")),
            (9, 5, number(3)),
        ]),
    ]
}

pub fn column() -> TestFields {
    vec![
        (1, 5, number(2)),
        (2, 9, list(5, &[number(0)])),
        (3, 9, list(8, &[binary(b"value")])),
        (4, 5, number(0)),
        (5, 6, number(10)),
        (6, 6, number(10)),
        (7, 6, number(10)),
        (9, 6, number(4)),
    ]
}

pub fn row_group(rows: i64, column: &TestFields) -> Vec<u8> {
    let chunk = structure(&vec![(2, 6, number(0)), (3, 12, structure(column))]);
    structure(&vec![
        (1, 9, list(12, &[chunk])),
        (2, 6, number(10)),
        (3, 6, number(rows)),
    ])
}

pub fn footer() -> TestFields {
    vec![
        (1, 5, number(1)),
        (2, 9, list(12, &schema())),
        (3, 6, number(10)),
        (4, 9, list(12, &[row_group(10, &column())])),
    ]
}

pub fn set(fields: &mut TestFields, id: i16, bytes: Vec<u8>) {
    fields.iter_mut().find(|field| field.0 == id).unwrap().2 = bytes;
}

pub async fn stored(footer: &[u8], body_bytes: usize) -> (Arc<TestBlocks>, FileRecord) {
    let mut bytes = b"PAR1".to_vec();
    bytes.resize(body_bytes, 0);
    bytes.extend(footer);
    bytes.extend(u32::try_from(footer.len()).unwrap().to_le_bytes());
    bytes.extend(b"PAR1");
    let store = Arc::new(TestBlocks::default());
    let owner = FileIdentity {
        table: TableLocation {
            catalog: CatalogId::random(),
            table: TableId::random(),
        },
        file: FileId::random(),
    };
    let mut writer = FileTreeWriter::new(store.clone(), owner, 37).unwrap();
    writer.push(&bytes).await.unwrap();
    let tree = writer.finish().await.unwrap();
    let record = FileRecord {
        file: owner.file,
        location: owner.table.file("data/file.parquet").unwrap(),
        kind: FileKind::Unbound,
        format: ContentFormat::Parquet,
        length: tree.length,
        digest: tree.digest,
        content: FileContent::Chunks { root: tree.root },
        hint: None,
    };
    (store, record)
}
