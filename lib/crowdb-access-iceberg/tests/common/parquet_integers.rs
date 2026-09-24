use super::{blocks::TestBlocks, deletes::page, fixture::*};
use crowdb_access_iceberg::{
    file::{FileRecord, TableLocation},
    key::{CatalogId, TableId},
};
use std::sync::Arc;

pub async fn file(
    physical: i64,
    encoding: i64,
    pages: &[(i64, Vec<u8>)],
    dictionary: Option<(i64, Vec<u8>)>,
    version: i64,
    codec: i64,
) -> (Arc<TestBlocks>, FileRecord) {
    let mut bytes = b"PAR1".to_vec();
    let mut uncompressed = 0;
    let dictionary_offset = dictionary.as_ref().map(|_| bytes.len());
    if let Some((count, payload)) = dictionary {
        uncompressed += page(&mut bytes, &payload, count, 0, 2, codec);
    }
    let data_offset = bytes.len();
    for (count, payload) in pages {
        uncompressed += page(&mut bytes, payload, *count, encoding, version, codec);
    }
    let rows = pages.iter().map(|(count, _)| count).sum();
    let mut column = vec![
        (1, 5, number(physical)),
        (2, 9, list(5, &[number(0), number(encoding)])),
        (3, 9, list(8, &[binary(b"value")])),
        (4, 5, number(codec)),
        (5, 6, number(rows)),
        (6, 6, number(uncompressed)),
        (7, 6, number(i64::try_from(bytes.len() - 4).unwrap())),
        (9, 6, number(i64::try_from(data_offset).unwrap())),
    ];
    if let Some(offset) = dictionary_offset {
        column.push((11, 6, number(i64::try_from(offset).unwrap())));
    }
    let nodes = vec![
        structure(&vec![(4, 8, binary(b"root")), (5, 5, number(1))]),
        structure(&vec![
            (1, 5, number(physical)),
            (3, 5, number(0)),
            (4, 8, binary(b"value")),
            (9, 5, number(2)),
        ]),
    ];
    let group = structure(&vec![
        (
            1,
            9,
            list(
                12,
                &[structure(&vec![(2, 6, number(0)), (3, 12, structure(&column))])],
            ),
        ),
        (2, 6, number(uncompressed)),
        (3, 6, number(rows)),
    ]);
    let footer = structure(&vec![
        (1, 5, number(2)),
        (2, 9, list(12, &nodes)),
        (3, 6, number(rows)),
        (4, 9, list(12, &[group])),
    ]);
    bytes.extend(&footer);
    bytes.extend(u32::try_from(footer.len()).unwrap().to_le_bytes());
    bytes.extend(b"PAR1");
    stored_content(
        &bytes,
        TableLocation {
            catalog: CatalogId::from_bytes(&[1; 16]).unwrap(),
            table: TableId::from_bytes(&[2; 16]).unwrap(),
        },
    )
    .await
}
