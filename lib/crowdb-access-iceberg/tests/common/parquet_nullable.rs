use super::{blocks::TestBlocks, fixture::*};
use crowdb_access_iceberg::{
    file::{FileRecord, TableLocation},
    key::{CatalogId, TableId},
};
use std::sync::Arc;

#[derive(Clone)]
pub struct TestPage {
    pub kind: i64,
    pub values: i64,
    pub nulls: i64,
    pub encoding: i64,
    pub level_encoding: i64,
    pub levels: Vec<u8>,
    pub data: Vec<u8>,
    pub compressed: bool,
}

pub async fn file(
    physical: i64,
    depth: usize,
    pages: &[TestPage],
    codec: i64,
) -> (Arc<TestBlocks>, FileRecord) {
    file_with_type_length(physical, None, depth, pages, codec).await
}

pub async fn file_with_type_length(
    physical: i64,
    type_length: Option<i64>,
    depth: usize,
    pages: &[TestPage],
    codec: i64,
) -> (Arc<TestBlocks>, FileRecord) {
    let mut bytes = b"PAR1".to_vec();
    let mut uncompressed = 0;
    let mut data_offset = None;
    let mut dictionary_offset = None;
    let mut rows = 0;
    for page in pages {
        if page.kind == 2 {
            dictionary_offset = Some(bytes.len());
        } else {
            data_offset.get_or_insert(bytes.len());
            rows += page.values;
        }
        uncompressed += append_page(&mut bytes, page, codec, depth);
    }
    let names: Vec<_> = (1..depth).map(|index| format!("parent{index}")).collect();
    let mut path: Vec<_> = names.iter().map(|name| binary(name.as_bytes())).collect();
    path.push(binary(b"value"));
    let mut column = vec![
        (1, 5, number(physical)),
        (2, 9, list(5, &[number(0), number(3), number(8)])),
        (3, 9, list(8, &path)),
        (4, 5, number(codec)),
        (5, 6, number(rows)),
        (6, 6, number(uncompressed)),
        (7, 6, number(i64::try_from(bytes.len() - 4).unwrap())),
        (9, 6, number(i64::try_from(data_offset.unwrap()).unwrap())),
    ];
    if let Some(offset) = dictionary_offset {
        column.push((11, 6, number(i64::try_from(offset).unwrap())));
    }
    let mut schema = vec![structure(&vec![(4, 8, binary(b"root")), (5, 5, number(1))])];
    for name in names {
        schema.push(structure(&vec![
            (3, 5, number(1)),
            (4, 8, binary(name.as_bytes())),
            (5, 5, number(1)),
        ]));
    }
    let mut leaf = vec![
        (1, 5, number(physical)),
        (3, 5, number(i64::from(depth > 0))),
        (4, 8, binary(b"value")),
        (9, 5, number(2)),
    ];
    if let Some(length) = type_length {
        leaf.push((2, 5, number(length)));
    }
    schema.push(structure(&leaf));
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
        (2, 9, list(12, &schema)),
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

fn append_page(bytes: &mut Vec<u8>, page: &TestPage, codec: i64, depth: usize) -> i64 {
    let mut levels = page.levels.clone();
    if page.kind == 0 && depth > 0 && page.level_encoding == 3 {
        let mut framed = u32::try_from(levels.len()).unwrap().to_le_bytes().to_vec();
        framed.extend(levels);
        levels = framed;
    }
    let decoded = levels.len() + page.data.len();
    let payload = if page.kind == 3 {
        levels.extend(compress(&page.data, if page.compressed { codec } else { 0 }));
        levels
    } else {
        levels.extend(&page.data);
        compress(&levels, codec)
    };
    let detail = match page.kind {
        0 => vec![
            (1, 5, number(page.values)),
            (2, 5, number(page.encoding)),
            (3, 5, number(page.level_encoding)),
            (4, 5, number(3)),
        ],
        2 => vec![(1, 5, number(page.values)), (2, 5, number(0))],
        _ => vec![
            (1, 5, number(page.values)),
            (2, 5, number(page.nulls)),
            (3, 5, number(page.values)),
            (4, 5, number(page.encoding)),
            (5, 5, number(i64::try_from(page.levels.len()).unwrap())),
            (6, 5, number(0)),
            (7, if page.compressed { 1 } else { 2 }, vec![]),
        ],
    };
    let header = structure(&vec![
        (1, 5, number(page.kind)),
        (2, 5, number(i64::try_from(decoded).unwrap())),
        (3, 5, number(i64::try_from(payload.len()).unwrap())),
        (
            4,
            5,
            number(i64::from(i32::from_ne_bytes(
                crc32fast::hash(&payload).to_ne_bytes(),
            ))),
        ),
        (
            match page.kind {
                0 => 5,
                2 => 7,
                _ => 8,
            },
            12,
            structure(&detail),
        ),
    ]);
    let size = header.len() + decoded;
    bytes.extend(header);
    bytes.extend(payload);
    i64::try_from(size).unwrap()
}

fn compress(bytes: &[u8], codec: i64) -> Vec<u8> {
    match codec {
        1 => snap::raw::Encoder::new().compress_vec(bytes).unwrap(),
        6 => zstd::stream::encode_all(bytes, 1).unwrap(),
        _ => bytes.to_vec(),
    }
}
