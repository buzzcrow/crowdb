use std::{io::Write, sync::Arc};

use crowdb_access_iceberg::file::{FileRecord, ParquetPageLimits, TableLocation};
use crowdb_access_iceberg::manifest::PositionDeleteLimits;

use super::{
    blocks::TestBlocks,
    fixture::{binary, limits, list, number, stored_content, structure},
};

pub struct TestColumn {
    pub physical: i64,
    pub encoding: i64,
    pub pages: Vec<(i64, Vec<u8>)>,
    pub dictionary: Option<(i64, Vec<u8>)>,
}

pub fn delete_limits() -> PositionDeleteLimits {
    PositionDeleteLimits {
        metadata: limits(),
        page: ParquetPageLimits {
            bytes: 1024 * 1024,
            values: 10000,
            pages: 100,
        },
        rows: 100_000,
    }
}

pub fn strings(values: &[String]) -> Vec<u8> {
    values
        .iter()
        .flat_map(|value| {
            let mut bytes = i32::try_from(value.len()).unwrap().to_le_bytes().to_vec();
            bytes.extend(value.as_bytes());
            bytes
        })
        .collect()
}

pub fn longs(values: &[i64]) -> Vec<u8> {
    values.iter().flat_map(|value| value.to_le_bytes()).collect()
}

pub async fn file(
    table: TableLocation,
    columns: [TestColumn; 2],
    v2: bool,
    codec: i64,
) -> (Arc<TestBlocks>, FileRecord) {
    let mut bytes = b"PAR1".to_vec();
    let mut chunks = vec![];
    let mut total_uncompressed = 0;
    let rows: i64 = columns[0].pages.iter().map(|(count, _)| count).sum();
    for (index, column) in columns.into_iter().enumerate() {
        let start = bytes.len();
        let mut uncompressed = 0;
        let mut dictionary = None;
        if let Some((count, payload)) = column.dictionary {
            dictionary = Some(start);
            uncompressed += page(&mut bytes, &payload, count, 0, 2, codec);
        }
        let data_offset = bytes.len();
        for (count, payload) in column.pages {
            uncompressed += page(
                &mut bytes,
                &payload,
                count,
                column.encoding,
                if v2 { 3 } else { 0 },
                codec,
            );
        }
        let mut fields = vec![
            (1, 5, number(column.physical)),
            (2, 9, list(5, &[number(column.encoding)])),
            (
                3,
                9,
                list(8, &[binary(if index == 0 { b"file_path" } else { b"pos" })]),
            ),
            (4, 5, number(codec)),
            (5, 6, number(rows)),
            (6, 6, number(uncompressed)),
            (7, 6, number(i64::try_from(bytes.len() - start).unwrap())),
            (9, 6, number(i64::try_from(data_offset).unwrap())),
        ];
        if let Some(offset) = dictionary {
            fields.push((11, 6, number(i64::try_from(offset).unwrap())));
        }
        chunks.push(structure(&vec![(2, 6, number(0)), (3, 12, structure(&fields))]));
        total_uncompressed += uncompressed;
    }
    let schema = vec![
        structure(&vec![(4, 8, binary(b"root")), (5, 5, number(2))]),
        structure(&vec![
            (1, 5, number(6)),
            (3, 5, number(0)),
            (4, 8, binary(b"file_path")),
            (6, 5, number(0)),
            (9, 5, number(2_147_483_546)),
        ]),
        structure(&vec![
            (1, 5, number(2)),
            (3, 5, number(0)),
            (4, 8, binary(b"pos")),
            (9, 5, number(2_147_483_545)),
        ]),
    ];
    let group = structure(&vec![
        (1, 9, list(12, &chunks)),
        (2, 6, number(total_uncompressed)),
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
    stored_content(&bytes, table).await
}

pub fn page(bytes: &mut Vec<u8>, payload: &[u8], count: i64, encoding: i64, kind: i64, codec: i64) -> i64 {
    let compressed = match codec {
        1 => snap::raw::Encoder::new().compress_vec(payload).unwrap(),
        2 => {
            let mut encoder = flate2::write::GzEncoder::new(vec![], flate2::Compression::default());
            encoder.write_all(payload).unwrap();
            encoder.finish().unwrap()
        }
        6 => zstd::stream::encode_all(payload, 1).unwrap(),
        7 => lz4_flex::block::compress(payload),
        _ => payload.to_vec(),
    };
    let detail = match kind {
        0 => vec![
            (1, 5, number(count)),
            (2, 5, number(encoding)),
            (3, 5, number(3)),
            (4, 5, number(3)),
        ],
        2 => vec![(1, 5, number(count)), (2, 5, number(encoding))],
        _ => vec![
            (1, 5, number(count)),
            (2, 5, number(0)),
            (3, 5, number(count)),
            (4, 5, number(encoding)),
            (5, 5, number(0)),
            (6, 5, number(0)),
        ],
    };
    let header = structure(&vec![
        (1, 5, number(kind)),
        (2, 5, number(i64::try_from(payload.len()).unwrap())),
        (3, 5, number(i64::try_from(compressed.len()).unwrap())),
        (
            4,
            5,
            number(i64::from(i32::from_ne_bytes(
                crc32fast::hash(&compressed).to_ne_bytes(),
            ))),
        ),
        (
            match kind {
                0 => 5,
                2 => 7,
                _ => 8,
            },
            12,
            structure(&detail),
        ),
    ]);
    let length = header.len() + payload.len();
    bytes.extend(header);
    bytes.extend(compressed);
    i64::try_from(length).unwrap()
}
