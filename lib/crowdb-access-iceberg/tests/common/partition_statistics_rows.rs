use super::{blocks::TestBlocks, fixture, parquet::*};
use crowdb_access_iceberg::{file::FileRecord, table::TableMetadataDocument};
use serde_json::json;
use std::sync::Arc;

pub struct TestColumn {
    pub id: i64,
    pub physical: i64,
    pub optional: bool,
    pub annotations: TestFields,
    pub values: Vec<Option<Vec<u8>>>,
}

pub fn column(id: i64, physical: i64, values: Vec<Option<Vec<u8>>>) -> TestColumn {
    TestColumn {
        id,
        physical,
        optional: id >= 1000,
        annotations: vec![],
        values,
    }
}

pub fn integers(id: i64, values: &[i64]) -> TestColumn {
    let physical = if matches!(id, 2 | 4 | 7 | 9 | 13) { 1 } else { 2 };
    column(
        id,
        physical,
        values
            .iter()
            .map(|value| {
                Some(if physical == 1 {
                    i32::try_from(*value).unwrap().to_le_bytes().to_vec()
                } else {
                    value.to_le_bytes().to_vec()
                })
            })
            .collect(),
    )
}

pub fn counts(rows: usize) -> Vec<TestColumn> {
    vec![
        integers(2, &vec![0; rows]),
        integers(3, &vec![10; rows]),
        integers(4, &vec![1; rows]),
        integers(5, &vec![100; rows]),
    ]
}

pub fn table(kinds: &[&str]) -> TableMetadataDocument {
    let mut table = fixture::metadata(2);
    table["schemas"][0]["fields"] = json!(kinds.iter().enumerate().map(|(index, kind)|
        json!({"id":index + 1,"name":format!("source{index}"),"type":kind,"required":false})).collect::<Vec<_>>());
    table["last-column-id"] = json!(kinds.len());
    table["partition-specs"][0]["fields"] = json!(kinds.iter().enumerate().map(|(index, _)|
        json!({"source-id":index + 1,"field-id":index + 1000,"name":format!("field{}", index + 1000),"transform":"identity"})).collect::<Vec<_>>());
    table["last-partition-id"] = json!(999 + kinds.len());
    fixture::parse(&table).unwrap()
}

pub async fn file(
    columns: &[TestColumn],
    group_rows: usize,
    page_rows: usize,
) -> (Arc<TestBlocks>, FileRecord) {
    let rows = columns[0].values.len();
    assert!(columns.iter().all(|column| column.values.len() == rows));
    let partition_count = columns.iter().filter(|column| column.id >= 1000).count();
    let mut schema = vec![
        structure(&vec![
            (4, 8, binary(b"stats")),
            (
                5,
                5,
                number(i64::try_from(columns.len() - partition_count + 1).unwrap()),
            ),
        ]),
        structure(&vec![
            (3, 5, number(0)),
            (4, 8, binary(b"partition")),
            (5, 5, number(i64::try_from(partition_count).unwrap())),
            (9, 5, number(1)),
        ]),
    ];
    for column in columns {
        let mut leaf = vec![
            (1, 5, number(column.physical)),
            (3, 5, number(i64::from(column.optional))),
            (4, 8, binary(format!("field{}", column.id).as_bytes())),
            (9, 5, number(column.id)),
        ];
        leaf.extend(column.annotations.clone());
        schema.push(structure(&leaf));
    }
    let mut bytes = b"PAR1".to_vec();
    let mut groups = Vec::new();
    for start in (0..rows).step_by(group_rows) {
        let end = (start + group_rows).min(rows);
        let mut chunks = Vec::new();
        let mut group_size = 0;
        for column in columns {
            let offset = bytes.len();
            for values in column.values[start..end].chunks(page_rows) {
                page(&mut bytes, column, values);
            }
            let length = bytes.len() - offset;
            group_size += length;
            let mut path = Vec::new();
            if column.id >= 1000 {
                path.push(binary(b"partition"));
            }
            path.push(binary(format!("field{}", column.id).as_bytes()));
            let metadata = structure(&vec![
                (1, 5, number(column.physical)),
                (2, 9, list(5, &[number(0), number(3)])),
                (3, 9, list(8, &path)),
                (4, 5, number(0)),
                (5, 6, number(i64::try_from(end - start).unwrap())),
                (6, 6, number(i64::try_from(length).unwrap())),
                (7, 6, number(i64::try_from(length).unwrap())),
                (9, 6, number(i64::try_from(offset).unwrap())),
            ]);
            chunks.push(structure(&vec![(2, 6, number(0)), (3, 12, metadata)]));
        }
        groups.push(structure(&vec![
            (1, 9, list(12, &chunks)),
            (2, 6, number(i64::try_from(group_size).unwrap())),
            (3, 6, number(i64::try_from(end - start).unwrap())),
        ]));
    }
    let footer = structure(&vec![
        (1, 5, number(1)),
        (2, 9, list(12, &schema)),
        (3, 6, number(i64::try_from(rows).unwrap())),
        (4, 9, list(12, &groups)),
    ]);
    bytes.extend(&footer);
    bytes.extend(u32::try_from(footer.len()).unwrap().to_le_bytes());
    bytes.extend(b"PAR1");
    stored_content(&bytes, fixture::table()).await
}

fn page(bytes: &mut Vec<u8>, column: &TestColumn, values: &[Option<Vec<u8>>]) {
    let mut payload = Vec::new();
    if column.optional {
        payload.extend(u32::try_from(values.len() * 2).unwrap().to_le_bytes());
        for value in values {
            payload.extend([2, u8::from(value.is_some())]);
        }
    }
    if column.physical == 0 {
        let values: Vec<_> = values.iter().flatten().collect();
        for values in values.chunks(8) {
            payload.push(
                values
                    .iter()
                    .enumerate()
                    .fold(0, |packed, (index, value)| packed | (value[0] << index)),
            );
        }
    } else {
        for value in values.iter().flatten() {
            if column.physical == 6 {
                payload.extend(i32::try_from(value.len()).unwrap().to_le_bytes());
            }
            payload.extend(value);
        }
    }
    let header = structure(&vec![
        (1, 5, number(0)),
        (2, 5, number(i64::try_from(payload.len()).unwrap())),
        (3, 5, number(i64::try_from(payload.len()).unwrap())),
        (
            5,
            12,
            structure(&vec![
                (1, 5, number(i64::try_from(values.len()).unwrap())),
                (2, 5, number(0)),
                (3, 5, number(3)),
                (4, 5, number(3)),
            ]),
        ),
    ]);
    bytes.extend(header);
    bytes.extend(payload);
}
