#[path = "common/file_blocks.rs"]
mod blocks;
#[path = "common/parquet_metadata.rs"]
mod fixture;

use crowdb_access_iceberg::file::read_parquet_metadata;
use fixture::{binary, column, footer, limits, list, number, row_group, schema, set, stored, structure};

#[tokio::test]
async fn column_paths_must_identify_the_exact_ordered_schema_leaf() {
    for path in [
        vec![],
        vec![binary(b"wrong")],
        vec![binary(b"schema"), binary(b"value")],
        vec![binary(&[255])],
        vec![binary(b"value")],
    ] {
        let valid = path == vec![binary(b"value")];
        let mut column = column();
        set(&mut column, 3, list(8, &path));
        let mut fields = footer();
        set(&mut fields, 4, list(12, &[row_group(10, &column)]));
        let (store, record) = stored(&structure(&fields), 64).await;
        assert_eq!(
            read_parquet_metadata(store, &record, limits()).await.is_ok(),
            valid
        );
    }
}

#[tokio::test]
async fn flat_column_value_counts_include_nulls_and_equal_row_counts() {
    for values in [0, 9, 10, 11] {
        let mut column = column();
        set(&mut column, 5, number(values));
        let mut fields = footer();
        set(&mut fields, 4, list(12, &[row_group(10, &column)]));
        let (store, record) = stored(&structure(&fields), 64).await;
        assert_eq!(
            read_parquet_metadata(store, &record, limits()).await.is_ok(),
            values == 10
        );
    }
}

#[tokio::test]
async fn nested_paths_exclude_the_root_and_repeated_ancestors_relax_value_counts() {
    for repetition in [0, 1, 2] {
        for values in [10, 30] {
            for include_parent in [false, true] {
                let nodes = vec![
                    schema()[0].clone(),
                    structure(&vec![
                        (3, 5, number(repetition)),
                        (4, 8, binary(b"parent")),
                        (5, 5, number(1)),
                    ]),
                    schema()[1].clone(),
                ];
                let mut path = vec![];
                if include_parent {
                    path.push(binary(b"parent"));
                }
                path.push(binary(b"value"));
                let mut column = column();
                set(&mut column, 3, list(8, &path));
                set(&mut column, 5, number(values));
                let mut fields = footer();
                set(&mut fields, 2, list(12, &nodes));
                set(&mut fields, 4, list(12, &[row_group(10, &column)]));
                let (store, record) = stored(&structure(&fields), 64).await;
                assert_eq!(
                    read_parquet_metadata(store, &record, limits()).await.is_ok(),
                    include_parent && (values == 10 || repetition == 2)
                );
            }
        }
    }
}

#[tokio::test]
async fn same_physical_type_cannot_hide_swapped_sibling_columns() {
    let nodes = vec![
        structure(&vec![(4, 8, binary(b"schema")), (5, 5, number(2))]),
        schema()[1].clone(),
        structure(&vec![
            (1, 5, number(2)),
            (3, 5, number(1)),
            (4, 8, binary(b"sibling")),
            (9, 5, number(4)),
        ]),
    ];
    let first = structure(&vec![(2, 6, number(0)), (3, 12, structure(&column()))]);
    let mut sibling = column();
    set(&mut sibling, 3, list(8, &[binary(b"sibling")]));
    let second = structure(&vec![(2, 6, number(0)), (3, 12, structure(&sibling))]);
    for swapped in [false, true] {
        let chunks = if swapped {
            vec![second.clone(), first.clone()]
        } else {
            vec![first.clone(), second.clone()]
        };
        let group = structure(&vec![
            (1, 9, list(12, &chunks)),
            (2, 6, number(20)),
            (3, 6, number(10)),
        ]);
        let mut fields = footer();
        set(&mut fields, 2, list(12, &nodes));
        set(&mut fields, 4, list(12, &[group]));
        let (store, record) = stored(&structure(&fields), 64).await;
        assert_eq!(
            read_parquet_metadata(store, &record, limits()).await.is_ok(),
            !swapped
        );
    }
}
