use crowdb_access_iceberg::file::{
    AvroContainerError, AvroDatumLimits, AvroMetricValue, AvroProjection, AvroScalar, AvroSchema,
};

fn schema() -> AvroSchema {
    AvroSchema::parse(br#"{"type":"record","name":"root","fields":[{"name":"metrics","field-id":109,"type":{"type":"array","logicalType":"map","items":{"type":"record","name":"kv","fields":[{"name":"key","field-id":119,"type":"int"},{"name":"value","field-id":120,"type":"long"}]}}}]}"#).unwrap()
}

fn limits() -> AvroDatumLimits {
    AvroDatumLimits {
        depth: 16,
        values: 100,
        value_bytes: 1024,
    }
}

#[test]
fn logical_map_visits_positive_negative_and_multiple_blocks_under_independent_limits() {
    let schema = schema();
    let projection = AvroProjection::new(&schema, &[109]).unwrap();
    assert_eq!(projection.map_ids(), &[Some((119, 120))]);
    for bytes in [
        vec![4, 6, 20, 8, 40, 0],
        vec![3, 8, 6, 20, 8, 40, 0],
        vec![2, 6, 20, 1, 4, 8, 40, 0],
    ] {
        let values = projection
            .records(&bytes, 1, limits())
            .unwrap()
            .next_record()
            .unwrap()
            .unwrap();
        let AvroScalar::MetricMap(map) = values[0] else {
            panic!("expected map")
        };
        let mut seen = Vec::new();
        map.visit(2, 1024, |key, value| {
            seen.push((key, value));
            Ok::<_, AvroContainerError>(())
        })
        .unwrap();
        assert_eq!(
            seen,
            vec![(3, AvroMetricValue::Long(10)), (4, AvroMetricValue::Long(20))]
        );
        assert!(map
            .visit(1, 1024, |_, _| Ok::<_, AvroContainerError>(()))
            .is_err());
        assert!(map
            .visit(2, bytes.len() - 1, |_, _| Ok::<_, AvroContainerError>(()))
            .is_err());
        assert!(matches!(
            map.visit(2, 1024, |_, _| Err(AvroContainerError::Failed)),
            Err(AvroContainerError::Failed)
        ));
    }
}

#[test]
fn invalid_map_framing_fails_before_exposing_borrowed_values() {
    let schema = schema();
    let projection = AvroProjection::new(&schema, &[109]).unwrap();
    for bytes in [
        vec![1, 2, 6, 20, 0],
        vec![1, 6, 6, 20, 0],
        vec![2, 6],
        vec![0, 0],
        vec![2, 128, 128, 128, 128, 16, 20, 0],
    ] {
        let mut records = projection.records(&bytes, 1, limits()).unwrap();
        assert!(records.next_record().is_err());
        assert!(matches!(records.next_record(), Err(AvroContainerError::Failed)));
    }
}
