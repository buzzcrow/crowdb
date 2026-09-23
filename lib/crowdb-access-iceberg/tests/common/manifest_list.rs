use crowdb_access_iceberg::file::{AvroSchema, TableLocation};
use crowdb_access_iceberg::key::{CatalogId, TableId};
use serde_json::{json, Value};

pub struct TestManifestList {
    pub fields: Vec<(i32, &'static str, Value)>,
    pub summary_schema: Option<Value>,
    pub summary_bytes: Vec<u8>,
}

pub fn table() -> TableLocation {
    TableLocation {
        catalog: CatalogId::from_bytes(&[1; 16]).unwrap(),
        table: TableId::from_bytes(&[2; 16]).unwrap(),
    }
}

impl TestManifestList {
    pub fn new() -> Self {
        Self {
            summary_schema: None,
            summary_bytes: Vec::new(),
            fields: vec![
                (
                    500,
                    "string",
                    json!(table().file("metadata/manifest.avro").unwrap().to_string()),
                ),
                (501, "long", json!(42)),
                (502, "int", json!(0)),
                (503, "long", json!(99)),
                (517, "int", json!(0)),
                (515, "long", json!(8)),
                (516, "long", json!(6)),
                (504, "int", json!(1)),
                (505, "int", json!(2)),
                (506, "int", json!(3)),
                (512, "long", json!(10)),
                (513, "long", json!(20)),
                (514, "long", json!(30)),
                (520, "long", json!(100)),
            ],
        }
    }

    pub fn set(&mut self, id: i32, value: Value) {
        self.fields.iter_mut().find(|field| field.0 == id).unwrap().2 = value;
    }

    pub fn schema(&self) -> AvroSchema {
        let mut fields: Vec<_> = self
            .fields
            .iter()
            .map(|(id, kind, _)| json!({"name":format!("renamed{id}"),"field-id":id,"type":["null",kind]}))
            .collect();
        if let Some(schema) = &self.summary_schema {
            fields.push(json!({"name":"partitions","field-id":507,"type":schema}));
        }
        AvroSchema::parse(
            &serde_json::to_vec(&json!({"type":"record","name":"List","fields":fields})).unwrap(),
        )
        .unwrap()
    }

    pub fn bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::new();
        for (_, kind, value) in &self.fields {
            if value.is_null() {
                bytes.push(0);
                continue;
            }
            bytes.push(2);
            if *kind == "string" {
                let string = value.as_str().unwrap();
                long(i64::try_from(string.len()).unwrap(), &mut bytes);
                bytes.extend_from_slice(string.as_bytes());
            } else {
                long(value.as_i64().unwrap(), &mut bytes);
            }
        }
        bytes.extend_from_slice(&self.summary_bytes);
        bytes
    }
}

fn long(value: i64, bytes: &mut Vec<u8>) {
    let mut encoded = (value.unsigned_abs() << 1).wrapping_sub(u64::from(value < 0));
    while encoded > 127 {
        bytes.push(u8::try_from(encoded & 127).unwrap() | 128);
        encoded >>= 7;
    }
    bytes.push(u8::try_from(encoded).unwrap());
}
