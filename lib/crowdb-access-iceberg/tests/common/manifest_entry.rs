use crowdb_access_iceberg::file::{AvroSchema, TableLocation};
use crowdb_access_iceberg::key::{CatalogId, TableId};
use crowdb_access_iceberg::manifest::ManifestVersion;
use serde_json::{json, Value};

pub struct TestManifestEntry {
    pub root: Vec<(i32, &'static str, Value)>,
    pub file: Vec<(i32, &'static str, Value)>,
    pub null_file: bool,
}

pub fn table() -> TableLocation {
    TableLocation {
        catalog: CatalogId::from_bytes(&[1; 16]).unwrap(),
        table: TableId::from_bytes(&[2; 16]).unwrap(),
    }
}

impl TestManifestEntry {
    pub fn new(version: ManifestVersion) -> Self {
        let mut fixture = Self {
            root: vec![
                (0, "int", json!(1)),
                (1, "long", json!(99)),
                (3, "long", json!(null)),
                (4, "long", json!(null)),
            ],
            file: vec![
                (134, "int", json!(0)),
                (
                    100,
                    "string",
                    json!(table().file("data/file.parquet").unwrap().to_string()),
                ),
                (101, "string", json!("PARQUET")),
                (103, "long", json!(10)),
                (104, "long", json!(42)),
                (140, "int", json!(null)),
                (142, "long", json!(null)),
                (143, "string", json!(null)),
                (144, "long", json!(null)),
                (145, "long", json!(null)),
                (105, "long", json!(0)),
            ],
            null_file: false,
        };
        if version == ManifestVersion::V1 {
            fixture.root.retain(|field| field.0 < 3);
            fixture.file.retain(|field| !matches!(field.0, 134 | 142..=145));
        } else {
            fixture.file.retain(|field| field.0 != 105);
        }
        fixture
    }

    pub fn set(&mut self, id: i32, value: Value) {
        self.root
            .iter_mut()
            .chain(&mut self.file)
            .find(|field| field.0 == id)
            .unwrap()
            .2 = value;
    }

    pub fn schema(&self) -> AvroSchema {
        AvroSchema::parse(&self.schema_bytes()).unwrap()
    }

    pub fn schema_bytes(&self) -> Vec<u8> {
        let field = |(id, kind, _): &(i32, &'static str, Value)| json!({"name":format!("renamed{id}"),"field-id":id,"type":["null",kind]});
        let mut root: Vec<_> = self.root.iter().map(field).collect();
        let mut file: Vec<_> = self.file.iter().map(field).collect();
        file.push(json!({"name":"partition","field-id":102,"type":{"type":"record","name":"Partition","fields":[]}}));
        root.push(json!({"name":"renamed_file","field-id":2,"type":["null",{"type":"record","name":"File","fields":file}]}));
        serde_json::to_vec(&json!({"type":"record","name":"Entry","fields":root})).unwrap()
    }

    pub fn bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::new();
        for field in &self.root {
            encode(field, &mut bytes);
        }
        if self.null_file {
            bytes.push(0);
        } else {
            bytes.push(2);
            for field in &self.file {
                encode(field, &mut bytes);
            }
        }
        bytes
    }
}

fn encode((_, kind, value): &(i32, &'static str, Value), bytes: &mut Vec<u8>) {
    if value.is_null() {
        bytes.push(0);
        return;
    }
    bytes.push(2);
    if *kind == "string" {
        let string = value.as_str().unwrap();
        long(i64::try_from(string.len()).unwrap(), bytes);
        bytes.extend_from_slice(string.as_bytes());
    } else {
        long(value.as_i64().unwrap(), bytes);
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
