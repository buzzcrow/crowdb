use reqwest::{Client, Response};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

fn identity() -> String {
    static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
    static BUCKETS: [std::sync::atomic::AtomicU64; 64] = [const { std::sync::atomic::AtomicU64::new(0) }; 64];
    for _ in 0..100_000 {
        let now = super::common::now_ms();
        let key = format!(
            "{:08x}-{:04x}-7000-8000-{:012x}",
            now >> 16,
            now & 0xffff,
            NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        );
        let operation: crowdb_access_iceberg::key::OperationId = key.parse().unwrap();
        let crowdb_access_iceberg::key::IcebergKey::System { suffix, .. } =
            crowdb_access_iceberg::operation::ledger_key(
                crowdb_access_iceberg::key::SystemScope::RetryBinding,
                operation,
            )
            .unwrap()
        else {
            unreachable!()
        };
        let bucket = usize::from(u16::from_be_bytes([suffix[14], suffix[15]])) - 1;
        let mask = 1_u64 << (bucket % 64);
        if BUCKETS[bucket / 64].fetch_or(mask, std::sync::atomic::Ordering::Relaxed) & mask == 0 {
            return key;
        }
    }
    panic!("crash fixture exhausted distinct retry admission buckets")
}

pub struct TestCommitCase {
    pub path: String,
    pub body: String,
    pub identity: String,
    pub name: String,
    pub staged: bool,
    pub generation: u64,
}

pub async fn post(
    endpoint: &str,
    path: &str,
    identity: &str,
    body: &str,
) -> Result<Response, reqwest::Error> {
    Client::new()
        .post(format!("{endpoint}{path}"))
        .bearer_auth("w".repeat(32))
        .header("content-type", "application/json")
        .header("idempotency-key", identity)
        .body(body.to_owned())
        .send()
        .await
}

pub async fn success(endpoint: &str, path: &str, body: &Value) -> Value {
    let response = post(endpoint, path, &identity(), &body.to_string())
        .await
        .unwrap();
    let status = response.status();
    let text = response.text().await.unwrap();
    assert_eq!(status, 200, "{text}");
    serde_json::from_str(&text).unwrap()
}

impl TestCommitCase {
    pub async fn prepare(endpoint: &str, kind: &str, name: String) -> Self {
        let mut create = json!({"name":name,"schema":{"type":"struct","schema-id":0,"fields":[
            {"id":1,"name":"id","type":"long","required":true}]},"properties":properties()});
        let path = format!("/v1/namespaces/analytics/tables/{name}");
        let (path, body, staged, generation) = match kind {
            "create" => ("/v1/namespaces/analytics/tables".into(), create, false, 1),
            "stage" => {
                create["stage-create"] = json!(true);
                ("/v1/namespaces/analytics/tables".into(), create, true, 0)
            }
            "update" => {
                success(endpoint, "/v1/namespaces/analytics/tables", &create).await;
                (
                    path,
                    json!({"requirements":[],"updates":[{"action":"set-properties","updates":{"owner":"after"}}]}),
                    false,
                    2,
                )
            }
            "publish-stage" => {
                create["stage-create"] = json!(true);
                let response = success(endpoint, "/v1/namespaces/analytics/tables", &create).await;
                (path, staged_commit(&response["metadata"]), false, 1)
            }
            _ => panic!("unknown case"),
        };
        Self {
            path,
            body: body.to_string(),
            identity: identity(),
            name,
            staged,
            generation,
        }
    }

    pub async fn replay(&self, endpoint: &str) -> Value {
        let first = post(endpoint, &self.path, &self.identity, &self.body)
            .await
            .unwrap();
        let status = first.status();
        let bytes = first.bytes().await.unwrap();
        assert_eq!(status, 200, "{}", String::from_utf8_lossy(&bytes));
        let replay = post(endpoint, &self.path, &self.identity, &self.body)
            .await
            .unwrap();
        assert_eq!(replay.status(), 200);
        assert_eq!(replay.bytes().await.unwrap(), bytes);
        let changed = post(endpoint, &self.path, &self.identity, &format!("{} ", self.body))
            .await
            .unwrap();
        assert_eq!(changed.status(), 409, "{}", changed.text().await.unwrap());
        let loaded = Client::new()
            .get(format!("{endpoint}/v1/namespaces/analytics/tables/{}", self.name))
            .bearer_auth("r".repeat(32))
            .send()
            .await
            .unwrap();
        let status = loaded.status();
        let loaded = loaded.text().await.unwrap();
        if self.staged {
            assert_eq!(status, 404, "{loaded}");
        } else {
            assert_eq!(status, 200, "{loaded}");
            let loaded: Value = serde_json::from_str(&loaded).unwrap();
            let result: Value = serde_json::from_slice(&bytes).unwrap();
            assert_eq!(loaded["metadata-location"], result["metadata-location"]);
            if self.generation == 2 {
                assert_eq!(loaded["metadata"]["properties"]["owner"], "after");
            }
        }
        serde_json::from_slice(&bytes).unwrap()
    }
}

fn properties() -> Value {
    let mut properties = serde_json::Map::new();
    for field in 0..16 {
        let mut value = String::new();
        for part in 0..32 {
            use std::fmt::Write;
            let digest = Sha256::digest(format!("field-{field}-part-{part}").as_bytes());
            for byte in digest {
                write!(value, "{byte:02x}").unwrap();
            }
        }
        properties.insert(format!("random-{field}"), json!(value));
    }
    Value::Object(properties)
}

fn staged_commit(metadata: &Value) -> Value {
    json!({"requirements":[{"type":"assert-create"}],"updates":[
        {"action":"assign-uuid","uuid":metadata["table-uuid"]},
        {"action":"upgrade-format-version","format-version":metadata["format-version"]},
        {"action":"add-schema","schema":metadata["schemas"][0]},
        {"action":"set-current-schema","schema-id":-1},
        {"action":"add-spec","spec":metadata["partition-specs"][0]},
        {"action":"set-default-spec","spec-id":-1},
        {"action":"add-sort-order","sort-order":metadata["sort-orders"][0]},
        {"action":"set-default-sort-order","sort-order-id":-1},
        {"action":"set-location","location":metadata["location"]},
        {"action":"set-properties","updates":metadata["properties"]}
    ]})
}
