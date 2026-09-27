use crowdb_protocol::common::{InstanceValue, KvServerExtra, KvServerIdentity, ServiceExtra};

#[test]
fn kv_service_registration_keeps_node_identity_separate_from_instance_identity() {
    let identity = KvServerIdentity {
        instance_id: 19,
        node_id: Some(7),
    };
    let value = InstanceValue {
        instance_id: identity.instance_id,
        rpc_endpoint: "http://127.0.0.1:10000".into(),
        last_heartbeat_ms: 123,
        extra: Some(ServiceExtra {
            kv_server: Some(KvServerExtra {
                node_id: identity.node_id,
                hosted_stores: vec![0],
                hosted_groups: Vec::new(),
                health: "ok".into(),
                data_root: "/data/node-7".into(),
            }),
            ..ServiceExtra::default()
        }),
    };
    let encoded = serde_json::to_vec(&value).unwrap();
    let decoded: InstanceValue = serde_json::from_slice(&encoded).unwrap();
    assert_eq!(decoded.instance_id, 19);
    assert_eq!(decoded.extra.unwrap().kv_server.unwrap().node_id, Some(7));
}
