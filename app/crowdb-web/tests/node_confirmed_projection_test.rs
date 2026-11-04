// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_console_shared::{
    config::{ServerEntry, ServiceType},
    deployment::registry::NodeRecord,
};
use crowdb_web::project_confirmed_servers_for_tests;

#[test]
fn auxiliary_projection_before_confirmation_keeps_bootstrap_kv_and_accepted_ports() {
    for host in ["192.0.2.42", "2001:db8::42"] {
        let mut auxiliary = ServerEntry::new("access-42", "http://original:16000");
        auxiliary.node_id = Some(42);
        auxiliary.service_type = ServiceType::AccessServer;
        auxiliary.rpc_url = Some("original:16001".into());
        let mut kv = ServerEntry::new("kv-42", "http://original:10042");
        kv.node_id = Some(42);
        kv.rpc_url = Some("original:10142".into());
        let nodes = vec![NodeRecord {
            discovery_id: "discovery-42".into(),
            node_id: 42,
            physical_host_id: "host-42".into(),
            rack_id: 1,
            host: host.into(),
            web_port: 9090,
            ssh_port: 22,
            ssh_user: "crowdb".into(),
            operation_id: "admit-42".into(),
            host_key: None,
            confirmed: true,
            cancelled: false,
        }];
        let mut servers = vec![auxiliary.clone()];
        project_confirmed_servers_for_tests(&mut servers, vec![kv.clone()], &nodes);
        assert_eq!(
            servers.len(),
            2,
            "auxiliary publication must not hide bootstrap KV"
        );
        assert_eq!(
            servers[0], auxiliary,
            "KV recovery must not rewrite an auxiliary endpoint"
        );
        let host = if host.contains(':') {
            format!("[{host}]")
        } else {
            host.into()
        };
        kv.url = format!("http://{host}:10042");
        kv.rpc_url = Some(format!("{host}:10142"));
        assert_eq!(servers[1], kv);
        let retained = servers.clone();
        project_confirmed_servers_for_tests(&mut servers, vec![kv], &nodes);
        assert_eq!(
            servers, retained,
            "repeated confirmation must preserve service identities"
        );
    }
}
