// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crate::common::management::{addresses, client, origin, request};
use reqwest::{Client, Method};
use serde_json::{json, Value};
use std::time::{Duration, Instant};

async fn ready_cluster(client: &Client, origins: &[String]) -> usize {
    let deadline = Instant::now() + Duration::from_secs(15);
    let mut previous = None;
    loop {
        let mut leaders = Vec::new();
        let mut all_ready = true;
        for (index, base) in origins.iter().enumerate() {
            let topology = request(client, base, Method::GET, "/topology", Value::Null, 200).await;
            if topology["stores"][0]["groups"][0]["local_replica"]["role"] == "leader" {
                leaders.push(index);
            }
            let response = client
                .get(format!("{base}/stores/{index}/groups/1/ready"))
                .send()
                .await
                .unwrap();
            assert!(matches!(response.status().as_u16(), 200 | 503));
            let ready: Value = response.json().await.unwrap();
            all_ready &= ready["ready"] == true;
            if ready["ready"] == true {
                assert!(ready["leader_id"].as_u64().unwrap() != 0);
                assert!(ready["voting_replicas"].as_u64().unwrap() >= 3);
            }
        }
        if all_ready && leaders.len() == 1 {
            if previous == Some(leaders[0]) {
                return leaders[0];
            }
            previous = Some(leaders[0]);
        } else {
            previous = None;
        }
        assert!(
            Instant::now() < deadline,
            "cluster readiness/election did not converge"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

async fn completed_operation(client: &Client, base: &str, operation: u64) {
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        let status = request(
            client,
            base,
            Method::GET,
            &format!("/operations/{operation}"),
            Value::Null,
            200,
        )
        .await;
        match status["status"].as_str().unwrap() {
            "completed" => return,
            "failed" => panic!("step-down failed: {status}"),
            _ => assert!(Instant::now() < deadline, "operation did not complete: {status}"),
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

#[tokio::test]
#[ignore = "requires three release-container nodes"]
async fn async_and_sync_leader_lifecycle() {
    let origins: Vec<_> = addresses(3).iter().map(|ip| origin(ip)).collect();
    let client = client();
    let leader = ready_cluster(&client, &origins).await;
    let submitted = request(
        &client,
        &origins[leader],
        Method::POST,
        &format!("/stores/{leader}/groups/1/step-down"),
        json!({"reason": "async lifecycle"}),
        202,
    )
    .await;
    let operation = submitted["operation_id"].as_u64().expect("operation ID");
    assert!(operation > 0);
    completed_operation(&client, &origins[leader], operation).await;
    let leader = ready_cluster(&client, &origins).await;
    let synchronous = request(
        &client,
        &origins[leader],
        Method::POST,
        &format!("/stores/{leader}/groups/1/step-down?sync=true"),
        json!({"reason": "sync lifecycle"}),
        200,
    )
    .await;
    assert!(synchronous["accepted"].as_bool().is_some());
    assert_eq!(synchronous["accepted"], true);
    ready_cluster(&client, &origins).await;
}
