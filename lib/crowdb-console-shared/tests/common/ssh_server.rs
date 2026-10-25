// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::sync::Arc;

use async_trait::async_trait;
use russh::keys::key::{KeyPair, PublicKey};
use russh::server::{Auth, Handler, Msg, Session};
use russh::{Channel, ChannelId, CryptoVec};

#[derive(Clone)]
#[allow(dead_code)]
pub enum TestReply {
    Stdout(Vec<u8>),
    Failure(String),
}

pub struct TestSshServer {
    pub port: u16,
    server: tokio::task::JoinHandle<()>,
}

impl Drop for TestSshServer {
    fn drop(&mut self) {
        self.server.abort();
    }
}

#[allow(dead_code)]
impl TestSshServer {
    pub async fn start(key: PublicKey) -> Self {
        Self::start_with_reply(key, None).await
    }

    pub async fn start_with_reply(key: PublicKey, reply: Option<Vec<u8>>) -> Self {
        Self::start_with_host_key(key, reply, KeyPair::generate_ed25519().unwrap()).await
    }

    pub async fn start_with_host_key(key: PublicKey, reply: Option<Vec<u8>>, host: KeyPair) -> Self {
        Self::start_server(key, reply.map(TestReply::Stdout), host).await
    }

    pub async fn start_with_result(key: PublicKey, reply: TestReply) -> Self {
        Self::start_server(key, Some(reply), KeyPair::generate_ed25519().unwrap()).await
    }

    async fn start_server(key: PublicKey, reply: Option<TestReply>, host: KeyPair) -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let config = Arc::new(russh::server::Config {
            keys: vec![host],
            ..Default::default()
        });
        let server = tokio::spawn(async move {
            let mut sessions = tokio::task::JoinSet::new();
            loop {
                tokio::select! {
                    accepted = listener.accept() => {
                        let (stream, _) = accepted.unwrap();
                        let config = config.clone();
                        let handler = TestHandler { key: key.clone(), reply: reply.clone() };
                        sessions.spawn(async move {
                            if let Ok(session) = russh::server::run_stream(config, stream, handler).await {
                                let _ = session.await;
                            }
                        });
                    }
                    Some(_) = sessions.join_next() => {}
                }
            }
        });
        Self { port, server }
    }
}

struct TestHandler {
    key: PublicKey,
    reply: Option<TestReply>,
}

#[async_trait]
impl Handler for TestHandler {
    type Error = russh::Error;

    async fn auth_publickey(&mut self, user: &str, key: &PublicKey) -> Result<Auth, Self::Error> {
        Ok(if user == "operator" && *key == self.key {
            Auth::Accept
        } else {
            Auth::Reject {
                proceed_with_methods: None,
            }
        })
    }

    async fn channel_open_session(&mut self, _: Channel<Msg>, _: &mut Session) -> Result<bool, Self::Error> {
        Ok(true)
    }

    async fn exec_request(
        &mut self,
        channel: ChannelId,
        data: &[u8],
        session: &mut Session,
    ) -> Result<(), Self::Error> {
        session.channel_success(channel);
        if let Some(reply) = &self.reply {
            match reply {
                TestReply::Stdout(bytes) => {
                    session.data(channel, CryptoVec::from_slice(bytes));
                    session.exit_status_request(channel, 0);
                }
                TestReply::Failure(error) => {
                    session.extended_data(channel, 1, CryptoVec::from_slice(error.as_bytes()));
                    session.exit_status_request(channel, 1);
                }
            }
            session.eof(channel);
            session.close(channel);
            return Ok(());
        }
        let output = tokio::process::Command::new("/bin/sh")
            .args(["-c", &String::from_utf8_lossy(data)])
            .output()
            .await?;
        session.data(channel, CryptoVec::from_slice(&output.stdout));
        session.exit_status_request(
            channel,
            u32::try_from(output.status.code().unwrap_or(1)).unwrap_or(1),
        );
        session.eof(channel);
        session.close(channel);
        Ok(())
    }
}
