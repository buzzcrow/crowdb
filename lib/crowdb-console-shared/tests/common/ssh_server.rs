// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::sync::Arc;

use async_trait::async_trait;
use russh::keys::key::{KeyPair, PublicKey};
use russh::server::{Auth, Handler, Msg, Session};
use russh::{Channel, ChannelId, CryptoVec};

pub struct TestSshServer {
    pub port: u16,
    server: tokio::task::JoinHandle<()>,
}

impl Drop for TestSshServer {
    fn drop(&mut self) {
        self.server.abort();
    }
}

impl TestSshServer {
    pub async fn start(key: PublicKey) -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let config = Arc::new(russh::server::Config {
            keys: vec![KeyPair::generate_ed25519().unwrap()],
            ..Default::default()
        });
        let server = tokio::spawn(async move {
            let mut sessions = tokio::task::JoinSet::new();
            loop {
                tokio::select! {
                    accepted = listener.accept() => {
                        let (stream, _) = accepted.unwrap();
                        let config = config.clone();
                        let handler = TestHandler { key: key.clone() };
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
