// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::sync::Arc;

use arc_swap::ArcSwap;
use crowdb_access_s3::native_buffer::NativeBodyAllocator;
use crowdb_access_server::s3::{HandlerFuture, ResponseBody, S3HttpHandler};
use crowdb_chunk_client::{ChunkClientConfig, ChunkIoClient, ChunkIoWriter, LargeWritePolicy};
use crowdb_common::ec::EcScheme;
use crowdb_protocol::chunkdb::rpc::{ChunkType, Location};
use futures::stream;
use http_body_util::{BodyExt, Full, StreamBody};
use hyper::body::{Bytes, Frame, Incoming};
use hyper::{Method, Request, Response};

pub struct TestHttp {
    chunks: Arc<ChunkIoClient>,
    pub allocator: Arc<NativeBodyAllocator>,
    pub locations: Arc<ArcSwap<Vec<Location>>>,
}

impl TestHttp {
    pub fn new(chunks: Arc<ChunkIoClient>) -> Self {
        let allocator = NativeBodyAllocator::new(2 << 20, 1 << 20)
            .unwrap()
            .with_producer_allocator(chunks.producer_allocator().unwrap())
            .unwrap();
        Self {
            chunks,
            allocator: Arc::new(allocator),
            locations: Arc::new(ArcSwap::from_pointee(Vec::new())),
        }
    }

    async fn put(
        chunks: Arc<ChunkIoClient>,
        allocator: Arc<NativeBodyAllocator>,
        locations: Arc<ArcSwap<Vec<Location>>>,
        request: Request<Incoming>,
    ) -> Response<ResponseBody> {
        let size = request.headers()["content-length"]
            .to_str()
            .unwrap()
            .parse()
            .unwrap();
        let mut config = ChunkClientConfig::new(ChunkType::S3);
        config.large_mirror_copies = Some(2);
        config.max_chunk_size = 2 << 20;
        config.large_parallel_strip_writes = 2;
        let policy = LargeWritePolicy {
            ec_scheme: EcScheme::new(1, 1),
            client: Arc::new(config),
        };
        let mut writer = chunks.prepare_large_write(Some(size), policy);
        writer.wait_until_prepared().await.unwrap();
        let (etag, _) = crowdb_access_server::s3::test_support::write_large_body_for_tests(
            request,
            &mut writer,
            &allocator,
        )
        .await
        .unwrap();
        locations.store(Arc::new(writer.on_finish().await.unwrap()));
        Response::builder().header("etag", etag).body(empty()).unwrap()
    }

    fn get(&self) -> Response<ResponseBody> {
        let read = self.chunks.read_stream(&self.locations.load()).unwrap();
        let body = stream::unfold((read, 0), |(mut read, delivered)| async move {
            if delivered == 1 && std::env::var("CROWDB_RDMA_DATAPATH_FAULT").as_deref() == Ok("1") {
                let marker = std::env::var("CROWDB_RDMA_DATAPATH_PAUSED").unwrap();
                let resume = std::env::var("CROWDB_RDMA_DATAPATH_RESUME").unwrap();
                std::fs::write(marker, b"verified frame delivered").unwrap();
                while !std::path::Path::new(&resume).exists() {
                    tokio::time::sleep(std::time::Duration::from_millis(1)).await;
                }
            }
            read.next_chunk().await.map(|result| {
                (
                    result
                        .map(Frame::data)
                        .map_err(|error| -> crowdb_access_server::s3::BoxError {
                            eprintln!("datapath HTTP download failed: {error:?}");
                            error.to_string().into()
                        }),
                    (read, delivered + 1),
                )
            })
        });
        Response::builder()
            .body(BodyExt::boxed(StreamBody::new(Box::pin(body))))
            .unwrap()
    }
}

impl S3HttpHandler for TestHttp {
    fn handle(&self, request: Request<Incoming>) -> HandlerFuture {
        if request.method() == Method::PUT {
            let chunks = Arc::clone(&self.chunks);
            let allocator = Arc::clone(&self.allocator);
            let locations = Arc::clone(&self.locations);
            Box::pin(async move { Ok(Self::put(chunks, allocator, locations, request).await) })
        } else {
            let response = self.get();
            Box::pin(async move { Ok(response) })
        }
    }
}

fn empty() -> ResponseBody {
    Full::new(Bytes::new()).map_err(|never| match never {}).boxed()
}
