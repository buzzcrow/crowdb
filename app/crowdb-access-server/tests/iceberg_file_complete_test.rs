#![cfg(feature = "iceberg")]

use std::future::pending;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use std::time::Duration;

use crowdb_access_server::iceberg::{active_io_for_tests, FileCompleteBody, FileS3ErrorCode};
use http_body_util::BodyExt;
use hyper::body::Body;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

const PREFIX: &[u8] = b"<?xml version=\"1.0\" encoding=\"UTF-8\"?>";

#[tokio::test(start_paused = true)]
async fn completion_streams_heartbeats_then_one_parseable_document() {
    let (sender, receiver) = tokio::sync::oneshot::channel();
    let mut body = FileCompleteBody::new(
        async move { receiver.await.unwrap() },
        "object",
        Duration::from_secs(10),
        Duration::from_secs(300),
    )
    .unwrap();
    assert_eq!(body.size_hint().exact(), None);
    let mut output = body.frame().await.unwrap().unwrap().into_data().unwrap().to_vec();
    assert_eq!(output, PREFIX);
    for _ in 0..3 {
        output.extend_from_slice(&body.frame().await.unwrap().unwrap().into_data().unwrap());
    }
    assert_eq!(&output[PREFIX.len()..], b"\n\n\n");
    sender
        .send(Ok([
            PREFIX,
            b"<CompleteMultipartUploadResult><ETag>etag</ETag></CompleteMultipartUploadResult>",
        ]
        .concat()))
        .unwrap();
    output.extend_from_slice(&body.collect().await.unwrap().to_bytes());
    let mut reader = quick_xml::Reader::from_reader(output.as_slice());
    let mut declarations = 0;
    loop {
        match reader.read_event().unwrap() {
            quick_xml::events::Event::Decl(_) => declarations += 1,
            quick_xml::events::Event::Eof => break,
            _ => {}
        }
    }
    assert_eq!(declarations, 1);
    assert!(output.ends_with(b"</CompleteMultipartUploadResult>"));
}

#[tokio::test(start_paused = true)]
async fn late_failure_and_work_deadline_return_error_xml() {
    let mut failure = FileCompleteBody::new(
        async { Err(FileS3ErrorCode::InvalidRequest) },
        "object<&>",
        Duration::from_secs(10),
        Duration::from_secs(300),
    )
    .unwrap();
    assert_eq!(
        failure.frame().await.unwrap().unwrap().into_data().unwrap(),
        PREFIX
    );
    let error = failure.frame().await.unwrap().unwrap().into_data().unwrap();
    let error = std::str::from_utf8(&error).unwrap();
    assert!(error.starts_with("<Error>"));
    assert!(error.contains("<Code>InvalidRequest</Code>"));
    assert!(error.contains("object&lt;&amp;&gt;"));
    assert!(failure.is_end_stream());
    assert_eq!(failure.size_hint().exact(), Some(0));
    assert!(failure.frame().await.is_none());

    let body = FileCompleteBody::new(
        pending(),
        "object",
        Duration::from_secs(10),
        Duration::from_secs(30),
    )
    .unwrap();
    let start = tokio::time::Instant::now();
    let output = body.collect().await.unwrap().to_bytes();
    assert_eq!(start.elapsed(), Duration::from_secs(30));
    assert!(std::str::from_utf8(&output)
        .unwrap()
        .contains("<Code>SlowDown</Code>"));
}

struct TestCancellation(Arc<AtomicBool>);

impl Drop for TestCancellation {
    fn drop(&mut self) {
        self.0.store(true, Ordering::SeqCst);
    }
}

#[tokio::test(start_paused = true)]
async fn disconnect_drops_pending_completion_without_detached_work() {
    let dropped = Arc::new(AtomicBool::new(false));
    let guard = TestCancellation(dropped.clone());
    let mut body = FileCompleteBody::new(
        async move {
            let _guard = guard;
            pending().await
        },
        "object",
        Duration::from_secs(10),
        Duration::from_secs(300),
    )
    .unwrap();
    body.frame().await.unwrap().unwrap();
    body.frame().await.unwrap().unwrap();
    assert!(!dropped.load(Ordering::SeqCst));
    drop(body);
    assert!(dropped.load(Ordering::SeqCst));
}

#[tokio::test(start_paused = true)]
async fn connection_activity_extends_idle_deadline() {
    let (stream, mut peer) = tokio::io::duplex(64);
    let (mut stream, expired) = active_io_for_tests(stream, Duration::from_secs(30));
    tokio::pin!(expired);
    for _ in 0..4 {
        tokio::select! {
            () = &mut expired => panic!("active connection expired"),
            () = tokio::time::sleep(Duration::from_secs(20)) => {}
        }
        stream.write_all(b" ").await.unwrap();
        assert_eq!(peer.read_u8().await.unwrap(), b' ');
        peer.write_all(b"x").await.unwrap();
        assert_eq!(stream.read_u8().await.unwrap(), b'x');
    }
    let start = tokio::time::Instant::now();
    expired.await;
    assert_eq!(start.elapsed(), Duration::from_secs(30));
}

#[tokio::test(start_paused = true)]
async fn active_response_transmission_survives_prior_absolute_lifetime() {
    let (stream, mut peer) = tokio::io::duplex(64);
    let (mut stream, expired) = active_io_for_tests(stream, Duration::from_secs(30));
    tokio::pin!(expired);
    let start = tokio::time::Instant::now();
    for _ in 0..4 {
        tokio::select! {
            () = &mut expired => panic!("connection expired before its deadline"),
            () = tokio::time::sleep(Duration::from_secs(20)) => {}
        }
        stream.write_all(b" ").await.unwrap();
        assert_eq!(peer.read_u8().await.unwrap(), b' ');
    }
    assert_eq!(start.elapsed(), Duration::from_secs(80));
    expired.await;
    assert_eq!(start.elapsed(), Duration::from_secs(110));
}
