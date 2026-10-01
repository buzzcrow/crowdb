use super::common::now_ms;
use base64::engine::general_purpose::STANDARD;
use base64::Engine;
use hmac::{Hmac, Mac};
use hyper::body::Bytes;
use md5::Md5;
use reqwest::{Client, Method, Response};
use sha2::{Digest, Sha256};
use std::fmt::Write;
use std::io;

fn hex(bytes: &[u8]) -> String {
    let mut result = String::new();
    for byte in bytes {
        write!(result, "{byte:02x}").unwrap();
    }
    result
}

fn mac(key: &[u8], input: &str) -> Vec<u8> {
    let mut signer = Hmac::<Sha256>::new_from_slice(key).unwrap();
    signer.update(input.as_bytes());
    signer.finalize().into_bytes().to_vec()
}

pub struct TestFileClient {
    pub client: Client,
    pub credentials: crowdb_access_iceberg::file::FileCredentials,
    pub address: std::net::SocketAddr,
}

struct SignedPayload {
    body: reqwest::Body,
    hash: String,
    content_md5: Option<String>,
}

impl TestFileClient {
    pub async fn send(&self, method: Method, path: &str, query: &str, body: &[u8], md5: bool) -> Response {
        self.send_range(method, path, query, body, md5, None).await
    }

    pub async fn send_range(
        &self,
        method: Method,
        path: &str,
        query: &str,
        body: &[u8],
        md5: bool,
        range: Option<&str>,
    ) -> Response {
        self.request(method, path, query, body, md5, range)
            .send()
            .await
            .unwrap()
    }

    pub fn request(
        &self,
        method: Method,
        path: &str,
        query: &str,
        body: &[u8],
        md5: bool,
        range: Option<&str>,
    ) -> reqwest::RequestBuilder {
        let content_md5 = md5.then(|| STANDARD.encode(Md5::digest(body)));
        let hash = if md5 {
            "UNSIGNED-PAYLOAD".to_owned()
        } else {
            hex(&Sha256::digest(body))
        };
        self.signed_request(
            method,
            path,
            query,
            SignedPayload {
                body: reqwest::Body::from(body.to_vec()),
                hash,
                content_md5,
            },
            range,
        )
    }

    #[allow(dead_code)]
    pub async fn send_repeated(
        &self,
        method: Method,
        path: &str,
        block: Bytes,
        repetitions: usize,
        md5: [u8; 16],
    ) -> Response {
        self.send_repeated_with_query(method, path, "", block, repetitions, md5)
            .await
    }

    #[allow(dead_code)]
    pub async fn send_repeated_with_query(
        &self,
        method: Method,
        path: &str,
        query: &str,
        block: Bytes,
        repetitions: usize,
        md5: [u8; 16],
    ) -> Response {
        let length = block.len() * repetitions;
        let frames = futures::stream::iter((0..repetitions).map(move |_| Ok::<_, io::Error>(block.clone())));
        self.signed_request(
            method,
            path,
            query,
            SignedPayload {
                body: reqwest::Body::wrap_stream(frames),
                hash: "UNSIGNED-PAYLOAD".to_owned(),
                content_md5: Some(STANDARD.encode(md5)),
            },
            None,
        )
        .header("content-length", length)
        .send()
        .await
        .unwrap()
    }

    #[allow(dead_code)]
    pub fn request_stream(
        &self,
        method: Method,
        path: &str,
        length: usize,
        md5: [u8; 16],
        receiver: tokio::sync::mpsc::Receiver<Result<Bytes, io::Error>>,
    ) -> reqwest::RequestBuilder {
        let frames = futures::stream::unfold(receiver, |mut receiver| async move {
            receiver.recv().await.map(|frame| (frame, receiver))
        });
        self.signed_request(
            method,
            path,
            "",
            SignedPayload {
                body: reqwest::Body::wrap_stream(frames),
                hash: "UNSIGNED-PAYLOAD".to_owned(),
                content_md5: Some(STANDARD.encode(md5)),
            },
            None,
        )
        .header("content-length", length)
    }

    fn signed_request(
        &self,
        method: Method,
        path: &str,
        query: &str,
        payload: SignedPayload,
        range: Option<&str>,
    ) -> reqwest::RequestBuilder {
        let SignedPayload {
            body,
            hash,
            content_md5,
        } = payload;
        let now =
            chrono::DateTime::<chrono::Utc>::from_timestamp_millis(i64::try_from(now_ms()).unwrap()).unwrap();
        let date = now.format("%Y%m%dT%H%M%SZ").to_string();
        let short = now.format("%Y%m%d").to_string();
        let host = self.address.to_string();
        let names = if content_md5.is_some() {
            "content-md5;host;x-amz-content-sha256;x-amz-date;x-amz-security-token"
        } else {
            "host;x-amz-content-sha256;x-amz-date;x-amz-security-token"
        };
        let md5_header = content_md5
            .as_ref()
            .map_or_else(String::new, |value| format!("content-md5:{value}\n"));
        let canonical = format!(
            "{}\n{path}\n{query}\n{md5_header}host:{host}\nx-amz-content-sha256:{hash}\nx-amz-date:{date}\nx-amz-security-token:{}\n\n{names}\n{hash}",
            method.as_str(), self.credentials.session_token()
        );
        let date_key = mac(
            format!("AWS4{}", self.credentials.secret_access_key()).as_bytes(),
            &short,
        );
        let region_key = mac(&date_key, "us-east-1");
        let service_key = mac(&region_key, "s3");
        let signing_key = mac(&service_key, "aws4_request");
        let scope = format!("{short}/us-east-1/s3/aws4_request");
        let string_to_sign = format!(
            "AWS4-HMAC-SHA256\n{date}\n{scope}\n{}",
            hex(&Sha256::digest(canonical))
        );
        let authorization = format!(
            "AWS4-HMAC-SHA256 Credential={}/{scope}, SignedHeaders={names}, Signature={}",
            self.credentials.access_key_id(),
            hex(&mac(&signing_key, &string_to_sign))
        );
        let url = if query.is_empty() {
            format!("http://{host}{path}")
        } else {
            format!("http://{host}{path}?{query}")
        };
        let mut request = self
            .client
            .request(method, url)
            .header("host", host)
            .header("x-amz-content-sha256", hash)
            .header("x-amz-date", date)
            .header("x-amz-security-token", self.credentials.session_token())
            .header("authorization", authorization)
            .body(body);
        if let Some(content_md5) = content_md5 {
            request = request.header("content-md5", content_md5);
        }
        if let Some(range) = range {
            request = request.header("range", range);
        }
        request
    }
}
