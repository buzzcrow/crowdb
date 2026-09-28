use super::common::now_ms;
use base64::engine::general_purpose::STANDARD;
use base64::Engine;
use hmac::{Hmac, Mac};
use md5::Md5;
use reqwest::{Client, Method, Response};
use sha2::{Digest, Sha256};
use std::fmt::Write;

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
        let now =
            chrono::DateTime::<chrono::Utc>::from_timestamp_millis(i64::try_from(now_ms()).unwrap()).unwrap();
        let date = now.format("%Y%m%dT%H%M%SZ").to_string();
        let short = now.format("%Y%m%d").to_string();
        let hash = hex(&Sha256::digest(body));
        let host = self.address.to_string();
        let names = "host;x-amz-content-sha256;x-amz-date;x-amz-security-token";
        let canonical = format!(
            "{}\n{path}\n{query}\nhost:{host}\nx-amz-content-sha256:{hash}\nx-amz-date:{date}\nx-amz-security-token:{}\n\n{names}\n{hash}",
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
            .body(body.to_vec());
        if md5 {
            request = request.header("content-md5", STANDARD.encode(Md5::digest(body)));
        }
        if let Some(range) = range {
            request = request.header("range", range);
        }
        request
    }
}
