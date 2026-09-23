use hmac::Mac;
use sha2::{Digest, Sha256};
use zeroize::ZeroizeOnDrop;

use super::{
    decode_hex, hex, signing_key, AuthError, CredentialProvider, HmacSha256, ParsedAuthorization,
    RawAuthRequest, SigV4Verifier,
};

pub(super) fn supported(value: &str) -> bool {
    matches!(
        value,
        "STREAMING-AWS4-HMAC-SHA256-PAYLOAD"
            | "STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER"
            | "STREAMING-UNSIGNED-PAYLOAD-TRAILER"
    )
}

#[derive(ZeroizeOnDrop)]
pub struct StreamingPayloadVerifier {
    key: Vec<u8>,
    date: String,
    scope: String,
    previous: String,
    signed: bool,
    trailer: bool,
}

impl<Provider: CredentialProvider> SigV4Verifier<Provider> {
    /// Authenticates the streaming seed only. The caller must verify every chunk,
    /// terminal chunk, declared checksum and trailer before publishing any bytes.
    /// # Errors
    /// Rejects unsupported streaming modes, unsigned framing headers and invalid seeds.
    pub fn verify_streaming(
        &self,
        request: RawAuthRequest<'_>,
        now: u64,
    ) -> Result<StreamingPayloadVerifier, AuthError> {
        let value = |name| {
            request
                .headers
                .get(name)
                .and_then(|value| value.to_str().ok())
                .ok_or(AuthError::Rejected)
        };
        let mode = value("x-amz-content-sha256")?;
        if !supported(mode) || request.method != hyper::Method::PUT {
            return Err(AuthError::Rejected);
        }
        let authorization = value("authorization")?;
        let parsed = ParsedAuthorization::parse(authorization)?;
        let trailer = mode.ends_with("-TRAILER");
        for name in [
            "authorization",
            "x-amz-content-sha256",
            "x-amz-date",
            "content-encoding",
            "x-amz-decoded-content-length",
            "x-amz-trailer",
        ] {
            if request.headers.get_all(name).iter().count() > 1 {
                return Err(AuthError::Rejected);
            }
        }
        for name in ["content-encoding", "x-amz-decoded-content-length", "x-amz-date"] {
            if !parsed.signed_headers.split(';').any(|signed| signed == name) {
                return Err(AuthError::Rejected);
            }
        }
        if trailer
            && !parsed
                .signed_headers
                .split(';')
                .any(|name| name == "x-amz-trailer")
        {
            return Err(AuthError::Rejected);
        }
        self.verify_header(request, authorization, now, true)?;
        let credential = self
            .provider
            .lookup(parsed.access_key)
            .ok_or(AuthError::Rejected)?;
        Ok(StreamingPayloadVerifier {
            key: signing_key(&credential.secret_key, parsed.date, parsed.region, "s3")?,
            date: value("x-amz-date")?.to_owned(),
            scope: format!("{}/{}/s3/aws4_request", parsed.date, parsed.region),
            previous: parsed.signature.to_owned(),
            signed: mode != "STREAMING-UNSIGNED-PAYLOAD-TRAILER",
            trailer,
        })
    }
}

impl StreamingPayloadVerifier {
    #[must_use]
    pub const fn is_signed(&self) -> bool {
        self.signed
    }

    #[must_use]
    pub const fn has_trailer(&self) -> bool {
        self.trailer
    }

    /// # Errors
    /// Rejects missing, extra or incorrect chunk signatures without advancing the chain.
    pub fn verify_chunk(&mut self, digest: [u8; 32], signature: Option<&str>) -> Result<(), AuthError> {
        let suffix = format!("{:x}\n{}", Sha256::digest([]), hex(&digest));
        self.verify("AWS4-HMAC-SHA256-PAYLOAD", &suffix, signature)
    }

    /// # Errors
    /// Rejects a trailer signature not chained to the verified terminal chunk.
    pub fn verify_trailer(&mut self, canonical: &str, signature: Option<&str>) -> Result<(), AuthError> {
        if !self.trailer {
            return Err(AuthError::Rejected);
        }
        self.verify(
            "AWS4-HMAC-SHA256-TRAILER",
            &hex(&Sha256::digest(canonical)),
            signature,
        )
    }

    fn verify(&mut self, algorithm: &str, suffix: &str, signature: Option<&str>) -> Result<(), AuthError> {
        if !self.signed {
            return if signature.is_none() {
                Ok(())
            } else {
                Err(AuthError::Rejected)
            };
        }
        let signature = signature.ok_or(AuthError::Rejected)?;
        let message = format!(
            "{algorithm}\n{}\n{}\n{}\n{suffix}",
            self.date, self.scope, self.previous
        );
        let mut signer = HmacSha256::new_from_slice(&self.key).map_err(|_| AuthError::Rejected)?;
        signer.update(message.as_bytes());
        signer
            .verify_slice(&decode_hex(signature)?)
            .map_err(|_| AuthError::Rejected)?;
        signature.clone_into(&mut self.previous);
        Ok(())
    }
}
