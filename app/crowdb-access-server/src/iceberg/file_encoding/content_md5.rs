use base64::{engine::general_purpose::STANDARD, Engine};
use hyper::HeaderMap;
use md5::{Digest, Md5};

use super::FileEncodingError;

pub(super) struct ContentMd5 {
    expected: [u8; 16],
    digest: Md5,
}

impl ContentMd5 {
    pub(super) fn from_headers(headers: &HeaderMap) -> Result<Option<Self>, FileEncodingError> {
        let Some(value) = super::header(headers, "content-md5")? else {
            return Ok(None);
        };
        let expected = STANDARD
            .decode(value)
            .map_err(|_| FileEncodingError::Framing)?
            .try_into()
            .map_err(|_| FileEncodingError::Framing)?;
        Ok(Some(Self {
            expected,
            digest: Md5::new(),
        }))
    }

    pub(super) fn update(&mut self, bytes: &[u8]) {
        self.digest.update(bytes);
    }

    pub(super) fn verify(&self) -> Result<(), FileEncodingError> {
        if <[u8; 16]>::from(self.digest.clone().finalize()) != self.expected {
            return Err(FileEncodingError::Checksum);
        }
        Ok(())
    }
}
