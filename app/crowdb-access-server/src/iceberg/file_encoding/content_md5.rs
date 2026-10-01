use base64::{engine::general_purpose::STANDARD, Engine};
use hyper::HeaderMap;
use md5::{Digest, Md5};

use super::FileEncodingError;

pub(super) struct ContentMd5 {
    expected: Option<[u8; 16]>,
    digest: Md5,
    deferred: bool,
}

impl ContentMd5 {
    pub(super) fn from_headers(headers: &HeaderMap) -> Result<Self, FileEncodingError> {
        let expected = super::header(headers, "content-md5")?
            .map(|value| {
                STANDARD
                    .decode(value)
                    .map_err(|_| FileEncodingError::Framing)?
                    .try_into()
                    .map_err(|_| FileEncodingError::Framing)
            })
            .transpose()?;
        Ok(Self {
            expected,
            digest: Md5::new(),
            deferred: false,
        })
    }

    pub(super) const fn is_declared(&self) -> bool {
        self.expected.is_some()
    }

    pub(super) fn digest(&self) -> [u8; 16] {
        self.digest.clone().finalize().into()
    }

    pub(super) fn update(&mut self, bytes: &[u8]) {
        if !self.deferred {
            self.digest.update(bytes);
        }
    }

    pub(super) fn defer(&mut self) {
        self.deferred = true;
    }

    pub(super) fn verify_deferred(&self, actual: [u8; 16]) -> Result<(), FileEncodingError> {
        if !self.deferred || self.expected.is_some_and(|expected| expected != actual) {
            return Err(FileEncodingError::Checksum);
        }
        Ok(())
    }

    pub(super) fn verify(&self) -> Result<(), FileEncodingError> {
        if self.deferred {
            return Ok(());
        }
        if self.expected.is_some_and(|expected| self.digest() != expected) {
            return Err(FileEncodingError::Checksum);
        }
        Ok(())
    }
}
