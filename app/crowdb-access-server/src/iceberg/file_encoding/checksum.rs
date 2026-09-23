use base64::{engine::general_purpose::STANDARD, Engine};
use hyper::HeaderMap;
use sha1::Sha1;
use sha2::{Digest, Sha256};

use super::FileEncodingError;

const CRC32: crc::Crc<u32> = crc::Crc::<u32>::new(&crc::CRC_32_ISO_HDLC);
const CRC32C: crc::Crc<u32> = crc::Crc::<u32>::new(&crc::CRC_32_ISCSI);
const CRC64: crc::Crc<u64> = crc::Crc::<u64>::new(&crc::CRC_64_NVME);
const NAMES: [&str; 5] = ["crc32", "crc32c", "crc64nvme", "sha1", "sha256"];

#[derive(Clone)]
enum DigestState {
    Crc32(crc::Digest<'static, u32>),
    Crc64(crc::Digest<'static, u64>),
    Sha1(Sha1),
    Sha256(Sha256),
}

pub(super) struct Checksum {
    name: String,
    state: DigestState,
    expected: Option<String>,
}

impl Checksum {
    pub(super) fn from_headers(
        headers: &HeaderMap,
        trailer: bool,
    ) -> Result<Option<Self>, FileEncodingError> {
        for name in headers.keys() {
            if let Some(algorithm) = name.as_str().strip_prefix("x-amz-checksum-") {
                if !NAMES.contains(&algorithm) && algorithm != "type" {
                    return Err(FileEncodingError::Framing);
                }
            }
        }
        let mut selected = None;
        for name in NAMES {
            let header = format!("x-amz-checksum-{name}");
            if let Some(value) = super::header(headers, &header)? {
                if selected.is_some() || trailer {
                    return Err(FileEncodingError::Framing);
                }
                selected = Some(Self::new(&header, Some(value.to_owned()))?);
            }
        }
        if trailer {
            selected = Some(Self::new(
                super::header(headers, "x-amz-trailer")?.ok_or(FileEncodingError::Framing)?,
                None,
            )?);
        }
        if let Some(algorithm) = super::header(headers, "x-amz-sdk-checksum-algorithm")? {
            if selected.as_ref().map_or(true, |selected| {
                selected.name != format!("x-amz-checksum-{}", algorithm.to_ascii_lowercase())
            }) {
                return Err(FileEncodingError::Framing);
            }
        }
        Ok(selected)
    }

    fn new(name: &str, expected: Option<String>) -> Result<Self, FileEncodingError> {
        let state = match name {
            "x-amz-checksum-crc32" => DigestState::Crc32(CRC32.digest()),
            "x-amz-checksum-crc32c" => DigestState::Crc32(CRC32C.digest()),
            "x-amz-checksum-crc64nvme" => DigestState::Crc64(CRC64.digest()),
            "x-amz-checksum-sha1" => DigestState::Sha1(Sha1::new()),
            "x-amz-checksum-sha256" => DigestState::Sha256(Sha256::new()),
            _ => return Err(FileEncodingError::Framing),
        };
        Ok(Self {
            name: name.to_owned(),
            state,
            expected,
        })
    }

    pub(super) fn update(&mut self, bytes: &[u8]) {
        match &mut self.state {
            DigestState::Crc32(digest) => digest.update(bytes),
            DigestState::Crc64(digest) => digest.update(bytes),
            DigestState::Sha1(digest) => digest.update(bytes),
            DigestState::Sha256(digest) => digest.update(bytes),
        }
    }

    pub(super) fn trailer(&mut self, line: &str) -> Result<(), FileEncodingError> {
        let (name, value) = line.split_once(':').ok_or(FileEncodingError::Framing)?;
        if name != self.name || self.expected.is_some() {
            return Err(FileEncodingError::Framing);
        }
        self.expected = Some(value.to_owned());
        self.verify()
    }

    pub(super) fn verify(&self) -> Result<(), FileEncodingError> {
        let value = match self.state.clone() {
            DigestState::Crc32(digest) => STANDARD.encode(digest.finalize().to_be_bytes()),
            DigestState::Crc64(digest) => STANDARD.encode(digest.finalize().to_be_bytes()),
            DigestState::Sha1(digest) => STANDARD.encode(digest.finalize()),
            DigestState::Sha256(digest) => STANDARD.encode(digest.finalize()),
        };
        if self.expected.as_deref() == Some(value.as_str()) {
            Ok(())
        } else {
            Err(FileEncodingError::Checksum)
        }
    }
}
