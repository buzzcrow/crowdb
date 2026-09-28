use crowdb_access_s3::auth::StreamingPayloadVerifier;
use hyper::body::Bytes;
use sha2::{Digest, Sha256};

use super::{checksum::Checksum, FileEncodingError};

enum State {
    Header,
    Data(u64),
    Separator,
    Checksum,
    Signature,
    End,
    Done,
}

pub(super) struct Chunks {
    verifier: StreamingPayloadVerifier,
    state: State,
    line: Vec<u8>,
    signature: Option<String>,
    hash: Sha256,
    checksum: Option<Checksum>,
    canonical_trailer: String,
    remaining: u64,
}

impl Chunks {
    pub(super) fn new(verifier: StreamingPayloadVerifier, checksum: Option<Checksum>, length: u64) -> Self {
        Self {
            verifier,
            state: State::Header,
            line: Vec::new(),
            signature: None,
            hash: Sha256::new(),
            checksum,
            canonical_trailer: String::new(),
            remaining: length,
        }
    }

    pub(super) fn next(&mut self, input: &mut Bytes) -> Result<Option<Bytes>, FileEncodingError> {
        while !input.is_empty() {
            if let State::Data(remaining) = self.state {
                let length = input
                    .len()
                    .min(64 * 1024)
                    .min(usize::try_from(remaining).unwrap_or(usize::MAX));
                let bytes = input.split_to(length);
                self.hash.update(&bytes);
                if let Some(checksum) = &mut self.checksum {
                    checksum.update(&bytes);
                }
                self.remaining -= length as u64;
                let remaining = remaining - length as u64;
                self.state = if remaining == 0 {
                    self.verify_chunk()?;
                    State::Separator
                } else {
                    State::Data(remaining)
                };
                return Ok(Some(bytes));
            }
            if matches!(self.state, State::Done) {
                return Err(FileEncodingError::Framing);
            }
            let byte = input.split_to(1)[0];
            self.line.push(byte);
            if self.line.len() > 1024 {
                return Err(FileEncodingError::Framing);
            }
            if byte == b'\n' {
                let line = std::mem::take(&mut self.line);
                let line = line.strip_suffix(b"\r\n").ok_or(FileEncodingError::Framing)?;
                let line = std::str::from_utf8(line).map_err(|_| FileEncodingError::Framing)?;
                self.line(line)?;
            }
        }
        Ok(None)
    }

    fn line(&mut self, line: &str) -> Result<(), FileEncodingError> {
        match self.state {
            State::Header => self.start_chunk(line)?,
            State::Separator if line.is_empty() => self.state = State::Header,
            State::Checksum => {
                self.checksum
                    .as_mut()
                    .ok_or(FileEncodingError::Framing)?
                    .trailer(line)?;
                self.canonical_trailer = format!("{line}\n");
                self.state = if self.verifier.is_signed() {
                    State::Signature
                } else {
                    State::End
                };
            }
            State::Signature => {
                let signature = line
                    .strip_prefix("x-amz-trailer-signature:")
                    .ok_or(FileEncodingError::Framing)?;
                self.verifier
                    .verify_trailer(&self.canonical_trailer, Some(signature))
                    .map_err(|_| FileEncodingError::Signature)?;
                self.state = State::End;
            }
            State::End if line.is_empty() => self.state = State::Done,
            _ => return Err(FileEncodingError::Framing),
        }
        Ok(())
    }

    fn start_chunk(&mut self, line: &str) -> Result<(), FileEncodingError> {
        let (length, signature) = if self.verifier.is_signed() {
            let (length, signature) = line
                .split_once(";chunk-signature=")
                .ok_or(FileEncodingError::Framing)?;
            if signature.len() != 64 || !signature.bytes().all(|byte| byte.is_ascii_hexdigit()) {
                return Err(FileEncodingError::Framing);
            }
            (length, Some(signature.to_owned()))
        } else {
            (line, None)
        };
        if length.is_empty() || length.len() > 16 || !length.bytes().all(|byte| byte.is_ascii_hexdigit()) {
            return Err(FileEncodingError::Framing);
        }
        let length = u64::from_str_radix(length, 16).map_err(|_| FileEncodingError::Framing)?;
        if length > self.remaining {
            return Err(FileEncodingError::Length);
        }
        self.signature = signature;
        if length == 0 {
            if self.remaining != 0 {
                return Err(FileEncodingError::Length);
            }
            self.verify_chunk()?;
            self.state = if self.verifier.has_trailer() {
                State::Checksum
            } else {
                State::End
            };
        } else {
            self.state = State::Data(length);
        }
        Ok(())
    }

    fn verify_chunk(&mut self) -> Result<(), FileEncodingError> {
        let digest = std::mem::take(&mut self.hash).finalize().into();
        self.verifier
            .verify_chunk(digest, self.signature.as_deref())
            .map_err(|_| FileEncodingError::Signature)
    }

    pub(super) fn finish(&self) -> Result<(), FileEncodingError> {
        if !matches!(self.state, State::Done) || !self.line.is_empty() || self.remaining != 0 {
            return Err(FileEncodingError::Framing);
        }
        if let Some(checksum) = &self.checksum {
            checksum.verify()?;
        }
        Ok(())
    }
}
