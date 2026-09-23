use sha2::{compress256, Digest, Sha256};

use super::{FileIdentity, FileIoError};

const INITIAL: [u32; 8] = [
    0x6a09_e667,
    0xbb67_ae85,
    0x3c6e_f372,
    0xa54f_f53a,
    0x510e_527f,
    0x9b05_688c,
    0x1f83_d9ab,
    0x5be0_cd19,
];
const CHECKPOINT_BYTES: usize = 189;
const CHECKSUM_OFFSET: usize = CHECKPOINT_BYTES - 32;

#[derive(Clone)]
pub struct FileDigest {
    owner: FileIdentity,
    state: [u32; 8],
    tail: [u8; 64],
    length: u64,
}

impl FileDigest {
    #[must_use]
    pub const fn new(owner: FileIdentity) -> Self {
        Self {
            owner,
            state: INITIAL,
            tail: [0; 64],
            length: 0,
        }
    }

    #[must_use]
    pub const fn length(&self) -> u64 {
        self.length
    }

    /// # Errors
    /// Rejects messages whose bit length cannot be represented by SHA-256.
    pub fn update(&mut self, mut bytes: &[u8]) -> Result<(), FileIoError> {
        let length = self
            .length
            .checked_add(bytes.len() as u64)
            .filter(|length| *length <= u64::MAX / 8)
            .ok_or(FileIoError::Bounds)?;
        let used = usize::try_from(self.length % 64).map_err(|_| FileIoError::Bounds)?;
        self.length = length;
        if used != 0 {
            let count = (64 - used).min(bytes.len());
            self.tail[used..used + count].copy_from_slice(&bytes[..count]);
            bytes = &bytes[count..];
            if used + count != 64 {
                return Ok(());
            }
            compress256(&mut self.state, &[self.tail.into()]);
            self.tail = [0; 64];
        }
        let mut blocks = bytes.chunks_exact(64);
        for block in &mut blocks {
            let block: [u8; 64] = block.try_into().map_err(|_| FileIoError::Bounds)?;
            compress256(&mut self.state, &[block.into()]);
        }
        let remaining = blocks.remainder();
        self.tail[..remaining.len()].copy_from_slice(remaining);
        Ok(())
    }

    #[must_use]
    pub fn finish(mut self) -> [u8; 32] {
        let used = (self.length % 64) as usize;
        self.tail[used] = 0x80;
        if used >= 56 {
            compress256(&mut self.state, &[self.tail.into()]);
            self.tail = [0; 64];
        }
        self.tail[56..].copy_from_slice(&(self.length * 8).to_be_bytes());
        compress256(&mut self.state, &[self.tail.into()]);
        let mut digest = [0; 32];
        for (output, word) in digest.chunks_exact_mut(4).zip(self.state) {
            output.copy_from_slice(&word.to_be_bytes());
        }
        digest
    }

    #[must_use]
    pub fn checkpoint(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(CHECKPOINT_BYTES);
        bytes.extend_from_slice(b"ICHS\x01");
        bytes.extend_from_slice(self.owner.table.catalog.as_bytes());
        bytes.extend_from_slice(self.owner.table.table.as_bytes());
        bytes.extend_from_slice(self.owner.file.as_bytes());
        bytes.extend_from_slice(&self.length.to_be_bytes());
        for word in self.state {
            bytes.extend_from_slice(&word.to_be_bytes());
        }
        bytes.extend_from_slice(&self.tail);
        let checksum = Sha256::digest(&bytes);
        bytes.extend_from_slice(&checksum);
        bytes
    }

    /// Restores trusted-storage state, not a client-supplied digest assertion.
    /// The checksum detects corruption; it is not an authentication token.
    /// # Errors
    /// Rejects wrong versions, identities, lengths, checksums and noncanonical tails.
    pub fn restore(owner: FileIdentity, bytes: &[u8]) -> Result<Self, FileIoError> {
        if bytes.len() != CHECKPOINT_BYTES
            || &bytes[..5] != b"ICHS\x01"
            || &bytes[5..21] != owner.table.catalog.as_bytes()
            || &bytes[21..37] != owner.table.table.as_bytes()
            || &bytes[37..53] != owner.file.as_bytes()
            || Sha256::digest(&bytes[..CHECKSUM_OFFSET]).as_slice() != &bytes[CHECKSUM_OFFSET..]
        {
            return Err(FileIoError::Bounds);
        }
        let length = u64::from_be_bytes(bytes[53..61].try_into().map_err(|_| FileIoError::Bounds)?);
        if length > u64::MAX / 8 {
            return Err(FileIoError::Bounds);
        }
        let mut state = [0; 8];
        for (word, encoded) in state.iter_mut().zip(bytes[61..93].chunks_exact(4)) {
            *word = u32::from_be_bytes(encoded.try_into().map_err(|_| FileIoError::Bounds)?);
        }
        let tail: [u8; 64] = bytes[93..157].try_into().map_err(|_| FileIoError::Bounds)?;
        let used = (length % 64) as usize;
        if tail[used..].iter().any(|byte| *byte != 0) || (length < 64 && state != INITIAL) {
            return Err(FileIoError::Bounds);
        }
        Ok(Self {
            owner,
            state,
            tail,
            length,
        })
    }
}
