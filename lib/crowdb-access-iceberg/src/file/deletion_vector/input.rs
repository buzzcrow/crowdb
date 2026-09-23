use super::{DeletionVectorError, FileReader};

pub(super) struct Input {
    reader: FileReader,
    frame: Vec<u8>,
    offset: usize,
    length: u64,
    pub(super) position: u64,
    crc: crc32fast::Hasher,
}

impl Input {
    pub(super) fn new(reader: FileReader, length: u64) -> Self {
        Self {
            reader,
            frame: Vec::new(),
            offset: 0,
            length,
            position: 0,
            crc: crc32fast::Hasher::new(),
        }
    }

    pub(super) fn crc(&self) -> u32 {
        self.crc.clone().finalize()
    }

    pub(super) async fn take<const SIZE: usize>(&mut self) -> Result<[u8; SIZE], DeletionVectorError> {
        if self
            .position
            .checked_add(SIZE as u64)
            .map_or(true, |end| end > self.length)
        {
            return Err(DeletionVectorError::Invalid);
        }
        let mut bytes = [0; SIZE];
        let mut copied = 0;
        while copied < SIZE {
            if self.offset == self.frame.len() {
                self.frame = self.reader.next().await?.ok_or(DeletionVectorError::Invalid)?;
                self.offset = 0;
                let start = usize::try_from(4_u64.saturating_sub(self.position))
                    .map_err(|_| DeletionVectorError::Bounds)?
                    .min(self.frame.len());
                let end = usize::try_from(
                    self.length
                        .saturating_sub(4)
                        .saturating_sub(self.position)
                        .min(self.frame.len() as u64),
                )
                .map_err(|_| DeletionVectorError::Bounds)?;
                if start < end {
                    self.crc.update(&self.frame[start..end]);
                }
            }
            let count = (SIZE - copied).min(self.frame.len() - self.offset);
            let chunk = &self.frame[self.offset..self.offset + count];
            bytes[copied..copied + count].copy_from_slice(chunk);
            self.offset += count;
            self.position += count as u64;
            copied += count;
        }
        Ok(bytes)
    }

    pub(super) async fn u16(&mut self) -> Result<u16, DeletionVectorError> {
        Ok(u16::from_le_bytes(self.take::<2>().await?))
    }
    pub(super) async fn u32(&mut self) -> Result<u32, DeletionVectorError> {
        Ok(u32::from_le_bytes(self.take::<4>().await?))
    }
    pub(super) async fn u64(&mut self) -> Result<u64, DeletionVectorError> {
        Ok(u64::from_le_bytes(self.take::<8>().await?))
    }
}
