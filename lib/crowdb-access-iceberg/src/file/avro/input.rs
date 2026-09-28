use super::{AvroContainerError, FileReader};

pub(super) struct Input {
    reader: FileReader,
    frame: Vec<u8>,
    offset: usize,
    pub(super) position: u64,
    pub(super) end: u64,
    pub(super) digest: Option<crate::file::FileDigest>,
}

impl Input {
    pub(super) fn new(reader: FileReader, end: u64) -> Self {
        Self {
            reader,
            frame: Vec::new(),
            offset: 0,
            position: 0,
            end,
            digest: None,
        }
    }

    async fn fill(&mut self) -> Result<(), AvroContainerError> {
        if self.position >= self.end {
            return Err(AvroContainerError::Bounds);
        }
        if self.offset == self.frame.len() {
            self.frame = self.reader.next().await?.ok_or(AvroContainerError::Framing)?;
            self.offset = 0;
        }
        Ok(())
    }

    pub(super) async fn take(&mut self, length: usize) -> Result<Vec<u8>, AvroContainerError> {
        if length as u64 > self.end - self.position {
            return Err(AvroContainerError::Bounds);
        }
        let mut result = Vec::with_capacity(length);
        while result.len() < length {
            self.fill().await?;
            let count = (length - result.len()).min(self.frame.len() - self.offset);
            result.extend_from_slice(&self.frame[self.offset..self.offset + count]);
            if let Some(digest) = &mut self.digest {
                digest.update(&self.frame[self.offset..self.offset + count])?;
            }
            self.offset += count;
            self.position += count as u64;
        }
        Ok(result)
    }

    pub(super) async fn long(&mut self) -> Result<i64, AvroContainerError> {
        let mut value = 0_u64;
        for shift in (0..70).step_by(7) {
            self.fill().await?;
            let byte = self.frame[self.offset];
            if let Some(digest) = &mut self.digest {
                digest.update(&[byte])?;
            }
            self.offset += 1;
            self.position += 1;
            if shift == 63 && byte > 1 {
                return Err(AvroContainerError::Framing);
            }
            value |= u64::from(byte & 127) << shift;
            if byte & 128 == 0 {
                let magnitude = i64::try_from(value >> 1).map_err(|_| AvroContainerError::Framing)?;
                return Ok(magnitude ^ -i64::from((value & 1) as u8));
            }
        }
        Err(AvroContainerError::Framing)
    }

    pub(super) async fn size(&mut self, limit: usize) -> Result<usize, AvroContainerError> {
        usize::try_from(self.long().await?)
            .ok()
            .filter(|size| *size <= limit)
            .ok_or(AvroContainerError::Bounds)
    }
}
