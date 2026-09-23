use flate2::{Decompress, FlushDecompress, Status};

use super::{AvroBlock, AvroContainerError};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AvroCodec {
    Null,
    Deflate,
}

impl AvroCodec {
    /// # Errors
    /// Rejects codecs not supported by the bounded block decoder.
    pub fn parse(name: &str) -> Result<Self, AvroContainerError> {
        match name {
            "null" => Ok(Self::Null),
            "deflate" => Ok(Self::Deflate),
            _ => Err(AvroContainerError::Codec),
        }
    }
}

impl AvroBlock {
    /// Decodes at most one bounded block; this does not validate record semantics.
    /// # Errors
    /// Rejects expansion beyond the independent output cap, truncation and suffixes.
    pub fn decode(self, codec: AvroCodec, max_decoded_bytes: usize) -> Result<Vec<u8>, AvroContainerError> {
        if max_decoded_bytes == 0
            || max_decoded_bytes > 8 * 1024 * 1024
            || self.encoded.len() > 8 * 1024 * 1024
        {
            return Err(AvroContainerError::Bounds);
        }
        match codec {
            AvroCodec::Null if self.encoded.len() <= max_decoded_bytes => Ok(self.encoded),
            AvroCodec::Null => Err(AvroContainerError::Bounds),
            AvroCodec::Deflate => inflate(&self.encoded, max_decoded_bytes),
        }
    }
}

fn inflate(encoded: &[u8], limit: usize) -> Result<Vec<u8>, AvroContainerError> {
    let mut decoder = Decompress::new(false);
    let mut output = vec![0; limit + 1];
    let status = decoder
        .decompress(encoded, &mut output, FlushDecompress::Finish)
        .map_err(|_| AvroContainerError::Framing)?;
    if decoder.total_out() > limit as u64 {
        return Err(AvroContainerError::Bounds);
    }
    if status != Status::StreamEnd || decoder.total_in() != encoded.len() as u64 {
        return Err(AvroContainerError::Framing);
    }
    let length = usize::try_from(decoder.total_out()).map_err(|_| AvroContainerError::Bounds)?;
    output.truncate(length);
    Ok(output)
}
