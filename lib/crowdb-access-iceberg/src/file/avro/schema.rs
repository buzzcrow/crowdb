use super::AvroContainerError;

mod binary;
mod parse;

#[derive(Clone, Copy, Debug)]
pub struct AvroDatumLimits {
    pub depth: usize,
    pub values: usize,
    pub value_bytes: usize,
}

impl AvroDatumLimits {
    pub(super) fn validate(self) -> Result<(), AvroContainerError> {
        if self.depth == 0
            || self.depth > 64
            || self.values == 0
            || self.values > 4_000_000
            || self.value_bytes == 0
            || self.value_bytes > 8 * 1024 * 1024
        {
            return Err(AvroContainerError::Bounds);
        }
        Ok(())
    }
}

pub struct AvroSchema {
    nodes: Vec<Node>,
    root: usize,
}

#[derive(Debug)]
enum Node {
    Null,
    Boolean,
    Int,
    Long,
    Float,
    Double,
    Bytes,
    String,
    Fixed(usize),
    Enum(usize),
    Record(Vec<usize>),
    Array(usize),
    Map(usize),
    Union(Vec<usize>),
}

impl AvroSchema {
    /// Compiles a writer's binary layout; reader-schema resolution and logical semantics are separate.
    /// # Errors
    /// Rejects malformed or excessive schemas, duplicate names and unresolved references.
    pub fn parse(bytes: &[u8]) -> Result<Self, AvroContainerError> {
        parse::compile(bytes)
    }

    /// Validates exactly the declared record count without retaining any decoded datum graph.
    /// # Errors
    /// Rejects malformed values, excessive work/depth, truncation and trailing payload bytes.
    pub fn validate_block(
        &self,
        bytes: &[u8],
        records: u64,
        limits: AvroDatumLimits,
    ) -> Result<(), AvroContainerError> {
        limits.validate()?;
        if bytes.len() > 8 * 1024 * 1024 || records > 1_000_000 {
            return Err(AvroContainerError::Bounds);
        }
        let mut input = binary::Input::new(bytes, limits);
        for _ in 0..records {
            input.datum(self, self.root, 1)?;
        }
        input.finish()
    }
}
