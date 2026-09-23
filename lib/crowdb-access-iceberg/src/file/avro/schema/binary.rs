use super::{AvroContainerError, AvroDatumLimits, AvroSchema, Node};

pub(super) struct Input<'data> {
    bytes: &'data [u8],
    offset: usize,
    remaining: usize,
    limits: AvroDatumLimits,
}

impl<'data> Input<'data> {
    pub(super) fn new(bytes: &'data [u8], limits: AvroDatumLimits) -> Self {
        Self {
            bytes,
            offset: 0,
            remaining: limits.values,
            limits,
        }
    }

    pub(super) fn finish(&self) -> Result<(), AvroContainerError> {
        if self.offset != self.bytes.len() {
            return Err(AvroContainerError::Schema);
        }
        Ok(())
    }

    pub(super) fn position(&self) -> usize {
        self.offset
    }

    pub(super) fn consume_value(&mut self, depth: usize) -> Result<(), AvroContainerError> {
        if depth > self.limits.depth {
            return Err(AvroContainerError::Bounds);
        }
        self.remaining = self.remaining.checked_sub(1).ok_or(AvroContainerError::Bounds)?;
        Ok(())
    }

    pub(super) fn datum(
        &mut self,
        schema: &AvroSchema,
        index: usize,
        depth: usize,
    ) -> Result<(), AvroContainerError> {
        self.consume_value(depth)?;
        match &schema.nodes[index] {
            Node::Null => {}
            Node::Boolean => {
                if self.take(1)?[0] > 1 {
                    return Err(AvroContainerError::Schema);
                }
            }
            Node::Int => {
                i32::try_from(self.long()?).map_err(|_| AvroContainerError::Schema)?;
            }
            Node::Long => {
                self.long()?;
            }
            Node::Float => {
                self.take(4)?;
            }
            Node::Double => {
                self.take(8)?;
            }
            Node::Bytes => {
                self.variable(false)?;
            }
            Node::String => {
                self.variable(true)?;
            }
            Node::Fixed(length) => {
                if *length > self.limits.value_bytes {
                    return Err(AvroContainerError::Bounds);
                }
                self.take(*length)?;
            }
            Node::Enum(symbols) => {
                if self.size()? >= *symbols {
                    return Err(AvroContainerError::Schema);
                }
            }
            Node::Record(fields) => {
                for field in fields {
                    self.datum(schema, field.node, depth + 1)?;
                }
            }
            Node::Array(child, _) | Node::LogicalMap(child) => {
                self.collection(schema, *child, false, depth)?;
            }
            Node::Map(child) => self.collection(schema, *child, true, depth)?,
            Node::Union(branches) => {
                let branch = *branches.get(self.size()?).ok_or(AvroContainerError::Schema)?;
                self.datum(schema, branch, depth + 1)?;
            }
        }
        Ok(())
    }

    fn collection(
        &mut self,
        schema: &AvroSchema,
        child: usize,
        map: bool,
        depth: usize,
    ) -> Result<(), AvroContainerError> {
        loop {
            let count = self.long()?;
            if count == 0 {
                return Ok(());
            }
            let items = count
                .checked_abs()
                .and_then(|count| usize::try_from(count).ok())
                .ok_or(AvroContainerError::Bounds)?;
            if items > self.remaining {
                return Err(AvroContainerError::Bounds);
            }
            let end = if count < 0 {
                let length = self.size()?;
                Some(
                    self.offset
                        .checked_add(length)
                        .filter(|end| *end <= self.bytes.len())
                        .ok_or(AvroContainerError::Schema)?,
                )
            } else {
                None
            };
            for _ in 0..items {
                if map {
                    self.variable(true)?;
                }
                self.datum(schema, child, depth + 1)?;
                if end.is_some_and(|end| self.offset > end) {
                    return Err(AvroContainerError::Schema);
                }
            }
            if end.is_some_and(|end| self.offset != end) {
                return Err(AvroContainerError::Schema);
            }
        }
    }

    pub(super) fn take(&mut self, length: usize) -> Result<&'data [u8], AvroContainerError> {
        let end = self
            .offset
            .checked_add(length)
            .ok_or(AvroContainerError::Bounds)?;
        let value = self
            .bytes
            .get(self.offset..end)
            .ok_or(AvroContainerError::Schema)?;
        self.offset = end;
        Ok(value)
    }

    pub(super) fn take_remaining(&mut self) -> Result<&'data [u8], AvroContainerError> {
        self.take(self.bytes.len() - self.offset)
    }

    pub(super) fn long(&mut self) -> Result<i64, AvroContainerError> {
        let mut value = 0_u64;
        for shift in (0..70).step_by(7) {
            let byte = self.take(1)?[0];
            if shift == 63 && byte > 1 {
                return Err(AvroContainerError::Schema);
            }
            value |= u64::from(byte & 127) << shift;
            if byte & 128 == 0 {
                let magnitude = i64::try_from(value >> 1).map_err(|_| AvroContainerError::Schema)?;
                return Ok(magnitude ^ -i64::from((value & 1) as u8));
            }
        }
        Err(AvroContainerError::Schema)
    }

    pub(super) fn size(&mut self) -> Result<usize, AvroContainerError> {
        usize::try_from(self.long()?).map_err(|_| AvroContainerError::Schema)
    }

    fn variable(&mut self, string: bool) -> Result<(), AvroContainerError> {
        let length = self.size()?;
        if length > self.limits.value_bytes {
            return Err(AvroContainerError::Bounds);
        }
        let bytes = self.take(length)?;
        if string {
            std::str::from_utf8(bytes).map_err(|_| AvroContainerError::Schema)?;
        }
        Ok(())
    }
}
