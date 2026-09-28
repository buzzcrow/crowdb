use super::{AvroContainerError, AvroDatumLimits, AvroScalarType, AvroSchema, Input, Node};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AvroMetricMap<'data> {
    bytes: &'data [u8],
    kind: AvroScalarType,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AvroMetricValue<'data> {
    Long(i64),
    Bytes(&'data [u8]),
}

impl<'data> AvroMetricMap<'data> {
    pub(super) fn new(bytes: &'data [u8], kind: AvroScalarType) -> Self {
        Self { bytes, kind }
    }

    /// Visits a validated integer-keyed logical map without allocating its values.
    /// # Errors
    /// Rejects excessive entries or bytes, invalid framing, or a visitor error.
    pub fn visit<Error: From<AvroContainerError>>(
        self,
        max_items: usize,
        max_bytes: usize,
        mut visitor: impl FnMut(i32, AvroMetricValue<'data>) -> Result<(), Error>,
    ) -> Result<(), Error> {
        if max_items > 4096 || max_bytes > 8 * 1024 * 1024 || self.bytes.len() > max_bytes {
            return Err(AvroContainerError::Bounds.into());
        }
        let mut input = Input::new(
            self.bytes,
            AvroDatumLimits {
                depth: 1,
                values: 1,
                value_bytes: max_bytes,
            },
        );
        let mut remaining = max_items;
        loop {
            let count = input.long()?;
            if count == 0 {
                input.finish()?;
                return Ok(());
            }
            let items = count
                .checked_abs()
                .and_then(|count| usize::try_from(count).ok())
                .ok_or(AvroContainerError::Bounds)?;
            remaining = remaining.checked_sub(items).ok_or(AvroContainerError::Bounds)?;
            let end = if count < 0 {
                let length = input.size()?;
                Some(
                    input
                        .position()
                        .checked_add(length)
                        .ok_or(AvroContainerError::Bounds)?,
                )
            } else {
                None
            };
            for _ in 0..items {
                let key = i32::try_from(input.long()?).map_err(|_| AvroContainerError::Schema)?;
                let value = match self.kind {
                    AvroScalarType::LongMap => AvroMetricValue::Long(input.long()?),
                    AvroScalarType::BytesMap => {
                        let length = input.size()?;
                        AvroMetricValue::Bytes(input.take(length)?)
                    }
                    _ => return Err(AvroContainerError::Schema.into()),
                };
                visitor(key, value)?;
            }
            if end.is_some_and(|end| input.position() != end) {
                return Err(AvroContainerError::Schema.into());
            }
        }
    }
}

pub(super) fn layout(schema: &AvroSchema, node: &Node) -> Option<(AvroScalarType, (i32, i32))> {
    let Node::LogicalMap(child) = node else {
        return None;
    };
    let Node::Record(fields) = &schema.nodes[*child] else {
        return None;
    };
    if fields.len() != 2 || !matches!(schema.nodes[fields[0].node], Node::Int) {
        return None;
    }
    let key = fields[0].id.filter(|id| *id >= 0)?;
    let value = fields[1].id.filter(|id| *id >= 0 && *id != key)?;
    let kind = match schema.nodes[fields[1].node] {
        Node::Long => AvroScalarType::LongMap,
        Node::Bytes => AvroScalarType::BytesMap,
        _ => return None,
    };
    Some((kind, (key, value)))
}
