use super::{
    binary::Input, projection::compile, AvroContainerError as Error, AvroDatumLimits, AvroFieldPath,
    AvroProjection, AvroScalar, AvroScalarType, AvroSchema, Node,
};
use std::collections::BTreeSet;

pub struct AvroRecordArray<'schema> {
    schema: &'schema AvroSchema,
    field_id: i32,
    child: Option<usize>,
    projection: Option<AvroProjection<'schema>>,
}

impl<'schema> AvroRecordArray<'schema> {
    /// Selects an optional root array of non-null records by field and element IDs.
    /// # Errors
    /// Rejects ambiguous IDs, wrong collection layout and invalid selected child fields.
    pub fn new(
        schema: &'schema AvroSchema,
        field_id: i32,
        element_id: i32,
        paths: &[AvroFieldPath<'_>],
    ) -> Result<Self, Error> {
        let Node::Record(fields) = &schema.nodes[schema.root] else {
            return Err(Error::Schema);
        };
        let mut seen = BTreeSet::new();
        for field in fields {
            if !seen.insert(field.id.filter(|id| *id >= 0).ok_or(Error::Schema)?) {
                return Err(Error::Schema);
            }
        }
        let child = fields
            .iter()
            .find(|field| field.id == Some(field_id))
            .map(|field| {
                let mut node = field.node;
                if let Node::Union(branches) = &schema.nodes[node] {
                    if branches.len() != 2 {
                        return Err(Error::Schema);
                    }
                    let live: Vec<_> = branches
                        .iter()
                        .filter(|branch| !matches!(schema.nodes[**branch], Node::Null))
                        .collect();
                    if live.len() != 1 {
                        return Err(Error::Schema);
                    }
                    node = *live[0];
                }
                match schema.nodes[node] {
                    Node::Array(child, Some(id))
                        if id == element_id && matches!(schema.nodes[child], Node::Record(_)) =>
                    {
                        Ok(child)
                    }
                    _ => Err(Error::Schema),
                }
            })
            .transpose()?;
        let projection = child
            .map(|child| compile::at_root(schema, child, paths))
            .transpose()?;
        Ok(Self {
            schema,
            field_id,
            child,
            projection,
        })
    }

    #[must_use]
    pub fn field_types(&self) -> Option<&[Option<AvroScalarType>]> {
        self.projection.as_ref().map(AvroProjection::field_types)
    }

    /// Decodes a bounded array from one complete record, preserving absent/null versus empty.
    /// # Errors
    /// Rejects malformed data, excess work, more than 256 elements or excessive encoded bytes.
    pub fn read<'data>(
        &self,
        bytes: &'data [u8],
        limits: AvroDatumLimits,
        max_items: usize,
        max_bytes: usize,
    ) -> Result<Option<Vec<Vec<AvroScalar<'data>>>>, Error> {
        limits.validate()?;
        if bytes.len() > 8 * 1024 * 1024 || max_items > 256 || max_bytes > 1024 * 1024 {
            return Err(Error::Bounds);
        }
        let Node::Record(fields) = &self.schema.nodes[self.schema.root] else {
            return Err(Error::Schema);
        };
        let mut input = Input::new(bytes, limits);
        input.consume_value(1)?;
        let mut result = None;
        for field in fields {
            let start = input.position();
            input.datum(self.schema, field.node, 2)?;
            if field.id != Some(self.field_id) {
                continue;
            }
            let encoded = &bytes[start..input.position()];
            if encoded.len() > max_bytes {
                return Err(Error::Bounds);
            }
            let mut array = Input::new(encoded, limits);
            if let Node::Union(branches) = &self.schema.nodes[field.node] {
                let node = *branches.get(array.size()?).ok_or(Error::Schema)?;
                if matches!(self.schema.nodes[node], Node::Null) {
                    array.finish()?;
                    continue;
                }
            }
            result = Some(self.elements(&mut array, encoded, limits, max_items)?);
        }
        input.finish()?;
        Ok(result)
    }

    fn elements<'data>(
        &self,
        input: &mut Input<'data>,
        bytes: &'data [u8],
        limits: AvroDatumLimits,
        max_items: usize,
    ) -> Result<Vec<Vec<AvroScalar<'data>>>, Error> {
        let child = self.child.ok_or(Error::Schema)?;
        let projection = self.projection.as_ref().ok_or(Error::Schema)?;
        let mut result = Vec::new();
        loop {
            let count = input.long()?;
            if count == 0 {
                input.finish()?;
                return Ok(result);
            }
            let count_abs = count
                .checked_abs()
                .and_then(|value| usize::try_from(value).ok())
                .ok_or(Error::Bounds)?;
            if count_abs > max_items - result.len() {
                return Err(Error::Bounds);
            }
            let end = if count < 0 {
                let size = input.size()?;
                Some(input.position().checked_add(size).ok_or(Error::Bounds)?)
            } else {
                None
            };
            for _ in 0..count_abs {
                let start = input.position();
                input.datum(self.schema, child, 1)?;
                let mut record = projection.records(&bytes[start..input.position()], 1, limits)?;
                result.push(record.next_record()?.ok_or(Error::Schema)?);
            }
            if end.is_some_and(|end| end != input.position()) {
                return Err(Error::Schema);
            }
        }
    }
}
