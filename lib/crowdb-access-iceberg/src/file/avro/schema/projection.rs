use std::collections::{BTreeMap, BTreeSet};

use super::{binary::Input, AvroContainerError, AvroDatumLimits, AvroSchema, Node};

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum AvroScalar<'data> {
    Null,
    Int(i32),
    Long(i64),
    String(&'data str),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AvroScalarType {
    Int,
    Long,
    String,
}

pub struct AvroProjection<'schema> {
    schema: &'schema AvroSchema,
    slots: Vec<Option<usize>>,
    count: usize,
    types: Vec<Option<AvroScalarType>>,
}

pub struct AvroProjectedRecords<'projection, 'schema, 'data> {
    projection: &'projection AvroProjection<'schema>,
    input: Input<'data>,
    bytes: &'data [u8],
    remaining: u64,
    limits: AvroDatumLimits,
    failed: bool,
}

impl<'schema> AvroProjection<'schema> {
    /// Selects root scalar fields by Iceberg IDs, independently of names and writer ordering.
    /// # Errors
    /// Rejects missing, duplicate or invalid IDs and non-scalar selected field layouts.
    pub fn new(schema: &'schema AvroSchema, ids: &[i32]) -> Result<Self, AvroContainerError> {
        Self::with_optional(schema, ids, &[])
    }

    /// Appends optional selections, returning null when the writer omits those fields.
    /// # Errors
    /// Applies the same ID, scalar-layout and combined selection bounds as required projection.
    pub fn with_optional(
        schema: &'schema AvroSchema,
        required: &[i32],
        optional: &[i32],
    ) -> Result<Self, AvroContainerError> {
        let count = required
            .len()
            .checked_add(optional.len())
            .ok_or(AvroContainerError::Bounds)?;
        if count == 0 || count > 64 {
            return Err(AvroContainerError::Bounds);
        }
        let Node::Record(fields) = &schema.nodes[schema.root] else {
            return Err(AvroContainerError::Schema);
        };
        let mut requested = BTreeMap::new();
        for (slot, id) in required.iter().chain(optional).enumerate() {
            if *id < 0 || requested.insert(*id, slot).is_some() {
                return Err(AvroContainerError::Schema);
            }
        }
        let mut seen = BTreeSet::new();
        let mut slots = Vec::with_capacity(fields.len());
        let mut types = vec![None; count];
        for field in fields {
            let id = field.id.filter(|id| *id >= 0).ok_or(AvroContainerError::Schema)?;
            if !seen.insert(id) {
                return Err(AvroContainerError::Schema);
            }
            let slot = requested.remove(&id);
            if slot.is_some() && !scalar_layout(schema, field.node) {
                return Err(AvroContainerError::Schema);
            }
            if let Some(slot) = slot {
                types[slot] = scalar_type(schema, field.node);
            }
            slots.push(slot);
        }
        if requested.values().any(|slot| *slot < required.len()) {
            return Err(AvroContainerError::Schema);
        }
        Ok(Self {
            schema,
            slots,
            count,
            types,
        })
    }

    #[must_use]
    pub fn field_types(&self) -> &[Option<AvroScalarType>] {
        &self.types
    }

    /// Opens a bounded cursor; each successful pull validates every field in that record.
    /// # Errors
    /// Rejects excessive bounds and nonempty payloads with zero declared records.
    pub fn records<'projection, 'data>(
        &'projection self,
        bytes: &'data [u8],
        records: u64,
        limits: AvroDatumLimits,
    ) -> Result<AvroProjectedRecords<'projection, 'schema, 'data>, AvroContainerError> {
        limits.validate()?;
        if bytes.len() > 8 * 1024 * 1024 || records > 1_000_000 {
            return Err(AvroContainerError::Bounds);
        }
        let input = Input::new(bytes, limits);
        if records == 0 {
            input.finish()?;
        }
        Ok(AvroProjectedRecords {
            projection: self,
            input,
            bytes,
            remaining: records,
            limits,
            failed: false,
        })
    }
}

impl<'data> AvroProjectedRecords<'_, '_, 'data> {
    /// Returns selected values in request order, borrowing strings from the decoded block.
    /// # Errors
    /// Poisons the cursor on invalid selected or skipped data, excess work, or trailing bytes.
    pub fn next_record(&mut self) -> Result<Option<Vec<AvroScalar<'data>>>, AvroContainerError> {
        if self.failed {
            return Err(AvroContainerError::Failed);
        }
        if self.remaining == 0 {
            return Ok(None);
        }
        self.failed = true;
        self.input.consume_value(1)?;
        let schema = self.projection.schema;
        let Node::Record(fields) = &schema.nodes[schema.root] else {
            return Err(AvroContainerError::Schema);
        };
        let mut values = vec![AvroScalar::Null; self.projection.count];
        for (field, slot) in fields.iter().zip(&self.projection.slots) {
            let start = self.input.position();
            self.input.datum(schema, field.node, 2)?;
            if let Some(slot) = slot {
                let mut value = Input::new(&self.bytes[start..self.input.position()], self.limits);
                values[*slot] = read_scalar(schema, field.node, &mut value)?;
                value.finish()?;
            }
        }
        self.remaining -= 1;
        if self.remaining == 0 {
            self.input.finish()?;
        }
        self.failed = false;
        Ok(Some(values))
    }
}

fn primitive(node: &Node) -> bool {
    matches!(node, Node::Int | Node::Long | Node::String)
}

fn scalar_type(schema: &AvroSchema, index: usize) -> Option<AvroScalarType> {
    match &schema.nodes[index] {
        Node::Int => Some(AvroScalarType::Int),
        Node::Long => Some(AvroScalarType::Long),
        Node::String => Some(AvroScalarType::String),
        Node::Union(branches) => branches.iter().find_map(|branch| scalar_type(schema, *branch)),
        _ => None,
    }
}

fn scalar_layout(schema: &AvroSchema, index: usize) -> bool {
    match &schema.nodes[index] {
        Node::Union(branches) if branches.len() == 2 => {
            branches
                .iter()
                .filter(|branch| matches!(schema.nodes[**branch], Node::Null))
                .count()
                == 1
                && branches
                    .iter()
                    .filter(|branch| primitive(&schema.nodes[**branch]))
                    .count()
                    == 1
        }
        node => primitive(node),
    }
}

fn read_scalar<'data>(
    schema: &AvroSchema,
    index: usize,
    input: &mut Input<'data>,
) -> Result<AvroScalar<'data>, AvroContainerError> {
    Ok(match &schema.nodes[index] {
        Node::Null => AvroScalar::Null,
        Node::Int => AvroScalar::Int(i32::try_from(input.long()?).map_err(|_| AvroContainerError::Schema)?),
        Node::Long => AvroScalar::Long(input.long()?),
        Node::String => {
            let length = input.size()?;
            AvroScalar::String(
                std::str::from_utf8(input.take(length)?).map_err(|_| AvroContainerError::Schema)?,
            )
        }
        Node::Union(branches) => {
            let branch = *branches.get(input.size()?).ok_or(AvroContainerError::Schema)?;
            read_scalar(schema, branch, input)?
        }
        _ => return Err(AvroContainerError::Schema),
    })
}
