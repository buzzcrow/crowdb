use super::{binary::Input, AvroContainerError, AvroDatumLimits, AvroSchema, Node};

mod compile;
mod int_list;

pub use int_list::AvroIntList;

#[derive(Clone, Copy)]
pub struct AvroFieldPath<'path> {
    pub ids: &'path [i32],
    pub required: bool,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum AvroScalar<'data> {
    Null,
    Int(i32),
    Long(i64),
    String(&'data str),
    IntList(AvroIntList<'data>),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AvroScalarType {
    Int,
    Long,
    String,
    IntList,
}

pub struct AvroProjection<'schema> {
    schema: &'schema AvroSchema,
    root: RecordSelection,
    count: usize,
    types: Vec<Option<AvroScalarType>>,
    element_ids: Vec<Option<i32>>,
}

struct RecordSelection {
    node: usize,
    fields: Vec<Selection>,
}

enum Selection {
    Skip(usize),
    Scalar { node: usize, slot: usize },
    Record(RecordSelection),
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
        if required.len().saturating_add(optional.len()) > 64 {
            return Err(AvroContainerError::Bounds);
        }
        let paths: Vec<_> = required
            .iter()
            .chain(optional)
            .enumerate()
            .map(|(slot, id)| AvroFieldPath {
                ids: std::slice::from_ref(id),
                required: slot < required.len(),
            })
            .collect();
        Self::paths(schema, &paths)
    }

    /// Selects at most 64 scalar paths through records and nullable records, up to 16 IDs deep.
    /// # Errors
    /// Rejects ambiguous paths, missing required fields and independently excessive compiled work.
    pub fn paths(
        schema: &'schema AvroSchema,
        paths: &[AvroFieldPath<'_>],
    ) -> Result<Self, AvroContainerError> {
        compile::projection(schema, paths)
    }

    #[must_use]
    pub fn field_types(&self) -> &[Option<AvroScalarType>] {
        &self.types
    }

    #[must_use]
    pub fn element_ids(&self) -> &[Option<i32>] {
        &self.element_ids
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
        let mut values = vec![AvroScalar::Null; self.projection.count];
        self.project_record(&self.projection.root, 1, &mut values)?;
        self.remaining -= 1;
        if self.remaining == 0 {
            self.input.finish()?;
        }
        self.failed = false;
        Ok(Some(values))
    }

    fn project_record(
        &mut self,
        record: &RecordSelection,
        mut depth: usize,
        values: &mut [AvroScalar<'data>],
    ) -> Result<(), AvroContainerError> {
        let schema = self.projection.schema;
        self.input.consume_value(depth)?;
        if let Node::Union(branches) = &schema.nodes[record.node] {
            let branch = *branches
                .get(self.input.size()?)
                .ok_or(AvroContainerError::Schema)?;
            depth += 1;
            self.input.consume_value(depth)?;
            if matches!(schema.nodes[branch], Node::Null) {
                return Ok(());
            }
        }
        for field in &record.fields {
            match field {
                Selection::Skip(node) => self.input.datum(schema, *node, depth + 1)?,
                Selection::Record(record) => self.project_record(record, depth + 1, values)?,
                Selection::Scalar { node, slot } => {
                    let start = self.input.position();
                    self.input.datum(schema, *node, depth + 1)?;
                    let mut value = Input::new(&self.bytes[start..self.input.position()], self.limits);
                    values[*slot] = read_scalar(schema, *node, &mut value)?;
                    value.finish()?;
                }
            }
        }
        Ok(())
    }
}

fn primitive(schema: &AvroSchema, node: &Node) -> bool {
    matches!(node, Node::Int | Node::Long | Node::String)
        || matches!(node, Node::Array(child, _) if matches!(schema.nodes[*child], Node::Int))
}

fn scalar_type(schema: &AvroSchema, index: usize) -> Option<AvroScalarType> {
    match &schema.nodes[index] {
        Node::Int => Some(AvroScalarType::Int),
        Node::Long => Some(AvroScalarType::Long),
        Node::String => Some(AvroScalarType::String),
        Node::Array(child, _) if matches!(schema.nodes[*child], Node::Int) => Some(AvroScalarType::IntList),
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
                    .filter(|branch| primitive(schema, &schema.nodes[**branch]))
                    .count()
                    == 1
        }
        node => primitive(schema, node),
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
        Node::Array(child, _) if matches!(schema.nodes[*child], Node::Int) => {
            AvroScalar::IntList(AvroIntList(input.take_remaining()?))
        }
        Node::Union(branches) => {
            let branch = *branches.get(input.size()?).ok_or(AvroContainerError::Schema)?;
            read_scalar(schema, branch, input)?
        }
        _ => return Err(AvroContainerError::Schema),
    })
}
