use super::{
    binary::Input, projection::read_scalar, AvroContainerError as Error, AvroDatumLimits, AvroScalar,
    AvroSchema, Node,
};
use std::collections::BTreeSet;

#[derive(Clone, Debug)]
pub struct AvroTupleField {
    pub id: i32,
    pub physical: &'static str,
    pub fixed_size: Option<usize>,
    pub annotation: Option<serde_json::Value>,
}

pub struct AvroTuple<'schema> {
    schema: &'schema AvroSchema,
    path: Vec<i32>,
    fields: Vec<AvroTupleField>,
}

impl<'schema> AvroTuple<'schema> {
    /// Compiles a required record path and retains its bounded field descriptors.
    /// # Errors
    /// Rejects ambiguous field IDs, non-record paths and excessive tuples.
    pub fn new(schema: &'schema AvroSchema, path: &[i32]) -> Result<Self, Error> {
        if path.is_empty() || path.len() > 16 {
            return Err(Error::Bounds);
        }
        let mut node = schema.root;
        for id in path {
            let Node::Record(fields) = &schema.nodes[non_null(schema, node)?] else {
                return Err(Error::Schema);
            };
            check_ids(fields)?;
            node = fields
                .iter()
                .find(|field| field.id == Some(*id))
                .ok_or(Error::Schema)?
                .node;
        }
        let Node::Record(fields) = &schema.nodes[non_null(schema, node)?] else {
            return Err(Error::Schema);
        };
        if fields.len() > 256 {
            return Err(Error::Bounds);
        }
        check_ids(fields)?;
        let mut descriptors = Vec::new();
        for field in fields {
            let index = non_null(schema, field.node)?;
            let (physical, fixed_size) = match schema.nodes[index] {
                Node::Null => ("null", None),
                Node::Boolean => ("boolean", None),
                Node::Int => ("int", None),
                Node::Long => ("long", None),
                Node::Float => ("float", None),
                Node::Double => ("double", None),
                Node::Bytes => ("bytes", None),
                Node::String => ("string", None),
                Node::Fixed(size) => ("fixed", Some(size)),
                _ => ("opaque", None),
            };
            descriptors.push(AvroTupleField {
                id: field.id.ok_or(Error::Schema)?,
                physical,
                fixed_size,
                annotation: schema.annotations.get(&index).cloned(),
            });
        }
        Ok(Self {
            schema,
            path: path.to_vec(),
            fields: descriptors,
        })
    }

    #[must_use]
    pub fn fields(&self) -> &[AvroTupleField] {
        &self.fields
    }

    /// Reads one tuple from a complete entry; returned bytes borrow the decoded block.
    /// # Errors
    /// Rejects null parent records, malformed data and excessive work or payloads.
    pub fn read<'data>(
        &self,
        bytes: &'data [u8],
        limits: AvroDatumLimits,
    ) -> Result<Vec<AvroScalar<'data>>, Error> {
        limits.validate()?;
        if bytes.len() > 8 * 1024 * 1024 {
            return Err(Error::Bounds);
        }
        let mut input = Input::new(bytes, limits);
        let mut result = Vec::new();
        self.record(
            self.schema.root,
            &self.path,
            &mut input,
            bytes,
            limits,
            1,
            &mut result,
        )?;
        input.finish()?;
        Ok(result)
    }

    #[allow(clippy::too_many_arguments)]
    fn record<'data>(
        &self,
        mut node: usize,
        path: &[i32],
        input: &mut Input<'data>,
        bytes: &'data [u8],
        limits: AvroDatumLimits,
        mut depth: usize,
        result: &mut Vec<AvroScalar<'data>>,
    ) -> Result<(), Error> {
        input.consume_value(depth)?;
        if let Node::Union(branches) = &self.schema.nodes[node] {
            node = *branches.get(input.size()?).ok_or(Error::Schema)?;
            depth += 1;
            input.consume_value(depth)?;
        }
        let Node::Record(fields) = &self.schema.nodes[node] else {
            return Err(Error::Schema);
        };
        for field in fields {
            if !path.is_empty() && field.id == Some(path[0]) {
                self.record(field.node, &path[1..], input, bytes, limits, depth + 1, result)?;
            } else {
                let start = input.position();
                input.datum(self.schema, field.node, depth + 1)?;
                if path.is_empty() {
                    let encoded = &bytes[start..input.position()];
                    if self.fields[result.len()].physical == "opaque" {
                        result.push(AvroScalar::Opaque(encoded));
                    } else {
                        let mut value = Input::new(encoded, limits);
                        result.push(read_scalar(self.schema, field.node, &mut value)?);
                        value.finish()?;
                    }
                }
            }
        }
        Ok(())
    }
}

fn non_null(schema: &AvroSchema, index: usize) -> Result<usize, Error> {
    if let Node::Union(branches) = &schema.nodes[index] {
        if branches.len() != 2 {
            return Err(Error::Schema);
        }
        let live: Vec<_> = branches
            .iter()
            .filter(|index| !matches!(schema.nodes[**index], Node::Null))
            .collect();
        if live.len() != 1 {
            return Err(Error::Schema);
        }
        Ok(*live[0])
    } else {
        Ok(index)
    }
}

fn check_ids(fields: &[super::Field]) -> Result<(), Error> {
    let mut seen = BTreeSet::new();
    for field in fields {
        let id = field.id.filter(|id| *id >= 0).ok_or(Error::Schema)?;
        if !seen.insert(id) {
            return Err(Error::Schema);
        }
    }
    Ok(())
}
