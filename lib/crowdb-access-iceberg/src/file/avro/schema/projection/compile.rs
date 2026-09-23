use std::collections::{BTreeMap, BTreeSet};

use super::{
    scalar_layout, scalar_type, AvroContainerError, AvroFieldPath, AvroProjection, AvroScalarType,
    AvroSchema, Node, RecordSelection, Selection,
};

pub(super) fn projection<'schema>(
    schema: &'schema AvroSchema,
    paths: &[AvroFieldPath<'_>],
) -> Result<AvroProjection<'schema>, AvroContainerError> {
    if paths.is_empty() || paths.len() > 64 {
        return Err(AvroContainerError::Bounds);
    }
    let mut seen = BTreeSet::new();
    for path in paths {
        if path.ids.is_empty() || path.ids.len() > 16 {
            return Err(AvroContainerError::Bounds);
        }
        if path.ids.iter().any(|id| *id < 0) || !seen.insert(path.ids) {
            return Err(AvroContainerError::Schema);
        }
    }
    let mut compiler = Compiler {
        schema,
        remaining: 16_384,
        types: vec![None; paths.len()],
    };
    let selections: Vec<_> = paths.iter().copied().enumerate().collect();
    let root = compiler.record(schema.root, &selections)?;
    Ok(AvroProjection {
        schema,
        root,
        count: paths.len(),
        types: compiler.types,
    })
}

struct Compiler<'schema> {
    schema: &'schema AvroSchema,
    remaining: usize,
    types: Vec<Option<AvroScalarType>>,
}

impl Compiler<'_> {
    fn record(
        &mut self,
        index: usize,
        paths: &[(usize, AvroFieldPath<'_>)],
    ) -> Result<RecordSelection, AvroContainerError> {
        let node = record_node(self.schema, index)?;
        let Node::Record(fields) = &self.schema.nodes[node] else {
            return Err(AvroContainerError::Schema);
        };
        self.remaining = self
            .remaining
            .checked_sub(fields.len())
            .ok_or(AvroContainerError::Bounds)?;
        let mut grouped = BTreeMap::<i32, Vec<_>>::new();
        for (slot, path) in paths {
            grouped.entry(path.ids[0]).or_default().push((
                *slot,
                AvroFieldPath {
                    ids: &path.ids[1..],
                    required: path.required,
                },
            ));
        }
        let mut seen = BTreeSet::new();
        let mut selections = Vec::with_capacity(fields.len());
        for field in fields {
            let id = field.id.filter(|id| *id >= 0).ok_or(AvroContainerError::Schema)?;
            if !seen.insert(id) {
                return Err(AvroContainerError::Schema);
            }
            selections.push(if let Some(paths) = grouped.remove(&id) {
                self.field(field.node, &paths)?
            } else {
                Selection::Skip(field.node)
            });
        }
        if grouped.values().flatten().any(|(_, path)| path.required) {
            return Err(AvroContainerError::Schema);
        }
        Ok(RecordSelection {
            node: index,
            fields: selections,
        })
    }

    fn field(
        &mut self,
        node: usize,
        paths: &[(usize, AvroFieldPath<'_>)],
    ) -> Result<Selection, AvroContainerError> {
        if paths.iter().any(|(_, path)| path.ids.is_empty()) {
            if paths.len() != 1 || !scalar_layout(self.schema, node) {
                return Err(AvroContainerError::Schema);
            }
            let slot = paths[0].0;
            self.types[slot] = scalar_type(self.schema, node);
            Ok(Selection::Scalar { node, slot })
        } else {
            Ok(Selection::Record(self.record(node, paths)?))
        }
    }
}

fn record_node(schema: &AvroSchema, index: usize) -> Result<usize, AvroContainerError> {
    match &schema.nodes[index] {
        Node::Record(_) => Ok(index),
        Node::Union(branches) if branches.len() == 2 => {
            let mut record = None;
            let mut null = false;
            for branch in branches {
                match schema.nodes[*branch] {
                    Node::Null => null = true,
                    Node::Record(_) => record = Some(*branch),
                    _ => return Err(AvroContainerError::Schema),
                }
            }
            record.filter(|_| null).ok_or(AvroContainerError::Schema)
        }
        _ => Err(AvroContainerError::Schema),
    }
}
