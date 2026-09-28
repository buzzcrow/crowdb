use std::collections::{BTreeMap, BTreeSet};

use crate::file::{ParquetLogicalType as Logical, ParquetMetadata, ParquetSchemaElement};
use crate::manifest::{FileContentKind, ManifestContext, ManifestScalarEntry, PrimitiveType};

use super::{primitive, SelectedParquetError as Error};

pub type ParquetFieldMapping = BTreeMap<Vec<String>, i32>;

#[derive(Debug, Eq, PartialEq)]
pub struct SelectedParquetSchema {
    fields: BTreeMap<i32, usize>,
}

impl SelectedParquetSchema {
    #[must_use]
    pub fn field_index(&self, id: i32) -> Option<usize> {
        self.fields.get(&id).copied()
    }
}

/// Checks present fields by identity against a trusted historical schema context.
/// Missing columns remain the read planner's default/null materialization responsibility.
/// Explicit name mappings use logical paths (element/key/value), not physical wrappers.
/// With no IDs or mapping, top-level ordinal IDs follow the legacy SDK fallback.
/// # Errors
/// Rejects mismatched types/parents, invalid containers and absent delete columns.
pub fn validate_parquet_schema(
    metadata: &ParquetMetadata,
    context: &ManifestContext,
    entry: &ManifestScalarEntry,
    mapping: Option<&ParquetFieldMapping>,
) -> Result<SelectedParquetSchema, Error> {
    let schema = &metadata.schema;
    if schema.is_empty() || schema.len() > 4096 || schema[0].physical_type.is_some() {
        return Err(Error::Schema);
    }
    if let Some(mapping) = mapping {
        if mapping.len() > 4096
            || mapping.keys().any(|path| path.is_empty() || path.len() > 32)
            || mapping
                .iter()
                .any(|(path, id)| *id <= 0 || path.iter().any(|name| name.is_empty() || name.len() > 1024))
            || mapping.keys().flatten().map(String::len).sum::<usize>() > 1024 * 1024
        {
            return Err(Error::Schema);
        }
    }
    let mut walker = Walker {
        schema,
        context,
        mapping,
        fields: BTreeMap::new(),
        fallback: mapping.is_none() && schema.iter().all(|field| field.field_id.is_none()),
        content: entry.entry.content,
    };
    let mut cursor = 1;
    let mut names = BTreeSet::new();
    for ordinal in 1..=schema[0].children {
        let field = schema.get(cursor).ok_or(Error::Schema)?;
        if !names.insert(&field.name) {
            return Err(Error::Schema);
        }
        cursor = walker.field(
            cursor,
            None,
            false,
            &mut vec![field.name.clone()],
            Some(ordinal),
            false,
        )?;
    }
    if cursor != schema.len() {
        return Err(Error::Schema);
    }
    match entry.entry.content {
        FileContentKind::Data => {
            if entry.file.equality_ids.is_some() || entry.file.referenced_data_file.is_some() {
                return Err(Error::Schema);
            }
            for (id, field) in context.fields() {
                if field.required
                    && field.initial_default == crate::manifest::SchemaDefault::Absent
                    && !walker.fields.contains_key(id)
                    && field
                        .parent
                        .map_or(true, |parent| walker.fields.contains_key(&parent))
                {
                    return Err(Error::Schema);
                }
            }
        }
        FileContentKind::EqualityDeletes => {
            let ids = entry
                .file
                .equality_ids
                .as_ref()
                .filter(|ids| !ids.is_empty() && ids.len() <= 4096)
                .ok_or(Error::Schema)?;
            let mut unique = BTreeSet::new();
            for id in ids {
                let field = context.retained_field(*id).ok_or(Error::Schema)?;
                if !unique.insert(id)
                    || !walker.fields.contains_key(id)
                    || field.repeated
                    || !field
                        .primitive
                        .as_ref()
                        .is_some_and(PrimitiveType::equality_eligible)
                {
                    return Err(Error::Schema);
                }
            }
        }
        FileContentKind::PositionDeletes => {
            if entry.file.equality_ids.is_some()
                || !walker.fields.contains_key(&2_147_483_546)
                || !walker.fields.contains_key(&2_147_483_545)
            {
                return Err(Error::Schema);
            }
        }
    }
    Ok(SelectedParquetSchema {
        fields: walker.fields,
    })
}

struct Walker<'schema> {
    schema: &'schema [ParquetSchemaElement],
    context: &'schema ManifestContext,
    mapping: Option<&'schema ParquetFieldMapping>,
    fields: BTreeMap<i32, usize>,
    fallback: bool,
    content: FileContentKind,
}

impl Walker<'_> {
    #[allow(clippy::too_many_arguments)]
    fn field(
        &mut self,
        index: usize,
        parent: Option<i32>,
        repeated: bool,
        path: &mut Vec<String>,
        ordinal: Option<usize>,
        legacy_element: bool,
    ) -> Result<usize, Error> {
        if path.len() > 32 {
            return Err(Error::Schema);
        }
        let field = self.schema.get(index).ok_or(Error::Schema)?;
        if !(matches!(field.repetition, Some(0 | 1)) || legacy_element && field.repetition == Some(2)) {
            return Err(Error::Schema);
        }
        let id = field
            .field_id
            .or_else(|| self.mapping.and_then(|mapping| mapping.get(path).copied()))
            .or_else(|| {
                self.fallback
                    .then_some(ordinal)
                    .flatten()
                    .and_then(|id| i32::try_from(id).ok())
            });
        let Some(id) = id else {
            return self.skip(index, path.len());
        };
        if id <= 0 || self.fields.insert(id, index).is_some() {
            return Err(Error::Schema);
        }
        if self.content == FileContentKind::PositionDeletes && path.len() == 1 {
            match id {
                2_147_483_546 | 2_147_483_545 => {
                    if field.repetition != Some(0)
                        || field.children != 0
                        || (id == 2_147_483_545 && field.physical_type != Some(2))
                    {
                        return Err(Error::Schema);
                    }
                    primitive::validate(
                        field,
                        if id == 2_147_483_546 {
                            &PrimitiveType::String
                        } else {
                            &PrimitiveType::Long
                        },
                    )?;
                    return Ok(index + 1);
                }
                2_147_483_544 if field.physical_type.is_none() && field.repetition == Some(0) => {
                    return self.children(index, None, false, path)
                }
                _ => return Err(Error::Schema),
            }
        }
        let expected = self.context.retained_field(id).ok_or(Error::Schema)?;
        if expected.parent != parent || expected.repeated != repeated {
            return Err(Error::Schema);
        }
        if parent
            .and_then(|id| self.context.retained_field(id))
            .is_some_and(|parent| matches!(parent.kind, "map" | "list"))
            && path.last() != Some(&expected.name)
        {
            return Err(Error::Schema);
        }
        if let Some(expected) = &expected.primitive {
            if *expected == PrimitiveType::Variant {
                return self.variant(index, path.len());
            }
            if self.context.version() == crate::manifest::ManifestVersion::V3
                && matches!(expected, PrimitiveType::Timestamp | PrimitiveType::TimestampNs)
                && matches!(primitive::annotation(field)?, Some(Logical::Date))
                && field.physical_type == Some(1)
            {
                return self.skip(index, path.len());
            }
            primitive::validate(field, expected)?;
            return self.skip(index, path.len());
        }
        if field.physical_type.is_some() {
            return Err(Error::Schema);
        }
        match (expected.kind, primitive::annotation(field)?) {
            ("struct", None) => self.children(index, Some(id), repeated, path),
            ("list", Some(Logical::List)) => self.list(index, id, path),
            ("map", Some(Logical::Map)) => self.map(index, id, path),
            _ => Err(Error::Schema),
        }
    }

    fn children(
        &mut self,
        index: usize,
        parent: Option<i32>,
        repeated: bool,
        path: &mut Vec<String>,
    ) -> Result<usize, Error> {
        let mut cursor = index + 1;
        let mut names = BTreeSet::new();
        for _ in 0..self.schema[index].children {
            let field = self.schema.get(cursor).ok_or(Error::Schema)?;
            if !names.insert(&field.name) {
                return Err(Error::Schema);
            }
            path.push(field.name.clone());
            cursor = self.field(cursor, parent, repeated, path, None, false)?;
            path.pop();
        }
        Ok(cursor)
    }

    fn list(&mut self, index: usize, id: i32, path: &mut Vec<String>) -> Result<usize, Error> {
        let field = &self.schema[index];
        let child = self.schema.get(index + 1).ok_or(Error::Schema)?;
        if field.children != 1 || child.repetition != Some(2) {
            return Err(Error::Schema);
        }
        let legacy = child.physical_type.is_some()
            || child.children > 1
            || child.name == "array"
            || child.name == format!("{}_tuple", field.name);
        if !legacy && (child.children != 1 || child.field_id.is_some()) {
            return Err(Error::Schema);
        }
        path.push("element".into());
        let result = self.field(
            index + if legacy { 1 } else { 2 },
            Some(id),
            true,
            path,
            None,
            legacy,
        );
        path.pop();
        result
    }

    fn map(&mut self, index: usize, id: i32, path: &mut Vec<String>) -> Result<usize, Error> {
        let child = self.schema.get(index + 1).ok_or(Error::Schema)?;
        if self.schema[index].children != 1
            || child.children != 2
            || child.physical_type.is_some()
            || child.repetition != Some(2)
            || child.field_id.is_some()
        {
            return Err(Error::Schema);
        }
        let key = self.schema.get(index + 2).ok_or(Error::Schema)?;
        if key.repetition != Some(0) {
            return Err(Error::Schema);
        }
        path.push("key".into());
        let value = self.field(index + 2, Some(id), true, path, None, false)?;
        path.pop();
        path.push("value".into());
        let end = self.field(value, Some(id), true, path, None, false)?;
        path.pop();
        Ok(end)
    }

    fn variant(&self, index: usize, depth: usize) -> Result<usize, Error> {
        super::variant::validate(self.schema, index, depth)
    }

    fn skip(&self, index: usize, depth: usize) -> Result<usize, Error> {
        if depth > 32 {
            return Err(Error::Schema);
        }
        let field = self.schema.get(index).ok_or(Error::Schema)?;
        if field.physical_type.is_some() && field.children != 0 {
            return Err(Error::Schema);
        }
        let mut cursor = index + 1;
        for _ in 0..field.children {
            cursor = self.skip(cursor, depth + 1)?;
        }
        Ok(cursor)
    }
}
