use super::{id, json, ManifestContextError as Error, ManifestVersion, PrimitiveType, SchemaField};
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};

pub(super) fn parse(
    bytes: &[u8],
    version: ManifestVersion,
    schema_id: i32,
) -> Result<BTreeMap<i32, SchemaField>, Error> {
    let root = json(bytes)?;
    if root["type"] != "struct"
        || root
            .get("schema-id")
            .is_some_and(|value| value.as_i64() != Some(i64::from(schema_id)))
        || (version != ManifestVersion::V1 && root.get("schema-id").is_none())
    {
        return Err(Error::Invalid);
    }
    let mut parser = Parser {
        fields: BTreeMap::new(),
        version,
    };
    parser.children(&root, None, true, false, 0)?;
    if let Some(ids) = root.get("identifier-field-ids") {
        let mut unique = BTreeSet::new();
        for value in ids.as_array().ok_or(Error::Invalid)? {
            let id = id(value)?;
            let field = parser.fields.get(&id).ok_or(Error::Invalid)?;
            if !unique.insert(id)
                || field.repeated
                || !field.required_path
                || !field
                    .primitive
                    .as_ref()
                    .is_some_and(PrimitiveType::equality_eligible)
            {
                return Err(Error::Invalid);
            }
        }
    }
    Ok(parser.fields)
}

struct Parser {
    fields: BTreeMap<i32, SchemaField>,
    version: ManifestVersion,
}

impl Parser {
    fn children(
        &mut self,
        value: &Value,
        parent: Option<i32>,
        required_path: bool,
        repeated: bool,
        depth: usize,
    ) -> Result<(), Error> {
        let fields = value["fields"].as_array().ok_or(Error::Invalid)?;
        let mut names = BTreeSet::new();
        for field in fields {
            let name = field["name"]
                .as_str()
                .filter(|name| !name.is_empty() && name.len() <= 1024)
                .ok_or(Error::Invalid)?;
            if !names.insert(name) {
                return Err(Error::Invalid);
            }
            let required = field["required"].as_bool().ok_or(Error::Invalid)?;
            let primitive = self.field(
                id(&field["id"])?,
                name,
                &field["type"],
                parent,
                required,
                required_path && required,
                repeated,
                depth + 1,
            )?;
            if matches!(
                primitive,
                Some(
                    PrimitiveType::Unknown
                        | PrimitiveType::Variant
                        | PrimitiveType::Geometry(_)
                        | PrimitiveType::Geography(_)
                )
            ) {
                if matches!(primitive, Some(PrimitiveType::Unknown)) && required {
                    return Err(Error::Invalid);
                }
                for key in ["initial-default", "write-default"] {
                    if field.get(key).is_some_and(|value| !value.is_null()) {
                        return Err(Error::Invalid);
                    }
                }
            }
        }
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn field(
        &mut self,
        field_id: i32,
        name: &str,
        value: &Value,
        parent: Option<i32>,
        required: bool,
        required_path: bool,
        repeated: bool,
        depth: usize,
    ) -> Result<Option<PrimitiveType>, Error> {
        if depth > 32 || self.fields.len() >= 4096 {
            return Err(Error::Bounds);
        }
        let (primitive, kind) = if let Some(name) = value.as_str() {
            (Some(PrimitiveType::parse(name, self.version)?), "primitive")
        } else {
            (
                None,
                match value["type"].as_str() {
                    Some("struct") => "struct",
                    Some("list") => "list",
                    Some("map") => "map",
                    _ => return Err(Error::Invalid),
                },
            )
        };
        if primitive == Some(PrimitiveType::Unknown) && required {
            return Err(Error::Invalid);
        }
        let field = SchemaField {
            name: name.into(),
            parent,
            primitive: primitive.clone(),
            required,
            required_path,
            repeated,
            kind,
        };
        if self.fields.insert(field_id, field).is_some() {
            return Err(Error::Invalid);
        }
        match kind {
            "struct" => self.children(value, Some(field_id), required_path, repeated, depth)?,
            "list" => {
                let required = value["element-required"].as_bool().ok_or(Error::Invalid)?;
                self.field(
                    id(&value["element-id"])?,
                    "element",
                    &value["element"],
                    Some(field_id),
                    required,
                    false,
                    true,
                    depth + 1,
                )?;
            }
            "map" => {
                let required = value["value-required"].as_bool().ok_or(Error::Invalid)?;
                self.field(
                    id(&value["key-id"])?,
                    "key",
                    &value["key"],
                    Some(field_id),
                    true,
                    false,
                    true,
                    depth + 1,
                )?;
                self.field(
                    id(&value["value-id"])?,
                    "value",
                    &value["value"],
                    Some(field_id),
                    required,
                    false,
                    true,
                    depth + 1,
                )?;
            }
            _ => {}
        }
        Ok(primitive)
    }
}
