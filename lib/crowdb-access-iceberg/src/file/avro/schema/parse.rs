use serde_json::{Map, Value};
use std::collections::{BTreeMap, BTreeSet};

use super::{AvroContainerError, AvroSchema, Field, Node};

mod names;

pub(super) fn compile(bytes: &[u8]) -> Result<AvroSchema, AvroContainerError> {
    if bytes.len() > 1024 * 1024 {
        return Err(AvroContainerError::Bounds);
    }
    let value: Value = serde_json::from_slice(bytes).map_err(|_| AvroContainerError::Schema)?;
    let mut parser = Parser {
        nodes: Vec::new(),
        names: BTreeMap::new(),
        edges: 0,
        annotations: BTreeMap::new(),
    };
    let root = parser.schema(&value, "", 1)?;
    Ok(AvroSchema {
        nodes: parser.nodes,
        root,
        annotations: parser.annotations,
    })
}

struct Parser {
    nodes: Vec<Node>,
    names: BTreeMap<String, usize>,
    edges: usize,
    annotations: BTreeMap<usize, Value>,
}

impl Parser {
    fn insert(&mut self, node: Node) -> Result<usize, AvroContainerError> {
        if self.nodes.len() >= 4096 {
            return Err(AvroContainerError::Bounds);
        }
        let index = self.nodes.len();
        self.nodes.push(node);
        Ok(index)
    }

    fn schema(&mut self, value: &Value, namespace: &str, depth: usize) -> Result<usize, AvroContainerError> {
        self.edges += 1;
        if depth > 64 || self.edges > 16_384 {
            return Err(AvroContainerError::Bounds);
        }
        match value {
            Value::String(name) => self.reference(name, namespace),
            Value::Array(branches) => self.union(branches, namespace, depth),
            Value::Object(object) => {
                let kind = text(object, "type")?;
                let index = match kind {
                    "record" | "enum" | "fixed" => self.named(object, kind, namespace, depth),
                    "array" | "map" => {
                        let property = if kind == "array" { "items" } else { "values" };
                        let child = self.schema(
                            object.get(property).ok_or(AvroContainerError::Schema)?,
                            namespace,
                            depth + 1,
                        )?;
                        self.insert(if kind == "array" {
                            if object.get("logicalType").and_then(Value::as_str) == Some("map") {
                                Node::LogicalMap(child)
                            } else {
                                Node::Array(child, field_id(object, "element-id"))
                            }
                        } else {
                            Node::Map(child)
                        })
                    }
                    _ => self.reference(kind, namespace),
                }?;
                let annotation: Map<String, Value> = ["logicalType", "precision", "scale", "adjust-to-utc"]
                    .into_iter()
                    .filter_map(|key| object.get(key).map(|value| (key.to_owned(), value.clone())))
                    .collect();
                if !annotation.is_empty() {
                    self.annotations.insert(index, Value::Object(annotation));
                }
                Ok(index)
            }
            _ => Err(AvroContainerError::Schema),
        }
    }

    fn reference(&mut self, name: &str, namespace: &str) -> Result<usize, AvroContainerError> {
        if let Some(node) = primitive(name) {
            return self.insert(node);
        }
        let name = names::qualify(name, namespace)?;
        self.names.get(&name).copied().ok_or(AvroContainerError::Schema)
    }

    fn named(
        &mut self,
        object: &Map<String, Value>,
        kind: &str,
        enclosing: &str,
        depth: usize,
    ) -> Result<usize, AvroContainerError> {
        let (name, namespace) = names::definition(object, enclosing)?;
        if self.names.contains_key(&name) {
            return Err(AvroContainerError::Schema);
        }
        let index = self.insert(Node::Record(Vec::new()))?;
        self.names.insert(name, index);
        let node = match kind {
            "record" => Node::Record(self.fields(object, &namespace, depth)?),
            "enum" => Node::Enum(enum_symbols(object)?),
            "fixed" => {
                let size = object
                    .get("size")
                    .and_then(Value::as_u64)
                    .and_then(|size| usize::try_from(size).ok())
                    .ok_or(AvroContainerError::Schema)?;
                if size > 8 * 1024 * 1024 {
                    return Err(AvroContainerError::Bounds);
                }
                Node::Fixed(size)
            }
            _ => return Err(AvroContainerError::Schema),
        };
        self.nodes[index] = node;
        Ok(index)
    }

    fn fields(
        &mut self,
        object: &Map<String, Value>,
        namespace: &str,
        depth: usize,
    ) -> Result<Vec<Field>, AvroContainerError> {
        let fields = object
            .get("fields")
            .and_then(Value::as_array)
            .ok_or(AvroContainerError::Schema)?;
        if fields.len() > 4096 {
            return Err(AvroContainerError::Bounds);
        }
        let mut names = BTreeSet::new();
        let mut nodes = Vec::with_capacity(fields.len());
        for field in fields {
            let field = field.as_object().ok_or(AvroContainerError::Schema)?;
            let name = text(field, "name")?;
            names::identifier(name)?;
            if !names.insert(name) {
                return Err(AvroContainerError::Schema);
            }
            let node = self.schema(
                field.get("type").ok_or(AvroContainerError::Schema)?,
                namespace,
                depth + 1,
            )?;
            let id = field_id(field, "field-id");
            nodes.push(Field { node, id });
        }
        Ok(nodes)
    }

    fn union(
        &mut self,
        branches: &[Value],
        namespace: &str,
        depth: usize,
    ) -> Result<usize, AvroContainerError> {
        if branches.is_empty() || branches.len() > 4096 {
            return Err(AvroContainerError::Schema);
        }
        let mut nodes = Vec::with_capacity(branches.len());
        let mut kinds = BTreeSet::new();
        for branch in branches {
            let index = self.schema(branch, namespace, depth + 1)?;
            let key = match &self.nodes[index] {
                Node::Null => (0, 0),
                Node::Boolean => (1, 0),
                Node::Int => (2, 0),
                Node::Long => (3, 0),
                Node::Float => (4, 0),
                Node::Double => (5, 0),
                Node::Bytes => (6, 0),
                Node::String => (7, 0),
                Node::Array(_, _) | Node::LogicalMap(_) => (8, 0),
                Node::Map(_) => (9, 0),
                Node::Record(_) | Node::Enum(_) | Node::Fixed(_) => (10, index),
                Node::Union(_) => return Err(AvroContainerError::Schema),
            };
            if !kinds.insert(key) {
                return Err(AvroContainerError::Schema);
            }
            nodes.push(index);
        }
        self.insert(Node::Union(nodes))
    }
}

fn primitive(name: &str) -> Option<Node> {
    Some(match name {
        "null" => Node::Null,
        "boolean" => Node::Boolean,
        "int" => Node::Int,
        "long" => Node::Long,
        "float" => Node::Float,
        "double" => Node::Double,
        "bytes" => Node::Bytes,
        "string" => Node::String,
        _ => return None,
    })
}

fn text<'value>(object: &'value Map<String, Value>, field: &str) -> Result<&'value str, AvroContainerError> {
    object
        .get(field)
        .and_then(Value::as_str)
        .ok_or(AvroContainerError::Schema)
}

fn field_id(object: &Map<String, Value>, name: &str) -> Option<i32> {
    object.get(name).map(|value| {
        value
            .as_i64()
            .and_then(|value| i32::try_from(value).ok())
            .unwrap_or(-1)
    })
}

fn enum_symbols(object: &Map<String, Value>) -> Result<usize, AvroContainerError> {
    let symbols = object
        .get("symbols")
        .and_then(Value::as_array)
        .ok_or(AvroContainerError::Schema)?;
    if symbols.len() > 4096 {
        return Err(AvroContainerError::Bounds);
    }
    let mut names = BTreeSet::new();
    for symbol in symbols {
        let symbol = symbol.as_str().ok_or(AvroContainerError::Schema)?;
        names::identifier(symbol)?;
        if !names.insert(symbol) {
            return Err(AvroContainerError::Schema);
        }
    }
    if let Some(default) = object.get("default") {
        if !names.contains(default.as_str().ok_or(AvroContainerError::Schema)?) {
            return Err(AvroContainerError::Schema);
        }
    }
    Ok(symbols.len())
}
