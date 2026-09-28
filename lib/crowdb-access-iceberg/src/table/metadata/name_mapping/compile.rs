use std::collections::{BTreeMap, BTreeSet};

use serde_json::Value;

use crate::{manifest::ParquetFieldMapping, table::TableMetadataError as Error};

type Paths = BTreeMap<Vec<String>, Option<i32>>;

pub(super) fn compile(value: &Value, work: usize) -> Result<ParquetFieldMapping, Error> {
    let mut compiler = Compiler {
        work,
        bytes: 1024 * 1024,
        idless: false,
    };
    Ok(compiler
        .fields(value, 0)?
        .into_iter()
        .filter_map(|(path, id)| id.map(|id| (path, id)))
        .collect())
}

struct Compiler {
    work: usize,
    bytes: usize,
    idless: bool,
}

impl Compiler {
    fn fields(&mut self, value: &Value, depth: usize) -> Result<Paths, Error> {
        if depth >= 32 {
            return Err(Error::Bounds);
        }
        let fields = value.as_array().ok_or(Error::Field("name-mapping"))?;
        let mut paths = Paths::new();
        let mut flattened = BTreeSet::new();
        for field in fields {
            self.charge(1)?;
            let id = field
                .get("field-id")
                .filter(|value| !value.is_null())
                .map(|value| super::id(value, "field-id"))
                .transpose()?;
            if id.is_none() {
                if self.idless {
                    return Err(Error::Field("name-mapping-sdk-null-id"));
                }
                self.idless = true;
            }
            let children = field
                .get("fields")
                .map(|value| self.fields(value, depth + 1))
                .transpose()?
                .unwrap_or_default();
            let mut aliases = BTreeSet::new();
            for name in field.get("names").and_then(Value::as_array).into_iter().flatten() {
                let name = name.as_str().ok_or(Error::Field("names"))?;
                if name.is_empty() || name.len() > 1024 {
                    return Err(Error::Field("names"));
                }
                if !aliases.insert(name) {
                    continue;
                }
                self.insert(&mut paths, &mut flattened, vec![name.into()], id)?;
                for (child, id) in &children {
                    let bytes = child.iter().map(String::len).sum::<usize>() + name.len();
                    self.charge(bytes)?;
                    let mut path = Vec::with_capacity(child.len() + 1);
                    path.push(name.into());
                    path.extend(child.iter().cloned());
                    self.insert(&mut paths, &mut flattened, path, *id)?;
                }
            }
        }
        Ok(paths)
    }

    fn insert(
        &mut self,
        paths: &mut Paths,
        flattened: &mut BTreeSet<String>,
        path: Vec<String>,
        id: Option<i32>,
    ) -> Result<(), Error> {
        if paths.len() >= 4096 || path.len() > 32 {
            return Err(Error::Bounds);
        }
        let bytes = path.iter().map(String::len).sum::<usize>() + path.len();
        self.charge(bytes)?;
        self.bytes = self.bytes.checked_sub(bytes).ok_or(Error::Bounds)?;
        if !flattened.insert(path.join(".")) || paths.insert(path, id).is_some() {
            return Err(Error::Field("name-mapping-sdk-path"));
        }
        Ok(())
    }

    fn charge(&mut self, work: usize) -> Result<(), Error> {
        self.work = self.work.checked_sub(work).ok_or(Error::Bounds)?;
        Ok(())
    }
}
