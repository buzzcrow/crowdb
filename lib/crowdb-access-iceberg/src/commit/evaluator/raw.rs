use std::{collections::BTreeMap, io::Write};

use serde::{de::DeserializeOwned, Serialize};
use serde_json::value::RawValue;

use crate::table::TableMetadataError as Error;

pub(super) type Object = BTreeMap<String, Box<RawValue>>;
pub(super) type Array = Vec<Box<RawValue>>;

pub(super) struct Document {
    pub fields: Object,
    pub limit: usize,
    remaining: usize,
}

impl Document {
    pub fn new(bytes: &[u8], limit: usize, work: usize) -> Result<Self, Error> {
        if bytes.len() > limit || work < bytes.len() {
            return Err(Error::Bounds);
        }
        Ok(Self {
            fields: serde_json::from_slice(bytes)?,
            limit,
            remaining: work - bytes.len(),
        })
    }

    pub fn charge(&mut self, bytes: usize) -> Result<(), Error> {
        self.remaining = self.remaining.checked_sub(bytes).ok_or(Error::Bounds)?;
        Ok(())
    }

    pub fn get<Value: DeserializeOwned>(&mut self, name: &'static str) -> Result<Value, Error> {
        let length = self.fields.get(name).ok_or(Error::Field(name))?.get().len();
        self.charge(length)?;
        Ok(serde_json::from_str(self.fields[name].get())?)
    }

    pub fn array(&mut self, name: &'static str) -> Result<Array, Error> {
        if !self.fields.contains_key(name) {
            return Ok(Vec::new());
        }
        self.get(name)
    }

    pub fn object(&mut self, name: &'static str) -> Result<Object, Error> {
        if !self.fields.contains_key(name) {
            return Ok(Object::new());
        }
        self.get(name)
    }

    pub fn set<Value: Serialize + ?Sized>(&mut self, name: &str, value: &Value) -> Result<(), Error> {
        let raw = encode(value, self.limit)?;
        self.charge(raw.get().len())?;
        self.fields.insert(name.into(), raw);
        Ok(())
    }

    pub fn finish(&mut self) -> Result<Vec<u8>, Error> {
        let raw = encode(&self.fields, self.limit)?;
        self.charge(raw.get().len())?;
        Ok(raw.get().as_bytes().to_vec())
    }
}

pub(crate) fn encode<Value: Serialize + ?Sized>(value: &Value, limit: usize) -> Result<Box<RawValue>, Error> {
    let mut output = Output {
        bytes: Vec::new(),
        limit,
    };
    if let Err(error) = serde_json::to_writer(&mut output, value) {
        return Err(if error.is_io() {
            Error::Bounds
        } else {
            Error::Json(error)
        });
    }
    let text = String::from_utf8(output.bytes).map_err(|_| Error::Field("json"))?;
    Ok(RawValue::from_string(text)?)
}

struct Output {
    bytes: Vec<u8>,
    limit: usize,
}

impl Write for Output {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        if bytes.len() > self.limit.saturating_sub(self.bytes.len()) {
            return Err(std::io::Error::other("metadata serialization limit"));
        }
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
