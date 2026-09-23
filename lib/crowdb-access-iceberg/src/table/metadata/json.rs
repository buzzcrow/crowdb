use std::fmt;

use serde::de::{DeserializeSeed, Error, MapAccess, SeqAccess, Visitor};
use serde_json::{Map, Number, Value};

use super::{TableMetadataError, TableMetadataLimits};

pub(super) fn parse(bytes: &[u8], limits: TableMetadataLimits) -> Result<Value, TableMetadataError> {
    let mut budget = Budget {
        values: limits.values,
        strings: limits.string_bytes,
        exhausted: false,
    };
    let mut decoder = serde_json::Deserializer::from_slice(bytes);
    let result = Seed {
        budget: &mut budget,
        depth: limits.depth,
    }
    .deserialize(&mut decoder);
    let value = match result {
        Err(_) if budget.exhausted => return Err(TableMetadataError::Bounds),
        result => result?,
    };
    decoder.end()?;
    Ok(value)
}

struct Budget {
    values: usize,
    strings: usize,
    exhausted: bool,
}

impl Budget {
    fn charge<Failure: Error>(&mut self, bytes: usize) -> Result<(), Failure> {
        if self.values == 0 || bytes > self.strings {
            self.exhausted = true;
            return Err(Failure::custom("table metadata budget exceeded"));
        }
        self.values -= 1;
        self.strings -= bytes;
        Ok(())
    }
}

struct Seed<'budget> {
    budget: &'budget mut Budget,
    depth: usize,
}

impl<'de> DeserializeSeed<'de> for Seed<'_> {
    type Value = Value;
    fn deserialize<Decoder: serde::Deserializer<'de>>(
        self,
        decoder: Decoder,
    ) -> Result<Value, Decoder::Error> {
        self.budget.charge::<Decoder::Error>(0)?;
        decoder.deserialize_any(self)
    }
}

impl<'de> Visitor<'de> for Seed<'_> {
    type Value = Value;
    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("bounded duplicate-free JSON")
    }
    fn visit_bool<Failure: Error>(self, value: bool) -> Result<Value, Failure> {
        Ok(Value::Bool(value))
    }
    fn visit_i64<Failure: Error>(self, value: i64) -> Result<Value, Failure> {
        Ok(Value::Number(value.into()))
    }
    fn visit_u64<Failure: Error>(self, value: u64) -> Result<Value, Failure> {
        Ok(Value::Number(value.into()))
    }
    fn visit_f64<Failure: Error>(self, value: f64) -> Result<Value, Failure> {
        Number::from_f64(value)
            .map(Value::Number)
            .ok_or_else(|| Failure::custom("non-finite JSON number"))
    }
    fn visit_unit<Failure: Error>(self) -> Result<Value, Failure> {
        Ok(Value::Null)
    }
    fn visit_str<Failure: Error>(self, value: &str) -> Result<Value, Failure> {
        self.budget.charge::<Failure>(value.len())?;
        Ok(Value::String(value.to_owned()))
    }
    fn visit_seq<Access: SeqAccess<'de>>(mut self, mut access: Access) -> Result<Value, Access::Error> {
        let depth = self.descend::<Access::Error>()?;
        let mut values = Vec::new();
        while let Some(value) = access.next_element_seed(Seed {
            budget: self.budget,
            depth,
        })? {
            values.push(value);
        }
        Ok(Value::Array(values))
    }
    fn visit_map<Access: MapAccess<'de>>(mut self, mut access: Access) -> Result<Value, Access::Error> {
        let depth = self.descend::<Access::Error>()?;
        let mut values = Map::new();
        while let Some(key) = access.next_key::<String>()? {
            self.budget.charge::<Access::Error>(key.len())?;
            if values.contains_key(&key) {
                return Err(Access::Error::custom("duplicate JSON object key"));
            }
            let value = access.next_value_seed(Seed {
                budget: self.budget,
                depth,
            })?;
            values.insert(key, value);
        }
        Ok(Value::Object(values))
    }
}

impl Seed<'_> {
    fn descend<Failure: Error>(&mut self) -> Result<usize, Failure> {
        self.depth.checked_sub(1).ok_or_else(|| {
            self.budget.exhausted = true;
            Failure::custom("table metadata depth exceeded")
        })
    }
}
