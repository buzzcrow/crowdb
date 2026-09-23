use std::collections::BTreeMap;

use super::{ParquetMetadataError as Error, ParquetMetadataLimits};

pub(super) enum Value<'data> {
    Boolean(bool),
    Integer(u8, i64),
    Bytes(&'data [u8]),
    List(u8, u8, Vec<Self>),
    Struct(BTreeMap<i16, Self>),
    Other,
}

pub(super) fn decode(bytes: &[u8], limits: ParquetMetadataLimits) -> Result<Value<'_>, Error> {
    let mut input = Input {
        bytes,
        offset: 0,
        remaining: limits.values,
        depth: limits.depth,
    };
    let value = input.value(12, 0, false)?;
    if input.offset != bytes.len() {
        return Err(Error::Invalid);
    }
    Ok(value)
}

struct Input<'data> {
    bytes: &'data [u8],
    offset: usize,
    remaining: usize,
    depth: usize,
}

impl<'data> Input<'data> {
    fn byte(&mut self) -> Result<u8, Error> {
        Ok(self.take(1)?[0])
    }

    fn take(&mut self, count: usize) -> Result<&'data [u8], Error> {
        let end = self.offset.checked_add(count).ok_or(Error::Invalid)?;
        let bytes = self.bytes.get(self.offset..end).ok_or(Error::Invalid)?;
        self.offset = end;
        Ok(bytes)
    }

    fn unsigned(&mut self) -> Result<u64, Error> {
        let mut value = 0_u64;
        for shift in (0..70).step_by(7) {
            let byte = self.byte()?;
            if shift == 63 && byte > 1 {
                return Err(Error::Invalid);
            }
            value |= u64::from(byte & 127) << shift;
            if byte & 128 == 0 {
                return Ok(value);
            }
        }
        Err(Error::Invalid)
    }

    fn integer(&mut self) -> Result<i64, Error> {
        let value = self.unsigned()?;
        Ok(i64::try_from(value >> 1).map_err(|_| Error::Invalid)?
            ^ -i64::try_from(value & 1).map_err(|_| Error::Invalid)?)
    }

    fn count(&self, value: u64) -> Result<usize, Error> {
        usize::try_from(value)
            .ok()
            .filter(|count| *count <= self.remaining && i32::try_from(*count).is_ok())
            .ok_or(Error::Bounds)
    }

    fn value(&mut self, kind: u8, depth: usize, field: bool) -> Result<Value<'data>, Error> {
        if depth > self.depth || self.remaining == 0 {
            return Err(Error::Bounds);
        }
        self.remaining -= 1;
        match kind {
            1 | 2 => {
                let boolean = if field { kind } else { self.byte()? };
                match boolean {
                    1 => Ok(Value::Boolean(true)),
                    2 => Ok(Value::Boolean(false)),
                    _ => Err(Error::Invalid),
                }
            }
            3 => Ok(Value::Integer(3, i64::from(i8::from_ne_bytes([self.byte()?])))),
            4..=6 => {
                let value = self.integer()?;
                if (kind == 4 && i16::try_from(value).is_err())
                    || (kind == 5 && i32::try_from(value).is_err())
                {
                    return Err(Error::Invalid);
                }
                Ok(Value::Integer(kind, value))
            }
            7 => {
                self.take(8)?;
                Ok(Value::Other)
            }
            8 => {
                let count = usize::try_from(self.unsigned()?).map_err(|_| Error::Bounds)?;
                Ok(Value::Bytes(self.take(count)?))
            }
            9 | 10 => self.list(kind, depth),
            11 => self.map(depth),
            12 => self.structure(depth),
            13 => {
                self.take(16)?;
                Ok(Value::Other)
            }
            _ => Err(Error::Invalid),
        }
    }

    fn list(&mut self, container: u8, depth: usize) -> Result<Value<'data>, Error> {
        let header = self.byte()?;
        let length = if header >> 4 == 15 {
            self.unsigned()?
        } else {
            u64::from(header >> 4)
        };
        let count = self.count(length)?;
        let kind = header & 15;
        if !(1..=13).contains(&kind) {
            return Err(Error::Invalid);
        }
        let mut values = Vec::new();
        for _ in 0..count {
            values.push(self.value(kind, depth + 1, false)?);
        }
        Ok(Value::List(container, kind, values))
    }

    fn map(&mut self, depth: usize) -> Result<Value<'data>, Error> {
        let count = self.unsigned()?;
        let count = self.count(count.checked_mul(2).ok_or(Error::Bounds)?)? / 2;
        if count != 0 {
            let header = self.byte()?;
            for _ in 0..count {
                self.value(header >> 4, depth + 1, false)?;
                self.value(header & 15, depth + 1, false)?;
            }
        }
        Ok(Value::Other)
    }

    fn structure(&mut self, depth: usize) -> Result<Value<'data>, Error> {
        let mut fields = BTreeMap::new();
        let mut previous = 0_i16;
        loop {
            let header = self.byte()?;
            if header == 0 {
                return Ok(Value::Struct(fields));
            }
            let id = if header >> 4 == 0 {
                i16::try_from(self.integer()?).map_err(|_| Error::Invalid)?
            } else {
                previous
                    .checked_add(i16::from(header >> 4))
                    .ok_or(Error::Invalid)?
            };
            if fields.contains_key(&id) {
                return Err(Error::Invalid);
            }
            fields.insert(id, self.value(header & 15, depth + 1, true)?);
            previous = id;
        }
    }
}

impl<'data> Value<'data> {
    pub(super) fn boolean(&self) -> Result<bool, Error> {
        if let Self::Boolean(value) = self {
            Ok(*value)
        } else {
            Err(Error::Invalid)
        }
    }
    pub(super) fn fields(&self) -> Result<&BTreeMap<i16, Self>, Error> {
        if let Self::Struct(fields) = self {
            Ok(fields)
        } else {
            Err(Error::Invalid)
        }
    }

    pub(super) fn integer(&self, kind: u8) -> Result<i64, Error> {
        if let Self::Integer(actual, value) = self {
            if *actual == kind {
                return Ok(*value);
            }
        }
        Err(Error::Invalid)
    }

    pub(super) fn list(&self, kind: u8) -> Result<&[Self], Error> {
        if let Self::List(9, actual, values) = self {
            if *actual == kind {
                return Ok(values);
            }
        }
        Err(Error::Invalid)
    }

    pub(super) fn bytes(&self) -> Result<&'data [u8], Error> {
        if let Self::Bytes(bytes) = self {
            Ok(bytes)
        } else {
            Err(Error::Invalid)
        }
    }
}
