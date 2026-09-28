use super::ManifestEntryError as Error;
use crate::file::AvroContainerError;
use std::collections::BTreeMap;

mod path;
mod primitive;
use primitive::Primitive;

pub(super) fn validate(lower: Option<&[u8]>, upper: Option<&[u8]>) -> Result<(), Error> {
    let lower = lower.map(decode).transpose()?;
    let upper = upper.map(decode).transpose()?;
    if let (Some(lower), Some(upper)) = (lower, upper) {
        for (path, lower) in lower {
            if let Some(upper) = upper.get(path) {
                if lower.compare(upper)? == std::cmp::Ordering::Greater {
                    return Err(Error::Field);
                }
            }
        }
    }
    Ok(())
}

fn decode(bytes: &[u8]) -> Result<BTreeMap<&str, Primitive<'_>>, Error> {
    if bytes.len() > 1024 * 1024 {
        return Err(AvroContainerError::Bounds.into());
    }
    let mut input = Input::new(bytes);
    let header = input.byte()?;
    if header & 15 != 1 {
        return Err(Error::Field);
    }
    let width = usize::from(header >> 6) + 1;
    let count = input.uint(width)?;
    if count > 4096 {
        return Err(AvroContainerError::Bounds.into());
    }
    let offsets = input.offsets(count + 1, width)?;
    if offsets[0] != 0 || offsets.windows(2).any(|pair| pair[0] > pair[1]) {
        return Err(Error::Field);
    }
    let text = input.take(offsets[count])?;
    let mut dictionary = Vec::with_capacity(count);
    for pair in offsets.windows(2) {
        let value = std::str::from_utf8(&text[pair[0]..pair[1]]).map_err(|_| Error::Field)?;
        if header & 16 != 0 && dictionary.last().is_some_and(|previous| *previous >= value) {
            return Err(Error::Field);
        }
        dictionary.push(value);
    }
    object(&mut input, &dictionary)
}

fn object<'data>(
    input: &mut Input<'data>,
    dictionary: &[&'data str],
) -> Result<BTreeMap<&'data str, Primitive<'data>>, Error> {
    let header = input.byte()?;
    if header & 3 != 2 {
        return Err(Error::Field);
    }
    let width = usize::from((header >> 2) & 3) + 1;
    let id_width = usize::from((header >> 4) & 3) + 1;
    let count = input.uint(if header & 64 == 0 { 1 } else { 4 })?;
    if count > 4096 {
        return Err(AvroContainerError::Bounds.into());
    }
    let ids = input.offsets(count, id_width)?;
    let offsets = input.offsets(count + 1, width)?;
    let payload = input.take(offsets[count])?;
    input.finish()?;
    let mut names = Vec::with_capacity(count);
    for id in ids {
        let name = *dictionary.get(id).ok_or(Error::Field)?;
        if names.last().is_some_and(|previous| *previous >= name) || !path::normalized(name) {
            return Err(Error::Field);
        }
        names.push(name);
    }
    let mut positions: Vec<_> = offsets[..count].iter().copied().enumerate().collect();
    positions.sort_unstable_by_key(|(_, offset)| *offset);
    let mut end = 0;
    let mut values = BTreeMap::new();
    for (position, (index, offset)) in positions.iter().enumerate() {
        if *offset != end {
            return Err(Error::Field);
        }
        end = positions
            .get(position + 1)
            .map_or(payload.len(), |(_, offset)| *offset);
        if end <= *offset {
            return Err(Error::Field);
        }
        let bytes = payload.get(*offset..end).ok_or(Error::Field)?;
        values.insert(names[*index], primitive::decode(bytes)?);
    }
    if end != payload.len() {
        return Err(Error::Field);
    }
    Ok(values)
}

struct Input<'data> {
    bytes: &'data [u8],
    offset: usize,
}

impl<'data> Input<'data> {
    fn new(bytes: &'data [u8]) -> Self {
        Self { bytes, offset: 0 }
    }

    fn byte(&mut self) -> Result<u8, Error> {
        Ok(self.take(1)?[0])
    }

    fn take(&mut self, length: usize) -> Result<&'data [u8], Error> {
        let end = self.offset.checked_add(length).ok_or(Error::Field)?;
        let bytes = self.bytes.get(self.offset..end).ok_or(Error::Field)?;
        self.offset = end;
        Ok(bytes)
    }

    fn uint(&mut self, width: usize) -> Result<usize, Error> {
        let mut bytes = [0; 4];
        bytes[..width].copy_from_slice(self.take(width)?);
        usize::try_from(u32::from_le_bytes(bytes)).map_err(|_| Error::Field)
    }

    fn offsets(&mut self, count: usize, width: usize) -> Result<Vec<usize>, Error> {
        (0..count).map(|_| self.uint(width)).collect()
    }

    fn finish(&self) -> Result<(), Error> {
        if self.offset == self.bytes.len() {
            Ok(())
        } else {
            Err(Error::Field)
        }
    }
}
