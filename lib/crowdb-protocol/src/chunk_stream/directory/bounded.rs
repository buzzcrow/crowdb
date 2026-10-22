// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use serde::de::{Error, SeqAccess, Visitor};
use serde::{Deserialize, Deserializer};
use std::fmt;
use std::marker::PhantomData;

use super::{
    StreamDirectoryRef, StreamExtentLocation, MAX_STREAM_DIRECTORY_CHILDREN, MAX_STREAM_DIRECTORY_LOCATIONS,
};

pub(super) fn locations<'de, D: Deserializer<'de>>(
    decoder: D,
) -> Result<Vec<StreamExtentLocation>, D::Error> {
    decoder.deserialize_seq(Bounded::<StreamExtentLocation, MAX_STREAM_DIRECTORY_LOCATIONS>(
        PhantomData,
    ))
}

pub(super) fn children<'de, D: Deserializer<'de>>(decoder: D) -> Result<Vec<StreamDirectoryRef>, D::Error> {
    decoder.deserialize_seq(Bounded::<StreamDirectoryRef, MAX_STREAM_DIRECTORY_CHILDREN>(
        PhantomData,
    ))
}

struct Bounded<T, const LIMIT: usize>(PhantomData<T>);

impl<'de, T: Deserialize<'de>, const LIMIT: usize> Visitor<'de> for Bounded<T, LIMIT> {
    type Value = Vec<T>;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "a sequence with at most {LIMIT} entries")
    }

    fn visit_seq<S: SeqAccess<'de>>(self, mut sequence: S) -> Result<Self::Value, S::Error> {
        let count = sequence.size_hint().unwrap_or(0);
        if count > LIMIT {
            return Err(S::Error::custom("directory sequence exceeds its bound"));
        }
        let mut values = Vec::with_capacity(count);
        while let Some(value) = sequence.next_element()? {
            if values.len() == LIMIT {
                return Err(S::Error::custom("directory sequence exceeds its bound"));
            }
            values.push(value);
        }
        Ok(values)
    }
}
