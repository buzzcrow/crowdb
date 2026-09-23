use serde::de::{Error, IgnoredAny, MapAccess, SeqAccess, Visitor};
use serde::{Deserialize, Deserializer};
use std::collections::BTreeMap;
use std::fmt;
use std::marker::PhantomData;

pub(super) fn valid_properties(properties: &BTreeMap<String, String>) -> bool {
    properties.len() <= 1024
        && properties
            .iter()
            .all(|(key, value)| key.len() <= 256 && value.len() <= 4096)
        && properties.iter().fold(0_usize, |bytes, (key, value)| {
            bytes.saturating_add(key.len()).saturating_add(value.len())
        }) <= 64 * 1024
}

pub(super) fn sequence<'de, Decoder: Deserializer<'de>, Item: Deserialize<'de>>(
    decoder: Decoder,
) -> Result<Vec<Item>, Decoder::Error> {
    struct Sequence<Item>(PhantomData<Item>);
    impl<'de, Item: Deserialize<'de>> Visitor<'de> for Sequence<Item> {
        type Value = Vec<Item>;
        fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.write_str("at most 4096 elements")
        }
        fn visit_seq<Access: SeqAccess<'de>>(self, mut access: Access) -> Result<Self::Value, Access::Error> {
            let mut values = Vec::new();
            while values.len() < 4096 {
                let Some(value) = access.next_element()? else {
                    return Ok(values);
                };
                values.push(value);
            }
            if access.next_element::<IgnoredAny>()?.is_some() {
                return Err(Access::Error::custom("element limit exceeded"));
            }
            Ok(values)
        }
    }
    decoder.deserialize_seq(Sequence(PhantomData))
}

pub(super) fn properties<'de, Decoder: Deserializer<'de>>(
    decoder: Decoder,
) -> Result<BTreeMap<String, String>, Decoder::Error> {
    struct Properties;
    impl<'de> Visitor<'de> for Properties {
        type Value = BTreeMap<String, String>;
        fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.write_str("bounded unique string properties")
        }
        fn visit_map<Access: MapAccess<'de>>(self, mut access: Access) -> Result<Self::Value, Access::Error> {
            let mut values = BTreeMap::new();
            let mut bytes = 0_usize;
            while let Some((key, value)) = access.next_entry::<String, String>()? {
                bytes = bytes.saturating_add(key.len()).saturating_add(value.len());
                if values.len() == 1024
                    || key.len() > 256
                    || value.len() > 4096
                    || bytes > 64 * 1024
                    || values.insert(key, value).is_some()
                {
                    return Err(Access::Error::custom("invalid or excessive properties"));
                }
            }
            Ok(values)
        }
    }
    decoder.deserialize_map(Properties)
}
