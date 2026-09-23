use super::{id, json, ManifestContextError as Error, ManifestVersion, PrimitiveType, SchemaField};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PartitionTransform {
    Identity,
    Bucket(i32),
    Truncate(i32),
    Year,
    Month,
    Day,
    Hour,
    Void,
    Unknown(String),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PartitionField {
    pub id: i32,
    pub name: String,
    pub sources: Vec<i32>,
    pub transform: PartitionTransform,
    pub result: Option<PrimitiveType>,
}

pub(super) fn parse(
    bytes: &[u8],
    version: ManifestVersion,
    fields: &BTreeMap<i32, SchemaField>,
) -> Result<Vec<PartitionField>, Error> {
    let root = json(bytes)?;
    let values = root.as_array().ok_or(Error::Invalid)?;
    if values.len() > 256 {
        return Err(Error::Bounds);
    }
    let mut ids = BTreeSet::new();
    let mut names = BTreeSet::new();
    let mut partitions = Vec::new();
    for (index, value) in values.iter().enumerate() {
        let field_id = if version == ManifestVersion::V1 && value.get("field-id").is_none() {
            1000 + i32::try_from(index).map_err(|_| Error::Bounds)?
        } else {
            id(&value["field-id"])?
        };
        let name = value["name"]
            .as_str()
            .filter(|name| !name.is_empty())
            .ok_or(Error::Invalid)?;
        if !ids.insert(field_id) || !names.insert(name) {
            return Err(Error::Invalid);
        }
        let sources = match (value.get("source-id"), value.get("source-ids")) {
            (Some(source), None) => vec![id(source)?],
            (None, Some(sources)) if version == ManifestVersion::V3 => {
                let sources = sources
                    .as_array()
                    .filter(|sources| sources.len() >= 2 && sources.len() <= 256)
                    .ok_or(Error::Invalid)?;
                sources.iter().map(id).collect::<Result<Vec<_>, _>>()?
            }
            _ => return Err(Error::Invalid),
        };
        for source in &sources {
            let field = fields.get(source).ok_or(Error::Invalid)?;
            if field.repeated || field.primitive.is_none() {
                return Err(Error::Invalid);
            }
        }
        let transform = PartitionTransform::parse(value["transform"].as_str().ok_or(Error::Invalid)?)?;
        if sources.len() != 1 && !matches!(transform, PartitionTransform::Unknown(_)) {
            return Err(Error::Invalid);
        }
        let source = fields[&sources[0]].primitive.as_ref().ok_or(Error::Invalid)?;
        let result = transform.result(source)?;
        partitions.push(PartitionField {
            id: field_id,
            name: name.into(),
            sources,
            transform,
            result,
        });
    }
    Ok(partitions)
}

impl PartitionTransform {
    fn parse(name: &str) -> Result<Self, Error> {
        Ok(match name {
            "identity" => Self::Identity,
            "year" => Self::Year,
            "month" => Self::Month,
            "day" => Self::Day,
            "hour" => Self::Hour,
            "void" => Self::Void,
            _ => {
                for prefix in ["bucket", "truncate"] {
                    if name
                        .strip_prefix(prefix)
                        .is_some_and(|suffix| suffix.starts_with('['))
                    {
                        let argument = name
                            .strip_prefix(prefix)
                            .and_then(|value| value.strip_prefix('['))
                            .and_then(|value| value.strip_suffix(']'))
                            .and_then(|value| value.parse::<i32>().ok())
                            .filter(|value| *value > 0)
                            .ok_or(Error::Invalid)?;
                        return Ok(if prefix == "bucket" {
                            Self::Bucket(argument)
                        } else {
                            Self::Truncate(argument)
                        });
                    }
                }
                if name.is_empty() || name.len() > 1024 {
                    return Err(Error::Invalid);
                }
                Self::Unknown(name.into())
            }
        })
    }

    fn result(&self, source: &PrimitiveType) -> Result<Option<PrimitiveType>, Error> {
        use PrimitiveType::{
            Binary, Date, Decimal, Fixed, Int, Long, String, Time, Timestamp, TimestampNs, Timestamptz,
            TimestamptzNs, Uuid,
        };
        let timestamp = matches!(source, Timestamp | Timestamptz | TimestampNs | TimestamptzNs);
        match self {
            Self::Unknown(_) => Ok(None),
            Self::Void => Ok(Some(source.clone())),
            Self::Identity
                if !matches!(
                    source,
                    PrimitiveType::Variant | PrimitiveType::Geometry(_) | PrimitiveType::Geography(_)
                ) =>
            {
                Ok(Some(source.clone()))
            }
            Self::Bucket(_)
                if matches!(
                    source,
                    Int | Long
                        | Decimal { .. }
                        | Date
                        | Time
                        | Timestamp
                        | Timestamptz
                        | TimestampNs
                        | TimestamptzNs
                        | String
                        | Uuid
                        | Fixed(_)
                        | Binary
                ) =>
            {
                Ok(Some(Int))
            }
            Self::Truncate(_) if matches!(source, Int | Long | Decimal { .. } | String | Binary) => {
                Ok(Some(source.clone()))
            }
            Self::Year | Self::Month | Self::Day if timestamp || *source == Date => Ok(Some(Int)),
            Self::Hour if timestamp => Ok(Some(Int)),
            _ => Err(Error::Invalid),
        }
    }
}
