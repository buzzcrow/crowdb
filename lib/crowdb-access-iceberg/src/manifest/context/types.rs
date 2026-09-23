use super::{ManifestContextError, ManifestVersion};

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PrimitiveType {
    Boolean,
    Int,
    Long,
    Float,
    Double,
    Date,
    Time,
    Timestamp,
    Timestamptz,
    TimestampNs,
    TimestamptzNs,
    String,
    Uuid,
    Fixed(usize),
    Binary,
    Decimal { precision: u32, scale: u32 },
    Unknown,
    Variant,
    Geometry(String),
    Geography(String),
}

impl PrimitiveType {
    pub(crate) fn parse(name: &str, version: ManifestVersion) -> Result<Self, ManifestContextError> {
        let primitive = match name {
            "boolean" => Self::Boolean,
            "int" => Self::Int,
            "long" => Self::Long,
            "float" => Self::Float,
            "double" => Self::Double,
            "date" => Self::Date,
            "time" => Self::Time,
            "timestamp" => Self::Timestamp,
            "timestamptz" => Self::Timestamptz,
            "timestamp_ns" => Self::TimestampNs,
            "timestamptz_ns" => Self::TimestamptzNs,
            "string" => Self::String,
            "uuid" => Self::Uuid,
            "binary" => Self::Binary,
            "unknown" => Self::Unknown,
            "variant" => Self::Variant,
            "geometry" => Self::Geometry("OGC:CRS84".into()),
            "geography" => Self::Geography("OGC:CRS84,spherical".into()),
            _ => Self::parameterized(name)?,
        };
        if version != ManifestVersion::V3
            && matches!(
                primitive,
                Self::TimestampNs
                    | Self::TimestamptzNs
                    | Self::Unknown
                    | Self::Variant
                    | Self::Geometry(_)
                    | Self::Geography(_)
            )
        {
            return Err(ManifestContextError::Invalid);
        }
        Ok(primitive)
    }

    fn parameterized(name: &str) -> Result<Self, ManifestContextError> {
        if let Some(length) = name
            .strip_prefix("fixed[")
            .and_then(|value| value.strip_suffix(']'))
        {
            let length = length
                .parse::<usize>()
                .ok()
                .filter(|length| *length > 0 && *length <= 1024 * 1024)
                .ok_or(ManifestContextError::Invalid)?;
            return Ok(Self::Fixed(length));
        }
        if let Some(args) = name
            .strip_prefix("decimal(")
            .and_then(|value| value.strip_suffix(')'))
        {
            let (precision, scale) = args.split_once(',').ok_or(ManifestContextError::Invalid)?;
            let precision = precision
                .trim()
                .parse::<u32>()
                .map_err(|_| ManifestContextError::Invalid)?;
            let scale = scale
                .trim()
                .parse::<u32>()
                .map_err(|_| ManifestContextError::Invalid)?;
            if precision == 0 || precision > 38 || scale > precision {
                return Err(ManifestContextError::Invalid);
            }
            return Ok(Self::Decimal { precision, scale });
        }
        for prefix in ["geometry(", "geography("] {
            if let Some(args) = name
                .strip_prefix(prefix)
                .and_then(|value| value.strip_suffix(')'))
            {
                if args.is_empty() || args.len() > 1024 || args.contains(['(', ')']) {
                    return Err(ManifestContextError::Invalid);
                }
                if prefix == "geometry(" {
                    return Ok(Self::Geometry(args.into()));
                }
                let (_, algorithm) = args.rsplit_once(',').ok_or(ManifestContextError::Invalid)?;
                if !matches!(
                    algorithm.trim(),
                    "spherical" | "vincenty" | "thomas" | "andoyer" | "karney"
                ) {
                    return Err(ManifestContextError::Invalid);
                }
                return Ok(Self::Geography(args.into()));
            }
        }
        Err(ManifestContextError::Invalid)
    }

    #[must_use]
    pub fn equality_eligible(&self) -> bool {
        !matches!(self, Self::Float | Self::Double | Self::Unknown | Self::Variant)
    }
}
