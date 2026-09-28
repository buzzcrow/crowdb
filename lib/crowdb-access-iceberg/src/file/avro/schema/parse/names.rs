use super::{primitive, text, AvroContainerError};
use serde_json::{Map, Value};

pub(super) fn identifier(name: &str) -> Result<(), AvroContainerError> {
    let mut bytes = name.bytes();
    if !bytes
        .next()
        .is_some_and(|byte| byte.is_ascii_alphabetic() || byte == b'_')
        || !bytes.all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
    {
        return Err(AvroContainerError::Schema);
    }
    Ok(())
}

pub(super) fn qualify(name: &str, namespace: &str) -> Result<String, AvroContainerError> {
    for component in name.split('.') {
        identifier(component)?;
    }
    Ok(if name.contains('.') || namespace.is_empty() {
        name.to_owned()
    } else {
        format!("{namespace}.{name}")
    })
}

pub(super) fn definition(
    object: &Map<String, Value>,
    enclosing: &str,
) -> Result<(String, String), AvroContainerError> {
    let name = text(object, "name")?;
    let local = name.rsplit('.').next().ok_or(AvroContainerError::Schema)?;
    if primitive(local).is_some() {
        return Err(AvroContainerError::Schema);
    }
    let namespace = if name.contains('.') {
        ""
    } else {
        match object.get("namespace") {
            Some(value) => value.as_str().ok_or(AvroContainerError::Schema)?,
            None => enclosing,
        }
    };
    if !namespace.is_empty() {
        for component in namespace.split('.') {
            identifier(component)?;
        }
    }
    let full = qualify(name, namespace)?;
    let namespace = full
        .rsplit_once('.')
        .map_or("", |(namespace, _)| namespace)
        .to_owned();
    Ok((full, namespace))
}
