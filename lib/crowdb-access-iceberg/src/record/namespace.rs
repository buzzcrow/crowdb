use std::collections::BTreeMap;

use crowdb_protocol::iceberg_fb::{
    FBNamespaceAuthority, FBNamespaceAuthorityArgs, FBNamespaceMapping, FBNamespaceMappingArgs,
    FBNamespaceProperty, FBNamespacePropertyArgs,
};
use flatbuffers::{FlatBufferBuilder, WIPOffset};

use crate::error::ValidationError;
use crate::key::{CatalogId, NamespaceId, OperationId};
use crate::namespace::{
    NamespaceAuthority, NamespaceIdentifier, NamespaceLifecycle, NamespaceMapping, NamespaceMappingState,
    NamespaceProperties, MAX_PROPERTIES,
};

pub(super) fn encode_authority<'buffer>(
    builder: &mut FlatBufferBuilder<'buffer>,
    authority: &NamespaceAuthority,
) -> Result<WIPOffset<FBNamespaceAuthority<'buffer>>, ValidationError> {
    authority.validate()?;
    let catalog = builder.create_vector(authority.catalog.as_bytes());
    let namespace_id = builder.create_vector(authority.namespace.as_bytes());
    let parent = authority
        .parent
        .map(|parent| builder.create_vector(parent.as_bytes()));
    let identifier = builder.create_vector(&authority.identifier.encode()?);
    let operation_bytes = authority
        .pending_operation
        .as_ref()
        .map_or(&[0; 16], OperationId::as_bytes);
    let pending_operation = Some(builder.create_vector(operation_bytes));
    let mut properties = Vec::with_capacity(authority.properties.entries().len());
    for (key, value) in authority.properties.entries() {
        let key = builder.create_string(key);
        let value = builder.create_string(value);
        properties.push(FBNamespaceProperty::create(
            builder,
            &FBNamespacePropertyArgs {
                key: Some(key),
                value: Some(value),
            },
        ));
    }
    let properties = builder.create_vector(&properties);
    Ok(FBNamespaceAuthority::create(
        builder,
        &FBNamespaceAuthorityArgs {
            catalog: Some(catalog),
            namespace_id: Some(namespace_id),
            parent,
            identifier: Some(identifier),
            name_epoch: authority.name_epoch,
            property_revision: authority.property_revision,
            admission_fence: authority.admission_fence,
            mutation_revision: authority.mutation_revision,
            lifecycle: match authority.lifecycle {
                NamespaceLifecycle::Ready => 0,
                NamespaceLifecycle::Dropping => 1,
                NamespaceLifecycle::Tombstone => 2,
            },
            pending_operation,
            properties: Some(properties),
        },
    ))
}

pub(super) fn decode_authority(
    value: FBNamespaceAuthority<'_>,
) -> Result<NamespaceAuthority, ValidationError> {
    if value.properties().len() > MAX_PROPERTIES {
        return Err(ValidationError::RecordTooLarge);
    }
    let mut properties = BTreeMap::new();
    let mut previous: Option<&str> = None;
    for property in value.properties() {
        if previous.is_some_and(|key| key >= property.key()) {
            return Err(ValidationError::Record);
        }
        previous = Some(property.key());
        properties.insert(property.key().to_owned(), property.value().to_owned());
    }
    let authority = NamespaceAuthority {
        catalog: CatalogId::from_bytes(value.catalog().bytes())?,
        namespace: NamespaceId::from_bytes(value.namespace_id().bytes())?,
        parent: value
            .parent()
            .map(|parent| NamespaceId::from_bytes(parent.bytes()))
            .transpose()?,
        identifier: NamespaceIdentifier::decode(value.identifier().bytes())?,
        name_epoch: value.name_epoch(),
        property_revision: value.property_revision(),
        admission_fence: value.admission_fence(),
        mutation_revision: value.mutation_revision(),
        lifecycle: match value.lifecycle() {
            0 => NamespaceLifecycle::Ready,
            1 => NamespaceLifecycle::Dropping,
            2 => NamespaceLifecycle::Tombstone,
            _ => return Err(ValidationError::Record),
        },
        pending_operation: if value.pending_operation().bytes() == [0; 16] {
            None
        } else {
            Some(OperationId::from_bytes(value.pending_operation().bytes())?)
        },
        properties: NamespaceProperties::new(properties)?,
    };
    authority.validate()?;
    Ok(authority)
}

pub(super) fn encode_mapping<'buffer>(
    builder: &mut FlatBufferBuilder<'buffer>,
    mapping: &NamespaceMapping,
) -> Result<WIPOffset<FBNamespaceMapping<'buffer>>, ValidationError> {
    mapping.validate()?;
    let catalog = builder.create_vector(mapping.catalog.as_bytes());
    let namespace_id = builder.create_vector(mapping.namespace.as_bytes());
    let parent = mapping
        .parent
        .map(|parent| builder.create_vector(parent.as_bytes()));
    let name = builder.create_string(&mapping.name);
    let operation = builder.create_vector(mapping.operation.as_bytes());
    Ok(FBNamespaceMapping::create(
        builder,
        &FBNamespaceMappingArgs {
            catalog: Some(catalog),
            namespace_id: Some(namespace_id),
            parent,
            name: Some(name),
            name_epoch: mapping.name_epoch,
            operation: Some(operation),
            state: match mapping.state {
                NamespaceMappingState::Reserved => 0,
                NamespaceMappingState::Published => 1,
            },
        },
    ))
}

pub(super) fn decode_mapping(value: FBNamespaceMapping<'_>) -> Result<NamespaceMapping, ValidationError> {
    let mapping = NamespaceMapping {
        catalog: CatalogId::from_bytes(value.catalog().bytes())?,
        namespace: NamespaceId::from_bytes(value.namespace_id().bytes())?,
        parent: value
            .parent()
            .map(|parent| NamespaceId::from_bytes(parent.bytes()))
            .transpose()?,
        name: value.name().to_owned(),
        name_epoch: value.name_epoch(),
        operation: OperationId::from_bytes(value.operation().bytes())?,
        state: match value.state() {
            0 => NamespaceMappingState::Reserved,
            1 => NamespaceMappingState::Published,
            _ => return Err(ValidationError::Record),
        },
    };
    mapping.validate()?;
    Ok(mapping)
}
