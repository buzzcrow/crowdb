use std::collections::BTreeMap;

use crowdb_protocol::iceberg_fb::{
    FBNameMove, FBNameMoveArgs, FBNamespaceAuthority, FBNamespaceAuthorityArgs, FBNamespaceMapping,
    FBNamespaceMappingArgs, FBNamespaceProperty, FBNamespacePropertyArgs, FBRetiredNamespace,
    FBRetiredNamespaceArgs,
};
use flatbuffers::{FlatBufferBuilder, WIPOffset};

use crate::error::ValidationError;
use crate::key::{CatalogId, NamespaceId, OperationId};
use crate::namespace::{
    NamespaceAuthority, NamespaceIdentifier, NamespaceLifecycle, NamespaceMapping, NamespaceMappingState,
    NamespaceProperties, MAX_PROPERTIES,
};

pub(super) fn encode_retired<'buffer>(
    builder: &mut FlatBufferBuilder<'buffer>,
    catalog: CatalogId,
    namespace: NamespaceId,
) -> Result<WIPOffset<FBRetiredNamespace<'buffer>>, ValidationError> {
    crate::namespace::authority_key(catalog, namespace).encode()?;
    let catalog = builder.create_vector(catalog.as_bytes());
    let namespace_id = builder.create_vector(namespace.as_bytes());
    Ok(FBRetiredNamespace::create(
        builder,
        &FBRetiredNamespaceArgs {
            catalog: Some(catalog),
            namespace_id: Some(namespace_id),
        },
    ))
}

pub(super) fn decode_retired(
    value: FBRetiredNamespace<'_>,
) -> Result<crate::record::StorageRecord, ValidationError> {
    Ok(crate::record::StorageRecord::RetiredNamespace {
        catalog: CatalogId::from_bytes(value.catalog().bytes())?,
        namespace: NamespaceId::from_bytes(value.namespace_id().bytes())?,
    })
}

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
    let rename = authority
        .rename
        .as_ref()
        .map(|rename| encode_rename(builder, rename))
        .transpose()?;
    let creating_child = authority
        .creating_child
        .map(|child| builder.create_vector(child.as_bytes()));
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
                NamespaceLifecycle::Ready if authority.creating_child.is_some() => 3,
                NamespaceLifecycle::Ready => 0,
                NamespaceLifecycle::Dropping if authority.pending_operation.is_none() => 4,
                NamespaceLifecycle::Dropping => 1,
                NamespaceLifecycle::Tombstone => 2,
            },
            pending_operation,
            properties: Some(properties),
            rename,
            creating_child,
        },
    ))
}

pub(super) fn decode_authority(
    value: FBNamespaceAuthority<'_>,
) -> Result<NamespaceAuthority, ValidationError> {
    if (value.lifecycle() == 3) != value.creating_child().is_some() {
        return Err(ValidationError::Record);
    }
    let unowned = value.pending_operation().bytes() == [0; 16];
    if (matches!(value.lifecycle(), 1 | 2) && unowned) || (value.lifecycle() == 4 && !unowned) {
        return Err(ValidationError::Record);
    }
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
            0 | 3 => NamespaceLifecycle::Ready,
            1 | 4 => NamespaceLifecycle::Dropping,
            2 => NamespaceLifecycle::Tombstone,
            _ => return Err(ValidationError::Record),
        },
        pending_operation: if value.pending_operation().bytes() == [0; 16] {
            None
        } else {
            Some(OperationId::from_bytes(value.pending_operation().bytes())?)
        },
        properties: NamespaceProperties::new(properties)?,
        rename: value.rename().map(decode_rename).transpose()?.map(Box::new),
        creating_child: value
            .creating_child()
            .map(|child| NamespaceId::from_bytes(child.bytes()))
            .transpose()?,
    };
    authority.validate()?;
    Ok(authority)
}

fn encode_rename<'buffer>(
    builder: &mut FlatBufferBuilder<'buffer>,
    rename: &crate::namespace::NamespaceRename,
) -> Result<WIPOffset<FBNameMove<'buffer>>, ValidationError> {
    rename.validate()?;
    let source = super::table::encode_mapping(builder, &rename.source)?;
    let destination_namespace = builder.create_vector(rename.destination_namespace.as_bytes());
    let destination_name = builder.create_string(&rename.destination_name);
    let operation = builder.create_vector(rename.operation.as_bytes());
    Ok(FBNameMove::create(
        builder,
        &FBNameMoveArgs {
            source: Some(source),
            destination_namespace: Some(destination_namespace),
            destination_name: Some(destination_name),
            operation: Some(operation),
            head_generation: rename.head_generation,
            head_fence: rename.head_fence,
        },
    ))
}

fn decode_rename(value: FBNameMove<'_>) -> Result<crate::namespace::NamespaceRename, ValidationError> {
    let rename = crate::namespace::NamespaceRename {
        source: super::table::decode_mapping(value.source())?,
        destination_namespace: NamespaceId::from_bytes(value.destination_namespace().bytes())?,
        destination_name: value.destination_name().to_owned(),
        operation: OperationId::from_bytes(value.operation().bytes())?,
        head_generation: value.head_generation(),
        head_fence: value.head_fence(),
    };
    rename.validate()?;
    Ok(rename)
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
                NamespaceMappingState::ReservedResource => 2,
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
            2 => NamespaceMappingState::ReservedResource,
            1 => NamespaceMappingState::Published,
            _ => return Err(ValidationError::Record),
        },
    };
    mapping.validate()?;
    Ok(mapping)
}
