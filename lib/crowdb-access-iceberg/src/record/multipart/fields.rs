use crowdb_protocol::iceberg_fb::{FBFileIdentity, FBFileIdentityArgs, FBFileTree, FBFileTreeArgs};
use flatbuffers::{FlatBufferBuilder, WIPOffset};

use crate::error::ValidationError;
use crate::file::{FileIdentity, FileTree, TableLocation};
use crate::key::{CatalogId, FileId, TableId};
use crate::record::file::{decode_root, encode_root};

pub(super) fn encode_owner<'buffer>(
    builder: &mut FlatBufferBuilder<'buffer>,
    owner: FileIdentity,
) -> WIPOffset<FBFileIdentity<'buffer>> {
    let catalog = builder.create_vector(owner.table.catalog.as_bytes());
    let table_id = builder.create_vector(owner.table.table.as_bytes());
    let file_id = builder.create_vector(owner.file.as_bytes());
    FBFileIdentity::create(
        builder,
        &FBFileIdentityArgs {
            catalog: Some(catalog),
            table_id: Some(table_id),
            file_id: Some(file_id),
        },
    )
}

pub(super) fn decode_owner(value: FBFileIdentity<'_>) -> Result<FileIdentity, ValidationError> {
    Ok(FileIdentity {
        table: TableLocation {
            catalog: CatalogId::from_bytes(value.catalog().bytes())?,
            table: TableId::from_bytes(value.table_id().bytes())?,
        },
        file: FileId::from_bytes(value.file_id().bytes())?,
    })
}

pub(super) fn encode_tree<'buffer>(
    builder: &mut FlatBufferBuilder<'buffer>,
    tree: &FileTree,
) -> WIPOffset<FBFileTree<'buffer>> {
    let digest = builder.create_vector(&tree.digest);
    let root = tree.root.as_ref().map(|root| encode_root(builder, root));
    FBFileTree::create(
        builder,
        &FBFileTreeArgs {
            length: tree.length,
            digest: Some(digest),
            root,
        },
    )
}

pub(super) fn decode_tree(value: FBFileTree<'_>) -> Result<FileTree, ValidationError> {
    Ok(FileTree {
        length: value.length(),
        digest: value
            .digest()
            .bytes()
            .try_into()
            .map_err(|_| ValidationError::Record)?,
        root: value.root().map(decode_root).transpose()?,
    })
}
