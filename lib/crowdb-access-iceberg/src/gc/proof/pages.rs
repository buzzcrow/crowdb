use sha2::{Digest, Sha256};

use crate::{
    catalog::CatalogError,
    error::ValidationError,
    key::{CatalogScope, IcebergKey},
    operation::{PayloadPage, PayloadReference, PAYLOAD_PAGE_BYTES},
    record::StorageRecord,
};

use super::super::{GcPage, GcRepository, GcTask};

impl GcRepository {
    pub(in crate::gc) async fn put_proof_page(
        &self,
        task: &GcTask,
        kind: u8,
        sequence: u64,
        entries: Vec<Vec<u8>>,
    ) -> Result<PayloadReference, CatalogError> {
        let page = GcPage {
            catalog: task.context.catalog,
            task: task.identity,
            kind,
            sequence,
            entries,
        };
        let bytes = StorageRecord::GcPage(Box::new(page)).encode()?;
        self.put_proof_bytes(task, bytes).await
    }

    pub(in crate::gc) async fn put_proof_bytes(
        &self,
        task: &GcTask,
        bytes: Vec<u8>,
    ) -> Result<PayloadReference, CatalogError> {
        if bytes.len() > PAYLOAD_PAGE_BYTES {
            return Err(ValidationError::RecordTooLarge.into());
        }
        let reference = PayloadReference {
            catalog: task.context.catalog,
            operation: task.identity,
            digest: Sha256::digest(&bytes).into(),
            length: bytes.len(),
        };
        self.change(
            &reference.page_key(0)?,
            None,
            &StorageRecord::PayloadPage(Box::new(PayloadPage {
                reference: reference.clone(),
                index: 0,
                bytes,
            })),
        )
        .await?;
        Ok(reference)
    }

    pub(in crate::gc) async fn proof_page(
        &self,
        task: &GcTask,
        key: &[u8],
    ) -> Result<(GcPage, PayloadReference), CatalogError> {
        let payload = self.proof_payload(task, key).await?;
        let nested = IcebergKey::Catalog {
            catalog: task.context.catalog,
            scope: CatalogScope::GcPage,
            suffix: {
                let mut suffix = task.identity.as_bytes().to_vec();
                let root = crowdb_protocol::iceberg_fb::root_as_fbiceberg_record(&payload.bytes)
                    .map_err(|_| ValidationError::Record)?;
                let page = root.value_as_fbgc_page().ok_or(ValidationError::Record)?;
                suffix.push(page.kind());
                suffix.extend_from_slice(&page.sequence().to_be_bytes());
                suffix
            },
        };
        let StorageRecord::GcPage(page) = StorageRecord::decode(&nested, &payload.bytes)? else {
            return Err(ValidationError::Record.into());
        };
        Ok((*page, payload.reference))
    }

    pub(in crate::gc) async fn proof_payload(
        &self,
        task: &GcTask,
        key: &[u8],
    ) -> Result<PayloadPage, CatalogError> {
        let decoded = IcebergKey::decode(key)?;
        if !matches!(&decoded, IcebergKey::Catalog { catalog, scope: CatalogScope::OperationPayload, suffix }
            if *catalog == task.context.catalog && suffix[..16] == *task.identity.as_bytes())
        {
            return Err(ValidationError::IdentityMismatch.into());
        }
        let value = self.store.get(key).await?.ok_or(ValidationError::Record)?;
        let StorageRecord::PayloadPage(payload) = StorageRecord::decode(&decoded, &value.bytes)? else {
            return Err(ValidationError::Record.into());
        };
        if payload.index != 0
            || payload.reference.page_count() != 1
            || <[u8; 32]>::from(Sha256::digest(&payload.bytes)) != payload.reference.digest
        {
            return Err(ValidationError::Record.into());
        }
        Ok(*payload)
    }
}
