use super::{
    is_session_key, DatasetUploads, MetadataScan, MetadataScanBudget, ReadCancellation, UploadError,
    UploadExpiryCursor, UploadState, PREFIX,
};
use std::time::Duration;

impl DatasetUploads {
    pub(super) async fn discover_expired(
        &self,
        now: u64,
        cursor: &mut UploadExpiryCursor,
    ) -> Result<usize, UploadError> {
        if cursor.full() {
            return Ok(0);
        }
        let page = self
            .store
            .scan_page(
                &MetadataScan {
                    prefix: PREFIX.to_vec(),
                    after: cursor.after.clone(),
                    budget: MetadataScanBudget {
                        items: 20,
                        bytes: 1024 * 1024,
                        timeout: Duration::from_secs(2),
                    },
                },
                &ReadCancellation::new(),
            )
            .await?;
        let scanned = page.entries.len();
        for (key, stored) in page.entries {
            if cursor.full() {
                return Ok(scanned);
            }
            if cursor.after.as_ref().is_some_and(|after| key <= *after) {
                continue;
            }
            cursor.after = Some(key.clone());
            if !is_session_key(&key)? {
                continue;
            }
            // Upload children append a zero separator; seek beyond that subtree
            // instead of spending discovery pages on all of a large upload's parts.
            let mut end = key.clone();
            end.push(0xff);
            cursor.after = Some(end);
            if stored.bytes == crate::authority::dataset_lifecycle::RETIRED_RESOURCE {
                continue;
            }
            let session = super::decode_session(&stored.bytes)?;
            if !session.cleanup_complete
                && session.owners.is_empty()
                && (session.state == UploadState::Aborted || now >= session.expires_at)
            {
                cursor.enqueue(key);
            }
        }
        if page.after.is_none() {
            cursor.after = None;
        }
        Ok(scanned)
    }
}
