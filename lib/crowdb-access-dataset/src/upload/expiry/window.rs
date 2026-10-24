use super::super::ownership::PartCleanupMode;
use super::super::{decode_part, decode_session, part_count, part_key};
use super::{
    cursor::{ExpiredUpload, PART_READS},
    DatasetUploads, MetadataScanBudget, ReadCancellation, UploadError, UploadExpiryStep,
};
use crate::MetadataScan;
use std::time::Duration;

impl DatasetUploads {
    pub(super) async fn expire_window(
        &self,
        work: &mut ExpiredUpload,
        report: &mut UploadExpiryStep,
        ranges: &mut usize,
    ) -> Result<bool, UploadError> {
        if work.parts.is_none() {
            let Some(stored) = self
                .store
                .get_bounded(&work.location, budget(256 * 1024), &ReadCancellation::new())
                .await?
            else {
                return Ok(true);
            };
            let record = match self.abort_record(&work.location, stored).await {
                Ok((record, _)) => record,
                Err(UploadError::Conflict | UploadError::NotFound) => return Ok(true),
                Err(error) => return Err(error),
            };
            if record.cleanup_complete {
                return Ok(true);
            }
            work.parts = Some(part_count(&record));
        }
        self.expire_parts(work, report, ranges).await
    }

    async fn expire_parts(
        &self,
        work: &mut ExpiredUpload,
        report: &mut UploadExpiryStep,
        ranges: &mut usize,
    ) -> Result<bool, UploadError> {
        let parts = work.parts.ok_or(UploadError::Integrity)?;
        let mut prefix = work.location.clone();
        prefix.extend(b"\0P");
        let page = self
            .store
            .scan_page(
                &MetadataScan {
                    prefix: prefix.clone(),
                    after: work.after.clone(),
                    budget: MetadataScanBudget {
                        items: PART_READS,
                        bytes: 1024 * 1024,
                        timeout: Duration::from_secs(2),
                    },
                },
                &ReadCancellation::new(),
            )
            .await?;
        report.part_reads += page.entries.len();
        if page.entries.is_empty() && page.after.is_some() {
            return Err(UploadError::Integrity);
        }
        for (key, stored) in page.entries {
            let part = decode_part(&stored.bytes)?;
            if !key.starts_with(&prefix)
                || part.part.index >= parts
                || key != part_key(&work.location, part.part.index)
                || work.after.as_ref().is_some_and(|after| key <= *after)
            {
                return Err(UploadError::Integrity);
            }
            if part.part.locations.len() > *ranges {
                return Ok(false);
            }
            work.clear &= self
                .cleanup_part(&key, part, ranges, PartCleanupMode::RemoveMetadata)
                .await?;
            work.after = Some(key);
        }
        if let Some(after) = page.after {
            if !after.starts_with(&prefix) || work.after.as_ref().map_or(true, |last| after < *last) {
                return Err(UploadError::Integrity);
            }
            work.after = Some(after);
            return Ok(false);
        }
        if !work.clear {
            work.after = None;
            work.clear = true;
            return Ok(false);
        }
        let Some(stored) = self
            .store
            .get_bounded(&work.location, budget(256 * 1024), &ReadCancellation::new())
            .await?
        else {
            return Ok(true);
        };
        let record = decode_session(&stored.bytes)?;
        self.finish_cleanup(&work.location, record, stored.bytes).await?;
        Ok(true)
    }
}

fn budget(bytes: usize) -> MetadataScanBudget {
    MetadataScanBudget {
        items: 1,
        bytes,
        timeout: Duration::from_secs(2),
    }
}
