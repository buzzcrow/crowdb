use super::{bad_request, bounds, Result, Selection};
use hmac::{Hmac, Mac};
use serde::{Deserialize, Serialize};
use sha2::Sha256;

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub(super) struct Cursor {
    pub block: u64,
    pub skip: u64,
    pub row_id: Option<i64>,
    pub index: usize,
}
#[derive(Serialize, Deserialize)]
struct Claim {
    metadata: String,
    snapshot: String,
    location: String,
    file_identity: String,
    cursor: Cursor,
}
impl Selection<'_> {
    pub(super) fn sign_cursor(
        &self,
        record: &crowdb_access_iceberg::file::FileRecord,
        cursor: Cursor,
    ) -> Result<String> {
        let claim = Claim {
            metadata: self.generation.clone(),
            snapshot: self.snapshot["snapshot-id"].to_string(),
            location: record.location.to_string(),
            file_identity: record.file.to_string(),
            cursor,
        };
        let bytes = serde_json::to_vec(&claim).map_err(|_| bad_request())?;
        if bytes.len() > 8192 {
            return Err(bounds());
        }
        let mut mac =
            Hmac::<Sha256>::new_from_slice(&self.service.inspection_key).map_err(|_| bad_request())?;
        mac.update(b"crowdb-inspection-cursor-v1");
        mac.update(&bytes);
        Ok(format!(
            "c.{}.{}",
            hex::encode(&bytes),
            hex::encode(mac.finalize().into_bytes())
        ))
    }
    pub(super) fn cursor(&self, record: &crowdb_access_iceberg::file::FileRecord) -> Result<Cursor> {
        let Some(token) = &self.cursor_token else {
            if self.offset != 0 {
                return Err(bad_request());
            }
            return Ok(Cursor::default());
        };
        if token.len() > 17000 {
            return Err(bad_request());
        }
        let (payload, signature) = token
            .strip_prefix("c.")
            .and_then(|s| s.split_once('.'))
            .ok_or_else(bad_request)?;
        let bytes = hex::decode(payload).map_err(|_| bad_request())?;
        let mut mac =
            Hmac::<Sha256>::new_from_slice(&self.service.inspection_key).map_err(|_| bad_request())?;
        mac.update(b"crowdb-inspection-cursor-v1");
        mac.update(&bytes);
        mac.verify_slice(&hex::decode(signature).map_err(|_| bad_request())?)
            .map_err(|_| bad_request())?;
        let claim: Claim = serde_json::from_slice(&bytes).map_err(|_| bad_request())?;
        if claim.metadata != self.generation
            || claim.snapshot.parse::<i64>().ok() != self.snapshot["snapshot-id"].as_i64()
            || claim.location != record.location.to_string()
            || claim.file_identity != record.file.to_string()
            || claim.cursor.skip > 100_000
        {
            return Err(bad_request());
        }
        Ok(claim.cursor)
    }
}
