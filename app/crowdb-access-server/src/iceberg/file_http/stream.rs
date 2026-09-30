use std::fmt::Write;

use crowdb_access_iceberg::file::{FileBlockStore, FileIdentity, FileLocation, FileRecord};
use crowdb_access_s3::native_buffer::NativeBodyReceiver;
use crowdb_chunk_client::LargeWritePolicy;
use http_body_util::BodyExt;
use hyper::body::Bytes;
use hyper::body::Incoming;
use sha2::{Digest, Sha256};

use super::{
    admission_error, multipart, FileS3ErrorCode, FileTransferAdmission, FileUploadBody, FileUploadBudget,
};

#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
pub(super) async fn upload(
    blocks: &dyn FileBlockStore,
    budget: &FileUploadBudget,
    admission: &FileTransferAdmission,
    body: &mut FileUploadBody<Incoming>,
    owner: FileIdentity,
    location: FileLocation,
    declared_length: Option<u64>,
    expected_sha256: Option<[u8; 32]>,
    native_receiver: Option<&NativeBodyReceiver>,
    small_threshold_exclusive: usize,
    large_write: &LargeWritePolicy,
) -> Result<FileRecord, FileS3ErrorCode> {
    let _permit = budget.acquire().map_err(|_| FileS3ErrorCode::SlowDown)?;
    let small = declared_length
        .and_then(|length| usize::try_from(length).ok())
        .filter(|length| *length < small_threshold_exclusive);
    let handoff = native_receiver.filter(|_| small.is_none() && body.native_handoff_eligible());
    let mut writer = blocks
        .prepare_upload_writer(&location.to_string(), small, declared_length, large_write)
        .await
        .map_err(|_| FileS3ErrorCode::SlowDown)?
        .ok_or(FileS3ErrorCode::SlowDown)?;
    if let Some(receiver) = handoff {
        receiver.enable_owner_handoff();
    }
    let mut length = 0u64;
    let mut sha256 = expected_sha256.map(|_| Sha256::new());
    let target_buffer = usize::try_from(declared_length.unwrap_or(1024 * 1024))
        .unwrap_or(1024 * 1024)
        .clamp(1, 1024 * 1024);
    let mut pending = Vec::with_capacity(if handoff.is_some() { 0 } else { target_buffer });
    let transfer = async {
        while let Some(frame) = body.frame().await {
            let mut bytes = frame
                .map_err(multipart::encoding_error)?
                .into_data()
                .map_err(|_| FileS3ErrorCode::InvalidRequest)?;
            if bytes.is_empty() {
                continue;
            }
            length = length
                .checked_add(bytes.len() as u64)
                .ok_or(FileS3ErrorCode::EntityTooLarge)?;
            admission.check_bytes(length, length).map_err(admission_error)?;
            if declared_length.is_some_and(|declared| length > declared) {
                return Err(FileS3ErrorCode::InvalidRequest);
            }
            if let Some(hash) = &mut sha256 {
                hash.update(&bytes);
            }
            while !writer.require_data() && !writer.input_complete() {
                writer.wait_for_capacity().await;
            }
            if let Some(receiver) = handoff {
                if let Some(owner) = receiver.take_ready_owner() {
                    writer
                        .on_framed_data(Box::new(owner))
                        .await
                        .map_err(|_| FileS3ErrorCode::SlowDown)?;
                }
            } else {
                while !bytes.is_empty() {
                    let count = (target_buffer - pending.len()).min(bytes.len());
                    pending.extend_from_slice(&bytes.split_to(count));
                    if pending.len() == target_buffer {
                        writer
                            .on_data(Bytes::from(std::mem::replace(
                                &mut pending,
                                Vec::with_capacity(target_buffer),
                            )))
                            .await
                            .map_err(|_| FileS3ErrorCode::SlowDown)?;
                    }
                }
            }
        }
        if let Some(receiver) = handoff {
            if let Some(owner) = receiver
                .finish_owner_when_ready()
                .await
                .map_err(|_| FileS3ErrorCode::InvalidRequest)?
            {
                writer
                    .on_framed_data(Box::new(owner))
                    .await
                    .map_err(|_| FileS3ErrorCode::SlowDown)?;
            }
        }
        if !pending.is_empty() {
            writer
                .on_data(Bytes::from(pending))
                .await
                .map_err(|_| FileS3ErrorCode::SlowDown)?;
        }
        if declared_length.is_some_and(|declared| declared != length)
            || expected_sha256
                .zip(sha256)
                .is_some_and(|(expected, hash)| <[u8; 32]>::from(hash.finalize()) != expected)
        {
            return Err(FileS3ErrorCode::InvalidRequest);
        }
        writer.on_finish().await.map_err(|_| FileS3ErrorCode::SlowDown)
    }
    .await;
    let locations = match transfer {
        Ok(locations) => locations,
        Err(error) => {
            let _ = writer.on_error().await;
            return Err(error);
        }
    };
    let mut etag = String::with_capacity(32);
    for byte in body.md5() {
        write!(&mut etag, "{byte:02x}").expect("string write cannot fail");
    }
    FileRecord::from_uploaded_locations(owner.file, location, &locations, length, etag)
        .map_err(|_| FileS3ErrorCode::InternalError)
}
