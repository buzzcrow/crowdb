// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
use crowdb_sdk_dataset::{v2, DatasetClient, Error};
use serde::{de::DeserializeOwned, Serialize};
use serde_json::Value;

pub(super) async fn dispatch(client: DatasetClient, name: &str, args: &str) -> Result<String, Error> {
    let max_bytes = if name == "prepare_snapshot" {
        8 * 1024 * 1024 + 64 * 1024
    } else {
        1024 * 1024
    };
    if args.len() > max_bytes {
        return Err(Error::Limit);
    }
    if name == "capabilities" {
        return encode(&client.capabilities().await?);
    }
    let value: Value = serde_json::from_str(args)?;
    let client = client.control();
    if name.starts_with("page_") {
        return pages(&client, name, &value).await;
    }
    if let Some(result) = lifecycle(&client, name, &value).await? {
        return Ok(result);
    }
    let id = || id(&value, "id");
    match name {
        "create" => encode(&client.create().await?),
        "open" => encode(&client.open().await?),
        "head" => encode(&client.head().await?),
        "update_snapshot_head" => encode(&client.update_snapshot_head(&request(&value)?).await?),
        "commit_status" => encode(&client.commit_status(&id()?).await?),
        "summary" => encode(&client.summary(&id()?).await?),
        "retain" => encode(&client.retain(&id()?).await?),
        "release" => encode(&client.release(&id()?).await?),
        "set_stable" => encode(&client.set_stable(&id()?).await?),
        "lifecycle" => encode(&client.lifecycle(&id()?).await?),
        "reclaim" => encode(&client.reclaim(&id()?).await?),
        "prepare_inspection" => encode(&client.prepare_inspection(&id()?).await?),
        "inspection_status" => encode(&client.inspection_status(&id()?).await?),
        "matrix" => encode(&client.matrix(&id()?, &request(&value)?).await?),
        "contains" => encode(
            &client
                .contains(&id()?, &serde_json::from_value(value["sample"].clone())?)
                .await?,
        ),
        "preview" => encode(
            &client
                .preview(
                    &id()?,
                    &serde_json::from_value(value["sample"].clone())?,
                    text(&value, "field")?,
                    u32::try_from(value["max_bytes"].as_u64().unwrap_or(65536)).map_err(|_| Error::Limit)?,
                )
                .await?,
        ),
        "create_upload" => encode(&client.create_upload(&request(&value)?).await?),
        "upload_status" => encode(&client.upload_status(&id()?).await?),
        "complete_upload" => encode(&client.complete_upload(&id()?).await?),
        "abort_upload" => encode(&client.abort_upload(&id()?).await?),
        "upload_file" => encode(&client.upload_file(text(&value, "path")?, &id()?).await?),
        "bind_read" => encode(&client.bind_read(&request(&value)?).await?),
        _ => Err(Error::Invalid("unknown v2 SDK operation".into())),
    }
}
async fn lifecycle(client: &v2::ControlClient, name: &str, value: &Value) -> Result<Option<String>, Error> {
    let id = || id(value, "id");
    let result = match name {
        "prepare_snapshot" => encode(&client.prepare_snapshot(&id()?, &request(value)?).await?),
        "prepare_split_revision" => encode(
            &client
                .prepare_split_revision(&id()?, &self::id(value, "revision")?, &request(value)?)
                .await?,
        ),
        "update_split_head" => encode(&client.update_split_head(&id()?, &request(value)?).await?),
        "split_head" => encode(&client.split_head(&id()?).await?),
        "split_revision" => encode(
            &client
                .split_revision(&id()?, &self::id(value, "revision")?)
                .await?,
        ),
        _ => return Ok(None),
    }?;
    Ok(Some(result))
}
async fn pages(client: &v2::ControlClient, name: &str, value: &Value) -> Result<String, Error> {
    let limit = u16::try_from(value["limit"].as_u64().unwrap_or(20)).map_err(|_| Error::Limit)?;
    let after = value["after"].as_str();
    match name {
        "page_datasets" => page(client.datasets(limit)?, after).await,
        "page_inventory" => page(client.inventory(limit)?, after).await,
        "page_roots" => page(client.roots(limit)?, after).await,
        "page_children" => page(client.children(id(value, "id")?, limit)?, after).await,
        "page_schema" => page(client.schema(id(value, "id")?, limit)?, after).await,
        "page_samples" => page(client.samples(id(value, "id")?, limit)?, after).await,
        "page_split_history" => page(client.split_history(id(value, "id")?, limit)?, after).await,
        "page_split_members" => page(client.split_members(request(value)?, limit)?, after).await,
        _ => Err(Error::Invalid("unknown v2 page".into())),
    }
}
async fn page<T: DeserializeOwned + Serialize>(
    mut pages: v2::PageCursor<T>,
    after: Option<&str>,
) -> Result<String, Error> {
    if let Some(after) = after {
        pages.resume_after(after.into())?;
    }
    encode(
        &pages
            .next_page()
            .await?
            .ok_or_else(|| Error::Invalid("missing initial page".into()))?,
    )
}
fn encode(value: &impl Serialize) -> Result<String, Error> {
    Ok(serde_json::to_string(value)?)
}
fn request<T: DeserializeOwned>(value: &Value) -> Result<T, Error> {
    Ok(serde_json::from_value(value["request"].clone())?)
}
fn text<'a>(value: &'a Value, name: &str) -> Result<&'a str, Error> {
    value[name]
        .as_str()
        .ok_or_else(|| Error::Invalid(format!("missing {name}")))
}
fn id(value: &Value, name: &str) -> Result<v2::OpaqueId, Error> {
    text(value, name)?.parse()
}
