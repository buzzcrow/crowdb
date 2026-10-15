// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
use crowdb_sdk_dataset::{v2, DatasetClient, Error};
use serde::{de::DeserializeOwned, Serialize};
use serde_json::Value;

pub(super) async fn dispatch(client: DatasetClient, name: &str, args: &str) -> Result<String, Error> {
    if args.len() > 1024 * 1024 {
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
        "commit_status" => encode(&client.commit_status(&id()?).await?),
        "summary" => encode(&client.summary(&id()?).await?),
        "retain" => encode(&client.retain(&id()?).await?),
        "release" => encode(&client.release(&id()?).await?),
        "set_stable" => encode(&client.set_stable(&id()?).await?),
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
        "prepare_read" => encode(&client.prepare_read(&request(&value)?).await?),
        "read_status" => encode(&client.read_status(&id()?).await?),
        "close_read" => encode(&client.close_read(&id()?).await?),
        "acknowledge" => encode(&client.acknowledge(&id()?, &request(&value)?).await?),
        "progress" => encode(&client.progress(&id()?).await?),
        _ => Err(Error::Invalid("unknown v2 SDK operation".into())),
    }
}
async fn lifecycle(client: &v2::ControlClient, name: &str, value: &Value) -> Result<Option<String>, Error> {
    let id = || id(value, "id");
    let result = match name {
        "begin_composition" => encode(&client.begin_composition(&request(value)?).await?),
        "composition" => encode(&client.composition(&id()?).await?),
        "edit_composition" => encode(&client.edit_composition(&id()?, &request(value)?).await?),
        "evaluate_composition" => encode(&client.evaluate_composition(&id()?, &request(value)?).await?),
        "prepare_composition" => encode(&client.prepare_composition(&id()?, &request(value)?).await?),
        "publish_composition" => encode(&client.publish_composition(&id()?, &request(value)?).await?),
        "abandon_composition" => encode(&client.abandon_composition(&id()?, &request(value)?).await?),
        "cleanup_composition" => encode(&client.cleanup_composition(&id()?).await?),
        "begin_split" => encode(&client.begin_split(&id()?, &request(value)?).await?),
        "split_draft" => encode(&client.split_draft(&id()?).await?),
        "edit_split" => encode(&client.edit_split(&id()?, &request(value)?).await?),
        "evaluate_split" => encode(&client.evaluate_split(&id()?, &request(value)?).await?),
        "prepare_split" => encode(&client.prepare_split(&id()?, &request(value)?).await?),
        "publish_split" => encode(&client.publish_split(&id()?, &request(value)?).await?),
        "abandon_split" => encode(&client.abandon_split(&id()?, &request(value)?).await?),
        "cleanup_split" => encode(&client.cleanup_split(&id()?).await?),
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
