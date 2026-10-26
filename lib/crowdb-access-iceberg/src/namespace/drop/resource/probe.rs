// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crate::{
    catalog::{CatalogContext, CatalogError, StoreError},
    error::ValidationError,
    key::CatalogScope,
    namespace::{child_range, ChildScan, NamespaceDropper},
};

use super::DropBinding;

mod children;

pub(super) enum DropProbe {
    Empty,
    Nonempty,
    Restart,
}

impl NamespaceDropper {
    pub(super) async fn probe_resource_drop(
        &self,
        context: CatalogContext,
        binding: &DropBinding,
        budget: &mut usize,
    ) -> Result<DropProbe, CatalogError> {
        for scope in [CatalogScope::NamespaceName, CatalogScope::TableName] {
            let result = self.probe_drop_range(context, binding, scope, budget).await?;
            if !matches!(result, DropProbe::Empty) {
                return Ok(result);
            }
        }
        Ok(DropProbe::Empty)
    }

    async fn probe_drop_range(
        &self,
        context: CatalogContext,
        binding: &DropBinding,
        scope: CatalogScope,
        budget: &mut usize,
    ) -> Result<DropProbe, CatalogError> {
        let range = child_range(context.catalog, Some(binding.mapping.namespace), scope)?;
        let mut scan = ChildScan {
            catalog: context.catalog,
            parent: Some(binding.mapping.namespace),
            scope,
            limit: 16,
            continuation: None,
        };
        let mut previous = Vec::new();
        while *budget > 0 {
            *budget -= 1;
            let page = self.creator.names.scan_children(scan.clone()).await?;
            if let Some(failure) = page.terminal_failure {
                return Err(StoreError::Rejected(failure).into());
            }
            if page.items.len() > scan.limit {
                return Err(ValidationError::RecordTooLarge.into());
            }
            let mut last = previous.clone();
            for item in &page.items {
                if !range.contains(&item.key) || item.key <= last {
                    return Err(ValidationError::Key.into());
                }
                last.clone_from(&item.key);
                let result = if scope == CatalogScope::NamespaceName {
                    self.inspect_drop_namespace(context, binding, &item.key, &item.value, budget)
                        .await?
                } else {
                    self.inspect_drop_table(context, binding, &item.key, &item.value, budget)
                        .await?
                };
                if !matches!(result, DropProbe::Empty) {
                    return Ok(result);
                }
            }
            let Some(continuation) = page.continuation else {
                return Ok(DropProbe::Empty);
            };
            if continuation.last_key < last || continuation.last_key <= previous {
                return Err(ValidationError::Key.into());
            }
            scan.continuation = Some(continuation);
            scan.request()?;
            previous = scan
                .continuation
                .as_ref()
                .ok_or(ValidationError::Key)?
                .last_key
                .clone();
        }
        Err(CatalogError::Busy)
    }
}
