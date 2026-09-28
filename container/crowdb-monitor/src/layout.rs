// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::path::{Component, Path};

pub(crate) fn is_clean_absolute(path: &Path) -> bool {
    path.is_absolute()
        && path
            .components()
            .all(|component| !matches!(component, Component::CurDir | Component::ParentDir))
}

pub(crate) fn is_strict_descendant(root: &Path, path: &Path) -> bool {
    is_clean_absolute(root) && is_clean_absolute(path) && path != root && path.starts_with(root)
}
