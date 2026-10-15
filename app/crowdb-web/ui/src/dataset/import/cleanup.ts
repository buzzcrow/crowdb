// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
import * as api from '../api';
import { saveOperation, type Operation } from '../operation';
import type { UploadStatus } from '../types';

/** Authority rejects aborts of claimed/shared uploads; process one bounded window. */
export async function cleanupUploads(operation: Operation): Promise<boolean> {
  for (const upload of (operation.uploads ?? []).slice(0, 20)) {
    const path = `${api.datasetPath(operation.dataset)}/uploads/${api.opaqueId(upload.id)}`;
    let value: UploadStatus;
    try { value = await api.mutate<UploadStatus>('uploads', path, 'DELETE'); }
    catch (reason) {
      if (!(reason instanceof api.DatasetError) || reason.status !== 409) throw reason;
      value = await api.control<UploadStatus>(path);
      if (value.state !== 'claimed') throw reason;
      // Another published owner retains these bytes after this draft was cleaned.
      operation.uploads = operation.uploads?.filter(item => item.id !== upload.id);
      saveOperation(operation); continue;
    }
    if (value.state !== 'aborted' || !value.cleanup_complete) return false;
    if (localStorage.getItem(upload.key) === upload.id) localStorage.removeItem(upload.key);
    operation.uploads = operation.uploads?.filter(item => item.id !== upload.id);
    saveOperation(operation);
  }
  return !operation.uploads?.length;
}
