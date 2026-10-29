// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
import { useEffect, useRef, useState } from 'react';
import * as api from '../api';
import type { UploadStatus } from '../types';
import type { useDraft } from '../operation/useDraft';
import { newId, upload } from '../import/upload';
import { cell, limits, setCell } from './model';
import { ensureDataset } from './createDataset';

export async function checkUpload(dataset: string, datasetId: string | undefined, id: string, length: string, md5: string, signal?: AbortSignal) {
  const status = await api.control<UploadStatus>(`${api.datasetPath(dataset)}/uploads/${id}`, 'GET', undefined, signal, datasetId);
  if (status.id !== id || !['complete', 'claimed'].includes(status.state) || status.length !== length || status.md5 !== md5
    || status.state !== 'claimed' && BigInt(status.expires_at) <= BigInt(Math.floor(Date.now() / 1000))) throw new Error('Upload is incomplete or expired. Reselect this file.');
  return status;
}
export function useUploads(draft: ReturnType<typeof useDraft>) {
  const controller = useRef<AbortController>(); const [busy, setBusy] = useState(false); const [progress, setProgress] = useState('');
  useEffect(() => () => controller.current?.abort(), []);
  const transfer = async (files: Array<{ id: string; field: string; file: File }>) => {
    if (busy) throw new Error('Wait for the current upload to finish.');
    if (files.length > limits.batch) throw new Error(`Choose at most ${limits.batch} files at once.`);
    if (files.some(({ file }) => !file.size || file.size > limits.fileBytes)) throw new Error('Files must be nonempty and at most 100 MiB.');
    setBusy(true); const request = new AbortController(); controller.current = request;
    try {
      await ensureDataset(draft, request.signal);
      for (const { id, field, file } of files) {
        const token = newId();
        draft.change(op => ({ ...setCell(op, id, field, { kind: 'file', file: token }), uploads: { ...op.uploads, [token]: { name: file.name, size: file.size, state: 'pending' } } }));
        await draft.saving.current;
        try {
          const op = draft.current.current!;
          const value = await upload(op.dataset, file, setProgress, request.signal, async (id, key) => {
            draft.change(current => ({ ...current, uploads: { ...current.uploads, [token]: { ...current.uploads![token], id, md5: key.slice(-32) } } }));
            await draft.saving.current;
          }, op.datasetId);
          const status = await checkUpload(op.dataset, op.datasetId, value.id, value.length, value.md5, request.signal);
          draft.change(current => {
            const selected = cell(current, id, field);
            const next = selected?.kind === 'file' && selected.file === token ? setCell(current, id, field, { kind: 'upload', ...value }) : current;
            return { ...next, uploads: { ...next.uploads, [token]: { name: file.name, size: file.size, id: value.id, md5: value.md5, expiresAt: status.expires_at, state: 'ready' } } };
          });
        } catch (reason) {
          draft.change(op => ({ ...op, uploads: { ...op.uploads, [token]: { ...op.uploads![token], state: 'failed', error: String(reason) } } }));
          throw reason;
        }
      }
      await draft.saving.current;
    } finally { setBusy(false); setProgress(''); controller.current = undefined; }
  };
  return { transfer, busy, progress, cancel: () => controller.current?.abort() };
}
