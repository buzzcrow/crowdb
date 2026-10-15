// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

import * as api from '../api';
import type { UploadStatus } from '../types';
import { randomUUID } from '../../utils/randomUUID';
import { digest } from './digest';
import { getApiBase } from '../../api';
export const newId = () => randomUUID().replaceAll('-', '');
const partSize = 4 * 1024 * 1024;
export async function upload(dataset: string, file: File, progress: (text: string) => void, signal?: AbortSignal): Promise<string> {
  if (Math.ceil(file.size / partSize) > 10000) throw new Error('File exceeds 10000 upload parts.');
  const md5 = await digest(file, signal, count => progress(`${file.name}: checksum ${count}/${file.size}`));
  const key = `crowdb.dataset.upload:${getApiBase()}:${dataset}:${file.size}:${md5}`;
  const saved = localStorage.getItem(key);
  const id = saved ?? newId();
  localStorage.setItem(key, id);
  const path = `${api.datasetPath(dataset)}/uploads/${id}`;
  let status: UploadStatus;
  try { status = await api.control<UploadStatus>(path, 'GET', undefined, signal); }
  catch (reason) {
    if (!(reason instanceof api.DatasetError) || reason.status !== 404) throw reason;
    status = await api.mutate<UploadStatus>('uploads', `${api.datasetPath(dataset)}/uploads`, 'POST', { id, length: String(file.size), part_size: partSize, md5 }, signal);
  }
  if (status.length !== String(file.size) || status.md5 !== md5 || status.part_size !== partSize) throw new Error('Reselected file does not match upload.');
  if (status.state === 'complete' || status.state === 'claimed') return id;
  if (status.state !== 'open') throw new Error('Upload was aborted. Start a new upload explicitly.');
  const received = new Set(status.parts.map(part => part.index));
  const count = Math.ceil(file.size / partSize);
  // Two requests at a time, with at most two Blob slices and their transport buffers.
  for (let offset = 0; offset < count; offset += 2) {
    await Promise.all(Array.from({ length: Math.min(2, count - offset) }, async (_, position) => {
      const index = offset + position; if (received.has(index)) return;
      signal?.throwIfAborted();
      await api.mutate('uploads', `${path}/parts/${index}`, 'PUT', file.slice(index * partSize, Math.min((index + 1) * partSize, file.size)), signal);
    }));
    progress(`${file.name}: uploaded ${Math.min(offset + 2, count)}/${count} parts`);
  }
  status = await api.mutate<UploadStatus>('uploads', `${path}/complete`, 'POST', undefined, signal);
  if (status.state !== 'complete' && status.state !== 'claimed') throw new Error('Upload did not complete. Inspect status before retrying.');
  progress(`${file.name}: complete`); return id;
}
