// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

import { s3, objectPath, xml, xmlText, xmlEscape, type S3Credentials } from './native';

export async function uploadObject(origin: string, credentials: S3Credentials, bucket: string, key: string, file: File, signal: AbortSignal, progress: (bytes: number, uploadId: string | null) => void): Promise<void> {
  const path = objectPath(bucket, key);
  const partSize = 8 * 1024 * 1024;
  if (file.size <= partSize) {
    await s3(origin, credentials, 'PUT', path, {}, file, { signal });
    progress(file.size, null);
    return;
  }
  if (Math.ceil(file.size / partSize) > 10_000) throw new Error('Upload exceeds 10,000 parts');
  const created = await xml(await s3(origin, credentials, 'POST', path, { uploads: '' }, '', { signal }));
  const uploadId = xmlText(created, 'UploadId');
  if (!uploadId) throw new Error('Multipart initiation returned no UploadId');
  progress(0, uploadId);
  const parts: string[] = [];
  for (let offset = 0, number = 1; offset < file.size; offset += partSize, number++) {
    const response = await s3(origin, credentials, 'PUT', path, { uploadId, partNumber: String(number) }, file.slice(offset, offset + partSize), { signal });
    const etag = response.headers.get('etag');
    if (!etag) throw new Error('Part response has no ETag; inspect multipart state');
    parts.push(`<Part><PartNumber>${number}</PartNumber><ETag>${xmlEscape(etag)}</ETag></Part>`);
    progress(Math.min(offset + partSize, file.size), uploadId);
  }
  const completed = await s3(origin, credentials, 'POST', path, { uploadId }, `<CompleteMultipartUpload>${parts.join('')}</CompleteMultipartUpload>`, { signal });
  const body = await xml(completed);
  if (body.querySelector('Error')) throw new Error(xmlText(body, 'Message') || 'Completion failed; inspect multipart state');
  progress(file.size, null);
}

export async function downloadObject(origin: string, credentials: S3Credentials, bucket: string, key: string): Promise<void> {
  const picker = (window as unknown as { showSaveFilePicker?: (options: object) => Promise<{ createWritable: () => Promise<WritableStream> }> }).showSaveFilePicker;
  if (picker) {
    const handle = await picker({ suggestedName: key.split('/').pop() || 'object' });
    const response = await s3(origin, credentials, 'GET', objectPath(bucket, key));
    if (!response.body) throw new Error('Download has no body');
    await response.body.pipeTo(await handle.createWritable());
    return;
  }
  const head = await s3(origin, credentials, 'HEAD', objectPath(bucket, key));
  const length = head.headers.get('content-length'); const size = Number(length);
  if (length === null || !Number.isFinite(size) || size > 16 * 1024 * 1024) throw new Error('Use a browser supporting streamed file downloads for objects larger than 16 MiB');
  const response = await s3(origin, credentials, 'GET', objectPath(bucket, key));
  const url = URL.createObjectURL(await response.blob());
  const link = document.createElement('a');
  link.href = url; link.download = key.split('/').pop() || 'object'; link.click();
  URL.revokeObjectURL(url);
}
