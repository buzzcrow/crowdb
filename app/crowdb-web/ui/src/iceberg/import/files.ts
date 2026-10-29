// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
import { getApiBase } from '../../api';
import { check, xml, xmlEscape, xmlText } from '../../access/native';
import { save, type ImportFile, type ImportRequest } from './storage';

export async function fileRequest(op: ImportRequest, location: string, method: string, body?: Blob | string, query: Record<string, string> = {}, signal?: AbortSignal): Promise<Response> {
  const credential = op.table?.config?.['client.refresh-credentials-endpoint'];
  if (!credential) throw new Error('Table FileIO credentials are unavailable.');
  const response = await fetch(`${getApiBase()}/access/iceberg-files`, { method, body, signal, headers: {
    'X-Crowdb-File-Credentials': credential, 'X-Crowdb-File-Location': location, 'X-Crowdb-File-Query': new URLSearchParams(query).toString(),
  } });
  await check(response); return response;
}
export const location = (op: ImportRequest, relative: string) => `${String(op.table!.metadata.location).replace(/\/$/, '')}/${relative}`;

export async function uploadFile(op: ImportRequest, input: ImportFile, file: File, progress: (value: string) => void, signal?: AbortSignal, persist: () => void = () => save(op)) {
  if (input.location) return;
  const path = location(op, `data/${input.identity}.parquet`);
  if (file.size <= 16 * 1024 * 1024 && op.table?.config?.['crowdb.native-injection'] === 'true') {
    const response = await fileRequest(op, path, 'PUT', file, { 'crowdb-native-upload': input.identity }, signal);
    const value = await response.json();
    if (typeof value.location !== 'string' || !value.location.startsWith(`${String(op.table.metadata.location).replace(/\/$/, '')}/`)) throw new Error('Native upload returned a foreign file location.');
    input.location = value.location; persist(); return;
  }
  if (file.size <= 16 * 1024 * 1024) { await fileRequest(op, path, 'PUT', file, {}, signal); input.location = path; persist(); return; }
  const partSize = 8 * 1024 * 1024;
  const count = Math.ceil(file.size / partSize);
  if (count > 10000) throw new Error('Parquet exceeds the 10000-part import limit.');
  if (!input.multipart) {
    const document = await xml(await fileRequest(op, path, 'POST', undefined, { uploads: '' }, signal));
    const id = xmlText(document, 'UploadId'); if (!id) throw new Error('Multipart creation returned no upload identity.');
    input.multipart = { id, parts: [] }; persist();
  }
  const session = input.multipart;
  for (let index = 1; index <= count; index++) {
    if (session.parts.some(value => value.number === index)) continue;
    progress(`${file.name}: uploading part ${index}/${count}`);
    const response = await fileRequest(op, path, 'PUT', file.slice((index - 1) * partSize, index * partSize), { uploadId: session.id, partNumber: String(index) }, signal);
    const etag = response.headers.get('etag'); if (!etag) throw new Error('Uploaded part has no ETag.');
    session.parts.push({ number: index, etag }); persist();
  }
  const complete = `<CompleteMultipartUpload>${session.parts.sort((a, b) => a.number - b.number).map(part => `<Part><PartNumber>${part.number}</PartNumber><ETag>${xmlEscape(part.etag)}</ETag></Part>`).join('')}</CompleteMultipartUpload>`;
  const document = await xml(await fileRequest(op, path, 'POST', complete, { uploadId: session.id }, signal));
  if (document.querySelector('Error') || !xmlText(document, 'ETag')) throw new Error(`Multipart completion failed: ${xmlText(document, 'Message')}`);
  // The normal multipart path is write-once; native single uploads return their canonical location above.
  input.location = path; persist();
}

export async function abortTransfers(op: ImportRequest) {
  for (const input of op.files) if (input.multipart && !input.location) {
    await fileRequest(op, location(op, `data/${input.identity}.parquet`), 'DELETE', undefined, { uploadId: input.multipart.id });
    delete input.multipart; save(op);
  }
}
