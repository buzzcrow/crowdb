// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

import { parseIcebergJson } from '../iceberg/json';
import { getApiBase } from '../api';

export interface Connections { iceberg: string | null; iceberg_ready: boolean; s3: string | null; configurable: boolean; max_request_bytes: number }
export async function connections(signal?: AbortSignal): Promise<Connections> {
  return readJson(await fetch(`${getApiBase()}/access/connections`, { signal }));
}
export async function configure(protocol: 'iceberg' | 's3', origin: string): Promise<Connections> {
  return readJson(await fetch(`${getApiBase()}/access/connections`, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ protocol, origin }) }));
}
export async function readJson<T>(response: Response): Promise<T> {
  await check(response);
  return response.status === 204 ? undefined as T : response.json();
}
export async function check(response: Response): Promise<void> {
  if (response.ok) return;
  const text = new TextDecoder().decode(await previewBytes(response));
  let message = text;
  try {
    const body = JSON.parse(text);
    message = body.error?.message ?? body.error ?? body.message ?? text;
  } catch {
    const xml = new DOMParser().parseFromString(text, 'application/xml');
    message = xml.querySelector('Message')?.textContent ?? text;
  }
  throw new Error(`HTTP ${response.status}: ${message || response.statusText}`);
}
export async function iceberg(path: string, token: string, method = 'GET', body?: object, signal?: AbortSignal): Promise<any> {
  const response = await fetch(`${getApiBase()}/access/iceberg${path}`, {
    method, signal, headers: { ...(token ? { Authorization: `Bearer ${token}` } : {}), ...(body ? { 'Content-Type': 'application/json' } : {}) },
    ...(body ? { body: JSON.stringify(body) } : {}),
  });
  await check(response);
  return response.status === 204 ? undefined : parseIcebergJson(await boundedText(response));
}

async function boundedText(response: Response): Promise<string> {
  const limit = 4 * 1024 * 1024;
  const reader = response.body?.getReader();
  if (!reader) return '';
  const decoder = new TextDecoder();
  let bytes = 0; let text = '';
  try {
    for (;;) {
      const chunk = await reader.read(); if (chunk.done) break;
      bytes += chunk.value.byteLength;
      if (bytes > limit) throw new Error('Metadata response exceeds the 4 MiB budget. Select a smaller page.');
      text += decoder.decode(chunk.value, { stream: true });
    }
    return text + decoder.decode();
  } finally { await reader.cancel(); }
}

export const awsEncode = (value: string): string => encodeURIComponent(value).replace(/[!'()*]/g, c => `%${c.charCodeAt(0).toString(16).toUpperCase()}`);
export const objectPath = (bucket: string, key?: string): string => `/${awsEncode(bucket)}${key === undefined ? '' : `/${key.split('/').map(awsEncode).join('/')}`}`;
export async function s3(method: string, path: string, query: Record<string, string> = {}, body?: Blob | string, options?: { signal?: AbortSignal; range?: string }): Promise<Response> {
  const encoded = Object.entries(query).map(([name, value]) => `${awsEncode(name)}=${awsEncode(value)}`).join('&');
  const response = await fetch(`${getApiBase()}/access/s3${path}${encoded ? `?${encoded}` : ''}`, {
    method, headers: { ...(options?.range ? { Range: options.range } : {}) }, body,
    signal: options?.signal,
  });
  await check(response);
  return response;
}
export async function xml(response: Response): Promise<Document> {
  const document = new DOMParser().parseFromString(await boundedText(response), 'application/xml');
  if (document.querySelector('parsererror')) throw new Error('Invalid S3 XML response');
  return document;
}
export async function previewBytes(response: Response, limit = 4096): Promise<Uint8Array> {
  if (!response.body) return new Uint8Array();
  const reader = response.body.getReader(); const chunks: Uint8Array[] = []; let size = 0;
  try {
    while (size < limit) {
      const result = await reader.read(); if (result.done) break;
      const chunk = result.value.slice(0, limit - size); chunks.push(chunk); size += chunk.length;
    }
  } finally { await reader.cancel(); }
  const result = new Uint8Array(size); let offset = 0;
  for (const chunk of chunks) { result.set(chunk, offset); offset += chunk.length; }
  return result;
}
export const xmlText = (element: ParentNode, tag: string): string => element.querySelector(tag)?.textContent ?? '';
export const xmlEscape = (value: string): string => value.replace(/[<>&"']/g, character => ({ '<': '&lt;', '>': '&gt;', '&': '&amp;', '"': '&quot;', "'": '&apos;' })[character]!);
