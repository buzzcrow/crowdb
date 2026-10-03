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

export interface S3Credentials { accessKey: string; secretKey: string; region: string; sessionToken: string }
export const awsEncode = (value: string): string => encodeURIComponent(value).replace(/[!'()*]/g, c => `%${c.charCodeAt(0).toString(16).toUpperCase()}`);
export const objectPath = (bucket: string, key?: string): string => `/${awsEncode(bucket)}${key === undefined ? '' : `/${key.split('/').map(awsEncode).join('/')}`}`;
const hex = (value: ArrayBuffer): string => Array.from(new Uint8Array(value), byte => byte.toString(16).padStart(2, '0')).join('');
const bytes = (value: string): Uint8Array<ArrayBuffer> => new TextEncoder().encode(value);
async function hash(value: Blob | string): Promise<string> { return hex(await crypto.subtle.digest('SHA-256', typeof value === 'string' ? bytes(value) : await value.arrayBuffer())); }
async function hmac(key: Uint8Array<ArrayBuffer>, value: string): Promise<Uint8Array<ArrayBuffer>> {
  const imported = await crypto.subtle.importKey('raw', key, { name: 'HMAC', hash: 'SHA-256' }, false, ['sign']);
  return new Uint8Array(await crypto.subtle.sign('HMAC', imported, bytes(value)));
}
export async function s3Headers(origin: string, credentials: S3Credentials, method: string, path: string, query: Record<string, string>, body: Blob | string = '', now = new Date()): Promise<{ headers: Record<string, string>; query: string }> {
  if (!credentials.accessKey || !credentials.secretKey) throw new Error('Enter S3 credentials');
  const timestamp = now.toISOString().replace(/[:-]|\.\d{3}/g, '');
  const date = timestamp.slice(0, 8);
  const payload = await hash(body);
  const canonicalQuery = Object.entries(query).map(([key, value]) => [awsEncode(key), awsEncode(value)]).sort(([ak, av], [bk, bv]) => ak < bk ? -1 : ak > bk ? 1 : av < bv ? -1 : av > bv ? 1 : 0).map(([key, value]) => `${key}=${value}`).join('&');
  const signed: Record<string, string> = { host: new URL(origin).host, 'x-amz-content-sha256': payload, 'x-amz-date': timestamp };
  if (credentials.sessionToken) signed['x-amz-security-token'] = credentials.sessionToken;
  const names = Object.keys(signed).sort();
  const canonical = [method, path, canonicalQuery, names.map(name => `${name}:${signed[name].trim()}\n`).join(''), names.join(';'), payload].join('\n');
  const scope = `${date}/${credentials.region}/s3/aws4_request`;
  const signingKey = await hmac(await hmac(await hmac(await hmac(bytes(`AWS4${credentials.secretKey}`), date), credentials.region), 's3'), 'aws4_request');
  const signature = hex((await hmac(signingKey, `AWS4-HMAC-SHA256\n${timestamp}\n${scope}\n${await hash(canonical)}`)).buffer);
  const { host: _host, ...headers } = signed;
  headers.Authorization = `AWS4-HMAC-SHA256 Credential=${credentials.accessKey}/${scope}, SignedHeaders=${names.join(';')}, Signature=${signature}`;
  return { headers, query: canonicalQuery };
}
export async function s3(origin: string, credentials: S3Credentials, method: string, path: string, query: Record<string, string> = {}, body?: Blob | string, options?: { signal?: AbortSignal; range?: string }): Promise<Response> {
  const signed = await s3Headers(origin, credentials, method, path, query, body);
  const response = await fetch(`${getApiBase()}/access/s3${path}${signed.query ? `?${signed.query}` : ''}`, {
    method, headers: { ...signed.headers, ...(options?.range ? { Range: options.range } : {}) }, body,
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
