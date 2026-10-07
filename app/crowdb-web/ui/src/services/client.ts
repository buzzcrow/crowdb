// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

import { getApiBase, getManagementToken } from '../api';
import { readJson } from '../access/native';

export const serviceNames = { chunkdb: 'crowdb-chunk-db', diskio: 'crowdb-disk-io', 'chunk-kv': 'crowdb-chunk-kv', 'access-server': 'crowdb-access-server', web: 'crowdb-web' } as const;
export const serviceDisplayNames = { ...serviceNames, 'paxos-kv': 'crowdb-paxos-kv', diskdb: 'crowdb-disk-db' } as const;
export type AuxiliaryKind = Exclude<keyof typeof serviceNames, 'web'>;
export function isAuxiliaryKind(value?: string): value is AuxiliaryKind { return value != null && value !== 'web' && Object.hasOwn(serviceNames, value); }
export function isTopologyServiceKind(value?: string): value is keyof typeof serviceNames { return value != null && Object.hasOwn(serviceNames, value); }
/** Compact instance labels keep Paxos-KV distinct from Chunk-KV. */
export function serviceInstanceLabel(kind: string | undefined, id: string): string {
  const prefix = { 'paxos-kv': 'PKV', diskdb: 'DDB', chunkdb: 'CDB', diskio: 'DIO', 'chunk-kv': 'CKV', 'access-server': 'AS', web: 'WEB' }[kind ?? ''];
  if (!prefix) return id;
  const suffix = id.startsWith(`${kind}-`) ? id.slice(kind!.length + 1) : id;
  return `${prefix}-${suffix}`;
}
export async function serviceRequest(path: string, method: string, body?: object): Promise<unknown> {
  const token = getManagementToken();
  return readJson(await fetch(`${getApiBase()}${path}`, {
    method, headers: { 'Content-Type': 'application/json', ...(token ? { Authorization: `Bearer ${token}` } : {}) },
    body: body ? JSON.stringify(body) : undefined,
  }));
}
