// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

import { getApiBase, getManagementToken } from '../api';
import { readJson } from '../access/native';

export const serviceNames = { chunkdb: 'CDB (ChunkDB)', diskio: 'DiskIO', 'chunk-kv': 'Chunk-KV', 'access-server': 'Access Server' } as const;
export type AuxiliaryKind = keyof typeof serviceNames;
export function isAuxiliaryKind(value?: string): value is AuxiliaryKind { return value != null && Object.hasOwn(serviceNames, value); }
export async function serviceRequest(path: string, method: string, body?: object): Promise<unknown> {
  const token = getManagementToken();
  return readJson(await fetch(`${getApiBase()}${path}`, {
    method, headers: { 'Content-Type': 'application/json', ...(token ? { Authorization: `Bearer ${token}` } : {}) },
    body: body ? JSON.stringify(body) : undefined,
  }));
}
