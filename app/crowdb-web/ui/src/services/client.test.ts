// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
import { describe, expect, it } from 'vitest';
import { serviceInstanceLabel } from './client';

describe('service instance labels', () => {
  it('shortens Access registration IDs without changing instance identity', () => {
    expect(serviceInstanceLabel('access-server', 'access-17')).toBe('AS-17');
    expect(serviceInstanceLabel('access-server', 'access-server-17')).toBe('AS-17');
  });
  it.each([
    ['paxos-kv', 'kv-1', 'PKV-1'],
    ['paxos-kv', 'paxos-kv-1', 'PKV-1'],
    ['paxos-kv', '1', 'PKV-1'],
    ['chunk-kv', 'chunk-kv-1', 'CKV-1'],
    ['chunkdb', 'kv-1', 'CDB-kv-1'],
  ])('labels %s instance %s as %s', (kind, id, label) => {
    expect(serviceInstanceLabel(kind, id)).toBe(label);
  });
});
