// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
import { describe, expect, it, vi } from 'vitest';
import { serviceLifecycle } from './serviceLifecycle';
import type { MenuContext } from './context';
import type { ServerSummary } from '../api';
import type { MenuItem } from '../components/ContextMenu';
import { restartServer, stopServer, removeServer } from '../api';
import { serviceRequest } from '../services/client';
vi.mock('../api', () => ({ restartServer: vi.fn(), stopServer: vi.fn(), removeServer: vi.fn(), restartDiskdb: vi.fn(), stopDiskdb: vi.fn(), removeDiskdb: vi.fn() }));
vi.mock('../services/client', () => ({ serviceDisplayNames: { 'paxos-kv': 'crowdb-paxos-kv' }, serviceRequest: vi.fn() }));
describe('service lifecycle routing', () => {
  it('routes canonical Paxos-KV actions through the node lifecycle endpoints', async () => {
    const run: MenuContext['runMutation'] = async (_action, _target, operation) => { await operation(); };
    const remove: MenuContext['requestDelete'] = (_type, _id, operation) => { void operation(); };
    const items = serviceLifecycle({ id: 'kv-server-7', node_id: 7, service_type: 'paxos-kv', pid: 123 } as ServerSummary, run, remove) as MenuItem[];
    for (const item of items) await item.onSelect?.();
    expect(restartServer).toHaveBeenCalledWith(7);
    expect(stopServer).toHaveBeenCalledWith(7);
    expect(removeServer).toHaveBeenCalledWith(7);
    expect(serviceRequest).not.toHaveBeenCalled();
  });
});
