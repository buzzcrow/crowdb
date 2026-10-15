// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

import { expect, it, vi } from 'vitest';
import { storageLifecycleMenu } from './storage';
import type { MenuTarget } from '../topology/TopologyCanvas';

it('routes managed disks to owner lifecycle while retaining diagnostic actions and legacy menus', () => {
  const items = ['ddb-scan', 'disk-change-status', 'del-disk'].map(id => ({ id, label: id, onSelect: vi.fn() }));
  const target = { type: 'Disk', id: 'disk', parentIds: { node_id: 7 } } as MenuTarget;
  expect(storageLifecycleMenu(items, target)).toBe(items);
  const open = vi.fn();
  const managed = storageLifecycleMenu(items, target, open);
  expect(managed.map(item => item.id)).toEqual(['manage-node-storage', 'ddb-scan']);
  const entry = managed[0];
  if (!('separator' in entry)) { expect(entry.onSelect).toBeTypeOf('function'); entry.onSelect?.(); }
  expect(open).toHaveBeenCalledWith(7);
});
