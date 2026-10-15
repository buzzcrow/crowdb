// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

import type { MenuItemOrSeparator } from '../components/ContextMenu';
import type { MenuTarget } from '../topology/TopologyCanvas';

/** Managed physical lifecycle belongs to the owner-verified Node workspace. */
export function storageLifecycleMenu(items: MenuItemOrSeparator[], target: MenuTarget, open?: (node: number) => void): MenuItemOrSeparator[] {
  if (!open || !['Disk', 'DiskGroup'].includes(target.type)) return items;
  const node = Number(target.parentIds?.node_id);
  return [{ id: 'manage-node-storage', label: 'Manage node storage', onSelect: () => open(node) },
    ...items.filter(item => !('separator' in item) && !['del-disk', 'disk-status', 'disk-change-status', 'dg-status', 'dg-change-status'].includes(item.id))];
}
