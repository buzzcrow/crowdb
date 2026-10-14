// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

import { Boxes, HardDrive } from 'lucide-react';
import type { TreeNode } from '../components/Tree';
import type { CapacityUsageResponse, HardwareCapacitySummary } from '../types';
import type { NodeDiskGroups } from '../data/useCapacityTree';

interface StorageTreeInput {
  rackId: number;
  nodeId: number;
  disks?: NodeDiskGroups;
  hardwareCapacity?: HardwareCapacitySummary | null;
  capacityUsage?: CapacityUsageResponse | null;
  onLoadGroupDisks?: (nodeId: number, groupId: number) => Promise<void>;
}

/** Physical disks remain visible regardless of the current DiskDB owner. */
export function storageTree({ rackId, nodeId, disks: ndg, hardwareCapacity, capacityUsage, onLoadGroupDisks }: StorageTreeInput): TreeNode[] {
  const dgStatusByKey = new Map<string, number>();
  const diskStatusById = new Map<string, number>();
  if (hardwareCapacity?.disk_groups) {
    for (const dg of hardwareCapacity.disk_groups) {
      dgStatusByKey.set(`${dg.rack_id}:${dg.node_id}:${dg.disk_group_id}`, dg.status);
      for (const disk of dg.disks || []) {
        diskStatusById.set(disk.disk_id, disk.status);
      }
    }
  }
  if (capacityUsage?.disk_groups) {
    for (const dg of capacityUsage.disk_groups) {
      const key = `${dg.rack_id}:${dg.node_id}:${dg.disk_group_id}`;
      if (!dgStatusByKey.has(key)) dgStatusByKey.set(key, dg.status);
      for (const disk of dg.disks || []) {
        if (!diskStatusById.has(disk.disk_id)) diskStatusById.set(disk.disk_id, disk.status);
      }
    }
  }

  const children: TreeNode[] = [];
  const allDgs = ndg?.diskGroups ?? [];
  for (const dg of allDgs) {
    const disks = ndg?.disksByDg[dg.id] || [];
    const dgStatus = dgStatusByKey.get(`${rackId}:${nodeId}:${dg.id}`);
    children.push({
      expandable: !!onLoadGroupDisks,
      onExpand: () => { void onLoadGroupDisks?.(nodeId, dg.id); },
      id: `CH-DG-${nodeId}-${dg.id}`,
      rawId: dg.id,
      label: dg.name ? `${dg.name} (DG-${dg.id})` : `DG-${dg.id}`,
      type: 'DiskGroup' as const,
      icon: <Boxes className="tw-h-4 tw-w-4 tw-text-muted" />,
      hwStatus: dgStatus ?? undefined,
      parentIds: { rack_id: rackId, node_id: nodeId, disk_group_id: dg.id },
      children: disks.map((d) => ({
        id: `CH-D-${nodeId}-${dg.id}-${d.disk_id}`,
        rawId: d.disk_id,
        label: d.disk_id.slice(0, 12) + '…',
        type: 'Disk' as const,
        icon: <HardDrive className="tw-h-4 tw-w-4 tw-text-muted" />,
        hwStatus: diskStatusById.get(d.disk_id) ?? undefined,
        parentIds: { rack_id: rackId, node_id: nodeId, disk_group_id: dg.id, disk_id: d.disk_id },
      })),
    });
  }

  return children;
}
