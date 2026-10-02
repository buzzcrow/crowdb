// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
import { useCallback } from 'react';
import { Server, Database, Plus, Trash2, Activity, RotateCw, Square, HardDrive, Boxes } from 'lucide-react';
import { triggerDiskdbScan, recalcDiskdbUsage, setDiskStatus, setDiskGroupStatus, restartDiskdb, stopDiskdb, removeDiskdb, removeDiskGroup, removeDisk } from '../api';

import type { MenuTarget } from '../topology/TopologyCanvas';
import type { MenuItemOrSeparator } from '../components/ContextMenu';
import { buildStatusSubmenu } from './status';
import type { MenuContext } from './context';
export function useCapacityMenus({ readonly, managed, diskdbNodeIds, capacityUsage, requestDelete, runMutation, setDialog }: MenuContext) {
  /** Capacity view has its own menu code path — rack/node management
   * belongs to the Physical view; here only disk-group/disk operations
   * and DiskDB deploy are exposed. */
  return useCallback(
    (t: MenuTarget): MenuItemOrSeparator[] => {
      if (readonly || managed) return [];
      const items: MenuItemOrSeparator[] = [];
      const p = t.parentIds || {};

      if (t.type === 'Datacenter') {
        // The default DC is immutable — only Add Rack is offered.
        items.push({
          id: 'add-rack',
          label: 'Add Rack',
          icon: <Plus className="tw-h-4 tw-w-4" />,
          onSelect: () => setDialog((d) => ({ ...d, addRack: true })),
        });
      } else if (t.type === 'Node') {
        const nodeId = Number(t.rawId ?? t.id);
        const hasDiskdb = diskdbNodeIds.has(nodeId);
        items.push({
          id: 'add-dg',
          label: 'Add Disk Group',
          icon: <Boxes className="tw-h-4 tw-w-4" />,
          onSelect: () => setDialog((d) => ({ ...d, addDiskGroup: { nodeId } })),
        });
        if (!hasDiskdb) {
          items.push({ id: 's1', separator: true });
          items.push({
            id: 'ddb-deploy',
            label: 'Deploy DiskDB',
            icon: <HardDrive className="tw-h-4 tw-w-4" />,
            onSelect: () => setDialog((d) => ({ ...d, deployDiskdb: { nodeId } })),
          });
        }
      } else if (t.type === 'Server') {
        // Chunk-domain DDB server context menu: restart, stop, delete.
        const nodeId = Number(p.node_id);
        items.push({
          id: 'ddb-restart',
          label: 'Restart DiskDB',
          icon: <RotateCw className="tw-h-4 tw-w-4" />,
          onSelect: () => runMutation('Restart DiskDB', t.label || t.id, () => restartDiskdb(nodeId)),
        });
        items.push({
          id: 'ddb-stop',
          label: 'Stop DiskDB',
          icon: <Square className="tw-h-4 tw-w-4" />,
          onSelect: () => runMutation('Stop DiskDB', t.label || t.id, () => stopDiskdb(nodeId)),
        });
        items.push({ id: 's1', separator: true });
        items.push({
          id: 'del-ddb',
          label: 'Delete DiskDB',
          icon: <Trash2 className="tw-h-4 tw-w-4" />,
          destructive: true,
          onSelect: () => requestDelete('DiskDB', t.label || t.id, async () => {
            await runMutation('Delete DiskDB', t.label || t.id, () => removeDiskdb(nodeId));
          }),
        });
      } else if (t.type === 'DiskGroup') {
        const dgId = Number(t.rawId);
        const dgNodeId = Number(p.node_id);
        const dgRackId = Number(p.rack_id);
        items.push({
          id: 'add-disk',
          label: 'Add Disk',
          icon: <HardDrive className="tw-h-4 tw-w-4" />,
          onSelect: () => setDialog((d) => ({ ...d, addDisk: { nodeId: dgNodeId, dgId } })),
        });
        items.push({ id: 's1', separator: true });
        items.push({
          id: 'dg-change-status',
          label: 'Change Status',
          icon: <Activity className="tw-h-4 tw-w-4" />,
          submenu: buildStatusSubmenu((status) => runMutation(`Set DG ${status}`, t.label || t.id, () => setDiskGroupStatus(dgRackId, dgNodeId, dgId, status))),
        });
        items.push({ id: 's2', separator: true });
        items.push({
          id: 'assign-dg',
          label: 'Assign to DiskDB',
          icon: <Server className="tw-h-4 tw-w-4" />,
          onSelect: () => setDialog((d) => ({ ...d, assignDiskGroup: { rackId: dgRackId, nodeId: dgNodeId, dgId, dgName: t.label } })),
        });
        items.push({ id: 's3', separator: true });
        items.push({
          id: 'del-dg',
          label: 'Delete Disk Group',
          icon: <Trash2 className="tw-h-4 tw-w-4" />,
          destructive: true,
          onSelect: () => requestDelete('Disk Group', dgId, async () => {
            await runMutation('Delete Disk Group', t.label || t.id, () => removeDiskGroup(dgNodeId, dgId));
          }, 'All disks in this disk group will also be removed.'),
        });
      } else if (t.type === 'Disk') {
        const diskId = String(p.disk_id || t.rawId || t.id);
        const diskNodeId = Number(p.node_id);
        const diskDgId = Number(p.disk_group_id);
        let diskZoneCount: number | undefined;
        for (const dg of capacityUsage?.disk_groups || []) {
          const found = (dg.disks || []).find((d) => d.disk_id === diskId);
          if (found) { diskZoneCount = found.zone_count; break; }
        }
        items.push({
          id: 'ddb-compact',
          label: 'Compact Zones',
          icon: <Database className="tw-h-4 tw-w-4" />,
          onSelect: () => setDialog((d) => ({ ...d, compactZones: { diskId, zoneCount: diskZoneCount } })),
        });
        items.push({
          id: 'ddb-rebuild',
          label: 'Rebuild Bitmap',
          icon: <RotateCw className="tw-h-4 tw-w-4" />,
          onSelect: () => setDialog((d) => ({ ...d, rebuildBitmap: { diskId, zoneCount: diskZoneCount } })),
        });
        items.push({ id: 's1', separator: true });
        items.push({
          id: 'ddb-scan',
          label: 'Trigger Consistency Scan',
          icon: <Activity className="tw-h-4 tw-w-4" />,
          onSelect: () => runMutation('Trigger Consistency Scan', t.label || t.id, () => triggerDiskdbScan(diskDgId)),
        });
        items.push({
          id: 'ddb-recalc',
          label: 'Recalc Usage',
          icon: <RotateCw className="tw-h-4 tw-w-4" />,
          onSelect: () => runMutation('Recalc Usage', t.label || t.id, () => recalcDiskdbUsage(diskDgId)),
        });
        items.push({ id: 's2', separator: true });
        items.push({
          id: 'disk-change-status',
          label: 'Change Status',
          icon: <Activity className="tw-h-4 tw-w-4" />,
          submenu: buildStatusSubmenu((status) => runMutation(`Set Disk ${status}`, t.label || t.id, () => setDiskStatus(diskId, status))),
        });
        items.push({ id: 's3', separator: true });
        items.push({
          id: 'del-disk',
          label: 'Delete Disk',
          icon: <Trash2 className="tw-h-4 tw-w-4" />,
          destructive: true,
          onSelect: () => requestDelete('Disk', diskId, async () => {
            await runMutation('Delete Disk', t.label || t.id, () => removeDisk(diskNodeId, diskDgId, diskId));
          }, 'All zones on this disk will be lost.'),
        });
      }
      return items;
    },
    [readonly, managed, diskdbNodeIds, capacityUsage, requestDelete, runMutation, setDialog, buildStatusSubmenu],
  );

}
