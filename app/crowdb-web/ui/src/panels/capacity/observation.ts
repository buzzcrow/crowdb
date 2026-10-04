// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

import type { CapacityUsageResponse, HardwareCapacitySummary, DiskInfoDto } from '../../types';

export interface CapacityScope { rackId?: number; nodeId?: number; dgId?: number; diskId?: string }
export const diskKey = (id: string) => id.replace(/-/g, '').toLowerCase();
const valid = (capacity: number, busy: number, free: number) =>
  [capacity, busy, free].every(value => Number.isFinite(value) && value >= 0) && busy + free <= capacity;

// Usage capacity excludes reserved zone space; allocation geometry identifies the disk.
const physicalCapacity = (disk: DiskInfoDto) => disk.capacity_units > 0 && disk.unit_size_bytes > 0
  ? disk.capacity_units * disk.unit_size_bytes : disk.capacity_bytes;

/** Hardware inventory defines coverage; missing or mismatched usage stays unknown. */
export function observeCapacity(hardware: HardwareCapacitySummary | null | undefined, usage: CapacityUsageResponse | null, scope: CapacityScope = {}) {
  const matches = (group: { rack_id: number; node_id: number; disk_group_id: number }) =>
    (scope.rackId === undefined || scope.rackId === group.rack_id) &&
    (scope.nodeId === undefined || scope.nodeId === group.node_id) &&
    (scope.dgId === undefined || scope.dgId === group.disk_group_id);
  const groups = hardware?.disk_groups.filter(matches);
  const reports = usage?.disk_groups.filter(matches) ?? [];
  if (scope.diskId !== undefined) {
    const key = diskKey(scope.diskId);
    const disk = groups?.flatMap(group => group.disks).find(disk => diskKey(disk.disk_id) === key);
    const report = reports.flatMap(group => group.disks).find(disk => diskKey(disk.disk_id) === key);
    const capacity = disk?.capacity_bytes ?? report?.capacity_bytes ?? null;
    const known = !!report && capacity === physicalCapacity(report) && valid(report.capacity_bytes, report.busy_bytes, report.free_bytes) && report.capacity_bytes <= capacity;
    return { capacity, busy: known ? report.busy_bytes : null, free: known ? report.free_bytes : null };
  }
  const inventoryMatches = !!groups;
  const capacity = groups && (groups.length > 0 || reports.length === 0) ? groups.reduce((sum, group) => sum + group.capacity_bytes, 0) : null;
  const complete = inventoryMatches && !!usage && groups!.every(group => {
    const report = reports.find(report => report.disk_group_id === group.disk_group_id && report.node_id === group.node_id);
    return !!report && report.disks.reduce((sum, disk) => sum + physicalCapacity(disk), 0) === group.capacity_bytes && valid(report.capacity_bytes, report.busy_bytes, report.free_bytes) && report.capacity_bytes <= group.capacity_bytes &&
      group.disks.every(disk => report.disks.some(observed => diskKey(disk.disk_id) === diskKey(observed.disk_id) && physicalCapacity(observed) === disk.capacity_bytes));
  });
  const covered = groups?.map(group => reports.find(report => report.disk_group_id === group.disk_group_id && report.node_id === group.node_id)!) ?? [];
  return {
    capacity,
    busy: complete ? covered.reduce((sum, group) => sum + group.busy_bytes, 0) : null,
    free: complete ? covered.reduce((sum, group) => sum + group.free_bytes, 0) : null,
  };
}
