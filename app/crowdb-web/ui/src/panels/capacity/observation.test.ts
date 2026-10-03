// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

import { expect, it } from 'vitest';
import type { CapacityUsageResponse, HardwareCapacitySummary } from '../../types';
import { observeCapacity } from './observation';

const hardware = { disk_groups: [1, 2].map(id => ({ rack_id: 1, node_id: id, disk_group_id: id, capacity_bytes: 100, disks: [{ disk_id: `disk${id}`, capacity_bytes: 100 }] })) } as HardwareCapacitySummary;
const usage = { disk_groups: [1, 2].map(id => ({ rack_id: 1, node_id: id, disk_group_id: id, capacity_bytes: 100, busy_bytes: 20, free_bytes: 70, disks: [{ disk_id: `disk${id}`, capacity_bytes: 100, busy_bytes: 20, free_bytes: 70 }] })) } as CapacityUsageResponse;

it('keeps unknown and partial usage separate from known zero and free space', () => {
  expect(observeCapacity(hardware, null)).toEqual({ capacity: 200, busy: null, free: null });
  const partial = { disk_groups: usage.disk_groups.slice(0, 1) };
  expect(observeCapacity(hardware, partial)).toEqual({ capacity: 200, busy: null, free: null });
  expect(observeCapacity(hardware, partial, { nodeId: 1 })).toEqual({ capacity: 100, busy: 20, free: 70 });
  expect(observeCapacity(hardware, partial, { dgId: 2, diskId: 'disk2' })).toEqual({ capacity: 100, busy: null, free: null });
});

it('uses reported free bytes and refuses mismatched inventory coverage', () => {
  expect(observeCapacity(hardware, usage)).toEqual({ capacity: 200, busy: 40, free: 140 });
  const changed = { disk_groups: usage.disk_groups.map(group => ({ ...group, capacity_bytes: 90 })) };
  expect(observeCapacity(hardware, changed).free).toBeNull();
  const missingDisk = { disk_groups: usage.disk_groups.map(group => ({ ...group, disks: [] })) };
  expect(observeCapacity(hardware, missingDisk).busy).toBeNull();
  expect(observeCapacity(null, usage).capacity).toBeNull();
});
