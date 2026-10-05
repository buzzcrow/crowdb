// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

import { cleanup, render, screen } from '@testing-library/react';
import { afterEach, expect, it } from 'vitest';
import type { CapacityUsageResponse, HardwareCapacitySummary } from '../../types';
import { ClusterView } from './ClusterView';
import { RackView } from './RackView';
import { DiskGroupView } from './DiskGroupView';
import { NodeView } from './NodeView';

afterEach(cleanup);
const hardware = { racks: [{ rack_id: 1, node_count: 1 }], nodes: [{ rack_id: 1, node_id: 1 }],
  disk_groups: [{ rack_id: 1, node_id: 1, disk_group_id: 1, capacity_bytes: 100, disks: [] }] } as unknown as HardwareCapacitySummary;
const usage = { disk_groups: [{ rack_id: 1, node_id: 1, disk_group_id: 99, capacity_bytes: 100, disks: [] },
  { rack_id: 99, node_id: 99, disk_group_id: 98, capacity_bytes: 100, disks: [] }] } as unknown as CapacityUsageResponse;

it('uses current hardware membership despite deleted groups in an old usage report', () => {
  render(<ClusterView hardwareCapacity={hardware} usage={usage} onSelectRack={() => {}} />);
  expect(screen.getAllByRole('button')).toHaveLength(1);
  expect(screen.getByRole('button')).toHaveTextContent('1 DG(s) · 1 node(s)');
  cleanup();
  render(<RackView rackId={1} hardwareCapacity={hardware} usage={usage} onSelectNode={() => {}} />);
  expect(screen.getAllByRole('button')).toHaveLength(1);
  expect(screen.getByRole('button')).toHaveTextContent('1 DG(s)');
  cleanup();
  render(<NodeView nodeId={1} hardwareCapacity={hardware} usage={usage} onSelectDg={() => {}} />);
  expect(screen.getAllByRole('button')).toHaveLength(1);
  expect(screen.getByRole('button')).toHaveTextContent('DG-1');
  expect(screen.getByRole('button')).toHaveTextContent('Unknown');
});


it('keeps hardware status and physical capacity ahead of older usage and removes deleted disks', () => {
  const current = { ...hardware, disk_groups: [{ ...hardware.disk_groups[0], disks: [
    { disk_id: 'disk1', disk_type: 0, status: 2, zone_count: 10, capacity_bytes: 100, unit_size_bytes: 10 },
  ] }] } as HardwareCapacitySummary;
  const previous = { disk_groups: [{ ...current.disk_groups[0], capacity_bytes: 90, busy_bytes: 20, free_bytes: 70,
    disks: [{ ...current.disk_groups[0].disks[0], status: 1, capacity_bytes: 90,
      capacity_units: 10, busy_bytes: 20, free_bytes: 70 }] }] } as unknown as CapacityUsageResponse;
  render(<DiskGroupView dgId={1} hardwareCapacity={current} usage={previous} onSelectDisk={() => {}} />);
  expect(screen.getByRole('button')).toHaveTextContent('Maintenance');
  expect(screen.getByRole('button')).toHaveAttribute('title', expect.stringContaining('100.0 B'));
  cleanup();
  render(<DiskGroupView dgId={1} hardwareCapacity={{ ...current, disk_groups: [] }} usage={previous} onSelectDisk={() => {}} />);
  expect(screen.queryByRole('button')).not.toBeInTheDocument();
  expect(screen.getByText('No disks in DG-1.')).toBeVisible();
});
