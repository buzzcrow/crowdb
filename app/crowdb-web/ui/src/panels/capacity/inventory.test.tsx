// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

import { cleanup, render, screen } from '@testing-library/react';
import { afterEach, expect, it } from 'vitest';
import type { CapacityUsageResponse, HardwareCapacitySummary } from '../../types';
import { ClusterView } from './ClusterView';
import { RackView } from './RackView';
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
