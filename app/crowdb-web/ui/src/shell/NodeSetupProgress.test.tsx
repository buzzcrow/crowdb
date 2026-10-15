// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

import { act, fireEvent, render, screen } from '@testing-library/react';
import { afterEach, expect, it, vi } from 'vitest';
import { nodeSetupStatus, retryNodeSetup } from '../api';
import { NodeSetupProgress } from './NodeSetupProgress';
import type { NodeSetupStatus } from './nodeSetup';
vi.mock('../api', () => ({ nodeSetupStatus: vi.fn(), retryNodeSetup: vi.fn() }));
afterEach(() => { vi.useRealTimers(); vi.clearAllMocks(); });
const ready: NodeSetupStatus = { version: 1, discovery_id: 'node', operation_id: 'accepted', node_id: 1, cluster_id: 'cluster',
  phase: 'ready', storage_mode: 'diskless', services: [], disks: [], error: null };

it('reopens target-owned progress and marks failed observations stale without losing rows', async () => {
  vi.useFakeTimers();
  vi.mocked(nodeSetupStatus).mockResolvedValue(ready);
  render(<NodeSetupProgress node={1} readonly={false} />);
  await act(async () => {});
  expect(screen.getByRole('status')).toHaveTextContent('Completed');
  expect(screen.getByText(/Diskless · no disk creation/)).toBeInTheDocument();
  vi.mocked(nodeSetupStatus).mockRejectedValue(new Error('authority unavailable'));
  await act(async () => { await vi.advanceTimersByTimeAsync(2000); });
  expect(screen.getByRole('alert')).toHaveTextContent('Last confirmed progress retained');
  expect(screen.getByRole('status')).toHaveTextContent('Completed');
});

it('keeps accepted retry results when an earlier observation finishes afterward', async () => {
  vi.useFakeTimers();
  const failed: NodeSetupStatus = { ...ready, phase: 'failed', error: 'prerequisite failed' };
  vi.mocked(nodeSetupStatus).mockResolvedValueOnce(failed);
  render(<NodeSetupProgress node={1} readonly={false} />);
  await act(async () => {});
  let finishRead!: (value: NodeSetupStatus) => void;
  vi.mocked(nodeSetupStatus).mockReturnValue(new Promise(resolve => { finishRead = resolve; }));
  await act(async () => { await vi.advanceTimersByTimeAsync(2000); });
  vi.mocked(retryNodeSetup).mockResolvedValue({ ...ready, phase: 'accepted' });
  await act(async () => { fireEvent.click(screen.getByRole('button', { name: 'Retry accepted setup' })); });
  await act(async () => { finishRead(failed); });
  expect(screen.getByRole('status')).toHaveTextContent('accepted');
  expect(screen.queryByText('prerequisite failed')).toBeNull();
  expect(retryNodeSetup).toHaveBeenCalledOnce();
  expect(retryNodeSetup).toHaveBeenCalledWith(1);
});
