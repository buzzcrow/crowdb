// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

import { act, render, screen, waitFor } from '@testing-library/react';
import { afterEach, expect, it, vi } from 'vitest';
import { listNodeCandidates, nodeAdmissions, nodeDeploymentStatus } from '../api';
import { CandidateNodes } from './CandidateNodes';

vi.mock('../api', () => ({ listNodeCandidates: vi.fn(), nodeAdmissions: vi.fn(),
  nodeDeploymentStatus: vi.fn(), cancelNodeAdmission: vi.fn(), discardCandidateReview: vi.fn() }));

afterEach(() => { vi.useRealTimers(); vi.clearAllMocks(); });

it('updates authority status without waiting for a hung admissions read', async () => {
  const advertisement = { discovery_id: 'owned', protocol_version: 1,
    monitor_endpoints: ['http://192.0.2.1:9095'], cluster_id: 'cluster' };
  vi.mocked(listNodeCandidates).mockResolvedValue({ local: { advertisement,
    physical_host_id: 'host', rack_hint: null },
    candidates: [{ advertisement, state: 'same_cluster' }], diagnostics: [] });
  vi.mocked(nodeAdmissions).mockResolvedValue([]);
  vi.mocked(nodeDeploymentStatus).mockResolvedValue({ phase: 'active', available: true });
  let rerender!: ReturnType<typeof render>['rerender'];
  vi.useFakeTimers();
  await act(async () => { ({ rerender } = render(<CandidateNodes racks={[]} deployment={{ phase: 'active', available: true }} />)); });
  expect(screen.getByTestId('deployment-phase')).toHaveTextContent('Active cluster');
  let rejectAdmissions!: (error: Error) => void;
  vi.mocked(nodeAdmissions).mockReturnValue(new Promise((_, reject) => { rejectAdmissions = reject; }));
  await act(async () => { await vi.advanceTimersByTimeAsync(2000); });
  await act(async () => { rerender(<CandidateNodes racks={[]} deployment={{ phase: 'authority_unavailable', available: false }} />); });
  vi.useRealTimers();
  await waitFor(() => expect(screen.getByTestId('deployment-phase'))
    .toHaveTextContent('Group 0 unavailable · confirmed view is stale'));
  expect(screen.getByTestId('candidate-owned')).toHaveTextContent('Cluster member');
  await act(async () => { rejectAdmissions(new Error('Group 0 unavailable')); });
  expect(screen.getByRole('status')).toHaveTextContent('last observations shown');
  expect(nodeDeploymentStatus).not.toHaveBeenCalled();
});
