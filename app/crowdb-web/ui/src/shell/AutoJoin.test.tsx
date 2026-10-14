// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

import { act, fireEvent, render, screen } from '@testing-library/react';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { nodeAutoJoin, setNodeAutoJoin, type AutoJoinStatus } from '../api';
import { AutoJoin } from './AutoJoin';

vi.mock('../api', () => ({ nodeAutoJoin: vi.fn(), setNodeAutoJoin: vi.fn() }));
const status: AutoJoinStatus = { enabled: true, target_cluster_id: null, mode: 'standard', required_voters: 3,
  observation: { phase: 'waiting', members: [2, 1] } };

describe('AutoJoin', () => {
  beforeEach(() => { vi.useFakeTimers(); vi.clearAllMocks(); });
  afterEach(() => { vi.useRealTimers(); });
  it('shows waiting members and persists disabling without cancelling initialization', async () => {
    vi.mocked(nodeAutoJoin).mockResolvedValue(status);
    vi.mocked(setNodeAutoJoin).mockResolvedValue({ ...status, enabled: false });
    render(<AutoJoin />);
    await act(async () => {});
    expect(screen.getByRole('checkbox', { name: 'Auto join' })).toBeChecked();
    expect(screen.getByRole('status')).toHaveTextContent('Waiting (2/3)');
    await act(async () => { fireEvent.click(screen.getByRole('checkbox', { name: 'Auto join' })); });
    expect(setNodeAutoJoin).toHaveBeenCalledWith(false);
    expect(screen.getByRole('checkbox')).not.toBeChecked();
    expect(screen.getByRole('status')).toHaveTextContent('Automatic admission paused');
  });
  it('retains last observations and disables changes when authority cannot be observed', async () => {
    vi.mocked(nodeAutoJoin).mockResolvedValueOnce(status).mockRejectedValueOnce(new Error('unreachable'));
    const view = render(<AutoJoin />);
    await act(async () => {});
    await act(async () => { await vi.advanceTimersByTimeAsync(2000); });
    expect(screen.getByRole('checkbox')).toBeChecked();
    expect(screen.getByRole('checkbox')).toBeDisabled();
    expect(screen.getByText('Status unavailable · last observation shown')).toBeVisible();
    view.unmount();
    await act(async () => { await vi.advanceTimersByTimeAsync(4000); });
    expect(nodeAutoJoin).toHaveBeenCalledTimes(2);
  });
  it('readonly keeps the policy visible and immutable', async () => {
    vi.mocked(nodeAutoJoin).mockResolvedValue(status);
    render(<AutoJoin readonly />);
    await act(async () => {});
    expect(screen.getByRole('checkbox')).toBeChecked();
    expect(screen.getByRole('checkbox')).toBeDisabled();
  });

  it('requires an explicit local draft choice when targets are ambiguous', async () => {
    vi.mocked(nodeAutoJoin).mockResolvedValue({ ...status, observation: { phase: 'needs-target' } });
    vi.mocked(setNodeAutoJoin).mockResolvedValue(status);
    render(<AutoJoin />);
    await act(async () => {});
    expect(screen.getByRole('status')).toHaveTextContent('Choose target');
    expect(setNodeAutoJoin).not.toHaveBeenCalled();
    await act(async () => { fireEvent.click(screen.getByRole('button', { name: 'Use initial local draft' })); });
    expect(setNodeAutoJoin).toHaveBeenCalledWith(true, 'local-draft');
  });
});
