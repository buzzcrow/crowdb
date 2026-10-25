// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

import { act, fireEvent, render, screen } from '@testing-library/react';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { clusterInitialization, initCluster } from '../api';
import { InitializationProgress } from './InitializationProgress';

vi.mock('../api', () => ({ clusterInitialization: vi.fn(), initCluster: vi.fn() }));
const observe = vi.mocked(clusterInitialization);

describe('InitializationProgress', () => {
  beforeEach(() => { vi.useFakeTimers(); vi.resetAllMocks(); });
  afterEach(() => { vi.useRealTimers(); });

  it('shows accepted work as running and retains the last observation on failure', async () => {
    observe.mockResolvedValueOnce({ phase: 'preparing', members: [3, 1, 2], operation_id: 'fixed' })
      .mockRejectedValueOnce(new Error('unreachable'));
    const view = render(<InitializationProgress />);
    await act(async () => {});
    expect(screen.getByRole('status')).toHaveTextContent('Preparing selected nodes');
    expect(screen.getByText('Selected voters: 3, 1, 2')).toBeVisible();
    expect(screen.queryByText('Initialization complete')).toBeNull();
    await act(async () => { await vi.advanceTimersByTimeAsync(1000); });
    expect(screen.getByText('Status unavailable · last observation shown')).toBeVisible();
    expect(screen.getByText('Operation fixed')).toBeVisible();
    view.unmount();
    await act(async () => { await vi.advanceTimersByTimeAsync(4000); });
    expect(observe).toHaveBeenCalledTimes(2);
    expect(initCluster).not.toHaveBeenCalled();
  });

  it('shows completed initialization within one second while accepted work is running', async () => {
    observe.mockResolvedValueOnce({ phase: 'data-group', members: [1, 2, 3], operation_id: 'fixed' })
      .mockResolvedValue({ phase: 'active', members: [1, 2, 3], operation_id: 'fixed' });
    render(<InitializationProgress />);
    await act(async () => {});
    expect(screen.getByRole('status')).toHaveTextContent('Creating initial data group');
    await act(async () => { await vi.advanceTimersByTimeAsync(1000); });
    expect(screen.getByRole('status')).toHaveTextContent('Initialization complete');
    expect(initCluster).not.toHaveBeenCalled();
  });

  it('retries only the accepted members without changing the data-group choice', async () => {
    observe.mockResolvedValueOnce({ phase: 'failed', members: [2, 3, 1], operation_id: 'fixed', error: 'peer unavailable' })
      .mockResolvedValueOnce({ phase: 'preparing', members: [2, 3, 1], operation_id: 'fixed' });
    vi.mocked(initCluster).mockResolvedValue({ phase: 'preparing' });
    render(<InitializationProgress />);
    await act(async () => {});
    expect(screen.getByRole('alert')).toHaveTextContent('peer unavailable');
    await act(async () => { fireEvent.click(screen.getByRole('button', { name: 'Retry accepted initialization' })); });
    expect(initCluster).toHaveBeenCalledWith({ nodes: [2, 3, 1] });
    expect(screen.getByRole('status')).toHaveTextContent('Preparing selected nodes');
  });

  it('keeps retry disabled in readonly mode', async () => {
    observe.mockResolvedValue({ phase: 'failed', members: [1], error: 'failed' });
    render(<InitializationProgress readonly />);
    await act(async () => {});
    expect(screen.getByRole('button', { name: 'Retry accepted initialization' })).toBeDisabled();
  });
});
