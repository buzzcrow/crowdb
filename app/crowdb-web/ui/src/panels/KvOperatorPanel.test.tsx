// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

import { act, fireEvent, render, screen } from '@testing-library/react';
import { afterEach, expect, it, vi } from 'vitest';
import { kvScan, type KvScanResponse } from '../api';
import { GroupHealth, type EnrichedStoreView } from '../types';
import { KvOperatorPanel } from './KvOperatorPanel';

const callbacks = vi.hoisted(() => ({ success: vi.fn(), error: vi.fn(), log: vi.fn() }));
vi.mock('../contexts/ToastContext', () => ({ useToast: () => callbacks }));
vi.mock('../contexts/ActivityContext', () => ({ useActivity: () => callbacks }));
vi.mock('../contexts/DomainContext', () => ({ useNavigationSnapshot: () => {} }));
vi.mock('../api', () => ({ kvGet: vi.fn(), kvScan: vi.fn() }));

afterEach(() => { vi.useRealTimers(); vi.clearAllMocks(); });

it('keeps the read-only scan surface stable and uses accessible controls', async () => {
  vi.useFakeTimers();
  vi.mocked(kvScan).mockResolvedValue({ items: [], truncated: false });
  const stores: EnrichedStoreView[] = [{ store_id: '1', nodes: [], groups: [
    { store_id: '1', group_id: '1', replicas: [], state: GroupHealth.Healthy },
  ] }];
  await act(async () => { render(<KvOperatorPanel stores={stores} selectedEntity={null} />); });
  let finishScan!: (result: KvScanResponse) => void;
  vi.mocked(kvScan).mockImplementationOnce(() => new Promise(resolve => { finishScan = resolve; }));
  fireEvent.click(screen.getByRole('button', { name: 'Scan' }));
  const signal = vi.mocked(kvScan).mock.calls.at(-1)![5]!.signal!;
  expect(signal.aborted).toBe(false);
  await act(async () => { vi.advanceTimersByTime(100); });
  expect(signal.aborted).toBe(false);
  await act(async () => { finishScan({ items: [
    { key_utf8: 'key', key_hex: '6b6579', value_utf8: 'value', value_hex: '76616c7565' },
  ], truncated: false }); });
  expect(screen.getByTestId('kv-scan-table')).toHaveTextContent('value');
  expect(screen.queryByRole('button', { name: 'Put' })).toBeNull();
  expect(screen.queryByRole('button', { name: 'Delete' })).toBeNull();
  expect(screen.queryByRole('button', { name: 'Inject' })).toBeNull();
});
