// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
import { act, renderHook } from '@testing-library/react';
import { beforeEach, expect, it, vi } from 'vitest';
import { iceberg } from '../access/native';
import { useInspection } from './useInspection';
import type { Inspection, Selection, TableLoad } from './types';

vi.mock('../contexts/DomainContext', () => ({ useDomain: () => ({ checkpoint: vi.fn() }) }));
vi.mock('../access/native', () => ({ iceberg: vi.fn() }));
const loaded: TableLoad = { 'metadata-location': 'table/metadata', metadata: {} };
const selection: Selection = { kind: 'snapshot', snapshot: { 'snapshot-id': '9007199254740993', 'manifest-list': 'table/list.avro' } };
const data: Inspection = { kind: 'manifest-list', location: 'table/list.avro', size: '99', metadata_location: loaded['metadata-location'], snapshot_id: '9007199254740993', rows: [], next: null };
beforeEach(() => { vi.mocked(iceberg).mockReset(); });
it('revalidates a cached branch and discards stale continuation links', async () => {
  vi.mocked(iceberg).mockResolvedValueOnce(data).mockRejectedValueOnce(new Error('HTTP 409: Table metadata changed'));
  const { result } = renderHook(() => useInspection(loaded, '/table', '', 'origin'));
  await act(() => result.current.select(selection));
  expect(result.current.data).toEqual(expect.objectContaining(data));
  await act(() => result.current.select(selection));
  expect(iceberg).toHaveBeenCalledTimes(2);
  expect(result.current.error).toContain('409');
  expect(result.current.data).toBeNull();
  expect(result.current.cache).toEqual({});
  expect(result.current.selection).toEqual(selection);
});
it('ignores a late reference response after selecting a newer snapshot', async () => {
  let resolve!: (value: Inspection) => void;
  vi.mocked(iceberg).mockImplementationOnce(() => new Promise<Inspection>(done => { resolve = done; }))
    .mockResolvedValueOnce({ ...data, snapshot_id: '7', location: 'table/new.avro' });
  const { result } = renderHook(() => useInspection(loaded, '/table', '', 'origin'));
  let pending!: ReturnType<typeof result.current.select>;
  act(() => { pending = result.current.select(selection); });
  await act(() => result.current.select({ kind: 'snapshot', snapshot: { 'snapshot-id': '7', 'manifest-list': 'table/new.avro' } }));
  await act(async () => { resolve(data); expect(await pending).toBeUndefined(); });
  expect(result.current.data?.snapshot_id).toBe('7');
  expect(Object.keys(result.current.cache)).toHaveLength(1);
});
it('rejects a response for another file despite matching table and snapshot', async () => {
  vi.mocked(iceberg).mockResolvedValue({ ...data, location: 'table/other.avro' });
  const { result } = renderHook(() => useInspection(loaded, '/table', '', 'origin'));
  await act(() => result.current.select(selection));
  expect(result.current.data).toBeNull();
  expect(result.current.error).toContain('selected reference');
});
