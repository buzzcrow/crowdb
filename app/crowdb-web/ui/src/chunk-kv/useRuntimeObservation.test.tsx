// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
import { afterEach, expect, it, vi } from 'vitest';
import { act, renderHook, waitFor } from '@testing-library/react';
import { useRuntimeObservation } from './useRuntimeObservation';
import type { Partition } from './catalog';
afterEach(() => { vi.unstubAllGlobals(); });
it('rejects a late old-owner observation and exposes stale runtime cursors', async () => {
  let oldResponse!: (response: Response) => void;
  const fetcher = vi.fn()
    .mockImplementationOnce(() => new Promise<Response>(resolve => { oldResponse = resolve; }))
    .mockResolvedValueOnce(new Response(JSON.stringify({ lifecycle: 'new-owner', journal: null })))
    .mockResolvedValueOnce(new Response('Catalog generation changed', { status: 409 }));
  vi.stubGlobal('fetch', fetcher);
  const partition = { id: 'partition', epoch: '1' } as Partition;
  const props = { active: true, partition, generation: '1', catalogPage: 0, catalogOffset: 0, cursor: { offset: 0 }, onCursor: vi.fn() };
  const { result, rerender } = renderHook(useRuntimeObservation, { initialProps: props });
  rerender({ ...props, partition: { ...partition, epoch: '2' }, generation: '2' });
  await waitFor(() => expect(result.current.value?.lifecycle).toBe('new-owner'));
  await act(async () => oldResponse(new Response(JSON.stringify({ lifecycle: 'old-owner', journal: null }))));
  expect(result.current.value?.lifecycle).toBe('new-owner');
  rerender({ ...props, cursor: { offset: 100 } });
  await waitFor(() => expect(result.current.error).toContain('409'));
  expect(result.current.value).toBeNull();
});

it('rejects inconsistent generations and malformed chunk records before publishing runtime data', async () => {
  const record = { chunk_id: 'ffffffffffffffff0000000000000002', state: 'Active', logical_start: '0', logical_end: '9007199254740999', mapping_count: '8', mapping_bytes: '10' };
  const journal = (generation: string) => ({ generation, offset: 0, chunk_record: record });
  const fetcher = vi.fn()
    .mockResolvedValueOnce(new Response(JSON.stringify({ journal: journal('18') })))
    .mockResolvedValueOnce(new Response(JSON.stringify({ journal: { ...journal('17'), chunk_record: { ...record, logical_start: '9999999999999999999' } } })))
    .mockResolvedValueOnce(new Response(JSON.stringify({ journal: journal('17') })));
  vi.stubGlobal('fetch', fetcher);
  const props = { active: true, partition: { id: 'partition', epoch: '9007199254740993' } as Partition,
    generation: '9007199254740997', catalogPage: 0, catalogOffset: 0,
    cursor: { generation: '17', offset: 0 }, onCursor: vi.fn() };
  const { result, rerender } = renderHook(useRuntimeObservation, { initialProps: props });
  await waitFor(() => expect(result.current.error).toContain('Stream chunk record changed'));
  expect(result.current.value).toBeNull();
  rerender({ ...props, cursor: { generation: '17', offset: 0 } });
  await waitFor(() => expect(result.current.error).toContain('Invalid stream chunk record'));
  expect(result.current.value).toBeNull();
  act(() => result.current.refresh());
  expect(props.onCursor).toHaveBeenCalledWith({ offset: 0 });
  await waitFor(() => expect(result.current.value?.journal?.chunk_record.chunk_id).toBe(record.chunk_id));
  expect(new URL(fetcher.mock.calls[0][0], 'http://localhost').searchParams.get('epoch')).toBe('9007199254740993');
});
