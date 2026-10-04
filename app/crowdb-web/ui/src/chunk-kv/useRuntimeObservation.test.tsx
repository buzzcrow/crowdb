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
