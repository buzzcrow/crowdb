// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
import { afterEach, describe, expect, it, vi } from 'vitest';
import { act, renderHook, waitFor } from '@testing-library/react';
import { useObjectLocations } from './useObjectLocations';

const extent = { index: '0', chunk_id: null, offset: '9007199254740993', length: '8', logical_offset: '0', logical_length: '8' };
const page = { bucket: 'bucket', key: 'object', generation: 'a'.repeat(64), etag: 'etag', logical_length: '8', locations: [extent], next_cursor: null };
afterEach(() => { vi.unstubAllGlobals(); });

describe('bounded object location inspection', () => {
  it.each([
    ['empty', { ...page, locations: [], logical_length: '0' }, ''],
    ['missing chunk', page, ''],
    ['corrupt integer', { ...page, locations: [{ ...extent, offset: '18446744073709551616' }] }, 'invalid exact integer'],
    ['corrupt generation', { ...page, generation: 'bad' }, 'Invalid or oversized'],
    ['too many rows', { ...page, locations: Array.from({ length: 21 }, () => extent) }, 'Invalid or oversized'],
    ['oversized response', { ...page, etag: 'x'.repeat(1024 * 1024) }, '1 MiB'],
  ])('handles %s explicitly', async (_name, response, error) => {
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue(new Response(JSON.stringify(response))));
    const { result } = renderHook(() => useObjectLocations(true, 'bucket', 'object'));
    await waitFor(() => expect(result.current.busy).toBe(false));
    expect(result.current.error).toContain(error);
    if (!error) expect(result.current.page?.locations).toEqual(response.locations);
    else expect(result.current.page).toBeNull();
  });

  it('captures an extent synchronously before cross-view navigation', async () => {
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue(new Response(JSON.stringify(page))));
    const { result } = renderHook(() => useObjectLocations(true, 'bucket', 'object'));
    await waitFor(() => expect(result.current.page).not.toBeNull());
    act(() => {
      result.current.select(extent);
      expect(result.current.snapshot()?.selected).toBe('0');
    });
  });

  it('restarts an interrupted first request on reactivation and rejects late results', async () => {
    let complete: (response: Response) => void = () => {};
    const fetch = vi.fn().mockImplementationOnce(() => new Promise<Response>(resolve => { complete = resolve; }))
      .mockImplementationOnce(() => Promise.resolve(new Response(JSON.stringify(page))));
    vi.stubGlobal('fetch', fetch);
    const { result, rerender } = renderHook(({ active }) => useObjectLocations(active, 'bucket', 'object'), { initialProps: { active: true } });
    rerender({ active: false });
    rerender({ active: true });
    await waitFor(() => expect(result.current.page?.etag).toBe('etag'));
    await act(async () => complete(new Response(JSON.stringify({ ...page, etag: 'late' }))));
    expect(result.current.page?.etag).toBe('etag');
    expect(fetch).toHaveBeenCalledTimes(2);
  });
});
