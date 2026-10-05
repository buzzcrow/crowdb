// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
import { cleanup, render, screen, fireEvent } from '@testing-library/react';
import { afterEach, expect, it, vi } from 'vitest';
import { JournalStorage } from './StorageObservation';
import { identity } from './catalog';
import type { JournalObservation } from './useRuntimeObservation';
afterEach(cleanup);

it('replaces extent windows and preserves exact identities and selected byte boundaries', () => {
  const chunk = identity({ high: '18446744073709551615', low: '2' });
  expect(chunk).toBe('ffffffffffffffff0000000000000002');
  const window = (offset: number, count: number): JournalObservation => ({
    generation: '9007199254740999', writer_epoch: '9007199254740993', metadata_group_id: '7',
    trim_offset: '4096', sealed_tail: '18446744073709551615', closed: false,
    active: { chunk_id: chunk, physical_start: '64', logical_start: '8192', acknowledged_cursor: '128', capacity: '4096' },
    offset, next_offset: offset ? null : 100,
    extent_pages: Array.from({ length: count }, (_, index) => ({ page_index: String(offset + index), first_logical: String((offset + index) * 64), end_logical: String((offset + index + 1) * 64) })),
  });
  const onPage = vi.fn(); const onChunk = vi.fn(); const onSelected = vi.fn();
  const propertyHost = document.createElement('div'); document.body.append(propertyHost);
  try {
    const props = { value: window(0, 100), disabled: false, onPage, onChunk, onSelected, propertyHost, selected: null as string | null };
    const { rerender } = render(<JournalStorage {...props} />);
    expect(screen.getByLabelText('Extent page map').querySelectorAll('button')).toHaveLength(100);
    expect(screen.getByLabelText('Journal storage')).toHaveTextContent('18446744073709551615');
    fireEvent.click(screen.getByRole('button', { name: 'Inspect active Chunk' })); expect(onChunk).toHaveBeenCalledWith(chunk);
    fireEvent.click(screen.getByRole('button', { name: 'Next extent pages' })); expect(onPage).toHaveBeenCalledWith(100);
    rerender(<JournalStorage {...props} value={window(100, 5)} selected="100" />);
    expect(screen.getByLabelText('Extent page map').querySelectorAll('button')).toHaveLength(5);
    expect(screen.getByLabelText('Selected extent page')).toHaveTextContent('6464');
    expect(screen.getByRole('button', { name: 'Next extent pages' })).toBeDisabled();
    expect(screen.queryByRole('button', { name: 'Extent page 0 [0, 64)' })).toBeNull();
    rerender(<JournalStorage {...props} value={window(0, 100)} />);
    expect(screen.queryByLabelText('Selected extent page')).toBeNull();
  } finally { propertyHost.remove(); }
});
