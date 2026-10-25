// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
import { cleanup, render, screen, fireEvent } from '@testing-library/react';
import { afterEach, expect, it, vi } from 'vitest';
import { JournalStorage } from './StorageObservation';
import { identity } from './catalog';
import type { JournalObservation } from './useRuntimeObservation';
afterEach(cleanup);

it('preserves exact chunk identities, logical boundaries and mapping sizes', () => {
  const chunk = identity({ high: '18446744073709551615', low: '2' });
  expect(chunk).toBe('ffffffffffffffff0000000000000002');
  const value: JournalObservation = {
    generation: '9007199254740999', writer_epoch: '9007199254740993', metadata_group_id: '7',
    trim_offset: '4096', published_tail: '18446744073709551615', closed: false, offset: 0, next_offset: null,
    chunk_record: { chunk_id: chunk, state: 'Active', logical_start: '8192', logical_end: '18446744073709551615', mapping_count: '4096', mapping_bytes: '247' },
  };
  const onChunk = vi.fn(); const onSelected = vi.fn();
  const propertyHost = document.createElement('div'); document.body.append(propertyHost);
  try {
    const props = { value, disabled: false, onChunk, onSelected, propertyHost, selected: null as string | null };
    const { rerender } = render(<JournalStorage {...props} />);
    expect(screen.getByLabelText('Journal storage')).toHaveTextContent('18446744073709551615');
    fireEvent.click(screen.getByRole('button', { name: 'Inspect Chunk' })); expect(onChunk).toHaveBeenCalledWith(chunk);
    fireEvent.click(screen.getByRole('button', { name: 'Select stream chunk record' })); expect(onSelected).toHaveBeenCalledWith(chunk);
    rerender(<JournalStorage {...props} selected={chunk} />);
    expect(screen.getByLabelText('Selected stream chunk record')).toHaveTextContent('8192');
    expect(screen.getByLabelText('Selected stream chunk record')).toHaveTextContent('247');
    rerender(<JournalStorage {...props} value={{ ...value, chunk_record: { ...value.chunk_record, chunk_id: '00000000000000000000000000000003' } }} selected={chunk} />);
    expect(screen.queryByLabelText('Selected stream chunk record')).toBeNull();
    rerender(<JournalStorage {...props} disabled />);
    expect(screen.getByRole('button', { name: 'Inspect Chunk' })).toBeDisabled();
  } finally { propertyHost.remove(); }
});
