// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
import { act, fireEvent, render, screen } from '@testing-library/react';
import { useState } from 'react';
import { expect, it, vi } from 'vitest';
import { Matrix, emptyWindow } from './Matrix';
import { DomainProvider } from '../contexts/DomainContext';
import type { DatasetSelection } from './types';
import * as api from './api';
vi.mock('./api', () => ({
  schemaPage: vi.fn(async () => ({ entries: [{ name: 'text', required: false }], after: null })),
  samplePage: vi.fn(async () => ({ entries: ['YQ=='], after: null })),
  matrix: vi.fn(async () => [{ id: 'YQ==', fields: [{ name: 'text', storage: 'inline', length: '0', md5: null, source_snapshot: '1'.repeat(32) }] }]),
}));
it('header, Sample and cell selections retain their type without reloading the same matrix window', async () => {
  function Example() {
    const [selected, select] = useState<Exclude<DatasetSelection, { kind: 'root' | 'dataset' }>>({ kind: 'snapshot', dataset: 'demo', snapshot: '1'.repeat(32) });
    const [window, updateWindow] = useState(emptyWindow);
    return <><output aria-label="Selected resource">{selected.kind}</output><Matrix scope={selected} active refresh={0} select={value => select(value as typeof selected)} window={window} updateWindow={updateWindow} /></>;
  }
  render(<DomainProvider><Example /></DomainProvider>);
  await act(async () => {});
  for (const [button, kind] of [['text Optional', 'schema'], ['a', 'sample'], ['inline · 0 B', 'cell']]) {
    fireEvent.click(screen.getByRole('button', { name: button }));
    await act(async () => {});
    expect(screen.getByLabelText('Selected resource')).toHaveTextContent(kind);
    expect(screen.getByRole('table', { name: 'Sample field matrix' })).toBeVisible();
  }
  expect(api.matrix).toHaveBeenCalledTimes(1);
});
