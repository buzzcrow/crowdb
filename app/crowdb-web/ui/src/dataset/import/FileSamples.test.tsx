// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
import { useState } from 'react';
import { act, fireEvent, render, screen } from '@testing-library/react';
import { expect, it, vi } from 'vitest';
import { defaultPolicy } from '../types';
import type { Operation } from '../operation';
import { FileSamples } from './FileSamples';
vi.mock('../api', async original => ({ ...await original<typeof import('../api')>(), control: vi.fn(async () => ({ contains: true })) }));
it('blocks inherited Sample replacement until explicitly allowed and preserves pending fields', async () => {
  let observed!: Operation;
  function Editor() {
    const [op, change] = useState<Operation>({ version: 2, dataset: 'demo', datasetId: '1'.repeat(32), kind: 'snapshot', resource: '2'.repeat(32), expectedHead: null,
      source: { parent: '3'.repeat(32), source: { kind: 'all' }, schema: null, splits: [], policy: defaultPolicy },
      mapping: { version: 1, schema: { version: 1, fields: [{ name: 'content', required: true }, { name: 'label', required: false }] }, samples: [{ kind: 'upsert', id: btoa('existing'), fields: [{ name: 'label', value: { kind: 'inline', data: btoa('keep') } }] }] },
      fileSamples: [{ file: 'payload.txt', sample: 'existing' }], fileChecks: { 'payload.txt': { size: 10, md5: '0'.repeat(32) } } });
    const [error, setError] = useState(''); observed = op;
    return <><FileSamples op={op} change={change} run={action => { void action().catch(reason => setError(String(reason))); }} /><p role="alert">{error}</p></>;
  }
  render(<Editor />);
  await act(async () => fireEvent.click(screen.getByRole('button', { name: 'Apply file mappings' })));
  expect(screen.getByRole('alert')).toHaveTextContent('already exists');
  expect(observed.mapping!.samples[0]).toMatchObject({ fields: [{ name: 'label' }] });
  fireEvent.click(screen.getByLabelText('Allow updating existing Sample IDs'));
  await act(async () => fireEvent.click(screen.getByRole('button', { name: 'Apply file mappings' })));
  expect(observed.mapping!.samples[0]).toMatchObject({ fields: [{ name: 'label' }, { name: 'content', value: { kind: 'file', file: 'payload.txt' } }] });
  fireEvent.click(screen.getByRole('button', { name: 'Remove file payload.txt' }));
  expect(observed.fileSamples).toEqual([]);
  expect(observed.mapping!.samples[0]).toMatchObject({ fields: [{ name: 'label' }] });
});
