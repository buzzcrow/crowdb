// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
import { act, fireEvent, render, screen, within } from '@testing-library/react';
import { beforeEach, expect, it, vi } from 'vitest';
import { ImportEditor } from '../import/Editor';
import { defaultPolicy } from '../types';
import type { Operation } from '../operation';
import { writeRequest } from '../operation/storage';
import { checkLimits, parentRows, setCell, textValue } from './model';
import * as api from '../api';
import { checkUpload } from './uploads';
import { Grid } from './Grid';
import { DraftDetails } from './Details';
vi.mock('../operation/storage', () => ({ writeRequest: vi.fn(async () => {}), removeRequest: vi.fn(), readRequest: vi.fn(), listRequests: vi.fn(async () => []) }));
vi.mock('../api', async original => ({ ...await original<typeof import('../api')>(), control: vi.fn(async () => ({ id: '1'.repeat(32) })), matrix: vi.fn(), mutate: vi.fn() }));
const draft = (): Operation => ({ version: 2, dataset: 'demo', datasetId: '1'.repeat(32), resource: '2'.repeat(32), kind: 'snapshot', editing: true, expectedHead: null,
  source: { parent: null, source: { kind: 'all' }, schema: null, splits: [], policy: defaultPolicy },
  mapping: { version: 1, schema: { version: 1, fields: [{ name: 'content', required: false }] }, samples: [] } });
beforeEach(() => { vi.clearAllMocks(); });
it('edits a Sample as one row, persists only metadata and does not publish on Save Draft', async () => {
  render(<ImportEditor dataset="demo" recovery={draft()} close={vi.fn()} complete={vi.fn()} />);
  await act(async () => {});
  await act(async () => fireEvent.click(screen.getByRole('button', { name: 'Add Sample' })));
  const input = screen.getByRole('textbox', { name: /^Sample ID for/ });
  fireEvent.change(input, { target: { value: 'note' } });
  await act(async () => fireEvent.blur(input));
  expect(screen.getByLabelText('Upload content for note')).toBeInTheDocument();
  await act(async () => fireEvent.change(screen.getByLabelText('content for note'), { target: { value: '(empty text)' } }));
  expect(screen.getByLabelText('content for note')).toHaveValue('(empty text)');
  expect(screen.queryByRole('dialog')).toBeNull();
  await act(async () => fireEvent.click(screen.getByRole('button', { name: 'Save Draft' })));
  expect(api.mutate).not.toHaveBeenCalled();
  const saved = vi.mocked(writeRequest).mock.calls.at(-1)![1];
  expect(saved.mapping!.samples).toEqual([{ kind: 'upsert', id: btoa('note'), fields: [{ name: 'content', value: { kind: 'inline', data: btoa('(empty text)') } }] }]);
  expect(JSON.stringify(saved)).not.toContain('Blob');
});
it('patches a cell without losing other Fields and rejects unbounded authoring input', () => {
  const op = setCell(draft(), btoa('note'), 'label', textValue('keep'));
  expect(setCell(op, btoa('note'), 'content', { kind: 'upload', id: '3'.repeat(32), length: '12', md5: '4'.repeat(32) }).mapping!.samples[0]).toMatchObject({ fields: [{ name: 'label' }, { name: 'content' }] });
  expect(() => textValue('字'.repeat(22000))).toThrow('64 KiB');
  expect(() => checkLimits({ ...op, mapping: { ...op.mapping!, samples: Array(1001).fill(op.mapping!.samples[0]) } })).toThrow('1000');
});
it('projects new columns as Missing without asking the parent for unknown Fields', async () => {
  const op = draft(); op.source!.parent = '3'.repeat(32); op.parentSchema = op.mapping!.schema!;
  vi.mocked(api.matrix).mockResolvedValueOnce([{ id: 'YQ==', fields: [null] }]);
  expect(await parentRows(op, ['YQ=='], ['label', 'content'])).toEqual([{ id: 'YQ==', fields: [null, null] }]);
  expect(api.matrix).toHaveBeenCalledWith('demo', '3'.repeat(32), ['YQ=='], ['content'], undefined);
});
it('checks complete upload identities, expiry and claimed ownership before Commit', async () => {
  const status = { id: '3'.repeat(32), length: '12', md5: '4'.repeat(32), state: 'complete', expires_at: '1' };
  vi.mocked(api.control).mockResolvedValueOnce(status);
  await expect(checkUpload('demo', '1'.repeat(32), status.id, status.length, status.md5)).rejects.toThrow('expired');
  vi.mocked(api.control).mockResolvedValueOnce({ ...status, state: 'claimed' });
  await expect(checkUpload('demo', '1'.repeat(32), status.id, status.length, status.md5)).resolves.toMatchObject({ state: 'claimed' });
  vi.mocked(api.control).mockResolvedValueOnce({ ...status, state: 'claimed', md5: '5'.repeat(32) });
  await expect(checkUpload('demo', '1'.repeat(32), status.id, status.length, status.md5)).rejects.toThrow();
});
it('shows the completed retry in the table and Properties instead of an older failed upload attempt', () => {
  const descriptor = { id: '3'.repeat(32), md5: '4'.repeat(32), length: '22' };
  const op = setCell(draft(), btoa('note'), 'content', { kind: 'upload', ...descriptor });
  op.uploads = {
    interrupted: { name: 'resume.txt', size: 22, id: descriptor.id, md5: descriptor.md5, state: 'failed', error: 'Network error' },
    resumed: { name: 'resume.txt', size: 22, id: descriptor.id, md5: descriptor.md5, state: 'ready' },
  };
  render(<><Grid op={op} rows={[{ id: btoa('note'), fields: [null] }]} fields={['content']} apply={vi.fn()} upload={vi.fn()} rename={vi.fn()} remove={vi.fn()} restore={vi.fn()} select={vi.fn()} selectField={vi.fn()} removeField={vi.fn()} />
    <DraftDetails op={op} id={btoa('note')} /></>);
  expect(screen.getByRole('button', { name: 'resume.txt · 22 B' })).toHaveTextContent('resume.txt · 22 B');
  expect(screen.getByRole('region', { name: 'Draft properties' })).toHaveTextContent('resume.txt · 22 B · ready');
  expect(screen.queryByText('Network error')).not.toBeInTheDocument();
});
it('routes text and file edits to the exact Sample and Field cell', () => {
  const op = draft();
  const apply = vi.fn(); const upload = vi.fn();
  render(<Grid op={op} rows={[{ id: btoa('first'), fields: [null] }, { id: btoa('second'), fields: [null] }]} fields={['content']} apply={apply} upload={upload} rename={vi.fn()} remove={vi.fn()} restore={vi.fn()} select={vi.fn()} selectField={vi.fn()} removeField={vi.fn()} />);
  fireEvent.change(screen.getByRole('textbox', { name: 'content for second' }), { target: { value: 'second value' } });
  expect(apply).toHaveBeenCalledWith(btoa('second'), 'content', expect.objectContaining({ kind: 'inline' }));
  const file = new File(['bytes'], 'second.parquet', { type: 'application/octet-stream' });
  fireEvent.change(screen.getByLabelText('Upload content for first'), { target: { files: [file] } });
  expect(upload).toHaveBeenCalledWith(btoa('first'), 'content', file);
});
it('keeps the first row in place when editing a newly added column', async () => {
  const op = draft();
  op.mapping!.samples = ['first', 'second'].map(id => ({ kind: 'upsert', id: btoa(id), fields: [] }));
  render(<ImportEditor dataset="demo" recovery={op} close={vi.fn()} complete={vi.fn()} />);
  await act(async () => {});
  await act(async () => fireEvent.click(screen.getByRole('button', { name: 'Add Field' })));
  const name = screen.getByLabelText('Field name');
  fireEvent.change(name, { target: { value: 'note' } });
  await act(async () => fireEvent.click(within(screen.getByRole('dialog', { name: 'Add Field' })).getByRole('button', { name: 'Add Field' })));
  await act(async () => fireEvent.change(screen.getByLabelText('note for first'), { target: { value: 'first only' } }));
  expect(screen.getAllByRole('textbox', { name: /^Sample ID for/ }).map(input => (input as HTMLInputElement).value)).toEqual(['first', 'second']);
  expect(screen.getByLabelText('note for first')).toHaveValue('first only');
  expect(screen.getByLabelText('note for second')).toHaveValue('');
});
