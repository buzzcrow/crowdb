// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
import { act, cleanup, fireEvent, render, screen, within } from '@testing-library/react';
import { afterEach, beforeEach, expect, it, vi } from 'vitest';
import { TableEditor } from './Editor';
import { submitImport } from './commit';
import { loadDraft, saveDraft } from './draft';
import { save } from './storage';
import { footer, type IceSchema } from './schema';
import { uploadFile } from './files';
import { filePage } from './source';
import { iceberg } from '../../access/native';
import type { TableLoad } from '../types';

vi.mock('./commit', async original => ({ ...await original<typeof import('./commit')>(), submitImport: vi.fn(async () => {}) }));
vi.mock('./schema', async original => ({ ...await original<typeof import('./schema')>(), footer: vi.fn() }));
vi.mock('./source', async original => ({ ...await original<typeof import('./source')>(), filePage: vi.fn() }));
vi.mock('./files', () => ({ uploadFile: vi.fn(), fileRequest: vi.fn() }));
vi.mock('../../access/native', async original => ({ ...await original<typeof import('../../access/native')>(), iceberg: vi.fn() }));
vi.mock('../../access/digest', () => ({ digest: vi.fn(async () => '0'.repeat(32)) }));
const schema: IceSchema = { type: 'struct', 'schema-id': 0, fields: [
  { id: 1, name: 'id', type: 'long', required: false }, { id: 2, name: 'message', type: 'string', required: false },
] };
const base = (definition = schema): TableLoad => ({ 'metadata-location': 's3://metadata.json', metadata: {
  'format-version': 2, 'current-schema-id': 0, schemas: [definition], 'partition-specs': [{ fields: [] }], snapshots: [],
} });
const metadata = (type: 'INT64' | 'BYTE_ARRAY' = 'INT64'): Awaited<ReturnType<typeof footer>> => ({ num_rows: 2n, schema: [
  { name: 'schema', num_children: 2 }, { name: 'id', type, repetition_type: 'OPTIONAL', ...(type === 'BYTE_ARRAY' ? { converted_type: 'UTF8' } : {}) },
  { name: 'message', type: 'BYTE_ARRAY', converted_type: 'UTF8', repetition_type: 'OPTIONAL' },
] } as Awaited<ReturnType<typeof footer>>);
beforeEach(() => {
  vi.mocked(footer).mockResolvedValue(metadata());
  vi.mocked(iceberg).mockResolvedValue(base());
  vi.mocked(filePage).mockResolvedValue({ rows: [] });
  vi.mocked(uploadFile).mockImplementation(async (_op, input, _file, _progress, _signal, persist) => { input.location = `s3://${input.name}`; persist?.(); });
});
afterEach(() => { cleanup(); localStorage.clear(); vi.restoreAllMocks(); vi.clearAllMocks(); });
const open = async (table?: string) => {
  const view = render(<TableEditor namespace={['editor-test']} table={table} close={vi.fn()} complete={vi.fn(async () => {})} />);
  await act(async () => {}); return view;
};
const click = async (name: string) => { await act(async () => { fireEvent.click(screen.getByRole('button', { name })); }); };
const upload = async (name = 'a.parquet') => {
  await act(async () => { fireEvent.change(screen.getByLabelText('Parquet files'), { target: { files: [new File(['parquet'], name)] } }); });
};

it('starts without invented fields and requires an upload to create a Table', async () => {
  await open();
  expect((screen.getByLabelText('Table name') as HTMLInputElement).value).toMatch(/^table-/);
  expect(screen.getByText('Upload Parquet to create a Table.')).toBeVisible();
  expect(screen.queryByRole('table')).toBeNull();
  expect(screen.queryByRole('button', { name: 'Add field' })).toBeNull();
  expect(screen.getByRole('button', { name: 'Commit' })).toBeDisabled();
  await click('Commit');
  expect(submitImport).not.toHaveBeenCalled();
  expect(iceberg).not.toHaveBeenCalled();
});

it('infers one read-only Schema, restores uploaded files and commits exactly those fields', async () => {
  const view = await open(); await upload();
  const preview = screen.getByRole('table', { name: 'Table schema' });
  expect(within(preview).getAllByRole('row')).toHaveLength(3);
  expect(preview).toHaveTextContent('id1longNomessage2stringNo');
  expect(within(preview).queryByRole('textbox')).toBeNull();
  expect(within(preview).queryByRole('combobox')).toBeNull();
  const files = screen.getByRole('table', { name: 'Parquet editor table' });
  expect(files).toHaveTextContent('a.parquet27Uploaded');
  expect(files).not.toHaveTextContent(/long|string/);
  expect(within(files).getAllByRole('columnheader').map(cell => cell.textContent)).toEqual(['Parquet file', 'Records', 'Bytes', 'Status', '']);
  await click('Save Draft'); view.unmount(); await open();
  expect(screen.getByRole('table', { name: 'Table schema' })).toHaveTextContent('message');
  expect(screen.getByRole('table', { name: 'Parquet editor table' })).toHaveTextContent('Uploaded');
  expect(uploadFile).toHaveBeenCalledTimes(1);
  await click('Commit');
  expect(submitImport).toHaveBeenCalledTimes(1);
  expect(vi.mocked(submitImport).mock.calls[0][0]).toMatchObject({ schema, files: [{ name: 'a.parquet', location: 's3://a.parquet' }] });
});

it('identifies an incompatible file and field before uploading it, preserving the accepted input', async () => {
  await open(); await upload();
  vi.mocked(footer).mockResolvedValue(metadata('BYTE_ARRAY'));
  await upload('bad.parquet');
  expect(screen.getByRole('alert')).toHaveTextContent('bad.parquet: id: string does not match "long"');
  expect(uploadFile).toHaveBeenCalledTimes(1);
  expect(loadDraft(['editor-test'])!.schema).toEqual(schema);
  expect(screen.getByRole('table', { name: 'Parquet editor table' })).not.toHaveTextContent('bad.parquet');
  expect(submitImport).not.toHaveBeenCalled();
});

it('resets inference when all new files are removed and accepts a different Schema', async () => {
  await open(); await upload(); await click('Remove a.parquet');
  expect(screen.queryByRole('table', { name: 'Table schema' })).toBeNull();
  expect(screen.getByRole('button', { name: 'Commit' })).toBeDisabled();
  expect(screen.getByLabelText('Table name')).toBeEnabled();
  const next = { ...schema, fields: schema.fields.map(field => ({ ...field, type: 'string' })) };
  vi.mocked(footer).mockResolvedValue(metadata('BYTE_ARRAY')); vi.mocked(iceberg).mockResolvedValue(base(next));
  await upload('different.parquet'); await click('Commit');
  expect(vi.mocked(submitImport).mock.calls[0][0].schema).toEqual(next);
});

it('keeps the existing Table Schema through file removal, undo and Snapshot commit', async () => {
  const table = base(); table.metadata.snapshots = [{ 'snapshot-id': '10', 'schema-id': 0 }]; table.metadata['current-snapshot-id'] = '10';
  vi.mocked(iceberg).mockResolvedValue(table);
  vi.mocked(filePage).mockResolvedValue({ rows: [{ manifest: 'manifest', file: { location: 's3://old.parquet', size: '9', records: '2', content: 'Data', format: 'PARQUET', status: 'Added' } }] });
  await open('existing');
  expect(screen.getByRole('button', { name: 'Commit' })).toBeDisabled();
  expect(screen.queryByRole('combobox')).toBeNull();
  await click('Remove old.parquet'); expect(screen.getByRole('button', { name: 'Commit' })).toBeEnabled();
  await click('Undo removal of old.parquet'); expect(screen.getByRole('button', { name: 'Commit' })).toBeDisabled();
  await click('Remove old.parquet'); await click('Commit');
  expect(vi.mocked(submitImport).mock.calls[0][0]).toMatchObject({ schema, removed: [{ manifest: 'manifest', location: 's3://old.parquet' }] });
});

it('preserves but blocks legacy manual field edits instead of silently publishing them', async () => {
  const edited = { ...schema, fields: [...schema.fields, { id: 3, name: 'manual', type: 'string', required: false }] };
  saveDraft({ version: 1, identity: 'draft', namespace: ['editor-test'], name: 'table', mode: 'parquet', schema: edited, base: base(), files: [] });
  await open();
  expect(screen.getByRole('alert')).toHaveTextContent('saved Draft contains manual Schema edits');
  expect(screen.getByRole('button', { name: 'Commit' })).toBeDisabled();
  expect(loadDraft(['editor-test'])!.schema).toEqual(edited);
  expect(screen.getByRole('button', { name: 'Discard Draft' })).toBeEnabled();
  expect(submitImport).not.toHaveBeenCalled();
});

it('does not show a successful save after browser storage fails', async () => {
  await open(); await click('Save Draft');
  expect(screen.getAllByText('Saved locally')).toHaveLength(1);
  vi.spyOn(Storage.prototype, 'setItem').mockImplementation(() => { throw new Error('Storage is full'); });
  await click('Save Draft');
  expect(screen.getByRole('alert')).toHaveTextContent('Storage is full');
  expect(screen.queryByText('Saved locally')).toBeNull();
  expect(submitImport).not.toHaveBeenCalled();
});

it('resumes an already frozen legacy Commit without changing its request', async () => {
  const edited = { ...schema, fields: [...schema.fields, { id: 3, name: 'manual', type: 'string', required: false }] };
  saveDraft({ version: 1, identity: 'pending', namespace: ['editor-test'], name: 'table', mode: 'parquet', schema: edited, base: base(), files: [] });
  const commit = '{"updates":[{"action":"add-schema"}]}';
  save({ version: 1, identity: 'pending', namespace: ['editor-test'], name: 'table', create: true, schema: edited, table: base(), files: [], snapshot: '42', commit, uncertain: true });
  await open();
  expect(screen.queryByRole('alert')).toBeNull();
  expect(screen.getByRole('button', { name: 'Discard Draft' })).toBeDisabled();
  await click('Retry Commit');
  expect(vi.mocked(submitImport).mock.calls[0][0]).toMatchObject({ schema: edited, commit, uncertain: true });
});
