// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
import { act, fireEvent, render, renderHook, screen, within } from '@testing-library/react';
import { beforeEach, expect, it, vi } from 'vitest';
import * as api from '../api';
import { writeRequest, removeRequest } from '../operation/storage';
import { useDraft } from '../operation/useDraft';
import { saveOperation } from '../operation';
import { ensureDataset, newDatasetDraft } from './createDataset';
import { commitDraft, loadSnapshotDraft } from './session';
import { setCell, textValue } from './model';
import { UploadFiles } from './UploadFiles';
import { ImportEditor } from '../import/Editor';

vi.mock('../operation/storage', () => ({ writeRequest: vi.fn(async () => {}), removeRequest: vi.fn(async () => {}), listRequests: vi.fn(async () => []) }));
vi.mock('../api', async original => ({ ...await original<typeof import('../api')>(), capabilities: vi.fn(async () => ['dataset-identity-fence']), control: vi.fn(), mutate: vi.fn() }));
beforeEach(() => { vi.clearAllMocks(); vi.mocked(api.mutate).mockReset(); });

it('saves an unallocated Dataset and renamed input under the same local identity, then restores without server reads', async () => {
  const op = await newDatasetDraft('dataset-original');
  const { result } = renderHook(() => useDraft(op));
  await act(async () => { result.current.change(value => ({ ...value, dataset: 'renamed' })); await result.current.saving.current; });
  const saved = result.current.current.current!;
  expect(saved.resource).toBe(op.resource);
  expect(vi.mocked(writeRequest).mock.calls.map(call => call[0])).toEqual([expect.any(String), vi.mocked(writeRequest).mock.calls[0][0]]);
  const restored = await loadSnapshotDraft('renamed', undefined, undefined, saved, new AbortController().signal);
  expect(restored.value.dataset).toBe('renamed'); expect(restored.error).toBe('');
  expect(api.control).not.toHaveBeenCalled(); expect(api.mutate).not.toHaveBeenCalled();
});

it('never adopts an existing Dataset after a conflict or lost creation response', async () => {
  const op = await newDatasetDraft('same-name'); const { result } = renderHook(() => useDraft(op));
  vi.mocked(api.mutate).mockRejectedValueOnce(new api.DatasetError(502, 'lost reply', true));
  await act(async () => { await expect(ensureDataset(result.current)).rejects.toThrow('lost reply'); });
  expect(result.current.current.current?.creationAttempt).toBe('same-name');
  const restored = await loadSnapshotDraft(op.dataset, undefined, undefined, result.current.current.current, new AbortController().signal);
  expect(restored.error).toContain('outcome is unknown');
  vi.mocked(api.mutate).mockRejectedValueOnce(new api.DatasetError(409, 'already exists'));
  await act(async () => { await expect(ensureDataset(result.current)).rejects.toThrow('already exists'); });
  expect(result.current.current.current?.datasetId).toBeUndefined();
  expect(api.control).not.toHaveBeenCalled();
});

it('separates creation Drafts from real Dataset names containing storage delimiters', async () => {
  const op = await newDatasetDraft('dataset-creation-drafts:');
  await saveOperation({ ...op, creatingDataset: false });
  const keys = vi.mocked(writeRequest).mock.calls.map(call => call[0]);
  expect(keys[0]).not.toBe(keys[1]); expect(keys[0]).toContain('\u001f');
});

it('allocates once, freezes the first Snapshot and retries its exact publication after a lost reply', async () => {
  const op = setCell(await newDatasetDraft('created'), btoa('note'), 'content', textValue('first Snapshot'));
  const { result } = renderHook(() => useDraft(op));
  vi.mocked(api.mutate).mockResolvedValueOnce({ id: '1'.repeat(32), name: op.dataset, namespace: 'dataset-ns' })
    .mockRejectedValueOnce(new api.DatasetError(502, 'lost preparation', true));
  await act(async () => { await expect(commitDraft(result.current)).rejects.toThrow('lost preparation'); });
  expect(result.current.current.current?.datasetId).toBe('1'.repeat(32));
  expect(result.current.current.current?.pending).toEqual({ kind: 'prepare' });
  const input = structuredClone(result.current.current.current!.input);
  vi.mocked(api.mutate).mockResolvedValueOnce({ snapshot: op.resource, sample_count: '1', initial_split: null })
    .mockResolvedValueOnce({ latest: op.resource, stable: null, generation: '2'.repeat(64) });
  await act(async () => { expect((await commitDraft(result.current)).target).toEqual({ kind: 'snapshot', snapshot: op.resource }); });
  const calls = vi.mocked(api.mutate).mock.calls;
  expect(calls.filter(call => call[0] === 'dataset-management')).toHaveLength(1);
  expect(calls[1]).toEqual(calls[2]); expect(calls[2][3]).toEqual(input);
  expect(calls[3][3]).toEqual({ snapshot: op.resource, expected_head: null });
  expect(calls[3][5]).toBe('1'.repeat(32)); expect(removeRequest).toHaveBeenCalledOnce();
});

it('keeps the allocated name read-only and preserves content after upload allocation', async () => {
  const op = setCell(await newDatasetDraft('allocated'), btoa('note'), 'content', textValue('keep'));
  op.datasetId = '1'.repeat(32); vi.mocked(api.control).mockResolvedValueOnce({ id: op.datasetId });
  render(<ImportEditor dataset={op.dataset} recovery={op} close={vi.fn()} complete={vi.fn()} />);
  await act(async () => {});
  expect(screen.getByLabelText('New Dataset name')).toHaveAttribute('readonly');
  expect(screen.getByLabelText('content for note')).toHaveValue('keep');
  expect(screen.getByRole('button', { name: 'Create Dataset' })).toBeEnabled();
});

it('chooses a target only when multiple Fields exist and binds the file picker to that choice', () => {
  const choose = vi.fn(); const click = vi.spyOn(HTMLInputElement.prototype, 'click').mockImplementation(() => {});
  const { rerender } = render(<UploadFiles fields={['content']} choose={choose} />);
  fireEvent.click(screen.getByRole('button', { name: 'Upload files' }));
  expect(click).toHaveBeenCalledOnce(); expect(screen.queryByRole('dialog')).toBeNull();
  rerender(<UploadFiles fields={['content', 'label']} choose={choose} />);
  fireEvent.click(screen.getByRole('button', { name: 'Upload files' }));
  const dialog = screen.getByRole('dialog', { name: 'Choose upload Field' });
  expect(click).toHaveBeenCalledOnce();
  fireEvent.click(within(dialog).getByRole('button', { name: 'label' }));
  const file = new File(['bytes'], 'example.txt');
  fireEvent.change(screen.getByLabelText('Local payload files'), { target: { files: [file] } });
  expect(choose).toHaveBeenLastCalledWith([file], 'label');
  expect(screen.queryByRole('dialog')).toBeNull();
  fireEvent.change(screen.getByLabelText('Local payload files'), { target: { files: [file] } });
  expect(choose).toHaveBeenLastCalledWith([file], undefined);
  click.mockRestore();
});
