// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
import { fireEvent, render, screen, waitFor } from '@testing-library/react';
import { afterEach, beforeEach, expect, it, vi } from 'vitest';
import { useIcebergMenu } from '../iceberg/ResourceMenu';
import { useS3Menu } from '../s3/ResourceMenu';
import { iceberg, s3 } from './native';
import { uploadObject } from './s3Transfer';
vi.mock('./native', () => ({ iceberg: vi.fn(async () => ({})), s3: vi.fn(async () => new Response()), objectPath: (bucket: string, key?: string) => `/${bucket}${key === undefined ? '' : '/' + key}`, previewBytes: vi.fn() }));
vi.mock('./s3Transfer', () => ({ uploadObject: vi.fn(), downloadObject: vi.fn() }));
beforeEach(() => { vi.stubGlobal('crypto', { getRandomValues: crypto.getRandomValues.bind(crypto) }); });
afterEach(() => { vi.clearAllMocks(); vi.unstubAllGlobals(); });
function Catalog({ namespace = 'first', readonly = false }) {
  const menu = useIcebergMenu(readonly, () => true, async () => {});
  return <><button onContextMenu={event => menu.open(event, { namespace: [namespace], table: 'table' })}>Resource</button>{menu.overlay}</>;
}
function NamespaceMenu() {
  const menu = useIcebergMenu(false, () => true, async () => {});
  return <><button onContextMenu={event => menu.open(event, { namespace: ['first'] })}>Namespace</button>{menu.overlay}</>;
}
it('pins the exact Iceberg target across selection changes and requires confirmation', async () => {
  const { rerender } = render(<Catalog />);
  fireEvent.contextMenu(screen.getByRole('button', { name: 'Resource' }));
  fireEvent.click(screen.getByRole('menuitem', { name: 'Drop table…' }));
  expect(iceberg).not.toHaveBeenCalled();
  rerender(<Catalog namespace="second" />);
  expect(screen.getByRole('dialog')).toHaveTextContent('first / table');
  fireEvent.click(screen.getByRole('button', { name: 'Drop table' }));
  await waitFor(() => expect(iceberg).toHaveBeenCalledWith('/v1/namespaces/first/tables/table', '', 'DELETE'));
});
it('keeps readonly Iceberg mutations disabled', () => {
  render(<Catalog readonly />); fireEvent.contextMenu(screen.getByRole('button', { name: 'Resource' }));
  for (const item of screen.getAllByRole('menuitem')) expect(item).toBeDisabled();
  expect(iceberg).not.toHaveBeenCalled();
});
it('prefills a valid table name in the Iceberg table editor', async () => {
  render(<NamespaceMenu />); fireEvent.contextMenu(screen.getByRole('button', { name: 'Namespace' }));
  fireEvent.click(screen.getByRole('menuitem', { name: 'Create table…' }));
  await waitFor(() => expect(screen.getByRole('textbox', { name: 'Table name' })).toBeVisible());
  expect((screen.getByRole('textbox', { name: 'Table name' }) as HTMLInputElement).value).toMatch(/^table-[0-9a-f]{8}$/);
});
it('prefills a valid namespace name for one-click creation', async () => {
  render(<><NamespaceMenu /></>); fireEvent.contextMenu(screen.getByRole('button', { name: 'Namespace' }));
  fireEvent.click(screen.getByRole('menuitem', { name: 'Create namespace…' }));
  const input = screen.getByRole('textbox', { name: 'Namespace name' });
  expect((input as HTMLInputElement).value).toMatch(/^namespace-[0-9a-f]{8}$/);
  fireEvent.click(screen.getByRole('button', { name: 'Create namespace' }));
  await waitFor(() => expect(iceberg).toHaveBeenCalledWith('/v1/namespaces', '', 'POST', expect.objectContaining({ namespace: ['first', expect.stringMatching(/^namespace-[0-9a-f]{8}$/)] })));
});
function ObjectMenu({ readonly = false }) {
  const menu = useS3Menu(readonly, true, async () => {}, () => {}, () => {});
  return <><button onContextMenu={event => menu.open(event, { bucket: 'photos', key: 'a/b' })}>Object</button>{menu.overlay}</>;
}
it('offers explicit object reads in readonly mode without permitting mutations', () => {
  render(<ObjectMenu readonly />); fireEvent.contextMenu(screen.getByRole('button', { name: 'Object' }));
  expect(screen.getByRole('menuitem', { name: 'Preview first 4 KiB' })).toBeEnabled();
  expect(screen.getByRole('menuitem', { name: 'Download' })).toBeEnabled();
  expect(screen.getByRole('menuitem', { name: 'Replace object…' })).toBeDisabled();
  expect(screen.getByRole('menuitem', { name: 'Delete object…' })).toBeDisabled();
  expect(s3).not.toHaveBeenCalled();
});
it('deletes only the confirmed S3 Object', async () => {
  render(<ObjectMenu />); fireEvent.contextMenu(screen.getByRole('button', { name: 'Object' }));
  fireEvent.click(screen.getByRole('menuitem', { name: 'Delete object…' }));
  expect(screen.getByRole('dialog')).toHaveTextContent('photos / a/b');
  expect(s3).not.toHaveBeenCalled();
  fireEvent.click(screen.getByRole('button', { name: 'Delete object' }));
  await waitFor(() => expect(s3).toHaveBeenCalledWith('DELETE', '/photos/a/b'));
});
function BucketMenu() {
  const menu = useS3Menu(false, true, async () => {}, () => {}, () => {});
  return <><button onContextMenu={event => menu.open(event, { bucket: 'photos' })}>Bucket</button>{menu.overlay}</>;
}
function S3RootMenu() {
  const menu = useS3Menu(false, true, async () => {}, () => {}, () => {});
  return <><button onContextMenu={event => menu.open(event, {})}>S3</button>{menu.overlay}</>;
}
it('prefills a valid bucket name for one-click creation', async () => {
  render(<S3RootMenu />); fireEvent.contextMenu(screen.getByRole('button', { name: 'S3' }));
  fireEvent.click(screen.getByRole('menuitem', { name: 'Create bucket…' }));
  const input = screen.getByRole('textbox', { name: 'New bucket' });
  expect((input as HTMLInputElement).value).toMatch(/^bucket-[0-9a-f]{8}$/);
  fireEvent.click(screen.getByRole('button', { name: 'Create bucket' }));
  await waitFor(() => expect(s3).toHaveBeenCalledWith('PUT', expect.stringMatching(/^\/bucket-[0-9a-f]{8}$/)));
});
it('uploads a batch using each filename as the object key', async () => {
  render(<BucketMenu />); fireEvent.contextMenu(screen.getByRole('button', { name: 'Bucket' }));
  fireEvent.click(screen.getByRole('menuitem', { name: 'Upload object…' }));
  const first = new File(['a'], 'a.txt'); const second = new File(['b'], 'b.txt');
  fireEvent.change(screen.getByLabelText('Object files'), { target: { files: [first, second] } });
  expect(screen.queryByLabelText('Object key')).toBeNull();
  fireEvent.click(screen.getByRole('button', { name: 'Upload object' }));
  await waitFor(() => expect(uploadObject).toHaveBeenCalledTimes(2));
  expect(uploadObject).toHaveBeenNthCalledWith(1, 'photos', 'a.txt', first, expect.any(AbortSignal), expect.any(Function));
  expect(uploadObject).toHaveBeenNthCalledWith(2, 'photos', 'b.txt', second, expect.any(AbortSignal), expect.any(Function));
});
it('reports each batch outcome and continues after an individual upload fails', async () => {
  vi.mocked(uploadObject).mockResolvedValueOnce(undefined).mockRejectedValueOnce(new Error('upload rejected')).mockRejectedValueOnce(new DOMException('stopped', 'AbortError'));
  render(<BucketMenu />); fireEvent.contextMenu(screen.getByRole('button', { name: 'Bucket' }));
  fireEvent.click(screen.getByRole('menuitem', { name: 'Upload object…' }));
  fireEvent.change(screen.getByLabelText('Object files'), { target: { files: [new File(['a'], 'a.txt'), new File(['b'], 'b.txt'), new File(['c'], 'c.txt')] } });
  fireEvent.click(screen.getByRole('button', { name: 'Upload object' }));
  const results = await screen.findByRole('list', { name: 'Object upload results' });
  await waitFor(() => {
    expect(results).toHaveTextContent('a.txt: success');
    expect(results).toHaveTextContent('b.txt: failed');
    expect(results).toHaveTextContent('c.txt: unknown');
  });
  expect(uploadObject).toHaveBeenCalledTimes(3);
});
