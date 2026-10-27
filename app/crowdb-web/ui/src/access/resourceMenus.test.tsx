// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
import { fireEvent, render, screen, waitFor } from '@testing-library/react';
import { afterEach, expect, it, vi } from 'vitest';
import { useIcebergMenu } from '../iceberg/ResourceMenu';
import { useS3Menu } from '../s3/ResourceMenu';
import { iceberg, s3 } from './native';
vi.mock('./native', () => ({ iceberg: vi.fn(async () => ({})), s3: vi.fn(async () => new Response()), objectPath: (bucket: string, key?: string) => `/${bucket}${key === undefined ? '' : '/' + key}`, previewBytes: vi.fn() }));
vi.mock('./s3Transfer', () => ({ uploadObject: vi.fn(), downloadObject: vi.fn() }));
afterEach(() => { vi.clearAllMocks(); });
function Catalog({ namespace = 'first', readonly = false }) {
  const menu = useIcebergMenu(readonly, () => true, async () => {});
  return <><button onContextMenu={event => menu.open(event, { namespace: [namespace], table: 'table' })}>Resource</button>{menu.overlay}</>;
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
