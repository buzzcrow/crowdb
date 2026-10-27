// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
import { act, render, screen } from '@testing-library/react';
import { expect, it, vi } from 'vitest';
import { CatalogTree } from './CatalogTree';
import { DomainProvider } from '../contexts/DomainContext';
import { SelectionProvider } from '../contexts/SelectionContext';
import type { Inspector } from './useInspection';
import { iceberg } from '../access/native';
vi.mock('../access/native', () => ({ iceberg: vi.fn(async (path: string) => path.includes('/tables?') ? { identifiers: [] } : { namespaces: [['a']] }) }));
it('locates a deep selected namespace through paged ancestors and refreshes the full path after mutation', async () => {
  const props = { active: true, revision: 1, namespace: ['a', 'child.literal', 'grandchild'], table: '', loaded: null,
    inspector: { selection: null } as Inspector, onCatalog: vi.fn(), onNamespace: vi.fn(), onTable: vi.fn(), onContextMenu: vi.fn() };
  const view = (revision: number) => <DomainProvider><SelectionProvider><CatalogTree {...props} revision={revision} /></SelectionProvider></DomainProvider>;
  const { rerender } = render(view(1));
  await act(async () => {});
  expect(screen.getByRole('button', { name: 'grandchild' })).toBeVisible();
  expect(screen.getByRole('treeitem', { selected: true })).toHaveTextContent('grandchild');
  vi.mocked(iceberg).mockClear();
  rerender(view(2)); await act(async () => {});
  expect(screen.getByRole('button', { name: 'child.literal' })).toBeVisible();
  expect(screen.getByRole('button', { name: 'grandchild' })).toBeVisible();
  const paths = vi.mocked(iceberg).mock.calls.map(call => call[0]);
  for (const namespace of [['a'], ['a', 'child.literal'], ['a', 'child.literal', 'grandchild']]) {
    expect(paths.some(path => path.startsWith(`/v1/namespaces/${encodeURIComponent(namespace.join('\x1f'))}/tables?`))).toBe(true);
  }
});
