// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
import { act, fireEvent, render, screen, within } from '@testing-library/react';
import { useState } from 'react';
import { expect, it, vi } from 'vitest';
import { Tree } from '../components/Tree';
import { SnapshotTree, type TreeState } from './Tree';
import { DomainProvider } from '../contexts/DomainContext';
import { SelectionProvider } from '../contexts/SelectionContext';
import type { DatasetSelection } from './types';
import * as api from './api';
const snapshot = '1'.repeat(32); const current = '2'.repeat(32); const old = '3'.repeat(32);
vi.mock('./api', () => ({ head: vi.fn(async () => ({})), datasetPath: (d: string) => d, snapshotPath: (d: string, s: string) => `${d}/${s}`,
  page: vi.fn(async (p: string) => ({ entries: p.endsWith('/roots') ? [{ id: snapshot }] : [], after: null })),
  splitHead: vi.fn(async () => ({ revision: current })),
  history: vi.fn(async () => ({ entries: [{ revision: current }, { revision: old }], after: 'next' })),
  revision: vi.fn(async (_d: string, _s: string, revision: string) => ({ revision, splits: [{ name: 'train' }] })),
}));
it('keeps lazy branches at their real depth and binds equal Split names to distinct revisions without counts', async () => {
  const select = vi.fn();
  function Workspace() {
    const [tree, setTree] = useState<TreeState>({ [`demo:node:${snapshot}`]: { expanded: true, splits: true } });
    const [selected, setSelected] = useState<DatasetSelection>({ kind: 'snapshot', dataset: 'demo', snapshot });
    return <Tree defaultExpandedIds={['root', 'demo']} nodes={[{ id: 'root', type: 'Dataset', label: 'Datasets', children: [{ id: 'demo', type: 'Dataset', label: 'demo',
      childrenContent: <SnapshotTree dataset="demo" active refresh={0} tree={tree} updateTree={(key, value) => setTree(previous => ({ ...previous, [key]: value }))}
        selected={selected} select={value => { select(value); setSelected(value); }} onContextMenu={vi.fn()} />,
    }] }]} />;
  }
  await act(async () => { render(<DomainProvider><SelectionProvider><Workspace /></SelectionProvider></DomainProvider>); });
  await screen.findByRole('button', { name: 'train' });
  for (const [name, level] of [['Datasets', 1], ['demo', 2], [snapshot.slice(0, 10), 3], ['Splits', 4], [current.slice(0, 8), 5], ['train', 6]] as const) {
    expect(screen.getByRole('button', { name }).closest('[role="treeitem"]')).toHaveAttribute('aria-level', String(level));
  }
  expect(api.history).toHaveBeenCalledTimes(1);
  const historical = screen.getByTestId(`tree-node-demo:${snapshot}:revision:${old}`);
  await act(async () => fireEvent.click(within(historical).getByRole('button', { name: 'Expand' })));
  fireEvent.click(within(historical).getByRole('button', { name: 'train' }));
  expect(select).toHaveBeenLastCalledWith({ kind: 'split', dataset: 'demo', snapshot, revision: old, split: 'train' });
  expect(screen.getAllByRole('button', { name: 'train' })).toHaveLength(2);
  expect(screen.queryByText(/Samples/)).toBeNull();
  expect(screen.queryByText('No children')).toBeNull();
  expect(screen.queryByText('No Split revisions')).toBeNull();
  expect(api.history).toHaveBeenCalledTimes(1);
});
