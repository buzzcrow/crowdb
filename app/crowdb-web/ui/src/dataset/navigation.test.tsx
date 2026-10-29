// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
import { fireEvent, render, screen, within } from '@testing-library/react';
import { expect, it, vi } from 'vitest';
import { DomainProvider, useDomain } from '../contexts/DomainContext';
import { SelectionProvider } from '../contexts/SelectionContext';
import { Domain } from '../types';
import { DatasetView } from '../views/DatasetView';

const first = '1'.repeat(32); const second = '2'.repeat(32);
vi.mock('./api', async original => ({ ...await original<typeof import('./api')>(),
  listDatasets: vi.fn(async (cursor?: string) => ({ entries: [{ id: cursor ? second : first, name: cursor ? 'other' : 'demo', namespace: 'dataset-ns', latest: null, stable: null }], after: cursor ? null : 'next-dataset' })),
  capabilities: vi.fn(async () => []), head: vi.fn(async () => ({ latest: null, stable: null })),
  page: vi.fn(async (path: string, cursor?: string) => path.endsWith('/snapshot-inventory')
    ? { entries: [{ id: cursor ? second : first, parent: null, sample_count: '0' }], after: cursor ? null : 'next-snapshot' }
    : { entries: [], after: null }),
  summary: vi.fn(async () => ({ sample_count: '0', field_count: 0, schema_version: 1, inspection: 'ready' })),
}));
vi.mock('./Properties', () => ({ Properties: () => null }));
vi.mock('./Matrix', () => ({ Matrix: () => null, emptyWindow: {} }));
vi.mock('./Ancestry', () => ({ Ancestry: () => null }));
vi.mock('./splits/History', () => ({ History: () => null }));
function Workspace() {
  const { back } = useDomain();
  return <><button onClick={back}>Navigate back</button><DatasetView active readonly /></>;
}
function setup() {
  render(<DomainProvider initialDomain={Domain.Dataset}><SelectionProvider><Workspace /></SelectionProvider></DomainProvider>);
}
it('keeps a fixed root, locates the selected resource and restores center pagination without tree paging controls', async () => {
  setup();
  const tree = within(screen.getByRole('navigation', { name: 'Dataset tree' }));
  const list = () => within(screen.getByRole('table', { name: 'Datasets' }));
  await tree.findByRole('button', { name: 'demo' });
  fireEvent.click(screen.getByRole('button', { name: 'Next Datasets' }));
  fireEvent.click(await list().findByRole('button', { name: 'other' }));
  await screen.findByRole('table', { name: 'Snapshots' });
  expect(tree.getByRole('button', { name: 'other' })).toBeVisible();
  fireEvent.click(screen.getByRole('button', { name: 'Navigate back' }));
  await screen.findByRole('table', { name: 'Datasets' });
  expect(await list().findByRole('button', { name: 'other' })).toBeVisible();
  expect(tree.getByRole('button', { name: 'demo' })).toBeVisible();
  expect(tree.queryByRole('button', { name: 'Next tree page' })).toBeNull();
  fireEvent.click(screen.getByRole('button', { name: 'Previous Datasets' }));
  expect(await list().findByRole('button', { name: 'demo' })).toBeVisible();
  expect(tree.getByRole('button', { name: 'Datasets' })).toBeVisible();
});
it('restores the Snapshot inventory page after drilling into an immutable Snapshot', async () => {
  setup();
  const tree = within(screen.getByRole('navigation', { name: 'Dataset tree' }));
  fireEvent.click(await tree.findByRole('button', { name: 'demo' }));
  await screen.findByRole('button', { name: first });
  fireEvent.click(screen.getByRole('button', { name: 'Next Snapshots' }));
  fireEvent.click(await screen.findByRole('button', { name: second }));
  await screen.findByRole('heading', { name: `Snapshot ${second}` });
  fireEvent.click(screen.getByRole('button', { name: 'Navigate back' }));
  expect(await screen.findByRole('button', { name: second })).toBeVisible();
  fireEvent.click(screen.getByRole('button', { name: 'Previous Snapshots' }));
  expect(await screen.findByRole('button', { name: first })).toBeVisible();
});
