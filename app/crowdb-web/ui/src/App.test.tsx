// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
import { act, fireEvent, render, screen } from '@testing-library/react';
import { afterEach, expect, it, vi } from 'vitest';
import App from './App';
import { Domain } from './types';
import * as api from './api';
const capacityRequests = vi.hoisted(() => vi.fn());
vi.mock('./shell/NodeCandidatesContext', () => ({ CandidateProvider: ({ children }: { children: import('react').ReactNode }) => children }));
vi.mock('./shell/CandidateNodes', () => ({ CandidateNodes: () => null }));

vi.mock('./api', async importOriginal => ({
  ...await importOriginal<typeof import('./api')>(),
  nodeDeploymentStatus: vi.fn(),
  listServers: vi.fn(async () => []),
}));
vi.mock('./data/useClusterTree', () => ({ useClusterTree: () => ({
  racks: [], nodes: [], services: [], nodeStores: {}, nodeHealthById: {}, nodeDiskGroups: {},
  loading: false, error: null, refresh: vi.fn(), loadNodeDisks: vi.fn(), loadGroupDisks: vi.fn(),
}) }));
vi.mock('./data/useLogicalTree', () => ({ useLogicalTree: () => ({
  stores: [{ store_id: 0, name: 'System', groups: [], nodes: [] }], groups: [], loading: false,
  error: null, refresh: vi.fn(),
}) }));
vi.mock('./data/useCapacityTree', () => ({ useCapacityTree: (options: unknown) => { capacityRequests(options); return ({
  instances: [], usage: null, hardwareCapacity: null, scanStatus: null, loading: false,
  error: null, refresh: vi.fn(),
}); } }));
vi.mock('./services/useNodeServicePlans', () => ({ useNodeServicePlans: () => ({ plans: {}, stop: vi.fn() }) }));
vi.mock('./views/ClusterView', () => ({ ClusterView: () => null }));
vi.mock('./views/KvView', () => ({ KvView: () => null }));
vi.mock('./views/CapacityView', () => ({ CapacityView: () => null }));
vi.mock('./chunk/ChunkBrowser', () => ({ ChunkBrowser: () => null }));
vi.mock('./chunk-kv/ChunkKvView', () => ({ ChunkKvView: () => null }));
vi.mock('./views/IcebergView', () => ({ IcebergView: () => null }));
vi.mock('./views/S3View', () => ({ S3View: () => null }));

afterEach(() => { vi.restoreAllMocks(); vi.unstubAllGlobals(); vi.resetAllMocks(); });

it('waits for initial node authority before offering store actions', async () => {
  let resolve!: (status: api.NodeDeploymentStatus) => void;
  vi.mocked(api.nodeDeploymentStatus).mockReturnValue(new Promise(done => { resolve = done; }));
  vi.stubGlobal('fetch', vi.fn(async () => new Response(JSON.stringify({ mode: 'node' }))));
  render(<App initialDomain={Domain.KV} />);
  await act(async () => {});
  expect(screen.queryByRole('button', { name: 'Add Store' })).not.toBeInTheDocument();
  await act(async () => resolve({ available: true, phase: 'active' }));
  fireEvent.contextMenu(screen.getByRole('button', { name: /^datacenter$/ }));
  fireEvent.click(screen.getByRole('menuitem', { name: 'Add Store' }));
  expect(screen.getByRole('dialog', { name: 'Add KV Store' })).toBeVisible();
});

it('shows a read-only console when the initial authority check fails', async () => {
  vi.mocked(api.nodeDeploymentStatus).mockRejectedValue(new Error('authority offline'));
  vi.stubGlobal('fetch', vi.fn(async () => new Response(JSON.stringify({ mode: 'node' }))));
  render(<App initialDomain={Domain.KV} />);
  await act(async () => {});
  expect(screen.queryByText('Loading cluster authority…')).not.toBeInTheDocument();
  expect(screen.getByRole('complementary', { name: 'Cluster tree sidebar' })).toBeVisible();
  expect(screen.queryByRole('button', { name: 'Add Store' })).not.toBeInTheDocument();
  expect(screen.getByRole('alert')).toHaveTextContent('Group 0 unavailable · confirmed view is stale');
});

it('automatically observes runtime capacity after selecting a Datacenter', async () => {
  vi.mocked(api.listServers).mockResolvedValue([]);
  vi.mocked(api.nodeDeploymentStatus).mockResolvedValue({ available: true, phase: 'active' });
  vi.stubGlobal('fetch', vi.fn(async () => new Response(JSON.stringify({ mode: 'node' }))));
  render(<App initialDomain={Domain.Cluster} />);
  await act(async () => {});
  expect(capacityRequests).toHaveBeenLastCalledWith(expect.objectContaining({ observeRuntime: false }));
  fireEvent.click(screen.getByRole('button', { name: /^datacenter$/ }));
  await act(async () => {});
  expect(capacityRequests).toHaveBeenLastCalledWith(expect.objectContaining({ observeRuntime: true, diskGroupId: undefined, diskId: undefined }));
});
