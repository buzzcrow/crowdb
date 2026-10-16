// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

import { act, fireEvent, render, screen, waitFor } from '@testing-library/react';
import { expect, it, vi } from 'vitest';
import { joinNodeCandidate } from '../api';
import { JoinCandidatesDialog } from './JoinCandidatesDialog';
import type { DiskObservation, JoinCandidateRequest, SuppliedOperation } from './types';
vi.mock('../api', () => ({ joinNodeCandidate: vi.fn() }));
const disks: DiskObservation[] = ['first', 'second', 'raw', 'unverified'].map((name, index) => ({ path: `/disks/${name}`,
  backend: name === 'raw' ? 'block-device' : 'file', supplied: true, error: null, backing_identity: name === 'raw' ? 'device:7' : `file:1:${index}`,
  resource_identity: name === 'unverified' ? undefined : `file:1:${index}`, capacity_bytes: 65 * 1024 ** 2 + 512, allocated_bytes: 0, backing_filesystem: null }));
function outcome(request: JoinCandidateRequest): SuppliedOperation { return { ...request, node_id: 1, disk_id: 'disk', phase: 'accepted', verified_bytes: 0, error: null }; }
function choose(path: string) {
  fireEvent.click(screen.getByRole('checkbox', { name: `Join ${path}` }));
  fireEvent.change(screen.getByLabelText(`Disk group for ${path}`), { target: { value: '7' } });
}
it('retains successful rows and reuses the sealed operation after response loss', async () => {
  const accept = vi.fn();
  vi.mocked(joinNodeCandidate).mockImplementation(async (_, request) => {
    if (request.medium.path === '/disks/second') throw new Error('response lost');
    return outcome(request);
  });
  render(<JoinCandidatesDialog nodeId={1} disks={disks} groups={[{ id: 7, rack_id: 1, node_id: 1 }]} onClose={vi.fn()} onAccepted={accept} />);
  expect(screen.getByRole('checkbox', { name: 'Join /disks/unverified' })).toBeDisabled();
  choose('/disks/first'); choose('/disks/second');
  fireEvent.click(screen.getByRole('checkbox', { name: 'Initialize the selected blank media and register them in Group 0' }));
  fireEvent.click(screen.getByRole('button', { name: 'Join selected disks' }));
  await screen.findByRole('alert');
  await waitFor(() => expect(screen.getByRole('button', { name: 'Retry unresolved submissions' })).toBeEnabled());
  const failed = vi.mocked(joinNodeCandidate).mock.calls.find(([, request]) => request.medium.path === '/disks/second')![1];
  vi.mocked(joinNodeCandidate).mockImplementation(async (_, request) => outcome(request));
  fireEvent.click(screen.getByRole('button', { name: 'Retry unresolved submissions' }));
  await screen.findByRole('button', { name: 'View node progress' });
  expect(joinNodeCandidate).toHaveBeenCalledTimes(3);
  expect(joinNodeCandidate).toHaveBeenLastCalledWith(1, failed);
  expect(accept).toHaveBeenCalledTimes(2);
  expect(failed.medium).toMatchObject({ capacity_bytes: 65 * 1024 ** 2 + 512, resource_identity: 'file:1:1', initialize: true });
});
it('requires per-device consent and stops unsent rows when closed with four requests in flight', async () => {
  vi.mocked(joinNodeCandidate).mockReset();
  const inputs = Array.from({ length: 6 }, (_, index) => ({ ...disks[2], path: `/disks/raw-${index}`, backing_identity: `device:${index}`, resource_identity: `file:2:${index}` }));
  let resolve!: () => void;
  const pending = new Promise<void>(done => { resolve = done; });
  vi.mocked(joinNodeCandidate).mockImplementation(async (_, request) => { await pending; return outcome(request); });
  const view = render(<JoinCandidatesDialog nodeId={1} disks={inputs} groups={[{ id: 7, node_id: 1, rack_id: 1 }]} onClose={vi.fn()} onAccepted={vi.fn()} />);
  for (const disk of inputs) choose(disk.path);
  fireEvent.click(screen.getByRole('checkbox', { name: 'Initialize the selected blank media and register them in Group 0' }));
  expect(screen.getByRole('button', { name: 'Join selected disks' })).toBeDisabled();
  for (const disk of inputs) fireEvent.click(screen.getByRole('checkbox', { name: `Authorize initialization of ${disk.path}` }));
  fireEvent.click(screen.getByRole('button', { name: 'Join selected disks' }));
  expect(joinNodeCandidate).toHaveBeenCalledTimes(4);
  view.unmount();
  await act(async () => { resolve(); });
  expect(joinNodeCandidate).toHaveBeenCalledTimes(4);
});
