// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

import { act, fireEvent, render, screen, waitFor } from '@testing-library/react';
import { afterEach, beforeEach, expect, it, vi } from 'vitest';
import { listNodeCandidates, nodeAdmissions, nodeAutoJoin, setNodeAutoJoin, type NodeAdmission } from '../api';
import { CandidateProvider } from './NodeCandidatesContext';
import { CandidateProperties } from './CandidateProperties';
import { DomainProvider } from '../contexts/DomainContext';
import { SelectionProvider } from '../contexts/SelectionContext';
import { CandidateNodes } from './CandidateNodes';

vi.mock('../api', () => ({ listNodeCandidates: vi.fn(), nodeAdmissions: vi.fn(), nodeAutoJoin: vi.fn(),
  setNodeAutoJoin: vi.fn(), cancelNodeAdmission: vi.fn(), discardCandidateReview: vi.fn() }));
const advertisement = (id: string, cluster: string | null = null) => ({ discovery_id: id, protocol_version: 1,
  monitor_endpoints: [`http://${id}:9095`], cluster_id: cluster });
const policy = { enabled: true, candidates: {}, target_cluster_id: 'cluster', mode: 'standard' as const,
  required_voters: 3, observation: { phase: 'ready' } };
const member = { discovery_id: 'owned', confirmed: true, cancelled: false, node_id: 1 } as NodeAdmission;
beforeEach(() => {
  vi.mocked(listNodeCandidates).mockResolvedValue({ local: { advertisement: advertisement('owned', 'cluster'),
    physical_host_id: 'host', rack_hint: null }, candidates: [
      { advertisement: advertisement('owned', 'cluster'), state: 'same_cluster' },
      { advertisement: advertisement('fresh'), state: 'unbound' },
      { advertisement: advertisement('other', 'another'), state: 'foreign_cluster' },
    ], diagnostics: ['Announce("internal")', 'Respond("eth0")'] });
  vi.mocked(nodeAdmissions).mockResolvedValue([member]);
  vi.mocked(nodeAutoJoin).mockResolvedValue(policy);
});
afterEach(() => { vi.useRealTimers(); vi.resetAllMocks(); });

function renderCandidates() {
  return render(<DomainProvider><SelectionProvider><CandidateProvider active><div className="crowdb-console">
    <aside aria-label="Candidate sidebar"><CandidateNodes racks={[{ id: 1, name: 'Default', nodes: [] }]} deployment={{ phase: 'active', available: true }} /></aside>
    <CandidateProperties id="fresh" />
  </div></CandidateProvider></SelectionProvider></DomainProvider>);
}
it('inherits enabled Auto policy and synchronizes menu and properties without row controls', async () => {
  vi.mocked(setNodeAutoJoin).mockResolvedValue({ ...policy, candidates: { fresh: false } });
  renderCandidates();
  const row = await screen.findByTestId('tree-node-candidate-fresh');
  await waitFor(() => expect(screen.getByLabelText('Auto join')).toHaveValue('true'));
  expect(screen.queryByRole('checkbox')).toBeNull();
  expect(screen.queryByText(/Announce|Respond/)).toBeNull();
  expect(screen.queryByTestId('tree-node-candidate-owned')).toBeNull();
  expect(screen.queryByTestId('tree-node-candidate-other')).toBeNull();
  expect(row).not.toHaveTextContent('http://fresh');
  fireEvent.contextMenu(row.querySelector('[role="treeitem"]')!);
  expect(screen.getByRole('menu').closest('.crowdb-console')).not.toBeNull();
  expect(screen.getByRole('complementary', { name: 'Candidate sidebar' })).not.toContainElement(screen.getByRole('menu'));
  fireEvent.click(screen.getByRole('menuitem', { name: 'Disable Auto join' }));
  await waitFor(() => expect(screen.getByLabelText('Auto join')).toHaveValue('false'));
  expect(setNodeAutoJoin).toHaveBeenCalledWith(false, undefined, 'fresh');
  expect(screen.getByLabelText('Auto join disabled')).toBeVisible();
});
it('removes confirmed candidates while retaining their admission properties', async () => {
  vi.useFakeTimers(); renderCandidates(); await act(async () => {});
  expect(screen.getByTestId('tree-node-candidate-fresh')).toBeVisible();
  vi.mocked(nodeAdmissions).mockResolvedValue([member, { ...member, discovery_id: 'fresh', node_id: 2 }]);
  await act(async () => { await vi.advanceTimersByTimeAsync(2000); });
  expect(screen.queryByTestId('tree-node-candidate-fresh')).toBeNull();
  expect(screen.getByText('Confirmed')).toBeVisible();
});
it('updates an open candidate menu when admission is accepted by a later observation', async () => {
  vi.useFakeTimers(); renderCandidates(); await act(async () => {});
  fireEvent.contextMenu(screen.getByRole('button', { name: 'node-fresh' }));
  expect(screen.getByRole('menuitem', { name: 'Join node…' })).toBeEnabled();
  vi.mocked(nodeAdmissions).mockResolvedValue([member, { ...member, discovery_id: 'fresh', confirmed: false, node_id: 2 }]);
  await act(async () => { await vi.advanceTimersByTimeAsync(2000); });
  expect(screen.queryByRole('menuitem', { name: 'Join node…' })).toBeNull();
  expect(screen.getByRole('menuitem', { name: 'Cancel admission' })).toBeEnabled();
});
it('retains observations but disables policy changes during admission failure', async () => {
  vi.useFakeTimers(); renderCandidates(); await act(async () => {});
  vi.mocked(nodeAdmissions).mockRejectedValue(new Error('authority unavailable'));
  await act(async () => { await vi.advanceTimersByTimeAsync(2000); });
  expect(screen.getByTestId('tree-node-candidate-fresh')).toBeVisible();
  expect(screen.getByRole('status')).toHaveTextContent('last observations shown');
  expect(screen.getByLabelText('Auto join')).toBeDisabled();
});
it('keeps manual admission in the keyboard menu when policy is unknown', async () => {
  vi.mocked(nodeAutoJoin).mockRejectedValue(new Error('Auto join unavailable'));
  renderCandidates();
  await screen.findByText('Auto join settings unavailable.');
  expect(screen.getByLabelText('Auto join')).toHaveValue('unknown');
  expect(screen.getByLabelText('Auto join')).toBeDisabled();
  const button = screen.getByRole('button', { name: 'node-fresh' }); button.focus();
  fireEvent.keyDown(button, { key: 'F10', shiftKey: true });
  expect(screen.getByRole('menuitem', { name: 'Join node…' })).toBeEnabled();
  expect(screen.getByRole('menuitem', { name: 'Enable Auto join' })).toBeDisabled();
});
