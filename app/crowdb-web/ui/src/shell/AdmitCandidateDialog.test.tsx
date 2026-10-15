// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

import { fireEvent, render, screen, waitFor, within } from '@testing-library/react';
import { beforeEach, expect, it, vi } from 'vitest';
import { admitCandidate, prepareCandidate, updateCandidate, discardCandidateReview } from '../api';
import { AdmitCandidateDialog } from './AdmitCandidateDialog';
import type { DiskInventory } from '../storage/types';
import { defaultNodeSetup } from './nodeSetup';
vi.mock('../api', () => ({ admitCandidate: vi.fn(), prepareCandidate: vi.fn(), updateCandidate: vi.fn(), discardCandidateReview: vi.fn() }));
const empty: DiskInventory = { observed_at_ms: 1, disks: [], filesystems: [] };
beforeEach(() => {
  vi.clearAllMocks();
  vi.mocked(prepareCandidate).mockResolvedValue({ operation_id: 'fixed-admission', inventory: empty });
  vi.mocked(admitCandidate).mockResolvedValue({});
  vi.mocked(discardCandidateReview).mockResolvedValue();
});

it('reviews authenticated storage before one immutable submission with all services and the default file', async () => {
  const close = vi.fn();
  render(<AdmitCandidateDialog discoveryId="candidate" racks={[{ id: 1, name: 'rack', nodes: [] }]} onClose={close} />);
  fireEvent.click(screen.getByRole('button', { name: 'Verify and review' }));
  await screen.findByText(/Access verified/);
  expect(admitCandidate).not.toHaveBeenCalled();
  expect(screen.getByLabelText('Initial SSH password')).toHaveValue('');
  expect(within(screen.getByRole('region', { name: 'Node service choices' })).getAllByRole('checkbox')).toHaveLength(6);
  expect(screen.getByLabelText('Managed file capacity (GiB)')).toHaveValue('16');
  expect(screen.getByLabelText('Sparse file')).not.toBeChecked();
  vi.mocked(admitCandidate).mockRejectedValueOnce(new Error('response lost'));
  fireEvent.click(screen.getByRole('button', { name: 'Join and start' }));
  await screen.findByText('response lost');
  const accepted = vi.mocked(admitCandidate).mock.calls[0][0];
  expect(accepted.ssh_password).toBe('crowdb');
  expect(accepted.preflight_operation_id).toBe('fixed-admission');
  expect(accepted.setup?.services.every(service => service.enabled)).toBe(true);
  expect(accepted.setup?.storage).toEqual({ mode: 'automatic', capacity_bytes: 16 * 1024 ** 3, reserve_bytes: 1024 ** 3, sparse: false });
  expect(screen.getByLabelText('Managed file capacity (GiB)')).toBeDisabled();
  fireEvent.click(screen.getByRole('button', { name: 'Retry same setup' }));
  await waitFor(() => expect(close).toHaveBeenCalledOnce());
  expect(vi.mocked(admitCandidate).mock.calls[1][0]).toEqual(accepted);
  expect(prepareCandidate).toHaveBeenCalledOnce();
});

it('requires raw-device agreement and never offers a fallback file for supplied inputs', async () => {
  vi.mocked(prepareCandidate).mockResolvedValue({ operation_id: 'raw-admission', inventory: { ...empty, disks: [
    { path: '/opt/crowdb/data/disks/raw', backend: 'block-device', capacity_bytes: 64 * 1024 ** 2,
      allocated_bytes: null, backing_identity: 'device:7', supplied: true, error: null, backing_filesystem: null },
    { path: '/opt/crowdb/data/disks/missing', backend: 'file', capacity_bytes: null,
      allocated_bytes: null, backing_identity: null, supplied: true, error: 'mapping missing', backing_filesystem: null },
  ] } });
  render(<AdmitCandidateDialog discoveryId="candidate" racks={[{ id: 1, nodes: [] }]} onClose={vi.fn()} />);
  fireEvent.click(screen.getByRole('button', { name: 'Verify and review' }));
  await screen.findByText(/Access verified/);
  expect(screen.queryByRole('option', { name: 'Create a managed file' })).toBeNull();
  expect(screen.getByLabelText('Use /opt/crowdb/data/disks/raw')).toBeChecked();
  expect(screen.getByLabelText('Use /opt/crowdb/data/disks/missing')).toBeDisabled();
  expect(screen.getByRole('button', { name: 'Join and start' })).toBeDisabled();
  fireEvent.click(screen.getByRole('checkbox', { name: /I authorize initialization/ }));
  expect(screen.getByRole('button', { name: 'Join and start' })).toBeEnabled();
  fireEvent.change(screen.getByLabelText('Storage setup'), { target: { value: 'diskless' } });
  fireEvent.click(screen.getByRole('button', { name: 'Join and start' }));
  await waitFor(() => expect(admitCandidate).toHaveBeenCalledOnce());
  expect(vi.mocked(admitCandidate).mock.calls[0][0].setup?.storage).toEqual({ mode: 'diskless' });
});

it('updates an existing node through the original authenticated operation', async () => {
  vi.mocked(updateCandidate).mockResolvedValue({});
  render(<AdmitCandidateDialog discoveryId="candidate" racks={[{ id: 1, nodes: [] }]} onClose={vi.fn()} current={{ discovery_id: 'candidate',
    node_id: 1, rack_id: 1, host: 'host', physical_host_id: 'host-id', ssh_user: 'crowdb', ssh_port: 2222,
    operation_id: 'admission', confirmed: true, cancelled: false }} />);
  fireEvent.click(screen.getByRole('button', { name: 'Verify and update' }));
  await waitFor(() => expect(updateCandidate).toHaveBeenCalledOnce());
  expect(prepareCandidate).not.toHaveBeenCalled();
  expect(admitCandidate).not.toHaveBeenCalled();
});

it('discarding a read-only review cannot submit or cancel an accepted node plan', async () => {
  const close = vi.fn();
  render(<AdmitCandidateDialog discoveryId="candidate" racks={[{ id: 1, nodes: [] }]} onClose={close} />);
  fireEvent.click(screen.getByRole('button', { name: 'Verify and review' }));
  await screen.findByText(/Access verified/);
  fireEvent.click(screen.getByRole('button', { name: 'Cancel' }));
  await waitFor(() => expect(close).toHaveBeenCalledOnce());
  expect(discardCandidateReview).toHaveBeenCalledWith('candidate', 'fixed-admission');
  expect(admitCandidate).not.toHaveBeenCalled();
});

it('resumes previously accepted choices without replacing them with new defaults', async () => {
  const choices = defaultNodeSetup();
  choices.storage = { mode: 'diskless' };
  choices.services.forEach(service => { service.enabled = false; });
  vi.mocked(prepareCandidate).mockResolvedValue({ operation_id: 'fixed-admission', inventory: empty, accepted_choices: choices });
  render(<AdmitCandidateDialog discoveryId="candidate" racks={[{ id: 1, nodes: [] }]} onClose={vi.fn()} />);
  fireEvent.click(screen.getByRole('button', { name: 'Verify and review' }));
  await screen.findByText(/Previously accepted choices are fixed/);
  expect(screen.getByLabelText('Storage setup')).toBeDisabled();
  expect(screen.getByLabelText('Storage setup')).toHaveValue('diskless');
  fireEvent.click(screen.getByRole('button', { name: 'Join and start' }));
  await waitFor(() => expect(admitCandidate).toHaveBeenCalledOnce());
  expect(vi.mocked(admitCandidate).mock.calls[0][0].setup).toEqual(choices);
});
