// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

import { fireEvent, render, screen, waitFor } from '@testing-library/react';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { createNodeDiskFile } from '../api';
import { CreateFileDiskDialog } from './CreateFileDiskDialog';
import type { CreateFileDiskRequest, FilePreparation } from './types';

vi.mock('../api', () => ({ createNodeDiskFile: vi.fn() }));
const create = vi.mocked(createNodeDiskFile);
const groups = [{ id: 7, rack_id: 1, node_id: 2, name: 'storage' }];
function accepted(request: CreateFileDiskRequest): FilePreparation {
  return { version: 1, request: { ...request, node_id: 2, cluster_id: 'one', reserve_bytes: 1024 ** 3,
    admission: { rack_id: 1, disk_group_id: 7, unit_size_bytes: 1024 ** 2, zone_size_bytes: request.capacity_bytes, media_type: null } },
    path: `/opt/crowdb/data/disks/managed-${request.operation_id}.img`, phase: 'accepted', backing_identity: null, preparation_complete: false, error: null };
}
beforeEach(() => { create.mockReset(); create.mockImplementation(async (_node, request) => accepted(request)); });

describe('managed file creation', () => {
  it('submits the default preallocation once and exposes durable acceptance', async () => {
    const onAccepted = vi.fn(); const onClose = vi.fn();
    render(<CreateFileDiskDialog nodeId={2} groups={groups} groupId={7} available={null} onAccepted={onAccepted} onClose={onClose} />);
    expect(screen.getByLabelText('Capacity (GiB)')).toHaveValue('16');
    expect(screen.getByLabelText('Sparse file')).not.toBeChecked();
    fireEvent.click(screen.getByRole('button', { name: 'Create and Join' }));
    await waitFor(() => expect(onAccepted).toHaveBeenCalledTimes(1));
    const [node, body] = create.mock.calls[0];
    expect(node).toBe(2); expect(body).toMatchObject({ disk_group_id: 7, capacity_bytes: 16 * 1024 ** 3, sparse: false });
    expect(body.operation_id).toMatch(/^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/);
    expect(onAccepted.mock.calls[0][0].phase).toBe('accepted');
    expect(onClose).toHaveBeenCalledTimes(1);
  });
  it('blocks invalid geometry and insufficient observed host space', () => {
    render(<CreateFileDiskDialog nodeId={2} groups={groups} groupId={7} available={2 * 1024 ** 3} onAccepted={vi.fn()} onClose={vi.fn()} />);
    expect(screen.getByRole('button', { name: 'Create and Join' })).toBeDisabled();
    fireEvent.change(screen.getByLabelText('Capacity (GiB)'), { target: { value: '-1' } });
    expect(screen.getByRole('button', { name: 'Create and Join' })).toBeDisabled();
    fireEvent.change(screen.getByLabelText('Capacity (GiB)'), { target: { value: '0.0000001' } });
    expect(screen.getByRole('button', { name: 'Create and Join' })).toBeDisabled();
    fireEvent.change(screen.getByLabelText('Capacity (GiB)'), { target: { value: '1' } });
    expect(screen.getByRole('button', { name: 'Create and Join' })).toBeEnabled();
    expect(create).not.toHaveBeenCalled();
  });
  it('requires bitmap-word and whole-zone capacity before submission', () => {
    render(<CreateFileDiskDialog nodeId={2} groups={groups} groupId={7} available={null} onAccepted={vi.fn()} onClose={vi.fn()} />);
    for (const [capacity, valid] of [['0.015625', false], ['0.0625', true], ['48', false], ['64', true]] as const) {
      fireEvent.change(screen.getByLabelText('Capacity (GiB)'), { target: { value: capacity } });
      const submit = screen.getByRole('button', { name: 'Create and Join' });
      if (valid) expect(submit).toBeEnabled(); else expect(submit).toBeDisabled();
    }
    expect(create).not.toHaveBeenCalled();
  });
  it('keeps identity and inputs fixed after an unconfirmed response', async () => {
    create.mockRejectedValueOnce(new Error('connection lost'));
    render(<CreateFileDiskDialog nodeId={2} groups={groups} groupId={7} available={null} onAccepted={vi.fn()} onClose={vi.fn()} />);
    fireEvent.click(screen.getByRole('button', { name: 'Create and Join' }));
    await screen.findByRole('alert');
    expect(screen.getByLabelText('Capacity (GiB)')).toBeDisabled();
    expect(screen.getByLabelText('Disk group')).toBeDisabled();
    fireEvent.click(screen.getByRole('button', { name: 'Retry same operation' }));
    await waitFor(() => expect(create).toHaveBeenCalledTimes(2));
    expect(create.mock.calls[1]).toEqual(create.mock.calls[0]);
  });
  it('requires an explicit sparse choice and reports its host-space meaning', async () => {
    render(<CreateFileDiskDialog nodeId={2} groups={groups} groupId={7} available={2 * 1024 ** 3} onAccepted={vi.fn()} onClose={vi.fn()} />);
    fireEvent.click(screen.getByText('Advanced allocation'));
    fireEvent.click(screen.getByLabelText('Sparse file'));
    expect(screen.getByText(/does not reserve host space/)).toBeVisible();
    fireEvent.click(screen.getByRole('button', { name: 'Create and Join' }));
    await waitFor(() => expect(create).toHaveBeenCalledTimes(1));
    expect(create.mock.calls[0][1].sparse).toBe(true);
  });
});
