// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

import { useEffect, useMemo, useRef, useState } from 'react';
import { Dialog } from '../Dialog';
import { Input } from '../ui/Input';
import { useToast } from '../../contexts/ToastContext';
import { addDiskGroup, listDiskGroupBindings, listNodeDiskGroups, listStores } from '../../api';
import { minUnusedId } from './defaults';

export interface AddDiskGroupDialogProps {
  isOpen: boolean;
  onClose: () => void;
  nodeId: number;
  existingDgIds: number[];
  onSuccess?: () => void | Promise<void>;
}

export function AddDiskGroupDialog({
  isOpen,
  onClose,
  nodeId,
  existingDgIds,
  onSuccess,
}: AddDiskGroupDialogProps) {
  const [dgId, setDgId] = useState('');
  const [name, setName] = useState('');
  const [binding, setBinding] = useState('');
  const [groups, setGroups] = useState<{ value: string; label: string; count: number }[]>([]);
  const [recommended, setRecommended] = useState('');
  const [isLoading, setIsLoading] = useState(false);
  const [submitError, setSubmitError] = useState('');
  const [registered, setRegistered] = useState(false);
  const userEditedIdRef = useRef(false);
  const { success } = useToast();

  // Fetch fresh DG list when the dialog opens, then compute the next
  // available ID. This avoids reusing an existing active DG id even if
  // the polled nodeDiskGroups state is stale. Uses a ref to track user
  // edits so the async fetch doesn't clobber a user-typed value.
  useEffect(() => {
    if (!isOpen) return;
    setName('');
    setBinding('');
    setGroups([]);
    setRecommended('');
    const controller = new AbortController();
    Promise.all([listStores(1), listDiskGroupBindings()]).then(([stores, bindings]) => {
      if (controller.signal.aborted) return;
      const counts = new Map<string, number>();
      for (const entry of bindings) {
        const key = `${entry.store_id}/${entry.group_id}`;
        counts.set(key, (counts.get(key) ?? 0) + 1);
      }
      const choices = [...stores].sort((a, b) => Number(a.store_id) - Number(b.store_id)).flatMap(store =>
        [...store.groups].filter(group => Number(group.group_id) !== 0)
          .sort((a, b) => Number(a.group_id) - Number(b.group_id)).map(group => ({
            value: `${store.store_id}/${group.group_id}`,
            label: `Store ${store.store_id} / Group ${group.group_id}`,
            count: counts.get(`${store.store_id}/${group.group_id}`) ?? 0,
          })));
      const best = choices.reduce<typeof choices[number] | undefined>((best, group) =>
        !best || group.count < best.count ? group : best, undefined);
      setGroups(choices);
      setRecommended(best?.value ?? '');
      setBinding(best?.value ?? '');
    }).catch(err => {
      if (!controller.signal.aborted) setSubmitError('Could not load KV group distribution: ' + String(err));
    });

    setSubmitError('');
    setRegistered(false);
    userEditedIdRef.current = false;
    setDgId('');
    listNodeDiskGroups(nodeId)
      .then((dgs) => {
        if (controller.signal.aborted) return;
        const ids = dgs.map((dg) => dg.id);
        if (!userEditedIdRef.current) setDgId(minUnusedId([...existingDgIds, ...ids], 1));
      })
      .catch(() => {
        if (controller.signal.aborted) return;
        if (!userEditedIdRef.current) setDgId(minUnusedId(existingDgIds, 1));
      });
    return () => controller.abort();
  }, [isOpen, nodeId]);

  const defaultDgId = useMemo(() => minUnusedId(existingDgIds, 1), [existingDgIds]);

  // Fallback: if the fetch hasn't completed yet, use the polled default.
  useEffect(() => {
    if (isOpen && !dgId && !userEditedIdRef.current) {
      setDgId(defaultDgId);
    }
  }, [isOpen, dgId, defaultDgId]);

  const isNumeric = (v: string) => /^\d+$/.test(v.trim());
  const valid = isNumeric(dgId) && Number(dgId) > 0 && groups.some(group => group.value === binding);

  const handleSubmit = async () => {
    if (!valid) return;
    setIsLoading(true);
    setSubmitError('');
    userEditedIdRef.current = true;
    try {
      await addDiskGroup(nodeId, {
        id: Number(dgId.trim()),
        store_id: Number(binding.split('/')[0]),
        group_id: Number(binding.split('/')[1]),
        name: name.trim() || undefined,
      });
      success(`Disk-group ${dgId} created on node ${nodeId}`);
      onClose();
      await onSuccess?.();
    } catch (err) {
      const message = err instanceof Error ? err.message : 'Failed to create disk-group';
      setSubmitError(message);
      try {
        const groups = await listNodeDiskGroups(nodeId);
        const existing = groups.find(group => group.id === Number(dgId));
        setRegistered(!!existing);
        if (existing) {
          setSubmitError(`DiskGroup ${dgId} is registered, but setup did not complete. Keep this ID and name when retrying. ${message}`);
          await onSuccess?.();
        }
      } catch (refreshError) {
        setSubmitError(`${message} Could not confirm registration: ${refreshError instanceof Error ? refreshError.message : 'refresh failed'}. Refresh before retrying this ID.`);
      }
    } finally {
      setIsLoading(false);
    }
  };

  return (
    <Dialog
      isOpen={isOpen}
      onClose={onClose}
      title="Add Disk Group"
      description={`Create a new disk-group on node ${nodeId}. Disk-groups are the allocation units managed by DiskDB.`}
      confirmLabel={registered ? 'Retry Setup' : 'Create Disk Group'}
      onConfirm={handleSubmit}
      confirmDisabled={!valid || isLoading}
      confirmLoading={isLoading}
    >
      <div className="tw-space-y-4">
        {submitError && <p role="alert" className="tw-text-sm tw-text-failed">{submitError}</p>}
        <Input
          label="Disk Group ID (auto-assigned)"
          inputMode="numeric"
          value={dgId}
          onChange={(e) => { setDgId(e.target.value); userEditedIdRef.current = true; setRegistered(false); setSubmitError(''); }}
          autoFocus
        />
        <label className="tw-block tw-text-sm">
          KV group
          <select aria-label="KV group" value={binding} disabled={registered || isLoading}
            onChange={event => setBinding(event.target.value)}
            className="tw-mt-1 tw-block tw-w-full tw-rounded tw-border tw-border-border tw-bg-panel tw-p-2">
            <option value="">Select a KV group</option>
            {groups.map(group => <option key={group.value} value={group.value}>{group.label} · {group.count} disk groups{group.value === recommended ? ' · Recommended' : ''}</option>)}
          </select>
          <span className="tw-mt-1 tw-block tw-text-muted">Automatically recommends the KV group with the fewest bound disk groups across the cluster. You can change it before creation. Create an ordinary KV group first if none are available.</span>
        </label>
        <Input
          label="Name (optional)"
          placeholder="ssd-group-1"
          value={name}
          onChange={(e) => setName(e.target.value)}
        />
      </div>
    </Dialog>
  );
}
