// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
import { useState } from 'react';
import { Dialog } from '../components/Dialog';
import { useQuery } from './query';
import * as api from './api';
import type { DatasetInfo } from './types';

export function DeleteDataset({ dataset: target, readonly, close, complete }: { dataset: string; readonly: boolean; close: () => void; complete: () => void }) {
  const [dataset] = useState(target);
  const info = useQuery(dataset, true, signal => api.control<DatasetInfo>(api.datasetPath(dataset), 'GET', undefined, signal));
  const [busy, setBusy] = useState(false); const [error, setError] = useState('');
  const remove = async () => {
    if (!info.value || readonly) return;
    setBusy(true); setError('');
    try {
      await api.mutate('dataset-delete', api.datasetPath(dataset), 'DELETE', { id: info.value.id });
      complete();
    } catch (reason) {
      setError(reason instanceof api.DatasetError && reason.message.includes('dataset_not_empty')
        ? 'Dataset is not empty. Snapshots, uploads, or pending creation and cleanup must be cleared first.'
        : `${String(reason)} Retry uses the same Dataset ID; it cannot delete a replacement Dataset.`);
    } finally { setBusy(false); }
  };
  return <Dialog isOpen title="Delete Dataset" description={`Target: ${dataset}`} onClose={() => { if (!busy) close(); }} confirmLabel="Delete Dataset" confirmDisabled={readonly || !info.value} confirmLoading={busy} onConfirm={remove} destructive>
    <p>Only an empty Dataset can be deleted. It disappears immediately; cleanup follows the configured grace period (20 minutes by default). Its name remains reserved until cleanup completes.</p>
    {info.value && <p>Dataset ID: {info.value.id}</p>}
    {info.loading && <p role="status">Loading Dataset identity…</p>}
    {(error || info.error) && <p role="alert">{error || info.error}</p>}
  </Dialog>;
}
