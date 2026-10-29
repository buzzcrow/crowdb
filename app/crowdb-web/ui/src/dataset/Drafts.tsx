// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
import { useEffect, useState } from 'react';
import { Tree } from '../components/Tree';
import { savedDatasetCreations, savedOperations, type Operation } from './operation';
import { useQuery } from './query';

export function Drafts({ dataset, snapshot, active, open }: { dataset?: string; snapshot?: string; active: boolean; open: (op: Operation) => void }) {
  const [version, setVersion] = useState(0);
  useEffect(() => {
    const update = () => setVersion(value => value + 1);
    window.addEventListener('crowdb-dataset-drafts', update); window.addEventListener('focus', update);
    return () => { window.removeEventListener('crowdb-dataset-drafts', update); window.removeEventListener('focus', update); };
  }, []);
  const result = useQuery(`${dataset}:${version}`, active, () => dataset === undefined ? savedDatasetCreations() : savedOperations(dataset));
  const drafts = result.value?.filter(op => snapshot ? op.kind === 'split' && op.snapshot === snapshot : op.kind === 'snapshot') ?? [];
  return <div aria-label={dataset === undefined ? 'Local Dataset Drafts' : snapshot ? 'Local Split Drafts' : 'Local Snapshot Drafts'}>
    {result.error && <p role="alert">{result.error}</p>}
    <Tree nodes={drafts.map(op => ({ id: op.resource, type: 'Dataset', label: dataset === undefined ? `Dataset Draft ${op.dataset} · ${op.resource.slice(0, 8)}` : `${op.kind === 'snapshot' ? 'Snapshot' : 'Split'} Draft ${op.resource.slice(0, 8)}`, title: op.resource,
      suffix: <span className="tw-text-xs tw-text-muted">{op.editing ? 'Local' : 'Pending Commit'}</span>, activate: () => open(op) }))} />
  </div>;
}
