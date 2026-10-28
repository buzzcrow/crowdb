// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

import { useState } from 'react';
import { buttonClass } from '../../access/Workbench';
import * as api from '../api';
import { useQuery } from '../query';
import type { DatasetSelection } from '../types';
export function History({ dataset, snapshot, active, refresh, select }: { dataset: string; snapshot: string; active: boolean; refresh: number; select: (value: DatasetSelection) => void }) {
  const [open, setOpen] = useState(false); const [cursor, setCursor] = useState<string>(); const [chosen, setChosen] = useState<string>();
  const history = useQuery(`${dataset}:${snapshot}:${cursor}:${refresh}`, active && open, signal => api.history(dataset, snapshot, cursor, signal));
  const current = useQuery(`${dataset}:${snapshot}:${refresh}`, active && open, signal => api.splitHead(dataset, snapshot, signal));
  const revision = useQuery(`${dataset}:${snapshot}:${chosen}`, active && open && !!chosen, signal => api.revision(dataset, snapshot, chosen!, signal));
  return <section className="tw-space-y-2 tw-text-xs">
    <button className={buttonClass} aria-expanded={open} onClick={() => setOpen(!open)}>Split revision history</button>
    {open && <>
      {history.error && <p role="alert">{history.error}</p>}
      <ul>{history.value?.entries.map(item => <li key={item.revision}><button className="tw-text-accent" onClick={() => { setChosen(item.revision); select({ kind: 'revision', dataset, snapshot, revision: item.revision }); }}>{item.revision} {current.value?.revision === item.revision && 'Current'}</button></li>)}</ul>
      <button className={buttonClass} disabled={!history.value?.after} onClick={() => setCursor(history.value!.after!)}>Next revisions</button>
      {revision.error && <p role="alert">{revision.error}</p>}
      {revision.value?.splits.map(split => <button key={split.name} className={buttonClass} onClick={() => select({ kind: 'split', dataset, snapshot, revision: revision.value!.revision, split: split.name })}>{split.name} ({split.count})</button>)}
    </>}
  </section>;
}
