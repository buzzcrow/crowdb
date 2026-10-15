// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

import { useState } from 'react';
import { Workbench, buttonClass, inputClass } from '../access/Workbench';
import { useDomain, useNavigationSnapshot } from '../contexts/DomainContext';
import { Domain } from '../types';
import * as api from '../dataset/api';
import { useQuery } from '../dataset/query';
import type { DatasetSelection, SnapshotSummary } from '../dataset/types';
import { SnapshotTree, type TreeState, type TreeEntry } from '../dataset/Tree';
import { Matrix, emptyWindow, type MatrixWindow } from '../dataset/Matrix';
import { Actions } from '../dataset/Actions';
import { History } from '../dataset/splits/History';
import { Properties } from '../dataset/Properties';

export function DatasetView({ active, readonly, showActivity = true }: { active: boolean; readonly: boolean; showActivity?: boolean }) {
  const [selected, setSelected] = useState<DatasetSelection>({ kind: 'root' });
  const [tree, setTree] = useState<TreeState>({});
  const updateTree = (key: string, value: TreeEntry) => setTree(previous => Object.fromEntries([...Object.entries(previous).filter(([name]) => name !== key).slice(-63), [key, value]]));
  const [windows, setWindows] = useState<Record<string, MatrixWindow>>({});
  const windowKey = JSON.stringify('snapshot' in selected ? [selected.dataset, selected.snapshot, selected.revision, selected.split] : []);
  const updateWindow = (value: MatrixWindow) => setWindows(previous => Object.fromEntries([...Object.entries(previous).filter(([key]) => key !== windowKey).slice(-31), [windowKey, value]]));
  const [cursor, setCursor] = useState<string>(); const [refresh, setRefresh] = useState(0);
  const [name, setName] = useState(''); const [error, setError] = useState(''); const [busy, setBusy] = useState(false);
  const { checkpoint } = useDomain();
  const select = (value: DatasetSelection) => { checkpoint(); setSelected(value); };
  useNavigationSnapshot(Domain.Dataset, 'workspace', () => () => { setSelected(selected); setCursor(cursor); setWindows(windows); setTree(tree); });
  const catalog = useQuery(`${cursor}:${refresh}`, active, signal => api.listDatasets(cursor, signal));
  const capabilities = useQuery(`capabilities:${refresh}`, active, signal => api.capabilities(signal));
  const create = async () => {
    setBusy(true); setError('');
    try { await api.mutate('dataset-management', api.datasetPath(name), 'PUT'); setRefresh(value => value + 1); select({ kind: 'dataset', dataset: name }); }
    catch (reason) { setError(String(reason)); } finally { setBusy(false); }
  };
  return <Workbench showActivity={showActivity} sidebar={<nav aria-label="Dataset tree" className="tw-space-y-3 tw-text-sm">
    <button className="tw-font-semibold" onClick={() => select({ kind: 'root' })}>Datasets</button>
    {catalog.value?.entries.map(dataset => <div key={dataset.id}>
      <button onClick={() => select({ kind: 'dataset', dataset: dataset.name })}>{dataset.name}</button>
      {selected.kind !== 'root' && selected.dataset === dataset.name && <SnapshotTree tree={tree} updateTree={updateTree} dataset={dataset.name} active={active} refresh={refresh} select={select} />}
    </div>)}
    {catalog.value?.after && <button className={buttonClass} onClick={() => setCursor(catalog.value!.after!)}>Next Datasets</button>}
    {cursor && <button className={buttonClass} onClick={() => setCursor(undefined)}>First Datasets</button>}
  </nav>} detail={<Properties selected={selected} active={active} />}>
    <div className="tw-flex tw-items-center tw-justify-between"><h1 className="tw-text-xl tw-font-semibold">Dataset{selected.kind !== 'root' && ` · ${selected.dataset}`}</h1>
      <button className={buttonClass} onClick={() => { setCursor(undefined); setWindows({}); setRefresh(value => value + 1); }}>Refresh Dataset</button></div>
    {(catalog.error || capabilities.error || error) && <p role="alert">{catalog.error || capabilities.error || error}</p>}
    <Actions key={`actions:${selected.kind === 'root' ? 'root' : selected.dataset}`} selected={selected} readonly={readonly} capabilities={capabilities.value ?? []} complete={receipt => { setRefresh(value => value + 1); if (receipt && selected.kind !== 'root') select({ kind: receipt.target.kind === 'snapshot' ? 'snapshot' : 'revision', dataset: selected.dataset, snapshot: receipt.target.snapshot, ...(receipt.target.kind === 'split_revision' ? { revision: receipt.target.revision } : {}) }); }} />
    {selected.kind === 'root' ? <>
      <p className="tw-text-sm tw-text-muted">Namespace: dataset-ns</p>
      {!readonly && <form className="tw-flex tw-gap-2" onSubmit={event => { event.preventDefault(); void create(); }}>
        <input className={inputClass} aria-label="New Dataset name" required maxLength={255} value={name} onChange={event => setName(event.target.value)} />
        <button className={buttonClass} disabled={busy || !capabilities.value?.includes('dataset-management')}>Create Dataset</button>
      </form>}
      {catalog.value?.entries.length === 0 && <p>No Datasets. Create one or import through the SDK.</p>}
      <table className="tw-w-full tw-text-sm"><thead><tr><th>Name</th><th>Dataset ID</th><th>Latest</th><th>Stable</th></tr></thead><tbody>
        {catalog.value?.entries.map(dataset => <tr key={dataset.id}><td><button className="tw-text-accent" onClick={() => select({ kind: 'dataset', dataset: dataset.name })}>{dataset.name}</button></td><td>{dataset.id}</td><td>{dataset.latest ?? 'None'}</td><td>{dataset.stable ?? 'None'}</td></tr>)}
      </tbody></table>
    </> : selected.kind === 'dataset' ? <Inventory key={selected.dataset} dataset={selected.dataset} active={active} refresh={refresh} select={select} /> : <>
      <SnapshotOverview selected={selected} active={active} refresh={refresh} select={select} />
      <History dataset={selected.dataset} snapshot={selected.snapshot} active={active} refresh={refresh} select={select} />
      <Matrix window={windows[windowKey] ?? emptyWindow} updateWindow={updateWindow} scope={selected} active={active} refresh={refresh} select={select} />
    </>}
  </Workbench>;
}
function Inventory({ dataset, active, refresh, select }: { dataset: string; active: boolean; refresh: number; select: (value: DatasetSelection) => void }) {
  const [cursor, setCursor] = useState<string>(); const [direct, setDirect] = useState(''); const [error, setError] = useState('');
  const result = useQuery(`${dataset}:${cursor}:${refresh}`, active, signal => api.page<SnapshotSummary>(`${api.datasetPath(dataset)}/snapshot-inventory`, cursor, 20, signal));
  const head = useQuery(`${dataset}:${refresh}`, active, signal => api.head(dataset, signal));
  return <section className="tw-space-y-3">
    <p className="tw-text-sm">Published Snapshots · parent relationships are in the tree.</p>
    <form className="tw-flex tw-gap-2" onSubmit={event => { event.preventDefault(); try { api.opaqueId(direct); select({ kind: 'snapshot', dataset, snapshot: direct }); setError(''); } catch (reason) { setError(String(reason)); } }}>
      <input className={inputClass} aria-label="Snapshot ID" value={direct} onChange={event => setDirect(event.target.value)} /><button className={buttonClass}>Open Snapshot</button>
    </form>
    {(result.error || error) && <p role="alert">{result.error || error}</p>}
    {result.value?.entries.length === 0 && <p>No published Snapshots.</p>}
    <table className="tw-w-full tw-text-xs"><thead><tr><th>Snapshot</th><th>Parent</th><th>Samples</th><th>References</th></tr></thead><tbody>
      {result.value?.entries.map(item => <tr key={item.id}><td><button className="tw-text-accent" onClick={() => select({ kind: 'snapshot', dataset, snapshot: item.id })}>{item.id}</button></td><td>{item.parent ?? 'Root'}</td><td>{item.sample_count}</td><td>{head.value?.latest === item.id && 'Latest '}{head.value?.stable === item.id && 'Stable'}</td></tr>)}
    </tbody></table>
    <button className={buttonClass} disabled={!result.value?.after} onClick={() => setCursor(result.value!.after!)}>Next Snapshots</button>
    {cursor && <button className={buttonClass} onClick={() => setCursor(undefined)}>First Snapshots</button>}
  </section>;
}
function SnapshotOverview({ selected, active, refresh, select }: { selected: Exclude<DatasetSelection, { kind: 'root' | 'dataset' }>; active: boolean; refresh: number; select: (value: DatasetSelection) => void }) {
  const result = useQuery(`${selected.dataset}:${selected.snapshot}:${refresh}`, active, signal => api.summary(selected.dataset, selected.snapshot, signal));
  return <section className="tw-text-sm tw-space-y-2">
    <h2 className="tw-font-semibold tw-break-all">Snapshot {selected.snapshot}</h2>
    {result.error && <p role="alert">{result.error} This immutable scope remains selected.</p>}
    {result.value && <><p>{result.value.sample_count} Samples · Schema v{result.value.schema_version} · {result.value.field_count} fields</p>
      {result.value.parent ? <button className="tw-text-accent" onClick={() => select({ kind: 'snapshot', dataset: selected.dataset, snapshot: result.value!.parent! })}>Parent {result.value.parent}</button> : <p>Independent root</p>}
      {result.value.inspection !== 'ready' && <p role="status">Inspection unavailable: {result.value.reason}</p>}</>}
    {selected.revision && <p>Split revision: {selected.revision} {selected.split && `· ${selected.split}`}</p>}
  </section>;
}
