// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

import { resourceMenuKey } from '../components/resourceMenuKey';
import { useState } from 'react';
import { Workbench, buttonClass, inputClass } from '../access/Workbench';
import { useDomain, useNavigationSnapshot } from '../contexts/DomainContext';
import { Domain } from '../types';
import * as api from '../dataset/api';
import { useQuery } from '../dataset/query';
import type { DatasetSelection, SnapshotSummary } from '../dataset/types';
import { SnapshotTree, type TreeState, type TreeEntry } from '../dataset/Tree';
import { Matrix, emptyWindow, type MatrixWindow } from '../dataset/Matrix';
import { InspectionPreparation } from '../dataset/InspectionPreparation';
import { Ancestry } from '../dataset/Ancestry';
import { Tree } from '../components/Tree';
import { Dialog } from '../components/Dialog';
import { useDatasetMenu } from '../dataset/ResourceMenu';
import type { DatasetAction } from '../dataset/Actions';
import { Actions } from '../dataset/Actions';
import { DeleteDataset } from '../dataset/DeleteDataset';
import { History } from '../dataset/splits/History';
import { Properties } from '../dataset/Properties';

interface PageWindow { cursor?: string; previous: Array<string | undefined> }
const firstPage: PageWindow = { previous: [] };
export function DatasetView({ active, readonly }: { active: boolean; readonly: boolean; showActivity?: boolean }) {
  const [selected, setSelected] = useState<DatasetSelection>({ kind: 'root' });
  const [tree, setTree] = useState<TreeState>({});
  const updateTree = (key: string, value: TreeEntry) => setTree(previous => Object.fromEntries([...Object.entries(previous).filter(([name]) => name !== key).slice(-63), [key, value]]));
  const [windows, setWindows] = useState<Record<string, MatrixWindow>>({});
  const windowKey = JSON.stringify('snapshot' in selected ? [selected.dataset, selected.snapshot, selected.revision, selected.split] : []);
  const updateWindow = (value: MatrixWindow) => setWindows(previous => Object.fromEntries([...Object.entries(previous).filter(([key]) => key !== windowKey).slice(-31), [windowKey, value]]));
  const [creating, setCreating] = useState(false);
  const [operation, setOperation] = useState<{ selected: Exclude<DatasetSelection, { kind: 'root' }>; action: DatasetAction; nonce: number }>();
  const [previous, setPrevious] = useState<Array<string | undefined>>([]);
  const [cursor, setCursor] = useState<string>(); const [refresh, setRefresh] = useState(0);
  const [inventories, setInventories] = useState<Record<string, PageWindow>>({});
  const [name, setName] = useState(''); const [error, setError] = useState(''); const [busy, setBusy] = useState(false);
  const { checkpoint } = useDomain();
  const select = (value: DatasetSelection) => { checkpoint(); setSelected(value); setOperation(undefined); };
  useNavigationSnapshot(Domain.Dataset, 'workspace', () => () => { setSelected(selected); setCursor(cursor); setPrevious(previous); setWindows(windows); setTree(tree); setInventories(inventories); });
  const treeCatalog = useQuery(`tree:${refresh}`, active, signal => api.listDatasets(undefined, signal));
  const pagedCatalog = useQuery(`${cursor}:${refresh}`, active && !!cursor, signal => api.listDatasets(cursor, signal));
  const catalog = cursor ? pagedCatalog : treeCatalog;
  const treeNames = (treeCatalog.value?.entries ?? []).map(dataset => dataset.name);
  if (selected.kind !== 'root' && !treeNames.includes(selected.dataset)) treeNames.push(selected.dataset);
  const capabilities = useQuery(`capabilities:${refresh}`, active, signal => api.capabilities(signal));
  const create = async () => {
    setBusy(true); setError('');
    try { await api.mutate('dataset-management', api.datasetPath(name), 'PUT'); setRefresh(value => value + 1); select({ kind: 'dataset', dataset: name }); setCreating(false); }
    catch (reason) { setError(String(reason)); } finally { setBusy(false); }
  };
  const resourceMenu = useDatasetMenu(readonly, capabilities.value ?? [], () => { setName(''); setError(''); setCreating(true); }, (selected, action) => { select(selected); setOperation({ selected, action, nonce: Date.now() }); });
  const editing = operation && ['composition', 'split', 'create-split', 'remove-split', 'resume'].includes(operation.action);
  return <Workbench showActivity={false} sidebar={<nav aria-label="Dataset tree" className="tw-text-sm -tw-mx-4">
    {treeCatalog.error && <p role="alert">{treeCatalog.error}</p>}
    <Tree nodes={[{ id: 'datasets', type: 'Dataset', label: 'Datasets', selected: selected.kind === 'root', activate: () => select({ kind: 'root' }), children: treeNames.map(name => ({ id: `dataset-${name}`, type: 'Dataset', label: name, selected: selected.kind === 'dataset' && selected.dataset === name, activate: () => select({ kind: 'dataset', dataset: name }), expandable: true, childrenContent: <SnapshotTree tree={tree} updateTree={updateTree} dataset={name} active={active} refresh={refresh} select={select} selected={selected} onContextMenu={resourceMenu.open} onSplitsContextMenu={resourceMenu.openSplits} /> })) }]} defaultExpandedIds={['datasets', ...(selected.kind !== 'root' ? [`dataset-${selected.dataset}`] : [])]} onNodeContextMenu={(node, event) => resourceMenu.open(event, node.id === 'datasets' ? { kind: 'root' } : { kind: 'dataset', dataset: node.label })} />
    {'snapshot' in selected && <Ancestry key={`${selected.dataset}:${selected.snapshot}`} dataset={selected.dataset} snapshot={selected.snapshot} active={active} select={select} />}
  </nav>} detail={<Properties selected={selected} active={active} />}>
    <div className="tw-flex tw-items-center tw-justify-between"><h1 tabIndex={0} onKeyDown={resourceMenuKey} data-testid="dataset-scope" onContextMenu={event => resourceMenu.open(event, selected)} className="tw-text-xl tw-font-semibold">Dataset{selected.kind !== 'root' && ` · ${selected.dataset}`}</h1>
      <button className={buttonClass} onClick={() => { setCursor(undefined); setPrevious([]); setWindows({}); setRefresh(value => value + 1); }}>Refresh Dataset</button></div>
    {(catalog.error || capabilities.error || error) && <p role="alert">{catalog.error || capabilities.error || error}</p>}
    {resourceMenu.overlay}
    {operation?.action === 'delete' && <DeleteDataset key={operation.nonce} dataset={operation.selected.dataset} readonly={readonly || !capabilities.value?.includes('dataset-delete')} close={() => setOperation(undefined)} complete={() => { setOperation(undefined); setTree({}); setCursor(undefined); setPrevious([]); setRefresh(value => value + 1); if (selected.kind !== 'root' && selected.dataset === operation.selected.dataset) select({ kind: 'root' }); }} />}
    {operation && operation.action !== 'delete' && <Actions key={operation.nonce} selected={operation.selected} action={operation.action} readonly={readonly} capabilities={capabilities.value ?? []} close={() => setOperation(undefined)} complete={receipt => { setRefresh(value => value + 1); if (receipt) { setOperation(undefined); select({ kind: receipt.target.kind === 'snapshot' ? 'snapshot' : 'revision', dataset: operation.selected.dataset, snapshot: receipt.target.snapshot, ...(receipt.target.kind === 'split_revision' ? { revision: receipt.target.revision } : {}) }); } }} />}
    {creating && <Dialog isOpen title="Create Dataset" onClose={() => { if (!busy) setCreating(false); }} onConfirm={create} confirmLabel="Create Dataset" confirmLoading={busy} confirmDisabled={readonly || !name || !capabilities.value?.includes('dataset-management')}>
      <label>New Dataset name<input className={`${inputClass} tw-w-full`} maxLength={255} value={name} onChange={event => setName(event.target.value)} /></label>{error && <p role="alert">{error}</p>}
    </Dialog>}
    {!editing && (selected.kind === 'root' ? <>
      <p className="tw-text-sm tw-text-muted">Namespace: dataset-ns</p>
      {catalog.loading && <p role="status">Loading Datasets…</p>}
      {catalog.value?.entries.length === 0 && <p>No Datasets. Right-click Datasets to create one.</p>}
      <table aria-label="Datasets" className="tw-w-full tw-text-sm"><thead><tr><th>Name</th><th>Dataset ID</th><th>Latest</th><th>Stable</th></tr></thead><tbody>
        {catalog.value?.entries.map(dataset => <tr key={dataset.id} onKeyDown={resourceMenuKey} onContextMenu={event => resourceMenu.open(event, { kind: 'dataset', dataset: dataset.name })}><td><button className="tw-text-accent" onClick={() => select({ kind: 'dataset', dataset: dataset.name })}>{dataset.name}</button></td><td>{dataset.id}</td><td>{dataset.latest ?? 'None'}</td><td>{dataset.stable ?? 'None'}</td></tr>)}
      </tbody></table>
      <div className="tw-flex tw-gap-2"><button className={buttonClass} disabled={catalog.loading || !previous.length} onClick={() => { setCursor(previous.at(-1)); setPrevious(previous.slice(0, -1)); }}>Previous Datasets</button><button className={buttonClass} disabled={catalog.loading || !catalog.value?.after} onClick={() => { setPrevious([...previous, cursor].slice(-32)); setCursor(catalog.value!.after!); }}>Next Datasets</button></div>
    </> : selected.kind === 'dataset' ? <Inventory key={selected.dataset} dataset={selected.dataset} active={active} refresh={refresh} select={select} onContextMenu={resourceMenu.open} window={inventories[selected.dataset] ?? firstPage} change={value => setInventories(previous => Object.fromEntries([...Object.entries(previous).filter(([name]) => name !== selected.dataset).slice(-31), [selected.dataset, value]]))} /> : <>
      <SnapshotOverview readonly={readonly} refreshed={() => setRefresh(value => value + 1)} selected={selected} active={active} refresh={refresh} select={select} />
      <History key={`${selected.dataset}:${selected.snapshot}`} dataset={selected.dataset} snapshot={selected.snapshot} active={active} refresh={refresh} select={select} />
      <Matrix window={windows[windowKey] ?? emptyWindow} updateWindow={updateWindow} scope={selected} active={active} refresh={refresh} select={select} onContextMenu={resourceMenu.open} />
    </>)}
  </Workbench>;
}
function Inventory({ dataset, active, refresh, select, onContextMenu, window, change }: { window: PageWindow; change: (value: PageWindow) => void; onContextMenu: (event: import('react').MouseEvent, target: DatasetSelection) => void; dataset: string; active: boolean; refresh: number; select: (value: DatasetSelection) => void }) {
  const { cursor, previous } = window;
  const [direct, setDirect] = useState(''); const [error, setError] = useState('');
  const result = useQuery(`${dataset}:${cursor}:${refresh}`, active, signal => api.page<SnapshotSummary>(`${api.datasetPath(dataset)}/snapshot-inventory`, cursor, 20, signal));
  const head = useQuery(`${dataset}:${refresh}`, active, signal => api.head(dataset, signal));
  return <section className="tw-space-y-3">
    <p className="tw-text-sm">Complete Snapshots · Latest and Stable mark published references.</p>
    <form className="tw-flex tw-gap-2" onSubmit={event => { event.preventDefault(); try { api.opaqueId(direct); select({ kind: 'snapshot', dataset, snapshot: direct }); setError(''); } catch (reason) { setError(String(reason)); } }}>
      <input className={inputClass} aria-label="Snapshot ID" value={direct} onChange={event => setDirect(event.target.value)} /><button className={buttonClass}>Open Snapshot</button>
    </form>
    {(result.error || error) && <p role="alert">{result.error || error}</p>}
    {result.loading && <p role="status">Loading Snapshots…</p>}
    {result.value?.entries.length === 0 && <p>No Snapshots.</p>}
    <table aria-label="Snapshots" className="tw-w-full tw-text-xs"><thead><tr><th>Snapshot</th><th>Parent</th><th>Samples</th><th>References</th></tr></thead><tbody>
      {result.value?.entries.map(item => <tr key={item.id} onKeyDown={resourceMenuKey} onContextMenu={event => onContextMenu(event, { kind: 'snapshot', dataset, snapshot: item.id })}><td><button className="tw-text-accent" onClick={() => select({ kind: 'snapshot', dataset, snapshot: item.id })}>{item.id}</button></td><td>{item.parent ?? 'Root'}</td><td>{item.sample_count}</td><td>{head.value?.latest === item.id && 'Latest '}{head.value?.stable === item.id && 'Stable'}</td></tr>)}
    </tbody></table>
    <button className={buttonClass} disabled={result.loading || !result.value?.after} onClick={() => change({ cursor: result.value!.after!, previous: [...previous, cursor].slice(-32) })}>Next Snapshots</button>
    <button className={buttonClass} disabled={result.loading || !previous.length} onClick={() => change({ cursor: previous.at(-1), previous: previous.slice(0, -1) })}>Previous Snapshots</button>
  </section>;
}
function SnapshotOverview({ selected, active, refresh, select, readonly, refreshed }: { readonly: boolean; refreshed: () => void; selected: Exclude<DatasetSelection, { kind: 'root' | 'dataset' }>; active: boolean; refresh: number; select: (value: DatasetSelection) => void }) {
  const result = useQuery(`${selected.dataset}:${selected.snapshot}:${refresh}`, active, signal => api.summary(selected.dataset, selected.snapshot, signal));
  const current = useQuery(`${selected.dataset}:${selected.snapshot}:${refresh}`, active && !!selected.revision, signal => api.splitHead(selected.dataset, selected.snapshot, signal));
  return <section className="tw-text-sm tw-space-y-2">
    <h2 className="tw-font-semibold tw-break-all">Snapshot {selected.snapshot}</h2>
    {result.error && <p role="alert">{result.error} This immutable scope remains selected.</p>}
    {result.value && <><p>{result.value.sample_count} Samples · Schema v{result.value.schema_version} · {result.value.field_count} fields</p>
      {result.value.parent ? <button className="tw-text-accent" onClick={() => select({ kind: 'snapshot', dataset: selected.dataset, snapshot: result.value!.parent! })}>Parent {result.value.parent}</button> : <p>Independent root</p>}
      {result.value.inspection !== 'ready' && <p role="status">Inspection unavailable: {result.value.reason}</p>}</>}
    {(result.error || result.value?.inspection !== 'ready' && result.value) && <InspectionPreparation key={`${selected.dataset}:${selected.snapshot}`} dataset={selected.dataset} snapshot={selected.snapshot} readonly={readonly} refreshed={refreshed} />}
    {selected.revision && current.value?.revision && selected.revision !== current.value.revision && <button className={buttonClass} onClick={() => select({ kind: 'revision', dataset: selected.dataset, snapshot: selected.snapshot, revision: current.value!.revision! })}>Go to current Split revision</button>}
    {selected.revision && <p>Split revision: {selected.revision} {selected.split && `· ${selected.split}`}</p>}
  </section>;
}
