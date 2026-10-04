// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

import { useState, useEffect, useRef } from 'react';
import { ResourceActions } from '../access/ResourceActions';
import { Workbench, inputClass, buttonClass } from '../access/Workbench';
import { connections, iceberg } from '../access/native';
import { Fields } from '../iceberg/Fields';
import { InspectionView } from '../iceberg/ReferenceExplorer';
import { TableContent } from '../iceberg/TableContent';
import { useInspection } from '../iceberg/useInspection';
import { CatalogTree, type NamespacePage } from '../iceberg/CatalogTree';
import { useDomain, useNavigationSnapshot } from '../contexts/DomainContext';
import { Domain } from '../types';
import { navigationSelection } from '../iceberg/navigation';
import type { Selection, ParquetQuery } from '../iceberg/types';
import { useActivity } from '../contexts/ActivityContext';

const namespacePath = (namespace: string[]): string => `/v1/namespaces/${encodeURIComponent(namespace.join('\x1f'))}`;
const initialFields = '[{"id":1,"name":"id","required":true,"type":"long"}]';
const sections = ['Overview', 'Schema', 'Files'] as const;
interface IcebergQuery {
  namespace: string[] | null; table: string; section: typeof sections[number];
  catalogCursor?: string; tableCursor?: string; namespaceCursor?: string;
  metadata?: string; selection: Selection | null; offset?: string;
  detail?: ParquetQuery;
}
export function IcebergView({ active, readonly: domainReadonly }: { active: boolean; readonly: boolean }) {
  const { log } = useActivity();
  const { checkpoint } = useDomain();
  const restoreQuery = useRef<(query: IcebergQuery) => void>(() => {});
  const pendingInspection = useRef<IcebergQuery | null>(null);
  const [catalogCursor, setCatalogCursor] = useState<string>();
  const [namespaceCursor, setNamespaceCursor] = useState<{ table?: string; namespace?: string }>({});
  const navigation = useRef(0);
  const operation = useRef(0);
  useEffect(() => { if (!active) { ++navigation.current; ++operation.current; } }, [active]);
  const catalogLoadedRetry = useRef(-1);
  const [demoNamespace, setDemoNamespace] = useState<string | null>(null);
  const [removals, setRemovals] = useState('[]');
  const [origin, setOrigin] = useState<string | null>(null);
  const token = '';
  const readonly = domainReadonly;
  const [retry, setRetry] = useState(0);
  const [propertyHost, setPropertyHost] = useState<HTMLDivElement | null>(null);
  const [namespacePages, setNamespacePages] = useState<Record<string, NamespacePage>>({});
  const [pagedNamespaces, setPagedNamespaces] = useState(false);
  const [nextNamespaces, setNextNamespaces] = useState<string | null>(null);
  const [catalog, setCatalog] = useState<any>(null);
  const [namespaces, setNamespaces] = useState<string[][]>([]);
  const [namespace, setNamespace] = useState<string[] | null>(null);
  const [properties, setProperties] = useState<any>(null);
  const [tables, setTables] = useState<Array<{ namespace: string[]; name: string }>>([]);
  const [table, setTable] = useState('');
  const [loaded, setLoaded] = useState<any>(null);
  const [section, setSection] = useState<typeof sections[number]>('Overview');
  const [name, setName] = useState('');
  const [propertyText, setPropertyText] = useState('{}');
  const [newTable, setNewTable] = useState('');
  const [fields, setFields] = useState(initialFields);
  const [rename, setRename] = useState('');
  const [updates, setUpdates] = useState('[{"action":"set-properties","updates":{"demo":"true"}}]');
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState('');
  const [outcome, setOutcome] = useState('');
  useEffect(() => {
    setCatalog(null); setNamespaces([]); setNamespace(null); setTables([]); setTable(''); setLoaded(null); setProperties(null); setDemoNamespace(null); setError(''); setOutcome('');
  }, [token]);
  useEffect(() => {
    if (!active || catalog && catalogLoadedRetry.current === retry) return;
    const controller = new AbortController();
    setBusy(true); setError('');
    void (async () => {
      const deployment = await connections(controller.signal);
      if (controller.signal.aborted) return;
      if (!deployment.iceberg_ready || !deployment.iceberg) throw new Error('The cluster Catalog is not ready. Check the Console deployment and retry.');
      setOrigin(deployment.iceberg);
      const [config, listed] = await Promise.all([
        iceberg('/v1/config', '', 'GET', undefined, controller.signal),
        iceberg('/v1/namespaces?pageSize=30', '', 'GET', undefined, controller.signal),
      ]);
      if (!controller.signal.aborted) { catalogLoadedRetry.current = retry; setCatalogCursor(undefined); setCatalog(config); setNamespaces(listed.namespaces ?? []); setNextNamespaces(listed['next-page-token'] ?? null); }
    })().catch(error => { if (!controller.signal.aborted) setError(String(error)); })
      .finally(() => { if (!controller.signal.aborted) setBusy(false); });
    return () => controller.abort();
  }, [active, token, retry]);
  const path = namespace ? namespacePath(namespace) : '';
  const tablePath = `${path}/tables/${encodeURIComponent(table)}`;
  const run = async (work: () => Promise<void>, label: string) => {
    const version = ++operation.current;
    setBusy(true); setError(''); setOutcome('');
    try { await work(); if (version !== operation.current) return; setOutcome(label.endsWith('loaded') ? '' : label); log({ action: label, target: `Iceberg / ${namespace?.join('.') ?? 'catalog'} / ${table}`, status: 'Success' }); }
    catch (error) { if (version !== operation.current) return; setError(`${String(error)}. Refresh metadata before retrying a mutation.`); log({ action: label, target: `Iceberg / ${namespace?.join('.') ?? 'catalog'} / ${table}`, status: 'Failed', message: 'Native request failed. Refresh resource state before retrying.' }); }
    finally { if (version === operation.current) setBusy(false); }
  };
  const loadNamespaces = async (cursor?: string) => { const request = ++navigation.current; setCatalogCursor(cursor); setPagedNamespaces(!!cursor); const response = await iceberg(`/v1/namespaces?pageSize=30${cursor ? `&pageToken=${encodeURIComponent(cursor)}` : ''}`, token); if (request !== navigation.current) throw new DOMException('Superseded navigation', 'AbortError'); setNamespaces(response.namespaces ?? []); setNextNamespaces(response['next-page-token'] ?? null); };
  const loadNamespace = async (value: string[], tableToken?: string, namespaceToken?: string) => {
    setNamespaceCursor({ table: tableToken, namespace: namespaceToken });
    const request = ++navigation.current;
    const pageQuery = (cursor?: string) => `pageSize=30${cursor ? `&pageToken=${encodeURIComponent(cursor)}` : ''}`;
    const [properties, listed, children] = await Promise.all([
      iceberg(namespacePath(value), token),
      iceberg(`${namespacePath(value)}/tables?${pageQuery(tableToken)}`, token),
      iceberg(`/v1/namespaces?parent=${encodeURIComponent(value.join('\x1f'))}&${pageQuery(namespaceToken)}`, token),
    ]);
    if (request !== navigation.current) throw new DOMException('Superseded navigation', 'AbortError');
    setProperties(properties); setTables(listed.identifiers ?? []);
    const page: NamespacePage = { paged: !!tableToken || !!namespaceToken, tables: listed.identifiers ?? [], children: (children.namespaces ?? []).filter((child: string[]) => child.length > value.length && value.every((part, i) => part === child[i])), nextTables: listed['next-page-token'], nextNamespaces: children['next-page-token'] };
    setNamespacePages(previous => Object.fromEntries([...Object.entries(previous).filter(([key]) => key !== JSON.stringify(value)).slice(-7), [JSON.stringify(value), page]]));
  };
  const loadTable = async (name: string, ns = namespace) => { if (!ns) return; const request = ++navigation.current; const value = await iceberg(`${namespacePath(ns)}/tables/${encodeURIComponent(name)}`, token); if (request === navigation.current) setLoaded(value); };
  const selectCatalog = () => { checkpoint(); navigation.current++; operation.current++; setBusy(false); setOutcome(''); setNamespace(null); setTable(''); setLoaded(null); };
  const selectNamespace = (value: string[]) => { checkpoint(); setNamespace(value); setTable(''); setLoaded(null); setTables([]); void run(() => loadNamespace(value), `Namespace ${value.join('.')} loaded`); };
  const selectTable = (name: string, ns = namespace) => { if (!ns) return; checkpoint(); setNamespace(ns); setTable(name); setLoaded(null); setSection('Overview'); void run(() => loadTable(name, ns), `Table ${name} loaded`); };
  const inspector = useInspection(loaded, tablePath, token, origin);
  useNavigationSnapshot(Domain.Iceberg, 'catalog-query', () => {
    const state: IcebergQuery = { namespace: namespace ? [...namespace] : null, table, section,
      catalogCursor, tableCursor: namespaceCursor.table, namespaceCursor: namespaceCursor.namespace,
      metadata: loaded?.['metadata-location'], selection: navigationSelection(inspector.selection),
      offset: inspector.pageToken, detail: inspector.detail };
    return () => restoreQuery.current(state);
  });
  restoreQuery.current = state => {
    pendingInspection.current = state.selection ? state : null;
    setNamespace(state.namespace); setTable(state.table); setSection(state.section);
    setLoaded(null); setProperties(null); setTables([]);
    void run(async () => {
      if (!state.namespace) await loadNamespaces(state.catalogCursor);
      else {
        await loadNamespace(state.namespace, state.tableCursor, state.namespaceCursor);
        if (state.table) await loadTable(state.table, state.namespace);
      }
    }, 'Navigation restored');
  };
  useEffect(() => {
    const pending = pendingInspection.current;
    if (!loaded || !pending?.selection) return;
    pendingInspection.current = null;
    if (pending.metadata !== loaded['metadata-location']) {
      setError('Saved table generation is stale. Refresh the table before inspecting its references.');
      return;
    }
    const snapshot = loaded.metadata?.snapshots?.find((value: { 'snapshot-id': string | number }) =>
      String(value['snapshot-id']) === String(pending.selection!.snapshot['snapshot-id']));
    if (!snapshot) { setError('Saved snapshot no longer exists. Refresh the table.'); return; }
    const request = navigation.current;
    void inspector.select({ ...pending.selection, snapshot }, pending.offset, false, false).then(current => {
      if (current && pending.detail && request === navigation.current) inspector.setDetail(pending.detail);
    });
  }, [loaded]);
  const metadata = loaded?.metadata;
  const supported = (method: string, template: string) => !catalog?.endpoints || catalog.endpoints.some((entry: string) => entry === `${method} ${template}` || entry === `${method} ${template.replace('/v1/', '/v1/{prefix}/')}`);
  const overview = metadata ? { uuid: metadata['table-uuid'], location: metadata.location, 'format-version': metadata['format-version'], 'metadata-location': loaded['metadata-location'], 'current-snapshot-id': metadata['current-snapshot-id'], 'last-updated-ms': metadata['last-updated-ms'], properties: metadata.properties } : null;
  const selected = inspector.selection;
  const selectedType = selected ? ({ snapshot: 'Snapshot', list: 'Manifest List', manifest: 'Manifest', file: selected.file?.format ?? 'File' }[selected.kind]) : table ? 'Table' : namespace ? 'Namespace' : 'Catalog';
  const selectedProperties = selected ? { Type: selectedType, ...(selected.kind === 'snapshot' ? { ...selected.snapshot, 'Manifest list size': inspector.cache[inspector.key(selected)]?.size } : selected.kind === 'file' ? selected.file : selected.kind === 'manifest' ? selected.manifest : { location: selected.snapshot['manifest-list'] }), 'Parent table': table, ...(selected.kind !== 'snapshot' ? { 'Snapshot ID': selected.snapshot['snapshot-id'] } : {}) } : table ? { Type: 'Table', ...overview } : namespace ? { Type: 'Namespace', Name: namespace.join('.'), ...properties } : { Type: 'Catalog', ...catalog };
  const focusTitle = selected ? selected.kind === 'snapshot' ? `Snapshot ${selected.snapshot['snapshot-id']}` : (selected.file?.location ?? selected.manifest?.location ?? selected.snapshot['manifest-list'] ?? '').split('/').pop() : table || namespace?.join('.') || 'Iceberg catalog';
  return <Workbench resizableSidebar showActivity={false} detail={<div aria-label="Iceberg properties" className="tw-space-y-4"><h2 className="tw-font-semibold">Properties</h2><div ref={setPropertyHost} /><Fields stacked values={selectedProperties} /></div>} sidebar={<>
    {busy && !catalog && <p role="status" className="tw-text-xs">Loading catalog…</p>}
    {catalog && <CatalogTree namespaces={namespaces} pages={namespacePages} namespace={namespace} table={table} loaded={loaded} inspector={inspector}
      onCatalog={selectCatalog} onNamespace={selectNamespace} onTable={selectTable}
      onPage={(ns, t, n) => void run(() => loadNamespace(ns, t, n), 'Namespace page loaded')}
      pagedNamespaces={pagedNamespaces} onFirstNamespaces={() => void run(() => loadNamespaces(), 'First namespaces loaded')} nextNamespaces={nextNamespaces} onNextNamespaces={() => void run(() => loadNamespaces(nextNamespaces!), 'Namespace page loaded')} />}
    {catalog && !namespaces.length && <p className="tw-text-xs tw-text-muted">No namespaces in this catalog.</p>}
  </>}>
    <nav aria-label="Iceberg breadcrumbs" className="tw-flex tw-flex-wrap tw-items-center tw-gap-2 tw-text-xs tw-text-muted">
      <button onClick={selectCatalog}>Catalog</button>{namespace && <><span>/</span><button onClick={() => selectNamespace(namespace)}>{namespace.join('.')}</button></>}{table && <><span>/</span><button onClick={() => inspector.clear()}>{table}</button></>}{selected && <><span>/</span><span>{selectedType}</span></>}
    </nav>
    <p className="tw-text-xs tw-text-muted">{selectedType}</p>
    <h1 className="tw-text-lg tw-font-semibold tw-break-all">{focusTitle}</h1>
    <ResourceActions key={`${selectedType}:${focusTitle}`} label={`${selectedType} actions`}>
      <p className="tw-text-xs tw-text-muted tw-break-all">Target: {namespace?.join('.')}{table ? ` / ${table}` : ''}{selected ? ` / ${focusTitle}` : ''}</p>
    {selected && <button className={buttonClass} disabled={inspector.busy} onClick={() => void inspector.select(selected, '0')}>Refresh {selectedType.toLowerCase()}</button>}
    {!readonly && catalog && !inspector.selection && <div className="tw-space-y-3">
      {loaded && <>      <form className="tw-space-y-2" onSubmit={event => { event.preventDefault(); void run(async () => { await iceberg('/v1/tables/rename', token, 'POST', { source: { namespace, name: table }, destination: { namespace, name: rename } }); setTable(rename); await loadNamespace(namespace!); await loadTable(rename); }, 'Table renamed'); }}>
        <label className="tw-text-xs">New table name<input className={`${inputClass} tw-w-full`} value={rename} required onChange={event => setRename(event.target.value)} /></label><button className={buttonClass} disabled={busy || !supported('POST', '/v1/tables/rename')}>Rename table</button>
      </form>
      <button className={`${buttonClass} tw-text-failed tw-border-failed/30`} disabled={busy || !supported('DELETE', '/v1/namespaces/{namespace}/tables/{table}')} onClick={() => { if (confirm(`Drop table ${namespace?.join('.')}.${table}?`)) void run(async () => { await iceberg(tablePath, token, 'DELETE'); setTable(''); setLoaded(null); await loadNamespace(namespace!); }, 'Table dropped'); }}>Drop table</button>
      <p className="tw-text-xs tw-text-muted">Drop removes the catalog reference. Physical reclamation follows service GC.</p>
      </>}
      {!namespace && <div className="tw-flex tw-gap-2 tw-items-center tw-flex-wrap"><button className={buttonClass} disabled={busy || !!demoNamespace} onClick={() => void run(async () => {
        const demo = `console_demo_${crypto.randomUUID().replace(/-/g, '')}`;
        await iceberg('/v1/namespaces', token, 'POST', { namespace: [demo], properties: { purpose: 'console demo' } }); setDemoNamespace(demo);
        await iceberg(`${namespacePath([demo])}/tables`, token, 'POST', { name: 'example', schema: { type: 'struct', 'schema-id': 0, fields: JSON.parse(initialFields) } }); await loadNamespaces();
      }, 'Demo namespace and table created')}>Create metadata demo</button>
      {demoNamespace && <><span className="tw-text-xs tw-break-all">Demo scope: {demoNamespace} / example</span><button className={buttonClass} disabled={busy} onClick={() => { if (confirm(`Remove only demo table ${demoNamespace}.example and its empty namespace?`)) void run(async () => {
        try { await iceberg(`${namespacePath([demoNamespace])}/tables/example`, token, 'DELETE'); } catch (error) { if (!String(error).startsWith('Error: HTTP 404:')) throw error; }
        await iceberg(namespacePath([demoNamespace]), token, 'DELETE'); if (namespace?.[0] === demoNamespace) { setNamespace(null); setTable(''); setLoaded(null); } setDemoNamespace(null); await loadNamespaces();
      }, 'Demo resources removed'); }}>Clean metadata demo</button></>}</div>}
      {!namespace && <form className="tw-space-y-2" onSubmit={event => { event.preventDefault(); void run(async () => { await iceberg('/v1/namespaces', token, 'POST', { namespace: name.split('.'), properties: JSON.parse(propertyText) }); await loadNamespaces(); }, 'Namespace created'); }}>
        <h2 className="tw-font-semibold">Create namespace</h2><label className="tw-text-xs">Namespace name<input className={`${inputClass} tw-ml-2`} value={name} required placeholder="demo.analytics" onChange={event => setName(event.target.value)} /></label><label className="tw-block tw-text-xs">Properties (JSON)<textarea className={`${inputClass} tw-w-full`} value={propertyText} onChange={event => setPropertyText(event.target.value)} /></label><button className={buttonClass} disabled={busy || !supported('POST', '/v1/namespaces')}>Create namespace</button>
      </form>}
      {namespace && !table && <>
        <form className="tw-space-y-2" onSubmit={event => { event.preventDefault(); void run(async () => { await iceberg(`${path}/properties`, token, 'POST', { updates: JSON.parse(propertyText), removals: JSON.parse(removals) }); await loadNamespace(namespace); }, 'Properties updated'); }}>
          <label className="tw-block tw-text-xs">Property updates (JSON)<textarea className={`${inputClass} tw-w-full`} value={propertyText} onChange={event => setPropertyText(event.target.value)} /></label><label className="tw-block tw-text-xs">Property removals (JSON array)<input className={`${inputClass} tw-w-full`} value={removals} onChange={event => setRemovals(event.target.value)} /></label><button className={buttonClass} disabled={busy || !supported('POST', '/v1/namespaces/{namespace}/properties')}>Update properties</button>
        </form>
        <form className="tw-space-y-2" onSubmit={event => { event.preventDefault(); void run(async () => { await iceberg(`${path}/tables`, token, 'POST', { name: newTable, schema: { type: 'struct', 'schema-id': 0, fields: JSON.parse(fields) } }); await loadNamespace(namespace); }, 'Table created'); }}>
          <h2 className="tw-font-semibold">Create table</h2><label className="tw-text-xs">Table name<input className={`${inputClass} tw-ml-2`} required value={newTable} onChange={event => setNewTable(event.target.value)} /></label><label className="tw-block tw-text-xs">Schema fields (JSON)<textarea className={`${inputClass} tw-w-full`} rows={3} value={fields} onChange={event => setFields(event.target.value)} /></label><button className={buttonClass} disabled={busy || !supported('POST', '/v1/namespaces/{namespace}/tables')}>Create table</button>
        </form>
        <button className={`${buttonClass} tw-text-failed tw-border-failed/30`} disabled={busy || !supported('DELETE', '/v1/namespaces/{namespace}')} onClick={() => { if (confirm(`Drop empty namespace ${namespace.join('.')}?`)) void run(async () => { await iceberg(path, token, 'DELETE'); setNamespace(null); setProperties(null); await loadNamespaces(); }, 'Namespace dropped'); }}>Drop namespace</button>
      </>}
      {loaded && <form className="tw-space-y-2" onSubmit={event => { event.preventDefault(); void run(async () => { await iceberg(tablePath, token, 'POST', { requirements: [{ type: 'assert-table-uuid', uuid: metadata['table-uuid'] }], updates: JSON.parse(updates) }); await loadTable(table); }, 'Metadata committed'); }}>
        <h2 className="tw-font-semibold">Commit table metadata</h2><p className="tw-text-xs tw-text-muted">Applies native Iceberg updates to this table UUID. Refresh after a conflict or interrupted request.</p><label className="tw-block tw-text-xs">Metadata updates (JSON)<textarea className={`${inputClass} tw-w-full`} rows={4} value={updates} onChange={event => setUpdates(event.target.value)} /></label><button className={buttonClass} disabled={busy || !supported('POST', '/v1/namespaces/{namespace}/tables/{table}')}>Commit metadata</button>
      </form>}
    </div>}
    </ResourceActions>
    {loaded && !selected && <button className={buttonClass} disabled={busy} onClick={() => void run(() => loadTable(table), 'Metadata refreshed')}>Refresh table</button>}

    {error && <p role="alert" className="tw-text-sm tw-text-failed tw-break-all">{error}</p>}{outcome && <p role="status" className="tw-text-xs tw-text-muted">{outcome}</p>}
    {error && <button className={buttonClass} disabled={busy} onClick={() => setRetry(value => value + 1)}>Retry catalog</button>}
    {!catalog && !error && <p className="tw-text-sm tw-text-muted">Loading the current cluster catalog…</p>}
    {catalog && !namespace && <div className="tw-grid tw-grid-cols-2 tw-gap-3">{namespaces.map(ns => <button key={JSON.stringify(ns)} className="tw-rounded tw-border tw-border-border tw-bg-panel tw-p-4 tw-text-left" onClick={() => selectNamespace(ns)}><span className="tw-block tw-text-xs tw-text-muted">Namespace</span>{ns.join('.')}</button>)}</div>}
    {namespace && !table && <><h2 className="tw-font-semibold">Tables</h2><div className="tw-grid tw-grid-cols-2 xl:tw-grid-cols-3 tw-gap-3">{tables.map(entry => <button key={entry.name} className="tw-rounded tw-border tw-border-border tw-bg-panel tw-p-4 tw-text-left" onClick={() => selectTable(entry.name)}><span className="tw-block tw-text-xs tw-text-muted">Table</span>{entry.name}</button>)}</div>{!tables.length && <p className="tw-text-xs tw-text-muted">No tables in this namespace.</p>}</>}
    {loaded && <>{!selected && <nav className="tw-flex tw-gap-2" aria-label="Table sections">{sections.map(value => <button key={value} className={buttonClass} aria-pressed={section === value} onClick={() => setSection(value)}>{value}</button>)}</nav>}
      {!inspector.selection && <TableContent loaded={loaded} section={section} inspector={inspector} />}
      <InspectionView inspector={inspector} propertyHost={propertyHost} />
      {section === 'Files' && !inspector.selection && <p className="tw-text-xs tw-text-muted">Select a snapshot in the reference tree to inspect its manifest list, manifests and file metadata.</p>}
    </>}

  </Workbench>;
}
