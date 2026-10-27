// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

import { resourceMenuKey } from '../components/resourceMenuKey';
import { useState, useEffect, useRef } from 'react';
import { useIcebergMenu } from '../iceberg/ResourceMenu';
import { DocsHelp, Workbench, buttonClass } from '../access/Workbench';
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
const sections = ['Overview', 'Schema', 'Files'] as const;
interface IcebergQuery {
  namespace: string[] | null; table: string; section: typeof sections[number];
  catalogCursor?: string; tableCursor?: string; namespaceCursor?: string;
  catalogPrevious: Array<string | undefined>; namespacePrevious: Array<{ table?: string; namespace?: string }>;
  metadata?: string; selection: Selection | null; offset?: string;
  detail?: ParquetQuery;
}
export function IcebergView({ active, readonly: domainReadonly }: { active: boolean; readonly: boolean }) {
  const { log } = useActivity();
  const { checkpoint } = useDomain();
  const restoreQuery = useRef<(query: IcebergQuery) => void>(() => {});
  const pendingInspection = useRef<IcebergQuery | null>(null);
  const [catalogPrevious, setCatalogPrevious] = useState<Array<string | undefined>>([]);
  const [namespacePrevious, setNamespacePrevious] = useState<Array<{ table?: string; namespace?: string }>>([]);
  const [catalogCursor, setCatalogCursor] = useState<string>();
  const [namespaceCursor, setNamespaceCursor] = useState<{ table?: string; namespace?: string }>({});
  const navigation = useRef(0);
  const operation = useRef(0);
  useEffect(() => { if (!active) { ++navigation.current; ++operation.current; } }, [active]);
  const catalogLoadedRetry = useRef(-1);
  const [origin, setOrigin] = useState<string | null>(null);
  const token = '';
  const readonly = domainReadonly;
  const [retry, setRetry] = useState(0);
  const [treeRevision, setTreeRevision] = useState(0);
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
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState('');
  const [outcome, setOutcome] = useState('');
  useEffect(() => {
    setCatalog(null); setNamespaces([]); setNamespace(null); setTables([]); setTable(''); setLoaded(null); setProperties(null); setError(''); setOutcome('');
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
      if (!controller.signal.aborted) { catalogLoadedRetry.current = retry; setCatalogCursor(undefined); setCatalogPrevious([]); setCatalog(config); setNamespaces(listed.namespaces ?? []); setNextNamespaces(listed['next-page-token'] ?? null); }
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
    // An absent Table pageToken requests a complete list; an empty token starts pagination.
    const pageQuery = (cursor?: string) => `pageSize=30&pageToken=${encodeURIComponent(cursor ?? '')}`;
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
  const selectNamespace = (value: string[]) => { checkpoint(); setNamespacePrevious([]); setNamespace(value); setTable(''); setLoaded(null); setTables([]); void run(() => loadNamespace(value), `Namespace ${value.join('.')} loaded`); };
  const selectTable = (name: string, ns = namespace) => { if (!ns) return; checkpoint(); setNamespace(ns); setTable(name); setLoaded(null); setSection('Overview'); void run(() => loadTable(name, ns), `Table ${name} loaded`); };
  const inspector = useInspection(loaded, tablePath, token, origin);
  useNavigationSnapshot(Domain.Iceberg, 'catalog-query', () => {
    const state: IcebergQuery = { namespace: namespace ? [...namespace] : null, table, section,
      catalogPrevious, namespacePrevious, catalogCursor, tableCursor: namespaceCursor.table, namespaceCursor: namespaceCursor.namespace,
      metadata: loaded?.['metadata-location'], selection: navigationSelection(inspector.selection),
      offset: inspector.pageToken, detail: inspector.detail };
    return () => restoreQuery.current(state);
  });
  restoreQuery.current = state => {
    setCatalogPrevious(state.catalogPrevious); setNamespacePrevious(state.namespacePrevious);
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
  const focusTitle = selected ? selected.kind === 'snapshot' ? `Snapshot ${selected.snapshot['snapshot-id']}` : (selected.file?.location ?? selected.manifest?.location ?? selected.snapshot['manifest-list'] ?? '').split('/').pop() : table || namespace?.join('.') || 'Namespaces';
  const resourceMenu = useIcebergMenu(readonly, (method, endpoint) => !!catalog && supported(method, endpoint), async (target, action) => {
    setTreeRevision(value => value + 1);
    if (action === 'drop-namespace') { selectCatalog(); await loadNamespaces(); return; }
    if (target.namespace) {
      setNamespace(target.namespace);
      setNamespacePrevious([]);
      setTable(action === 'commit' ? target.table ?? '' : ''); setLoaded(null);
      await loadNamespace(target.namespace);
      if (target.table && action === 'commit') await loadTable(target.table, target.namespace);
    } else await loadNamespaces();
  });
  return <Workbench resizableSidebar showActivity={false} help={<DocsHelp href="https://crowdb.dev/docs/manual/iceberg/" title="iceberg" auth="The Console uses its server-side bearer token. External clients use ICEBERG_TOKEN; there is no separate username or password." />} detail={<div aria-label="Iceberg properties" className="tw-space-y-4"><h2 className="tw-font-semibold">Properties</h2><div ref={setPropertyHost} /><Fields stacked values={selectedProperties} /></div>} sidebar={<>
    {busy && !catalog && <p role="status" className="tw-text-xs">Loading catalog…</p>}
    {catalog && <CatalogTree active={active} revision={treeRevision + retry} namespace={namespace} table={table} loaded={loaded} inspector={inspector}
      onContextMenu={resourceMenu.open} onCatalog={selectCatalog} onNamespace={selectNamespace} onTable={selectTable} />}
  </>}>
    <h1 tabIndex={0} onKeyDown={resourceMenuKey} onContextMenu={event => { if (!selected) resourceMenu.open(event, { namespace: namespace ?? undefined, table: table || undefined }); }} className="tw-text-lg tw-font-semibold tw-break-all">{focusTitle}</h1>
    {resourceMenu.overlay}
    {selected && <button className={buttonClass} disabled={inspector.busy} onClick={() => void inspector.select(selected, '0')}>Refresh {selectedType.toLowerCase()}</button>}
    {loaded && !selected && <button className={buttonClass} disabled={busy} onClick={() => void run(() => loadTable(table), 'Metadata refreshed')}>Refresh table</button>}

    {error && <p role="alert" className="tw-text-sm tw-text-failed tw-break-all">{error}</p>}{outcome && <p role="status" className="tw-text-xs tw-text-muted">{outcome}</p>}
    {error && <button className={buttonClass} disabled={busy} onClick={() => setRetry(value => value + 1)}>Retry catalog</button>}
    {!catalog && !error && <p className="tw-text-sm tw-text-muted">Loading the current cluster catalog…</p>}
    {catalog && busy && <p role="status">Loading resource…</p>}
    {catalog && !namespace && <>
      <table aria-label="Namespaces" className="tw-w-full tw-text-sm tw-text-left"><thead><tr><th>Name</th><th>Type</th></tr></thead><tbody>{namespaces.map(ns => <tr key={JSON.stringify(ns)} onKeyDown={resourceMenuKey} onContextMenu={event => resourceMenu.open(event, { namespace: ns })}><td><button className="tw-text-accent" onClick={() => selectNamespace(ns)}>{ns.join('.')}</button></td><td>Namespace</td></tr>)}</tbody></table>
      {!namespaces.length && !busy && <p>No namespaces. Right-click Catalog to create one.</p>}
      <div className="tw-flex tw-gap-2"><button className={buttonClass} disabled={busy || !catalogPrevious.length} onClick={() => { setCatalogPrevious(catalogPrevious.slice(0, -1)); void run(() => loadNamespaces(catalogPrevious.at(-1)), 'Previous namespaces loaded'); }}>Previous namespaces</button><button className={buttonClass} disabled={busy || !pagedNamespaces} onClick={() => { setCatalogPrevious([]); void run(() => loadNamespaces(), 'First namespaces loaded'); }}>First namespaces</button><button className={buttonClass} disabled={busy || !nextNamespaces} onClick={() => { setCatalogPrevious([...catalogPrevious, catalogCursor].slice(-32)); void run(() => loadNamespaces(nextNamespaces!), 'Next namespaces loaded'); }}>Next namespaces</button></div>
    </>}
    {namespace && !table && <>
      <table aria-label="Namespace resources" className="tw-w-full tw-text-sm tw-text-left"><thead><tr><th>Name</th><th>Type</th></tr></thead><tbody>
        {(namespacePages[JSON.stringify(namespace)]?.children ?? []).map(ns => <tr key={JSON.stringify(ns)} onKeyDown={resourceMenuKey} onContextMenu={event => resourceMenu.open(event, { namespace: ns })}><td><button className="tw-text-accent" onClick={() => selectNamespace(ns)}>{ns.join('.')}</button></td><td>Namespace</td></tr>)}
        {tables.map(entry => <tr key={entry.name} onKeyDown={resourceMenuKey} onContextMenu={event => resourceMenu.open(event, { namespace, table: entry.name })}><td><button className="tw-text-accent" onClick={() => selectTable(entry.name)}>{entry.name}</button></td><td>Table</td></tr>)}
      </tbody></table>
      {!tables.length && !busy && <p>No tables. Right-click this Namespace to create a table.</p>}
      <div className="tw-flex tw-gap-2"><button className={buttonClass} disabled={busy || !namespacePrevious.length} onClick={() => { const previous = namespacePrevious.at(-1)!; setNamespacePrevious(namespacePrevious.slice(0, -1)); void run(() => loadNamespace(namespace, previous.table, previous.namespace), 'Previous page loaded'); }}>Previous page</button><button className={buttonClass} disabled={busy || !namespacePages[JSON.stringify(namespace)]?.paged} onClick={() => { setNamespacePrevious([]); void run(() => loadNamespace(namespace), 'First page loaded'); }}>First page</button><button className={buttonClass} disabled={busy || !namespacePages[JSON.stringify(namespace)]?.nextTables} onClick={() => { setNamespacePrevious([...namespacePrevious, namespaceCursor].slice(-32)); void run(() => loadNamespace(namespace, namespacePages[JSON.stringify(namespace)].nextTables!, namespaceCursor.namespace), 'Next tables loaded'); }}>Next tables</button><button className={buttonClass} disabled={busy || !namespacePages[JSON.stringify(namespace)]?.nextNamespaces} onClick={() => { setNamespacePrevious([...namespacePrevious, namespaceCursor].slice(-32)); void run(() => loadNamespace(namespace, namespaceCursor.table, namespacePages[JSON.stringify(namespace)].nextNamespaces!), 'Next namespaces loaded'); }}>Next namespaces</button></div>
    </>}
    {loaded && <>{!selected && <nav className="tw-flex tw-gap-2" aria-label="Table sections">{sections.map(value => <button key={value} className={buttonClass} aria-pressed={section === value} onClick={() => setSection(value)}>{value}</button>)}</nav>}
      {!inspector.selection && <TableContent loaded={loaded} section={section} inspector={inspector} />}
      <InspectionView inspector={inspector} propertyHost={propertyHost} />
      {section === 'Files' && !inspector.selection && <p className="tw-text-xs tw-text-muted">Select a snapshot in the reference tree to inspect its manifest list, manifests and file metadata.</p>}
    </>}

  </Workbench>;
}
