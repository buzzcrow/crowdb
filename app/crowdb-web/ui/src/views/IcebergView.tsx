// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

import { useState, useEffect } from 'react';
import { Workbench, inputClass, buttonClass } from '../access/Workbench';
import { connections, iceberg } from '../access/native';
import { Fields, Structured } from '../iceberg/Fields';
import { ReferenceTree, InspectionView } from '../iceberg/ReferenceExplorer';
import { useInspection } from '../iceberg/useInspection';
import { useActivity } from '../contexts/ActivityContext';

const namespacePath = (namespace: string[]): string => `/v1/namespaces/${encodeURIComponent(namespace.join('\x1f'))}`;
const initialFields = '[{"id":1,"name":"id","required":true,"type":"long"}]';
const sections = ['Overview', 'Schema', 'Snapshots', 'Files'] as const;
export function IcebergView({ active, readonly: domainReadonly }: { active: boolean; readonly: boolean }) {
  const { log } = useActivity();
  const [demoNamespace, setDemoNamespace] = useState<string | null>(null);
  const [removals, setRemovals] = useState('[]');
  const [origin, setOrigin] = useState<string | null>(null);
  const [token, setToken] = useState('');
  const [writeToken, setWriteToken] = useState('');
  const readonly = domainReadonly || !token;
  const [retry, setRetry] = useState(0);
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
    if (!active) return;
    const controller = new AbortController();
    setBusy(true); setError('');
    void (async () => {
      const deployment = await connections();
      if (controller.signal.aborted) return;
      if (!deployment.iceberg_ready || !deployment.iceberg) throw new Error('The cluster Catalog is not ready. Check the Console deployment and retry.');
      setOrigin(deployment.iceberg);
      const [config, listed] = await Promise.all([
        iceberg('/v1/config', '', 'GET', undefined, controller.signal),
        iceberg('/v1/namespaces', '', 'GET', undefined, controller.signal),
      ]);
      if (!controller.signal.aborted) { setCatalog(config); setNamespaces(listed.namespaces ?? []); }
    })().catch(error => { if (!controller.signal.aborted) setError(String(error)); })
      .finally(() => { if (!controller.signal.aborted) setBusy(false); });
    return () => controller.abort();
  }, [active, token, retry]);
  const path = namespace ? namespacePath(namespace) : '';
  const tablePath = `${path}/tables/${encodeURIComponent(table)}`;
  const run = async (operation: () => Promise<void>, label: string) => {
    setBusy(true); setError(''); setOutcome('');
    try { await operation(); setOutcome(label); log({ action: label, target: `Iceberg / ${namespace?.join('.') ?? 'catalog'} / ${table}`, status: 'Success' }); }
    catch (error) { setError(`${String(error)}. Refresh metadata before retrying a mutation.`); log({ action: label, target: `Iceberg / ${namespace?.join('.') ?? 'catalog'} / ${table}`, status: 'Failed', message: 'Native request failed. Refresh resource state before retrying.' }); }
    finally { setBusy(false); }
  };
  const loadNamespaces = async () => { const response = await iceberg('/v1/namespaces', token); setNamespaces(response.namespaces ?? []); };
  const loadNamespace = async (value: string[]) => {
    const [properties, listed] = await Promise.all([iceberg(namespacePath(value), token), iceberg(`${namespacePath(value)}/tables`, token)]);
    setProperties(properties); setTables(listed.identifiers ?? []);
  };
  const loadTable = async (name: string) => { setLoaded(await iceberg(`${path}/tables/${encodeURIComponent(name)}`, token)); };
  const selectNamespace = (value: string[]) => { setNamespace(value); setTable(''); setLoaded(null); setTables([]); void run(() => loadNamespace(value), `Namespace ${value.join('.')} loaded`); };
  const selectTable = (name: string) => { setTable(name); setLoaded(null); setSection('Overview'); void run(() => loadTable(name), `Table ${name} loaded`); };
  const inspector = useInspection(loaded, tablePath, token, origin);
  const metadata = loaded?.metadata;
  const supported = (method: string, template: string) => !catalog?.endpoints || catalog.endpoints.some((entry: string) => entry === `${method} ${template}` || entry === `${method} ${template.replace('/v1/', '/v1/{prefix}/')}`);
  const overview = metadata ? { uuid: metadata['table-uuid'], location: metadata.location, 'format-version': metadata['format-version'], 'metadata-location': loaded['metadata-location'], 'current-snapshot-id': metadata['current-snapshot-id'], 'last-updated-ms': metadata['last-updated-ms'], properties: metadata.properties } : null;
  const schema = metadata ? { 'current-schema-id': metadata['current-schema-id'], schemas: metadata.schemas, 'default-spec-id': metadata['default-spec-id'], 'partition-specs': metadata['partition-specs'], 'default-sort-order-id': metadata['default-sort-order-id'], 'sort-orders': metadata['sort-orders'] } : null;
  const files = metadata ? { 'metadata-location': loaded['metadata-location'], 'metadata-log': metadata['metadata-log'], 'manifest-lists': (metadata.snapshots ?? []).map((snapshot: any) => ({ 'snapshot-id': snapshot['snapshot-id'], 'manifest-list': snapshot['manifest-list'] })) } : null;
  return <Workbench sidebar={<>
    <h2 className="tw-font-semibold">Iceberg</h2>
    <p className="tw-text-xs tw-text-muted">Current cluster catalog</p>
    {busy && !catalog && <p role="status" className="tw-text-xs">Loading catalog…</p>}
    {catalog && <button className={`${buttonClass} tw-w-full tw-text-left`} disabled={busy} onClick={() => { setNamespace(null); setTable(''); setLoaded(null); void run(loadNamespaces, 'Catalog refreshed'); }}>Catalog</button>}
    {catalog && !namespaces.length && <p className="tw-text-xs tw-text-muted">No namespaces in this catalog.</p>}
    {namespace && <button className={buttonClass} disabled={busy} onClick={() => void run(async () => { const response = await iceberg(`/v1/namespaces?parent=${encodeURIComponent(namespace.join('\x1f'))}`, token); setNamespaces(response.namespaces ?? []); }, 'Child namespaces loaded')}>Browse child namespaces</button>}
    <nav aria-label="Iceberg namespaces" className="tw-space-y-1">{namespaces.map(value => <div key={JSON.stringify(value)}>
      <button className={`${buttonClass} tw-w-full tw-text-left`} aria-pressed={JSON.stringify(namespace) === JSON.stringify(value)} disabled={busy} onClick={() => selectNamespace(value)}>{value.join('.')}</button>
      {JSON.stringify(namespace) === JSON.stringify(value) && <nav aria-label="Iceberg tables" className="tw-pl-4 tw-flex tw-flex-col">{tables.map(identifier => <div key={identifier.name}><button className={`${buttonClass} tw-text-left`} aria-pressed={table === identifier.name} disabled={busy} onClick={() => selectTable(identifier.name)}>{identifier.name}</button>{table === identifier.name && loaded && <ReferenceTree loaded={loaded} inspector={inspector} />}</div>)}</nav>}
    </div>)}</nav>
  </>}>
    <div><h1 className="tw-text-lg tw-font-semibold">{namespace ? `Catalog / ${namespace.join('.')}${table ? ` / ${table}` : ''}` : 'Iceberg catalog'}</h1><p className="tw-text-xs tw-text-muted">Current cluster · {readonly ? 'Read only' : 'Native REST metadata operations'}</p></div>
    {!domainReadonly && <details className="tw-text-xs tw-space-y-2"><summary className="tw-cursor-pointer">Catalog write authorization</summary>
      <p>Use a native Iceberg write token for catalog changes. The service checks its privileges on each operation. The token is kept only in this page session.</p>
      {token ? <button className={buttonClass} disabled={busy} onClick={() => setToken('')}>Clear catalog write token</button>
        : <form className="tw-flex tw-gap-2 tw-items-end" onSubmit={event => { event.preventDefault(); setToken(writeToken.trim()); setWriteToken(''); }}>
          <label>Catalog write token<input type="password" autoComplete="off" required className={`${inputClass} tw-block`} value={writeToken} onChange={event => setWriteToken(event.target.value)} /></label>
          <button className={buttonClass} disabled={busy || !writeToken.trim()}>Use catalog write token</button>
        </form>}
    </details>}
    {loaded && <div className="tw-space-y-3">
      <button className={buttonClass} disabled={busy} onClick={() => void run(() => loadTable(table), 'Metadata refreshed')}>Refresh table</button>
      {!readonly && <details className="tw-space-y-2"><summary className="tw-text-xs tw-cursor-pointer">Table actions</summary>
      <form className="tw-space-y-2" onSubmit={event => { event.preventDefault(); void run(async () => { await iceberg('/v1/tables/rename', token, 'POST', { source: { namespace, name: table }, destination: { namespace, name: rename } }); setTable(rename); await loadNamespace(namespace!); await loadTable(rename); }, 'Table renamed'); }}>
        <label className="tw-text-xs">New table name<input className={`${inputClass} tw-w-full`} value={rename} required onChange={event => setRename(event.target.value)} /></label><button className={buttonClass} disabled={busy || !supported('POST', '/v1/tables/rename')}>Rename table</button>
      </form>
      <button className={buttonClass} disabled={busy || !supported('DELETE', '/v1/namespaces/{namespace}/tables/{table}')} onClick={() => { if (confirm(`Drop table ${namespace?.join('.')}.${table}?`)) void run(async () => { await iceberg(tablePath, token, 'DELETE'); setTable(''); setLoaded(null); await loadNamespace(namespace!); }, 'Table dropped'); }}>Drop table</button>
      <p className="tw-text-xs tw-text-muted">Drop removes the catalog reference. Physical reclamation follows service GC.</p>
      </details>}
    </div>}
    {error && <p role="alert" className="tw-text-sm tw-text-failed tw-break-all">{error}</p>}{outcome && <p role="status" className="tw-text-xs tw-text-muted">{outcome}</p>}
    {error && <button className={buttonClass} disabled={busy} onClick={() => setRetry(value => value + 1)}>Retry catalog</button>}
    {!catalog && !error && <p className="tw-text-sm tw-text-muted">Loading the current cluster catalog…</p>}
    {catalog && !namespace && <Fields values={catalog} />}
    {namespace && !table && <><h2 className="tw-font-semibold">Namespace properties</h2><Fields values={properties ?? {}} />{!tables.length && <p className="tw-text-xs tw-text-muted">No tables in this namespace.</p>}</>}
    {loaded && <><nav className="tw-flex tw-gap-2" aria-label="Table sections">{sections.map(value => <button key={value} className={buttonClass} aria-pressed={section === value} onClick={() => { inspector.clear(); setSection(value); }}>{value}</button>)}</nav>
      {!inspector.selection && <Structured value={section === 'Overview' ? overview : section === 'Schema' ? schema : section === 'Snapshots' ? { snapshots: metadata?.snapshots, refs: metadata?.refs, 'snapshot-log': metadata?.['snapshot-log'] } : files} />}
      <InspectionView inspector={inspector} />
      {section === 'Files' && !inspector.selection && <p className="tw-text-xs tw-text-muted">Select a snapshot in the reference tree to inspect its manifest list, manifests and file metadata.</p>}
    </>}
    {!readonly && catalog && !inspector.selection && <section className="tw-space-y-3 tw-rounded tw-border tw-border-border tw-p-4">
      <div className="tw-flex tw-gap-2 tw-items-center tw-flex-wrap"><button className={buttonClass} disabled={busy || !!demoNamespace} onClick={() => void run(async () => {
        const demo = `console_demo_${crypto.randomUUID().replace(/-/g, '')}`;
        await iceberg('/v1/namespaces', token, 'POST', { namespace: [demo], properties: { purpose: 'console demo' } }); setDemoNamespace(demo);
        await iceberg(`${namespacePath([demo])}/tables`, token, 'POST', { name: 'example', schema: { type: 'struct', 'schema-id': 0, fields: JSON.parse(initialFields) } }); await loadNamespaces();
      }, 'Demo namespace and table created')}>Create metadata demo</button>
      {demoNamespace && <><span className="tw-text-xs tw-break-all">Demo scope: {demoNamespace} / example</span><button className={buttonClass} disabled={busy} onClick={() => { if (confirm(`Remove only demo table ${demoNamespace}.example and its empty namespace?`)) void run(async () => {
        try { await iceberg(`${namespacePath([demoNamespace])}/tables/example`, token, 'DELETE'); } catch (error) { if (!String(error).startsWith('Error: HTTP 404:')) throw error; }
        await iceberg(namespacePath([demoNamespace]), token, 'DELETE'); if (namespace?.[0] === demoNamespace) { setNamespace(null); setTable(''); setLoaded(null); } setDemoNamespace(null); await loadNamespaces();
      }, 'Demo resources removed'); }}>Clean metadata demo</button></>}</div>
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
        <button className={buttonClass} disabled={busy || !supported('DELETE', '/v1/namespaces/{namespace}')} onClick={() => { if (confirm(`Drop empty namespace ${namespace.join('.')}?`)) void run(async () => { await iceberg(path, token, 'DELETE'); setNamespace(null); setProperties(null); await loadNamespaces(); }, 'Namespace dropped'); }}>Drop namespace</button>
      </>}
      {loaded && <form className="tw-space-y-2" onSubmit={event => { event.preventDefault(); void run(async () => { await iceberg(tablePath, token, 'POST', { requirements: [{ type: 'assert-table-uuid', uuid: metadata['table-uuid'] }], updates: JSON.parse(updates) }); await loadTable(table); }, 'Metadata committed'); }}>
        <h2 className="tw-font-semibold">Commit table metadata</h2><p className="tw-text-xs tw-text-muted">Applies native Iceberg updates to this table UUID. Refresh after a conflict or interrupted request.</p><label className="tw-block tw-text-xs">Metadata updates (JSON)<textarea className={`${inputClass} tw-w-full`} rows={4} value={updates} onChange={event => setUpdates(event.target.value)} /></label><button className={buttonClass} disabled={busy || !supported('POST', '/v1/namespaces/{namespace}/tables/{table}')}>Commit metadata</button>
      </form>}
    </section>}
  </Workbench>;
}
