// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
import { useEffect, useState, type MouseEvent } from 'react';
import { ContextMenu, useContextMenu, type MenuItem } from '../components/ContextMenu';
import { Dialog } from '../components/Dialog';
import { inputClass } from '../access/Workbench';
import { iceberg } from '../access/native';
export interface CatalogTarget { namespace?: string[]; table?: string }
const path = (ns: string[]) => `/v1/namespaces/${encodeURIComponent(ns.join('\x1f'))}`;
type Action = 'namespace' | 'table' | 'properties' | 'rename' | 'commit' | 'drop-table' | 'drop-namespace';
const labels: Record<Action, string> = { namespace: 'Create namespace', table: 'Create table', properties: 'Edit properties', rename: 'Rename table', commit: 'Commit metadata', 'drop-table': 'Drop table', 'drop-namespace': 'Drop namespace' };
export function useIcebergMenu(readonly: boolean, supported: (method: string, endpoint: string) => boolean, changed: (target: CatalogTarget, action: string) => Promise<void>) {
  const menu = useContextMenu();
  const [dialog, setDialog] = useState<{ action: Action; target: CatalogTarget }>();
  const [uuid, setUuid] = useState<string>();
  useEffect(() => {
    setUuid(undefined);
    if (dialog?.action !== 'commit') return;
    const controller = new AbortController();
    void iceberg(`${path(dialog.target.namespace!)}/tables/${encodeURIComponent(dialog.target.table!)}`, '', 'GET', undefined, controller.signal)
      .then(value => { if (!controller.signal.aborted) setUuid(value.metadata['table-uuid']); })
      .catch(reason => { if (!controller.signal.aborted) setError(String(reason)); });
    return () => controller.abort();
  }, [dialog]);
  const [name, setName] = useState('');
  const [json, setJson] = useState('{}');
  const [removals, setRemovals] = useState('[]');
  const [busy, setBusy] = useState(false); const [error, setError] = useState('');
  const open = (event: MouseEvent, target: CatalogTarget) => {
    const items: MenuItem[] = [];
    const add = (action: Action, method: string, endpoint: string) => {
      if (!supported(method, endpoint)) return;
      items.push({ id: action, label: labels[action] + '…', disabled: readonly || busy, destructive: action.startsWith('drop-'), onSelect: () => {
        setName(''); setRemovals('[]'); setError('');
        setJson(action === 'table' ? '[{"id":1,"name":"id","required":true,"type":"long"}]' : action === 'commit' ? '[]' : '{}');
        setDialog({ action, target: { ...target, namespace: target.namespace && [...target.namespace] } });
      } });
    };
    if (target.table) { add('rename', 'POST', '/v1/tables/rename'); add('commit', 'POST', '/v1/namespaces/{namespace}/tables/{table}'); add('drop-table', 'DELETE', '/v1/namespaces/{namespace}/tables/{table}'); }
    else { add('namespace', 'POST', '/v1/namespaces'); if (target.namespace) { add('table', 'POST', '/v1/namespaces/{namespace}/tables'); add('properties', 'POST', '/v1/namespaces/{namespace}/properties'); add('drop-namespace', 'DELETE', '/v1/namespaces/{namespace}'); } }
    if (items.length) menu.openMenu(event, items);
  };
  const submit = async () => {
    if (!dialog || readonly || busy) return;
    const { action, target } = dialog; const ns = target.namespace ?? []; const tablePath = `${path(ns)}/tables/${encodeURIComponent(target.table ?? '')}`;
    setBusy(true); setError('');
    try {
      if (action === 'namespace') await iceberg('/v1/namespaces', '', 'POST', { namespace: [...ns, ...name.split('.')], properties: JSON.parse(json) });
      if (action === 'table') await iceberg(`${path(ns)}/tables`, '', 'POST', { name, schema: { type: 'struct', 'schema-id': 0, fields: JSON.parse(json) } });
      if (action === 'properties') await iceberg(`${path(ns)}/properties`, '', 'POST', { updates: JSON.parse(json), removals: JSON.parse(removals) });
      if (action === 'rename') await iceberg('/v1/tables/rename', '', 'POST', { source: { namespace: ns, name: target.table }, destination: { namespace: ns, name } });
      if (action === 'commit') { if (!uuid) throw new Error('Table identity is not loaded'); await iceberg(tablePath, '', 'POST', { requirements: [{ type: 'assert-table-uuid', uuid }], updates: JSON.parse(json) }); }
      if (action === 'drop-table') await iceberg(tablePath, '', 'DELETE');
      if (action === 'drop-namespace') await iceberg(path(ns), '', 'DELETE');
      await changed(target, action); setDialog(undefined);
    } catch (reason) { setError(`${String(reason)}. Refresh resource state before retrying.`); }
    finally { setBusy(false); }
  };
  const needsName = dialog && ['namespace', 'table', 'rename'].includes(dialog.action);
  const needsJson = dialog && ['namespace', 'table', 'properties', 'commit'].includes(dialog.action);
  const jsonLabel = dialog?.action === 'table' ? 'Schema fields (JSON)' : dialog?.action === 'commit' ? 'Metadata updates (JSON)' : 'Properties (JSON)';
  const overlay = <>
    {menu.menuState && <ContextMenu {...menu.menuState} onClose={menu.closeMenu} />}
    {dialog && <Dialog isOpen title={labels[dialog.action]} description={`Target: ${dialog.target.namespace?.join('.') || 'Catalog'}${dialog.target.table ? ` / ${dialog.target.table}` : ''}`} onClose={() => { if (!busy) setDialog(undefined); }} confirmLabel={labels[dialog.action]} confirmDisabled={readonly || dialog.action === 'commit' && !uuid || !!needsName && !name.trim()} confirmLoading={busy} onConfirm={submit} destructive={dialog.action.startsWith('drop-')}>
      <div className="tw-space-y-3">
        {needsName && <label className="tw-block tw-text-sm">{dialog.action === 'namespace' ? 'Namespace name' : dialog.action === 'rename' ? 'New table name' : 'Table name'}<input className={`${inputClass} tw-w-full`} value={name} onChange={event => setName(event.target.value)} /></label>}
        {needsJson && <label className="tw-block tw-text-sm">{jsonLabel}<textarea className={`${inputClass} tw-w-full`} rows={5} value={json} onChange={event => setJson(event.target.value)} /></label>}
        {dialog.action === 'properties' && <label className="tw-block tw-text-sm">Property removals (JSON array)<input className={`${inputClass} tw-w-full`} value={removals} onChange={event => setRemovals(event.target.value)} /></label>}
        {dialog.action === 'drop-table' && <p>Remove this Catalog reference. Physical reclamation follows service GC.</p>}
        {dialog.action === 'drop-namespace' && <p>The namespace must be empty before it can be dropped.</p>}
        {error && <p role="alert" className="tw-text-failed">{error}</p>}
      </div>
    </Dialog>}
  </>;
  return { open, overlay };
}
