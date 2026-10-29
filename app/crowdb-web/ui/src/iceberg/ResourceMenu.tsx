// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
import { randomUUID } from '../utils/randomUUID';
import { useState, type MouseEvent } from 'react';
import { ContextMenu, useContextMenu, type MenuItem } from '../components/ContextMenu';
import { Dialog } from '../components/Dialog';
import { inputClass } from '../access/Workbench';
import { iceberg } from '../access/native';
import { TableEditor } from './import/Editor';
import { MetadataEditor } from './MetadataEditor';
import type { Selection, TableLoad } from './types';
import { MaintenanceEditor } from './maintenance/Editor';
import { load as loadMaintenance, protection } from './maintenance/operation';
export type ReferenceMenu = (event: MouseEvent, selection: Selection) => void;
export interface CatalogTarget { namespace?: string[]; table?: string; selection?: Selection; loaded?: TableLoad }
const path = (ns: string[]) => `/v1/namespaces/${encodeURIComponent(ns.join('\x1f'))}`;
const defaultNamespaceName = () => `namespace-${randomUUID().replaceAll('-', '').slice(0, 8)}`;
type Action = 'namespace' | 'table' | 'append' | 'properties' | 'rename' | 'commit' | 'drop-table' | 'drop-namespace' | 'remove-file' | 'expire-snapshot';
const labels: Record<Action, string> = { 'remove-file': 'Remove from table', 'expire-snapshot': 'Expire snapshot', namespace: 'Create namespace', table: 'Create table', append: 'Edit as Draft', properties: 'Edit properties', rename: 'Rename table', commit: 'Commit metadata', 'drop-table': 'Drop table', 'drop-namespace': 'Drop namespace' };
export function useIcebergMenu(readonly: boolean, supported: (method: string, endpoint: string) => boolean, changed: (target: CatalogTarget, action: string) => Promise<void>, guarded = false) {
  const menu = useContextMenu();
  const [dialog, setDialog] = useState<{ action: Action; target: CatalogTarget; legacyMaintenance?: boolean }>();
  const [name, setName] = useState('');
  const [json, setJson] = useState('{}');
  const [removals, setRemovals] = useState('[]');
  const [busy, setBusy] = useState(false); const [error, setError] = useState('');
  const open = (event: MouseEvent, target: CatalogTarget) => {
    const items: MenuItem[] = [];
    if (target.table && target.namespace && !target.selection) {
      try {
        const saved = loadMaintenance(target.namespace, target.table);
        if (saved) items.push({ id: 'resume-maintenance', label: 'Resume maintenance…', disabled: readonly || !guarded,
          onSelect: () => setDialog({ action: saved.action, legacyMaintenance: true, target: { ...target, loaded: saved.table, selection: saved.target } }) });
      } catch (reason) { items.push({ id: 'invalid-maintenance', label: 'Saved maintenance is invalid', hint: String(reason), disabled: true }); }
    }
    const add = (action: Action, method: string, endpoint: string) => {
      if (!supported(method, endpoint)) return;
      const maintenance = action === 'remove-file' || action === 'expire-snapshot';
      const problem = maintenance && target.loaded && target.selection ? protection(target.loaded, target.selection, action) : undefined;
      items.push({ id: action, label: labels[action] + '…', hint: problem, disabled: readonly || busy || !!problem || maintenance && !guarded, destructive: action.startsWith('drop-') || maintenance, onSelect: () => {
        setName(action === 'namespace' ? defaultNamespaceName() : ''); setRemovals('[]'); setError('');
        setJson(action === 'table' ? '[{"id":1,"name":"id","required":true,"type":"long"}]' : action === 'commit' ? '[]' : '{}');
        setDialog({ action, target: { ...target, namespace: target.namespace && [...target.namespace] } });
      } });
    };
    if (target.selection) {
      if (target.selection.kind === 'snapshot') { add('append', 'POST', '/v1/namespaces/{namespace}/tables/{table}'); add('expire-snapshot', 'POST', '/v1/namespaces/{namespace}/tables/{table}'); }
      if (target.selection.kind === 'file') add('remove-file', 'POST', '/v1/namespaces/{namespace}/tables/{table}');
    } else if (target.table) { add('append', 'POST', '/v1/namespaces/{namespace}/tables/{table}'); add('rename', 'POST', '/v1/tables/rename'); add('commit', 'POST', '/v1/namespaces/{namespace}/tables/{table}'); add('drop-table', 'DELETE', '/v1/namespaces/{namespace}/tables/{table}'); }
    else { add('namespace', 'POST', '/v1/namespaces'); if (target.namespace) { add('table', 'POST', '/v1/namespaces/{namespace}/tables'); add('properties', 'POST', '/v1/namespaces/{namespace}/properties'); add('drop-namespace', 'DELETE', '/v1/namespaces/{namespace}'); } }
    if (items.length) menu.openMenu(event, items);
  };
  const submit = async () => {
    if (!dialog || readonly || busy) return;
    const { action, target } = dialog; const ns = target.namespace ?? []; const tablePath = `${path(ns)}/tables/${encodeURIComponent(target.table ?? '')}`;
    setBusy(true); setError('');
    try {
      if (action === 'namespace') await iceberg('/v1/namespaces', '', 'POST', { namespace: [...ns, name], properties: JSON.parse(json) });
      if (action === 'properties') await iceberg(`${path(ns)}/properties`, '', 'POST', { updates: JSON.parse(json), removals: JSON.parse(removals) });
      if (action === 'rename') await iceberg('/v1/tables/rename', '', 'POST', { source: { namespace: ns, name: target.table }, destination: { namespace: ns, name } });
      if (action === 'drop-table') await iceberg(tablePath, '', 'DELETE');
      if (action === 'drop-namespace') await iceberg(path(ns), '', 'DELETE');
      await changed(target, action); setDialog(undefined);
    } catch (reason) { setError(`${String(reason)}. Refresh resource state before retrying.`); }
    finally { setBusy(false); }
  };
  const needsName = dialog && ['namespace', 'rename'].includes(dialog.action);
  const needsJson = dialog && ['namespace', 'properties'].includes(dialog.action);
  const jsonLabel = 'Properties (JSON)';
  const editing = dialog && ['table', 'append', 'commit', 'remove-file', 'expire-snapshot'].includes(dialog.action);
  const overlay = <>
    {menu.menuState && <ContextMenu {...menu.menuState} onClose={menu.closeMenu} />}
    {editing && (dialog.action === 'expire-snapshot' || dialog.legacyMaintenance ? <MaintenanceEditor namespace={dialog.target.namespace!} name={dialog.target.table!} table={dialog.target.loaded!} target={dialog.target.selection!} action={dialog.action as 'remove-file' | 'expire-snapshot'} close={() => setDialog(undefined)} complete={async () => { await changed(dialog.target, 'commit'); setDialog(undefined); }} /> : dialog.action === 'commit' ? <MetadataEditor namespace={dialog.target.namespace!} name={dialog.target.table!} close={() => setDialog(undefined)} complete={async () => { await changed(dialog.target, 'commit'); setDialog(undefined); }} /> : <TableEditor key={`${dialog.action}:${dialog.target.table}:${dialog.target.selection?.snapshot['snapshot-id']}`} namespace={dialog.target.namespace!} table={dialog.action === 'table' ? undefined : dialog.target.table} selection={dialog.target.selection} guarded={guarded} close={() => setDialog(undefined)} complete={async name => { await changed({ ...dialog.target, table: name }, 'commit'); setDialog(undefined); }} />)}
    {dialog && !editing && <Dialog isOpen title={labels[dialog.action]} description={`Target: ${dialog.target.namespace?.join('.') || 'Catalog'}${dialog.target.table ? ` / ${dialog.target.table}` : ''}`} onClose={() => { if (!busy) setDialog(undefined); }} confirmLabel={labels[dialog.action]} confirmDisabled={readonly || !!needsName && !name.trim()} confirmLoading={busy} onConfirm={submit} destructive={dialog.action.startsWith('drop-')}>
      <div className="tw-space-y-3">
        {needsName && <label className="tw-block tw-text-sm">{dialog.action === 'namespace' ? 'Namespace name' : dialog.action === 'rename' ? 'New table name' : 'Table name'}<input className={`${inputClass} tw-w-full`} value={name} onChange={event => setName(event.target.value)} /></label>}
        {needsJson && <label className="tw-block tw-text-sm">{jsonLabel}<textarea aria-label={jsonLabel} className={`${inputClass} tw-w-full`} rows={5} value={json} onChange={event => setJson(event.target.value)} /></label>}
        {dialog.action === 'properties' && <label className="tw-block tw-text-sm">Property removals (JSON array)<input className={`${inputClass} tw-w-full`} value={removals} onChange={event => setRemovals(event.target.value)} /></label>}
        {dialog.action === 'drop-table' && <p>Remove this Catalog reference. Physical reclamation follows service GC.</p>}
        {dialog.action === 'drop-namespace' && <p>The namespace must be empty before it can be dropped.</p>}
        {error && <p role="alert" className="tw-text-failed">{error}</p>}
      </div>
    </Dialog>}
  </>;
  return { open, overlay, editing: !!editing, closeEditor: () => setDialog(undefined) };
}
