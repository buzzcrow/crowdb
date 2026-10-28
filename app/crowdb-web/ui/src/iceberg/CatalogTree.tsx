// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
import { useEffect, useState, type MouseEvent } from 'react';
import { Database, Folder, Table2 } from 'lucide-react';
import type { CatalogTarget } from './ResourceMenu';
import { Tree } from '../components/Tree';
import { referenceNodes, type IcebergNode } from './ReferenceTree';
import type { Inspector } from './useInspection';
import type { TableLoad } from './types';
import { useCatalogTree } from './useCatalogTree';

export interface NamespacePage {
  tables: Array<{ namespace: string[]; name: string }>;
  children: string[][];
  nextTables?: string | null;
  paged?: boolean;
  nextNamespaces?: string | null;
}
interface Props {
  active: boolean;
  revision: number;
  onContextMenu: (event: MouseEvent, target: CatalogTarget) => void;
  namespace: string[] | null;
  table: string;
  loaded: TableLoad | null;
  inspector: Inspector;
  onCatalog: () => void;
  onNamespace: (namespace: string[]) => void;
  onTable: (name: string, namespace: string[]) => void;
}
export function CatalogTree(props: Props) {
  const { namespace, table, inspector, loaded } = props;
  const catalog = useCatalogTree(props.active, props.revision, namespace);
  const [snapshotPage, setSnapshotPage] = useState(0);
  useEffect(() => setSnapshotPage(0), [loaded]);
  const activeNs = JSON.stringify(namespace);
  const expanded = ['ice-catalog'];
  if (namespace) for (let level = 1; level <= namespace.length; level++) expanded.push(`ice-ns-${JSON.stringify(namespace.slice(0, level))}`);
  if (loaded) expanded.push(`ice-table-${activeNs}-${table}`, 'ice-snapshots');
  if (inspector.selection) {
    const s = inspector.selection;
    expanded.push(`ice-snapshot-${inspector.key({ snapshot: s.snapshot, kind: 'snapshot' })}`);
    if (s.kind !== 'snapshot') expanded.push(`ice-list-${inspector.key({ snapshot: s.snapshot, kind: 'list' })}`);
    if (s.manifest) expanded.push(`ice-manifest-${inspector.key({ snapshot: s.snapshot, kind: 'manifest', manifest: s.manifest })}`);
  }
  const namespaceNode = (ns: string[]): IcebergNode => {
    const key = JSON.stringify(ns);
    const page = catalog.pages[key];
    const children = [...(page?.children ?? [])];
    const tables = [...(page?.tables ?? [])];
    const onPath = namespace && ns.every((part, index) => part === namespace[index]);
    if (onPath && namespace.length > ns.length && !children.some(child => child.length === ns.length + 1 && child.every((part, index) => part === namespace[index]))) children.push(namespace.slice(0, ns.length + 1));
    if (activeNs === key && table && !tables.some(entry => entry.name === table)) tables.push({ namespace: ns, name: table });
    return { resource: { namespace: ns }, id: `ice-ns-${key}`, type: 'Iceberg', label: ns.at(-1) ?? '', title: `Namespace · ${ns.join('.')}`,
      icon: <Folder className="tw-h-4 tw-w-4 tw-text-muted" />, expandable: true,
      selected: activeNs === key && !table,
      activate: () => props.onNamespace(ns), onExpand: () => { if (!page) void catalog.loadNamespace(ns); },
      children: [...children.map(namespaceNode), ...tables.map(entry => {
        const selected = activeNs === key && table === entry.name;
        return { resource: { namespace: ns, table: entry.name }, id: `ice-table-${key}-${entry.name}`, type: 'Iceberg' as const, label: entry.name, title: `Table · ${ns.join('.')}.${entry.name}`,
          icon: <Table2 className="tw-h-4 tw-w-4 tw-text-muted" />, expandable: selected && !!loaded, selected: selected && !inspector.selection,
          activate: () => props.onTable(entry.name, ns),
          footer: selected && loaded && (loaded.metadata.snapshots?.length ?? 0) > 20 && <div className="tw-flex tw-gap-2 tw-text-xs"><button disabled={!snapshotPage} onClick={() => setSnapshotPage(snapshotPage - 1)}>Previous snapshots</button><button disabled={(snapshotPage + 1) * 20 >= (loaded.metadata.snapshots?.length ?? 0)} onClick={() => setSnapshotPage(snapshotPage + 1)}>Next snapshots</button></div>,
          children: selected && loaded ? [
            ...referenceNodes(loaded, inspector, snapshotPage, { namespace: ns, table: entry.name }),
          ] : [],
        };
      })],
      footer: page && <div className="tw-text-xs tw-text-muted tw-py-2 tw-space-y-1"><p>{page.tables.length} tables loaded</p>{page.paged && <button onClick={() => void catalog.loadNamespace(ns)}>First page</button>}{page.nextTables && <button onClick={() => void catalog.loadNamespace(ns, page.nextTables!)}>Next tables</button>}{page.nextNamespaces && <button onClick={() => void catalog.loadNamespace(ns, undefined, page.nextNamespaces!)}>Next namespaces</button>}</div>,
    };
  };
  const roots = [...catalog.namespaces];
  if (namespace?.length && !roots.some(ns => ns[0] === namespace[0])) roots.push(namespace.slice(0, 1));
  const root: IcebergNode & { resource: CatalogTarget } = { resource: {}, id: 'ice-catalog', type: 'Iceberg', label: 'Catalog', title: 'Current cluster Iceberg catalog',
    icon: <Database className="tw-h-4 tw-w-4 tw-text-muted" />, selected: !namespace,
    activate: props.onCatalog, children: roots.map(namespaceNode),
    footer: <>{catalog.cursor && <button className="tw-text-xs tw-text-accent tw-mr-2" onClick={() => void catalog.loadRoot()}>First namespaces</button>}{catalog.next && <button className="tw-text-xs tw-text-accent" onClick={() => void catalog.loadRoot(catalog.next!)}>Next namespaces</button>}</>,
  };
  return <nav aria-label="Iceberg tree" className="-tw-mx-4">{catalog.error && <p role="alert">{catalog.error}</p>}<Tree nodes={[root]} defaultExpandedIds={expanded} onNodeClick={node => (node as IcebergNode).activate?.()} onNodeContextMenu={(node, event) => { const resource = (node as IcebergNode & { resource?: CatalogTarget }).resource; if (resource) props.onContextMenu(event, resource); }} /></nav>;
}
