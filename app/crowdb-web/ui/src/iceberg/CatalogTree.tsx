// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
import { useEffect, useState } from 'react';
import { Database, Folder, Table2 } from 'lucide-react';
import { Tree } from '../components/Tree';
import { referenceNodes, type IcebergNode } from './ReferenceTree';
import type { Inspector } from './useInspection';
import type { TableLoad } from './types';

export interface NamespacePage {
  tables: Array<{ namespace: string[]; name: string }>;
  children: string[][];
  nextTables?: string | null;
  paged?: boolean;
  nextNamespaces?: string | null;
}
interface Props {
  namespaces: string[][];
  pages: Record<string, NamespacePage>;
  namespace: string[] | null;
  table: string;
  loaded: TableLoad | null;
  inspector: Inspector;
  onCatalog: () => void;
  onNamespace: (namespace: string[]) => void;
  onTable: (name: string, namespace: string[]) => void;
  onPage: (namespace: string[], tableToken?: string, namespaceToken?: string) => void;
  nextNamespaces?: string | null;
  onNextNamespaces: () => void;
  pagedNamespaces: boolean;
  onFirstNamespaces: () => void;
}
export function CatalogTree(props: Props) {
  const { namespace, table, inspector, loaded } = props;
  const [snapshotPage, setSnapshotPage] = useState(0);
  useEffect(() => setSnapshotPage(0), [loaded]);
  const activeNs = JSON.stringify(namespace);
  const expanded = ['ice-catalog'];
  if (namespace) expanded.push(`ice-ns-${activeNs}`);
  if (loaded) expanded.push(`ice-table-${activeNs}-${table}`, 'ice-snapshots');
  if (inspector.selection) {
    const s = inspector.selection;
    expanded.push(`ice-snapshot-${inspector.key({ snapshot: s.snapshot, kind: 'snapshot' })}`);
    if (s.kind !== 'snapshot') expanded.push(`ice-list-${inspector.key({ snapshot: s.snapshot, kind: 'list' })}`);
    if (s.manifest) expanded.push(`ice-manifest-${inspector.key({ snapshot: s.snapshot, kind: 'manifest', manifest: s.manifest })}`);
  }
  const namespaceNode = (ns: string[]): IcebergNode => {
    const key = JSON.stringify(ns);
    const page = props.pages[key];
    return { id: `ice-ns-${key}`, type: 'Iceberg', label: ns.at(-1) ?? '', title: `Namespace · ${ns.join('.')}`,
      icon: <Folder className="tw-h-4 tw-w-4 tw-text-muted" />, expandable: true,
      selected: activeNs === key && !table,
      activate: () => props.onNamespace(ns), onExpand: () => { if (!page) props.onNamespace(ns); },
      children: [...(page?.children ?? []).map(namespaceNode), ...(page?.tables ?? []).map(entry => {
        const selected = activeNs === key && table === entry.name;
        return { id: `ice-table-${key}-${entry.name}`, type: 'Iceberg' as const, label: entry.name, title: `Table · ${ns.join('.')}.${entry.name}`,
          icon: <Table2 className="tw-h-4 tw-w-4 tw-text-muted" />, expandable: true, selected: selected && !inspector.selection,
          activate: () => props.onTable(entry.name, ns), onExpand: () => { if (!selected) props.onTable(entry.name, ns); },
          footer: selected && loaded && (loaded.metadata.snapshots?.length ?? 0) > 20 && <div className="tw-flex tw-gap-2 tw-text-xs"><button disabled={!snapshotPage} onClick={() => setSnapshotPage(snapshotPage - 1)}>Previous snapshots</button><button disabled={(snapshotPage + 1) * 20 >= (loaded.metadata.snapshots?.length ?? 0)} onClick={() => setSnapshotPage(snapshotPage + 1)}>Next snapshots</button></div>,
          children: selected && loaded ? [
            ...referenceNodes(loaded, inspector, snapshotPage),
          ] : [],
        };
      })],
      footer: page && <div className="tw-text-xs tw-text-muted tw-py-2 tw-space-y-1"><p>{page.tables.length} tables loaded</p>{page.paged && <button onClick={() => props.onPage(ns)}>First page</button>}{page.nextTables && <button onClick={() => props.onPage(ns, page.nextTables!)}>Next tables</button>}{page.nextNamespaces && <button onClick={() => props.onPage(ns, undefined, page.nextNamespaces!)}>Next namespaces</button>}</div>,
    };
  };
  const root: IcebergNode = { id: 'ice-catalog', type: 'Iceberg', label: 'Catalog', title: 'Current cluster Iceberg catalog',
    icon: <Database className="tw-h-4 tw-w-4 tw-text-muted" />, selected: !namespace,
    activate: props.onCatalog, children: props.namespaces.map(namespaceNode),
    footer: <>{props.pagedNamespaces && <button className="tw-text-xs tw-text-accent tw-mr-2" onClick={props.onFirstNamespaces}>First namespaces</button>}{props.nextNamespaces && <button className="tw-text-xs tw-text-accent" onClick={props.onNextNamespaces}>Next namespaces</button>}</>,
  };
  return <nav aria-label="Iceberg tree" className="-tw-mx-4"><Tree nodes={[root]} defaultExpandedIds={expanded} onNodeClick={node => (node as IcebergNode).activate?.()} /></nav>;
}
