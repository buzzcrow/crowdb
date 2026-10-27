// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
import { useEffect, useRef, useState } from 'react';
import { iceberg } from '../access/native';
import type { NamespacePage } from './CatalogTree';

/** Tree expansion and continuation never change the center's resource or page. */
export function useCatalogTree(active: boolean, revision: number, namespace: string[] | null) {
  const [namespaces, setNamespaces] = useState<string[][]>([]);
  const [next, setNext] = useState<string | null>(null);
  const [pages, setPages] = useState<Record<string, NamespacePage>>({});
  const [error, setError] = useState('');
  const [cursor, setCursor] = useState<string>();
  const requests = useRef(new Map<string, AbortController>());
  const start = (key: string) => {
    requests.current.get(key)?.abort();
    const controller = new AbortController(); requests.current.set(key, controller);
    return controller;
  };
  const loadRoot = async (token?: string) => {
    const controller = start('root'); setError('');
    try {
      const value = await iceberg(`/v1/namespaces?pageSize=30${token ? `&pageToken=${encodeURIComponent(token)}` : ''}`, '', 'GET', undefined, controller.signal);
      if (controller.signal.aborted) return;
      setNamespaces(value.namespaces ?? []); setNext(value['next-page-token'] ?? null); setCursor(token);
    } catch (reason) { if (!controller.signal.aborted) setError(String(reason)); }
    finally { if (requests.current.get('root') === controller) requests.current.delete('root'); }
  };
  const loadNamespace = async (ns: string[], table?: string, child?: string) => {
    const key = JSON.stringify(ns); const controller = start(key); setError('');
    // An absent Table pageToken requests a complete list; an empty token starts pagination.
    const query = (token?: string) => `pageSize=30&pageToken=${encodeURIComponent(token ?? '')}`;
    try {
      const encoded = encodeURIComponent(ns.join('\x1f'));
      const [tables, children] = await Promise.all([
        iceberg(`/v1/namespaces/${encoded}/tables?${query(table)}`, '', 'GET', undefined, controller.signal),
        iceberg(`/v1/namespaces?parent=${encoded}&${query(child)}`, '', 'GET', undefined, controller.signal),
      ]);
      if (controller.signal.aborted) return;
      const page: NamespacePage = { paged: !!table || !!child, tables: tables.identifiers ?? [],
        children: (children.namespaces ?? []).filter((value: string[]) => value.length > ns.length && ns.every((part, index) => part === value[index])),
        nextTables: tables['next-page-token'], nextNamespaces: children['next-page-token'] };
      setPages(previous => Object.fromEntries([...Object.entries(previous).filter(([name]) => name !== key).slice(-7), [key, page]]));
    } catch (reason) { if (!controller.signal.aborted) setError(String(reason)); }
    finally { if (requests.current.get(key) === controller) requests.current.delete(key); }
  };
  useEffect(() => {
    if (!active) return;
    setPages({}); void loadRoot(cursor);
    return () => { requests.current.forEach(controller => controller.abort()); requests.current.clear(); };
  }, [active, revision]);
  useEffect(() => {
    if (active && namespace) void loadNamespace(namespace);
  }, [active, revision, JSON.stringify(namespace)]);
  return { namespaces, pages, next, cursor, error, loadRoot, loadNamespace };
}
