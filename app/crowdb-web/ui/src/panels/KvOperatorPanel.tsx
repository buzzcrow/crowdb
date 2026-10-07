// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

import { useState, useCallback, useEffect, useMemo, useRef } from 'react';
import { Search, Info, Loader2, Copy, AlertTriangle } from 'lucide-react';
import { useNavigationSnapshot } from '../contexts/DomainContext';
import { Domain } from '../types';
import { displayBytes, printableBytes } from '../kv/displayBytes';
import { ByteDisplay } from '../kv/ByteDisplay';
import { OwnershipPanel } from '../chunk/ownership/OwnershipPanel';
import { buttonClass } from '../access/Workbench';
import { useToast } from '../contexts/ToastContext';
import { useActivity } from '../contexts/ActivityContext';
import { kvGet, kvScan, type KvGetResponse, type KvScanItem } from '../api';
import type { EnrichedStoreView, GroupView } from '../types';
import type { SelectedEntity } from '../contexts/SelectionContext';

type Cursor = Map<string, { lastKey: string; truncated: boolean }>;
interface QueryState {
  storeId: string; groupId: string; prefix: string;
  start: Array<[string, { lastKey: string; truncated: boolean }]>;
  previous: Array<Array<[string, { lastKey: string; truncated: boolean }]>>;
  number: number; focused?: { group: string; key: string };
}

const ALL_GROUPS = '__all__';

interface ScanRow extends KvScanItem {
  groupId: string;
  revision?: number;
}

interface KvOperatorPanelProps {
  active?: boolean;
  stores: EnrichedStoreView[];
  selectedEntity: SelectedEntity | null;
  readonly?: boolean;
  /** True when the backend is unreachable (fetch error). */
  backendError?: boolean;
  /** True when the initial data load is in progress. */
  loading?: boolean;
}

export function KvOperatorPanel({ stores, selectedEntity, backendError, loading, active = true }: KvOperatorPanelProps) {
  const { success, error } = useToast();
  const { log } = useActivity();

  const [storeId, setStoreId] = useState('');
  const [groupId, setGroupId] = useState('');
  const [scanPrefix, setScanPrefix] = useState('');
  const [scanRows, setScanRows] = useState<ScanRow[]>([]);
  const [scanTruncated, setScanTruncated] = useState(false);
  const [scanLoading, setScanLoading] = useState(false);
  const [scanCursors, setScanCursors] = useState<Map<string, { lastKey: string; truncated: boolean }>>(new Map());
  const [loadingMore, setLoadingMore] = useState(false);
  const [pageStarts, setPageStarts] = useState<Cursor[]>([]);
  const [pageStart, setPageStart] = useState<Cursor>(new Map());
  const [pageNumber, setPageNumber] = useState(1);
  const [focusedRow, setFocusedRow] = useState<ScanRow | null>(null);
  const [autoScanned, setAutoScanned] = useState(false);
  const [scanDone, setScanDone] = useState(false);
  const [errorMsg, setErrorMsg] = useState<string | null>(null);

  const [getKey, setGetKey] = useState('');
  const [getResult, setGetResult] = useState<KvGetResponse | null>(null);
  const [getLoading, setGetLoading] = useState(false);

  const scanReqIdRef = useRef(0);
  const scanAbortRef = useRef<AbortController>();
  const refreshTimerRef = useRef<ReturnType<typeof setTimeout>>();
  const cancelRefresh = useCallback(() => {
    clearTimeout(refreshTimerRef.current);
    refreshTimerRef.current = undefined;
  }, []);
  const restoreQuery = useRef<QueryState | null>(null);
  const [restoreVersion, setRestoreVersion] = useState(0);
  useNavigationSnapshot(Domain.KV, 'operator-query', () => {
    const state: QueryState = { storeId, groupId, prefix: scanPrefix, start: [...pageStart],
      previous: pageStarts.slice(-32).map(cursor => [...cursor]), number: pageNumber,
      focused: focusedRow ? { group: focusedRow.groupId, key: focusedRow.key_hex } : undefined };
    return () => {
      restoreQuery.current = state;
      setStoreId(state.storeId); setGroupId(state.groupId); setScanPrefix(state.prefix);
      setRestoreVersion(value => value + 1);
    };
  });
  const activeRef = useRef(active);
  activeRef.current = active;

  const groupsInStore = useMemo(() => {
    if (!storeId) return [] as GroupView[];
    const store = stores.find((s) => String(s.store_id) === storeId);
    return store?.groups || [];
  }, [storeId, stores]);

  const groupIdsInStore = useMemo(() => groupsInStore.map((g) => String(g.group_id)), [groupsInStore]);

  // Group 0 is the system group (topology metadata). It is queryable but
  // read-only from the KV operator panel — no put/delete/demo inject.
  const isSystemGroup = storeId === '0' && groupId === '0';
  useEffect(() => {
    if (stores.length > 0 && !storeId) {
      // Prefer the first non-system store (store 0 is read-only topology).
      const firstWritable = stores.find((s) => String(s.store_id) !== '0');
      setStoreId(String((firstWritable ?? stores[0]).store_id));
    }
  }, [stores, storeId]);

  useEffect(() => {
    if (groupIdsInStore.length > 0 && !groupId) {
      // Prefer the first non-system group (group 0 in store 0 is read-only).
      const firstWritable = groupIdsInStore.find((gid) => !(storeId === '0' && gid === '0'));
      setGroupId(firstWritable ?? groupIdsInStore[0]);
    }
    if (groupId && !groupIdsInStore.includes(groupId) && groupId !== ALL_GROUPS) {
      setGroupId(groupIdsInStore[0] || '');
    }
  }, [groupIdsInStore, groupId, storeId]);

  useEffect(() => {
    if (selectedEntity?.domain === 'KV') {
      const sid = selectedEntity.type === 'Store' ? selectedEntity.id : selectedEntity.parentIds?.store_id;
      const gid = selectedEntity.type === 'Group' ? selectedEntity.id : selectedEntity.parentIds?.group_id;
      if (sid != null) {
        setStoreId(String(sid));
        setGroupId(gid == null ? ALL_GROUPS : String(gid));
        setScanRows([]);
        setAutoScanned(false);
        setScanDone(false);
        setGetResult(null);
        setErrorMsg(null);
      }
    }
  }, [selectedEntity]);

  const handleStoreChange = useCallback((sid: string) => {
    setStoreId(sid);
    setGroupId('');
    setScanRows([]);
    setAutoScanned(false);
    setScanDone(false);
    setGetResult(null);
    setErrorMsg(null);
  }, []);

  const handleGroupChange = useCallback((gid: string) => {
    setGroupId(gid);
    setScanRows([]);
    setAutoScanned(false);
    setScanDone(false);
    setGetResult(null);
    setErrorMsg(null);
  }, []);

  // Guard against stale scan responses overwriting current state. When
  // store/group changes, a new handleScan closure is created but the old
  // one's await kvScan may still be in flight; without this guard the old
  // response silently overwrites the table with wrong-store data.
  useEffect(() => {
    cancelRefresh();
    ++scanReqIdRef.current;
    scanAbortRef.current?.abort();
    setScanLoading(false);
    setLoadingMore(false);
    setAutoScanned(false);
    setGetResult(null);
    setFocusedRow(null);
    setScanRows([]); setScanDone(false); setScanCursors(new Map()); setScanTruncated(false);
    setPageStarts([]); setPageStart(new Map()); setPageNumber(1);
    return () => { cancelRefresh(); ++scanReqIdRef.current; scanAbortRef.current?.abort(); };
  }, [storeId, groupId, scanPrefix, cancelRefresh]);

  const fetchPage = useCallback(async (start: Cursor, direction: 'first' | 'next' | 'previous' | 'restore', focus?: QueryState['focused']) => {
    cancelRefresh();
    if (!storeId || !groupId || !activeRef.current) return;
    if (groupId === ALL_GROUPS && groupIdsInStore.length > 10) { setErrorMsg('All Groups is limited to 10 groups. Select a specific group.'); return; }
    scanAbortRef.current?.abort();
    const controller = new AbortController(); scanAbortRef.current = controller;
    const request = ++scanReqIdRef.current;
    setScanLoading(true); setLoadingMore(true); setErrorMsg(null);
    try {
      const gids = groupId === ALL_GROUPS ? groupIdsInStore : [groupId];
      const cursors = new Map(start);
      const rows: ScanRow[] = [];
      for (const gid of gids) {
        const cursor = cursors.get(gid);
        if (cursor && !cursor.truncated) continue;
        if (rows.length >= 20) break;
        const result = await kvScan(storeId, gid, scanPrefix, 20 - rows.length, undefined, { signal: controller.signal }, cursor?.lastKey);
        if (request !== scanReqIdRef.current) return;
        rows.push(...result.items.map(item => ({ ...item, groupId: gid })));
        cursors.set(gid, { lastKey: result.items.at(-1)?.key_hex ?? cursor?.lastKey ?? '', truncated: result.truncated });
      }
      setScanRows(rows); setScanCursors(cursors); setScanTruncated(gids.some(gid => !cursors.has(gid) || cursors.get(gid)!.truncated));
      setScanDone(true); setFocusedRow(focus ? rows.find(row => row.groupId === focus.group && row.key_hex === focus.key) ?? null : null); setPageStart(start);
      if (direction === 'first') { setPageStarts([]); setPageNumber(1); }
      else if (direction === 'next') { setPageStarts(previous => [...previous, pageStart].slice(-32)); setPageNumber(value => value + 1); }
      else if (direction === 'previous') { setPageStarts(previous => previous.slice(0, -1)); setPageNumber(value => value - 1); }
    } catch (err) {
      if (request === scanReqIdRef.current) setErrorMsg(err instanceof Error ? err.message : 'Scan failed');
    } finally {
      if (request === scanReqIdRef.current) { setScanLoading(false); setLoadingMore(false); }
    }
  }, [storeId, groupId, groupIdsInStore, scanPrefix, pageStart, cancelRefresh]);
  useEffect(() => {
    if (!active) {
      cancelRefresh();
      scanAbortRef.current?.abort(); ++scanReqIdRef.current;
      setScanLoading(false); setLoadingMore(false);
      if (!scanDone) setAutoScanned(false);
      return;
    }
    const state = restoreQuery.current;
    if (!state || state.storeId !== storeId || state.groupId !== groupId || state.prefix !== scanPrefix) return;
    restoreQuery.current = null;
    setPageStarts(state.previous.map(cursor => new Map(cursor))); setPageNumber(state.number);
    setAutoScanned(true);
    void fetchPage(new Map(state.start), 'restore', state.focused);
  }, [active, storeId, groupId, scanPrefix, restoreVersion, fetchPage, scanDone, cancelRefresh]);

  const handleScan = useCallback(() => fetchPage(new Map(), 'first'), [fetchPage]);

  useEffect(() => {
    if (active && !restoreQuery.current && storeId && groupId && !autoScanned && !scanLoading && scanRows.length === 0) {
      setAutoScanned(true);
      handleScan();
    }
  }, [active, storeId, groupId, autoScanned, scanLoading, scanRows.length, handleScan]);

  const handleLoadMore = () => fetchPage(scanCursors, 'next');

  const handleGet = useCallback(async () => {
    if (!getKey || !storeId || !groupId || groupId === ALL_GROUPS) return;
    setGetLoading(true);
    setErrorMsg(null);
    try {
      const result = await kvGet(storeId, groupId, getKey);
      setGetResult(result);
      log({ action: 'KV Get', target: `${storeId}/${groupId}`, status: 'Success', message: `key: "${getKey}"` });
      success(result.found ? `Retrieved "${getKey}"` : `Key "${getKey}" not found`);
    } catch (err) {
      const msg = err instanceof Error ? err.message : 'Get failed';
      setErrorMsg(msg);
      log({ action: 'KV Get', target: `${storeId}/${groupId}`, status: 'Failed', message: msg });
      error(msg);
    } finally {
      setGetLoading(false);
    }
  }, [getKey, storeId, groupId, log, success, error]);

  const copy = useCallback((text: string) => {
    navigator.clipboard.writeText(text).then(
      () => success('Copied to clipboard'),
      () => error('Copy failed'),
    );
  }, [success, error]);

  const showGroupColumn = groupId === ALL_GROUPS;

  if (stores.length === 0) {
    return (
      <div className="tw-flex tw-items-center tw-justify-center tw-h-full tw-text-muted tw-text-sm">
        {backendError
          ? 'Backend unreachable — retrying'
          : loading
            ? 'Loading…'
            : 'No stores available. Create a store first.'}
      </div>
    );
  }

  return (
    <div className="tw-h-full tw-overflow-y-auto tw-bg-bg tw-text-text">
      <div className="tw-p-5 tw-space-y-4">
        <h1 className="tw-text-lg tw-font-semibold">PaxosKV</h1>
        {selectedEntity?.type === 'Group' && selectedEntity.id !== '0' && <OwnershipPanel active={active} selection={selectedEntity} nodes={[]} servers={[]} stores={stores} />}
        {/* Selector bar */}
        <div className="tw-flex tw-items-center tw-gap-3 tw-flex-wrap">
          <div className="tw-flex tw-items-center tw-gap-1.5">
            <label htmlFor="kv-store-select" className="tw-text-xs tw-text-muted">Store</label>
            <select
              id="kv-store-select"
              data-testid="kv-store-select"
              aria-label="Store"
              value={storeId}
              onChange={(e) => handleStoreChange(e.target.value)}
              className="tw-bg-panel tw-border tw-border-border tw-rounded tw-px-2 tw-py-1 tw-text-xs tw-text-text"
            >
              {stores.map((s) => (
                <option key={s.store_id} value={String(s.store_id)}>Store {s.store_id}</option>
              ))}
            </select>
          </div>
          <div className="tw-flex tw-items-center tw-gap-1.5">
            <label htmlFor="kv-group-select" className="tw-text-xs tw-text-muted">Group</label>
            <select
              id="kv-group-select"
              data-testid="kv-group-select"
              aria-label="Group"
              value={groupId}
              onChange={(e) => handleGroupChange(e.target.value)}
              className="tw-bg-panel tw-border tw-border-border tw-rounded tw-px-2 tw-py-1 tw-text-xs tw-text-text"
            >
              <option value={ALL_GROUPS}>All Groups</option>
              {groupsInStore.map((g) => (
                <option key={g.group_id} value={String(g.group_id)}>Group {g.group_id}</option>
              ))}
            </select>
          </div>
          <div className="tw-flex tw-items-center tw-gap-1.5 tw-ml-auto">
            <input
              type="text"
              value={scanPrefix}
              onChange={(e) => setScanPrefix(e.target.value)}
              placeholder="Key prefix (empty = all)"
              aria-label="Scan prefix"
              className="tw-bg-panel tw-border tw-border-border tw-rounded tw-px-2 tw-py-1 tw-text-xs tw-text-text placeholder:tw-text-muted tw-w-48"
              onKeyDown={(e) => e.key === 'Enter' && handleScan()}
            />
            <button
              onClick={handleScan}
              disabled={scanLoading || !storeId || !groupId}
              className="tw-flex tw-items-center tw-gap-1 tw-px-3 tw-py-1 tw-border tw-border-border hover:tw-bg-accent/10 tw-rounded tw-text-xs disabled:tw-opacity-50"
            >
              {scanLoading ? <Loader2 className="tw-h-3 tw-w-3 tw-animate-spin" /> : <Search className="tw-h-3 tw-w-3" />}
              Scan
            </button>
          </div>
        </div>

        <div className="tw-flex tw-items-center tw-gap-2 tw-flex-wrap">
          <label htmlFor="kv-query-key" className="tw-text-xs tw-text-muted">Query key</label>
          <input id="kv-query-key" type="text" value={getKey} onChange={(event) => setGetKey(event.target.value)} placeholder="Key" aria-label="Query key" className="tw-bg-panel tw-border tw-border-border tw-rounded tw-px-2 tw-py-1 tw-text-xs tw-w-48" onKeyDown={(event) => { if (event.key === 'Enter') void handleGet(); }} />
          <button onClick={() => void handleGet()} disabled={getLoading || !getKey || groupId === ALL_GROUPS} className="tw-flex tw-items-center tw-gap-1 tw-px-2 tw-py-1 tw-border tw-border-border hover:tw-bg-accent/10 tw-rounded tw-text-xs disabled:tw-opacity-50">
            {getLoading ? <Loader2 className="tw-h-3 tw-w-3 tw-animate-spin" /> : <Info className="tw-h-3 tw-w-3" />} Query
          </button>
          {getResult && <span className="tw-text-xs tw-flex tw-items-center tw-gap-1">{getResult.found ? <><span className="tw-font-mono" data-testid="kv-get-result"><ByteDisplay text={getResult.value_utf8} hex={getResult.value_hex} /></span><span className="tw-text-muted tw-text-[10px]">rev: {getResult.revision}</span><button onClick={() => copy(displayBytes(getResult.value_utf8, getResult.value_hex))} className="tw-text-muted hover:tw-text-text" aria-label="Copy query value"><Copy className="tw-h-3 tw-w-3" /></button></> : <span className="tw-text-muted" data-testid="kv-not-found">not found</span>}</span>}
        </div>

        {errorMsg && (
          <div className="tw-flex tw-items-start tw-gap-2 tw-p-2 tw-rounded tw-bg-failed/10 tw-border tw-border-failed/30 tw-text-failed tw-text-xs">
            <AlertTriangle className="tw-h-4 tw-w-4 tw-flex-shrink-0" />
            <span>{errorMsg}</span>
          </div>
        )}

        {isSystemGroup && (
          <div className="tw-flex tw-items-start tw-gap-2 tw-p-2 tw-rounded tw-bg-panel/50 tw-border tw-border-border tw-text-muted tw-text-xs">
            <AlertTriangle className="tw-h-4 tw-w-4 tw-flex-shrink-0" />
            <span>Group 0 is the system group (topology metadata). Use Scan and Get to inspect.</span>
          </div>
        )}

        {/* Results table — rendered whenever a scan has been executed,
            even with 0 rows, so the table DOM is always present after
            scan (tests and UX rely on `kv-scan-table` being visible). */}
        {scanDone && !scanLoading && (
          <div className="tw-space-y-1">
            <span className="tw-text-xs tw-text-muted">
              {scanRows.length} result(s){scanTruncated && ' (truncated)'}
            </span>
            <div className="tw-border tw-border-border tw-rounded tw-overflow-x-auto">
              <table className="tw-w-full tw-text-xs" data-testid="kv-scan-table">
                <thead className="tw-bg-panel tw-sticky tw-top-0">
                  <tr>
                    <th className="tw-text-left tw-p-2 tw-text-muted tw-border-b tw-border-border">Key</th>
                    <th className="tw-text-left tw-p-2 tw-text-muted tw-border-b tw-border-border">Value</th>
                    {showGroupColumn && (
                      <th className="tw-text-left tw-p-2 tw-text-muted tw-border-b tw-border-border">Group</th>
                    )}
                  </tr>
                </thead>
                <tbody className="tw-divide-y tw-divide-border">
                  {scanRows.length === 0 ? (
                    <tr>
                      <td colSpan={showGroupColumn ? 3 : 2} className="tw-p-4 tw-text-center tw-text-muted">
                        No results. Click Scan to list keys.
                      </td>
                    </tr>
                  ) : (
                    scanRows.map((row, idx) => (
                      <tr
                        key={`${row.groupId}-${row.key_utf8}-${idx}`}
                        className="hover:tw-bg-panel/30 tw-cursor-pointer"
                        onClick={() => { setFocusedRow(row); const readable = printableBytes(row.key_utf8, row.key_hex); setGetKey(readable ? row.key_utf8 : ''); }}
                      >
                        <td className="tw-p-2 tw-font-mono tw-truncate tw-max-w-[200px]" title={displayBytes(row.key_utf8, row.key_hex)}>
                          <ByteDisplay text={row.key_utf8} hex={row.key_hex} />
                        </td>
                        <td className="tw-p-2 tw-font-mono tw-truncate tw-max-w-[200px]" title={displayBytes(row.value_utf8, row.value_hex)}>
                          <ByteDisplay text={row.value_utf8} hex={row.value_hex} />
                        </td>
                        {showGroupColumn && (
                          <td className="tw-p-2 tw-text-muted">{row.groupId}</td>
                        )}

                      </tr>
                    ))
                  )}
                </tbody>
              </table>
            </div>
            <nav aria-label="Key pages" className="tw-flex tw-items-center tw-gap-3 tw-py-2">
              <button className={buttonClass} disabled={loadingMore || pageNumber === 1} onClick={handleScan}>First</button>
              <button className={buttonClass} disabled={loadingMore || !pageStarts.length} onClick={() => void fetchPage(pageStarts[pageStarts.length - 1], 'previous')}>Previous</button>
              <span className="tw-text-xs tw-text-muted">Page {pageNumber} · {scanRows.length} keys</span>
              <button className={buttonClass} disabled={loadingMore || !scanTruncated} onClick={handleLoadMore}>Next</button>
            </nav>
          </div>
        )}

        {scanLoading && (
          <div className="tw-text-center tw-text-muted tw-text-xs tw-py-8">
            Scanning...
          </div>
        )}

        {!scanDone && !scanLoading && (
          <div className="tw-text-center tw-text-muted tw-text-xs tw-py-8">
            No results. Click Scan to list keys.
          </div>
        )}
        {focusedRow && <section aria-label="Selected key" className="tw-rounded tw-border tw-border-border tw-bg-panel tw-p-4 tw-space-y-3"><h2 className="tw-font-semibold">Key / Value · Store {storeId} / Group {focusedRow.groupId}</h2><div className="tw-text-xs tw-text-muted">Key</div><pre className="tw-whitespace-pre-wrap tw-break-all tw-text-xs"><ByteDisplay text={focusedRow.key_utf8} hex={focusedRow.key_hex} /></pre><div className="tw-text-xs tw-text-muted">Value</div><pre className="tw-whitespace-pre-wrap tw-break-all tw-text-xs"><ByteDisplay text={focusedRow.value_utf8} hex={focusedRow.value_hex} /></pre></section>}

      </div>

    </div>
  );
}
