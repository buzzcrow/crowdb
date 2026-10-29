// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

import { resourceMenuKey } from '../components/resourceMenuKey';
import { useState, useRef, useEffect } from 'react';
import { DocsHelp, Workbench, buttonClass } from '../access/Workbench';
import { s3, xml, xmlText, objectPath } from '../access/native';
import { useDomain, useNavigationSnapshot } from '../contexts/DomainContext';
import { Domain } from '../types';
import { useActivity } from '../contexts/ActivityContext';
import { useS3Menu } from '../s3/ResourceMenu';
import { Fields, Records } from '../iceberg/Fields';
import { Tree, type TreeNode } from '../components/Tree';
import { Database, Folder } from 'lucide-react';
import { useObjectLocations, type LocationQuery } from '../s3/useObjectLocations';
import { StorageLocations } from '../s3/StorageLocations';
import { useClusterOrigin } from '../s3/useClusterOrigin';
import { PrefixList } from '../access/PrefixList';

interface ObjectRow { key: string; size: string; etag: string; modified: string }
interface Upload { key: string; id: string }
interface S3Query {
  multipartView: boolean;
  bucket: string; bucketPrefix: string; bucketListedPrefix: string;
  bucketCursor?: string; bucketPrevious: Array<string | undefined>; prefix: string;
  listedPrefix: string; cursor?: string; previous: Array<string | undefined>;
  locations: LocationQuery | null;
  selected: ObjectRow | null; upload: { key: string; id: string; marker?: string } | null;
}
const BUCKET_PAGE = 20;
export function S3View({ active, readonly, onChunk }: { active: boolean; readonly: boolean; onChunk: (id: string) => void }) {
  const { log } = useActivity();
  const { checkpoint } = useDomain();
  const restoreQuery = useRef<(query: S3Query) => void>(() => {});
  const [uploadsNext, setUploadsNext] = useState<{ key: string; id: string } | null>(null);
  const [partsNext, setPartsNext] = useState<{ key: string; id: string; marker: string } | null>(null);
  const { origin, error: originError, loading: originLoading, retry: retryOrigin } = useClusterOrigin(active);
  const [buckets, setBuckets] = useState<string[]>([]);
  const [bucketPrefix, setBucketPrefix] = useState('');
  const [bucketListedPrefix, setBucketListedPrefix] = useState('');
  const [bucketCursor, setBucketCursor] = useState<string>();
  const [bucketPrevious, setBucketPrevious] = useState<Array<string | undefined>>([]);
  const [bucketNext, setBucketNext] = useState<string | null>(null);
  const [bucket, setBucket] = useState('');
  const [prefix, setPrefix] = useState('');
  const [listedPrefix, setListedPrefix] = useState('');
  const [rows, setRows] = useState<ObjectRow[]>([]);
  const [cursor, setCursor] = useState<string | undefined>();
  const [previous, setPrevious] = useState<Array<string | undefined>>([]);
  const [preview, setPreview] = useState('');
  const [next, setNext] = useState<string | null>(null);
  const [selected, setSelected] = useState<ObjectRow | null>(null);
  const locationInspection = useObjectLocations(active, bucket, selected && selected.size !== 'pending' ? selected.key : undefined);
  const [detail, setDetail] = useState<unknown>(null);
  const [multipartView, setMultipartView] = useState(false);
  const [uploads, setUploads] = useState<Upload[]>([]);
  const [selectedUpload, setSelectedUpload] = useState<{ key: string; id: string; marker?: string } | null>(null);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState('');
  const bucketWindows = useRef<Record<string, { prefix: string; listedPrefix: string; cursor?: string; previous: Array<string | undefined> }>>({});
  const operationVersion = useRef(0);
  useEffect(() => {
    ++operationVersion.current;
    setBuckets([]); setBucketPrefix(''); setBucketListedPrefix(''); setBucketCursor(undefined); setBucketPrevious([]); setBucketNext(null); bucketWindows.current = {}; setBucket(''); setRows([]); setNext(null); setSelected(null); setDetail(null); setUploads([]); setUploadsNext(null); setPartsNext(null); setError('');
  }, [origin]);
  const connected = !!origin;
  useEffect(() => { if (!active) { ++operationVersion.current; setBusy(false); } }, [active]);
  const request = async (method: string, path: string, query: Record<string, string> = {}, body?: Blob | string) => {
    if (!origin) throw new Error('This cluster has no available S3 endpoint');
    const version = operationVersion.current;
    const response = await s3(method, path, query, body);
    if (version !== operationVersion.current) throw new DOMException('Superseded observation', 'AbortError');
    return response;
  };
  const run = async (operation: () => Promise<void>, label: string) => {
    const version = ++operationVersion.current;
    setBusy(true); setError('');
    try { await operation(); if (version !== operationVersion.current) return; log({ action: label, target: `S3 / ${bucket || 'buckets'}`, status: 'Success' }); }
    catch (error) { if (version !== operationVersion.current) return; setError(`${String(error)}. Refresh the resource before retrying a mutation.`); log({ action: label, target: `S3 / ${bucket || 'buckets'}`, status: 'Failed', message: 'Native request failed. Refresh resource state before retrying.' }); }
    finally { if (version === operationVersion.current) { setBusy(false); } }
  };
  const readXml = async (response: Promise<Response>) => {
    const version = operationVersion.current;
    const document = await xml(await response);
    if (version !== operationVersion.current) throw new DOMException('Superseded observation', 'AbortError');
    return document;
  };
  const loadObjects = async (scope: string, token?: string, listingPrefix = token ? listedPrefix : prefix) => {
    const document = await readXml(request('GET', objectPath(scope), { 'list-type': '2', 'max-keys': '20', prefix: listingPrefix, ...(token ? { 'continuation-token': token } : {}) }));
    const objects = Array.from(document.querySelectorAll('Contents')).map(element => ({ key: xmlText(element, 'Key'), size: xmlText(element, 'Size'), etag: xmlText(element, 'ETag'), modified: xmlText(element, 'LastModified') }));
    if (objects.length > 20) throw new Error('S3 returned more than the requested 20 objects');
    setRows(objects);
    if (!token) setListedPrefix(listingPrefix);
    setCursor(token);
    if (!token) setPrevious([]);
    setNext(xmlText(document, 'IsTruncated') === 'true' ? xmlText(document, 'NextContinuationToken') : null);
  };
  const rememberBucket = () => {
    if (bucket) bucketWindows.current = Object.fromEntries([...Object.entries(bucketWindows.current).filter(([name]) => name !== bucket).slice(-31),
      [bucket, { prefix, listedPrefix, cursor, previous: [...previous] }]]);
  };
  const chooseBucket = (value: string) => {
    checkpoint(); rememberBucket();
    const saved = bucketWindows.current[value] ?? { prefix: '', listedPrefix: '', cursor: undefined, previous: [] };
    setPrefix(saved.prefix); setListedPrefix(saved.listedPrefix); setCursor(saved.cursor); setPrevious(saved.previous);
    setMultipartView(false); setBucket(value); setPreview(''); setSelected(null); setDetail(null); setRows([]); setUploads([]); setUploadsNext(null); setPartsNext(null); setNext(null);
    void run(async () => { await loadObjects(value, saved.cursor, saved.listedPrefix); setPrevious(saved.previous); }, `List ${value}`);
  };
  const listBuckets = async (token?: string, listingPrefix = bucketPrefix) => {
    const document = await readXml(request('GET', '/', { prefix: listingPrefix, 'max-buckets': String(BUCKET_PAGE), ...(token ? { 'continuation-token': token } : {}) }));
    setBuckets(Array.from(document.querySelectorAll('Bucket')).map(element => xmlText(element, 'Name')));
    setBucketCursor(token); setBucketListedPrefix(listingPrefix);
    if (!token) setBucketPrevious([]);
    setBucketNext(xmlText(document, 'ContinuationToken') || null);
  };
  const listUploads = async (after?: { key: string; id: string }, scope = bucket) => {
    const document = await readXml(request('GET', objectPath(scope), { uploads: '', 'max-uploads': '100', ...(after ? { 'key-marker': after.key, 'upload-id-marker': after.id } : {}) }));
    const rows = Array.from(document.querySelectorAll('Upload')).map(element => ({ key: xmlText(element, 'Key'), id: xmlText(element, 'UploadId') }));
    if (rows.length > 100) throw new Error('S3 returned more than the requested 100 uploads');
    setUploads(rows);
    setUploadsNext(xmlText(document, 'IsTruncated') === 'true' ? { key: xmlText(document, 'NextKeyMarker'), id: xmlText(document, 'NextUploadIdMarker') } : null);
  };
  const inspectParts = async (upload: Upload, marker?: string, scope = bucket, record = true) => {
    if (record) checkpoint();
    setSelectedUpload({ ...upload, marker });
    setSelected({ key: upload.key, size: 'pending', etag: '', modified: '' });
    setDetail(null); setPartsNext(null);
    const document = await readXml(request('GET', objectPath(scope, upload.key), { uploadId: upload.id, 'max-parts': '100', ...(marker ? { 'part-number-marker': marker } : {}) }));
    setDetail({ parts: Array.from(document.querySelectorAll('Part')).map(part => ({ number: xmlText(part, 'PartNumber'), etag: xmlText(part, 'ETag'), size: xmlText(part, 'Size') })), is_truncated: xmlText(document, 'IsTruncated') === 'true' });
    setPartsNext(xmlText(document, 'IsTruncated') === 'true' ? { ...upload, marker: xmlText(document, 'NextPartNumberMarker') } : null);
  };
  const inspect = (row: ObjectRow) => { checkpoint(); setSelectedUpload(null); setSelected(row); setDetail(null); setPreview(''); setPartsNext(null); void run(async () => {
    const response = await request('HEAD', objectPath(bucket, row.key));
    setDetail(Object.fromEntries(response.headers.entries()));
  }, `Inspected ${row.key}`); };
  useEffect(() => { if (active && origin) void run(async () => { await listBuckets(bucketCursor, bucketListedPrefix); }, 'List buckets'); }, [active, origin]);
  useNavigationSnapshot(Domain.S3, 'object-query', () => {
    const state = { bucket, bucketPrefix, bucketListedPrefix, bucketCursor, bucketPrevious: [...bucketPrevious], prefix, listedPrefix, cursor, multipartView,
      previous: [...previous], selected: selected ? { ...selected } : null,
      locations: locationInspection.snapshot(),
      upload: selected?.size === 'pending' && selectedUpload ? { ...selectedUpload } : null };
    return () => restoreQuery.current(state);
  });
  restoreQuery.current = state => {
    setMultipartView(state.multipartView);
    setBucket(state.bucket); setBucketPrefix(state.bucketPrefix); setBucketListedPrefix(state.bucketListedPrefix); setBucketCursor(state.bucketCursor); setBucketPrevious(state.bucketPrevious);
    setPrefix(state.prefix); setListedPrefix(state.listedPrefix); setPrevious(state.previous);
    setCursor(state.cursor); setSelected(state.selected); setDetail(null); setPreview('');
    setPartsNext(null); setUploads([]); setUploadsNext(null); locationInspection.restore(state.locations);
    if (!state.bucket) { void run(async () => { await listBuckets(state.bucketCursor, state.bucketListedPrefix); setBucketPrevious(state.bucketPrevious); }, 'List buckets'); return; }
    void run(async () => {
      await loadObjects(state.bucket, state.cursor, state.listedPrefix);
      setPrevious(state.previous);
      if (state.multipartView) await listUploads(undefined, state.bucket);
      if (state.upload) {
        await inspectParts(state.upload, state.upload.marker, state.bucket, false);
      } else if (state.selected) {
        const response = await request('HEAD', objectPath(state.bucket, state.selected.key));
        setDetail(Object.fromEntries(response.headers.entries()));
      }
    }, `Restored ${state.bucket}`);
  };
  const root = () => { if (busy) return; ++operationVersion.current; checkpoint(); rememberBucket(); setBucket(''); setSelected(null); setDetail(null); setPreview(''); };
  const treeNames = bucket && !buckets.includes(bucket) ? [...buckets, bucket] : buckets;
  const tree: TreeNode[] = [{ id: 's3-root', type: 'S3', label: 'S3', selected: !bucket, icon: <Database className="tw-h-4 tw-w-4 tw-text-muted" />, children: treeNames.map(name => ({ id: name, type: 'S3', label: name, selected: name === bucket, icon: <Folder className="tw-h-4 tw-w-4 tw-text-muted" /> })) }];
  const scope = selected ? 'Object' : bucket ? 'Bucket' : 'S3';
  const paging = <nav aria-label="Bucket pages" className="tw-flex tw-gap-2"><button className={buttonClass} disabled={busy || !bucketPrevious.length} onClick={() => { checkpoint(); const trail = bucketPrevious.slice(0, -1); void run(async () => { await listBuckets(bucketPrevious.at(-1), bucketListedPrefix); setBucketPrevious(trail); }, 'Previous buckets'); }}>Previous buckets</button><button className={buttonClass} disabled={busy || !bucketNext} onClick={() => { checkpoint(); const trail = [...bucketPrevious, bucketCursor]; void run(async () => { await listBuckets(bucketNext!, bucketListedPrefix); setBucketPrevious(trail); }, 'Next buckets'); }}>Next buckets</button></nav>;
  const resourceMenu = useS3Menu(readonly, connected, async (target, action) => {
    if (action === 'abort') { await listUploads(undefined, target.bucket); return; }
    if (action === 'create' || action === 'delete-bucket') { if (target.bucket === bucket) root(); await listBuckets(undefined, bucketListedPrefix); }
    else if (target.bucket === bucket) {
      if (selected?.key === target.key) { setPreview(''); setDetail(null); if (action === 'delete-object') setSelected(null); }
      await loadObjects(bucket, undefined, listedPrefix);
      if (action === 'replace' && selected && selected.key === target.key) {
        const response = await request('HEAD', objectPath(bucket, target.key));
        setDetail(Object.fromEntries(response.headers.entries()));
        setSelected({ ...selected, size: response.headers.get('content-length') ?? '', etag: response.headers.get('etag') ?? '', modified: response.headers.get('last-modified') ?? '' });
      }
    }
  }, (target, text) => { if (target.bucket === bucket) { const row = rows.find(row => row.key === target.key); if (row) inspect(row); setPreview(text); } }, scope => { setBucket(scope); setSelected(null); setMultipartView(true); void run(() => listUploads(undefined, scope), 'Multipart state loaded'); });
  return <Workbench resizableSidebar showActivity={false} help={<DocsHelp href="https://crowdb.dev/docs/manual/s3/" title="s3" auth="The Console signs requests with server-side AWS credentials. External clients use AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY; there is no separate username or password." />} sidebar={<>
    {originLoading && <p role="status">Loading cluster S3 endpoint…</p>}
    <nav aria-label="S3 buckets" className="-tw-mx-4"><Tree onNodeContextMenu={(node, event) => resourceMenu.open(event, node.id === 's3-root' ? {} : { bucket: node.id })} nodes={tree} defaultExpandedIds={['s3-root']} onNodeClick={node => { if (!busy) { if (node.id === 's3-root') root(); else chooseBucket(node.id); } }} /></nav>
  </>} detail={<div aria-label="S3 properties"><h2 className="tw-font-semibold tw-mb-4">Properties</h2><Fields stacked values={selected ? { Type: 'Object', Bucket: bucket, Key: selected.key, Size: selected.size, ETag: selected.etag, Modified: selected.modified } : { Type: scope, Name: bucket || 'S3' }} />{selected && locationInspection.selected && <section aria-label="Storage extent properties"><h3 className="tw-font-semibold tw-mt-4 tw-mb-3">Storage extent</h3><Fields stacked values={{ 'Extent index': locationInspection.selected.index, 'Chunk ID': locationInspection.selected.chunk_id, 'Logical offset (bytes)': locationInspection.selected.logical_offset, 'Logical length (bytes)': locationInspection.selected.logical_length, 'Chunk offset (bytes)': locationInspection.selected.offset, 'Physical length (bytes)': locationInspection.selected.length, Generation: locationInspection.page?.generation }} /></section>}</div>}>
    {bucket && <h1 tabIndex={0} onKeyDown={resourceMenuKey} className="tw-text-lg tw-font-semibold tw-break-all" onContextMenu={event => resourceMenu.open(event, { bucket, key: selected?.size === 'pending' ? undefined : selected?.key })}>{selected?.key || bucket}</h1>}
    {resourceMenu.overlay}
    {multipartView && !selected && <section aria-label="Multipart uploads" className="tw-space-y-3">
      <button className={buttonClass} disabled={busy} onClick={() => void run(() => listUploads(), 'Multipart state refreshed')}>Refresh multipart uploads</button>
      <button className={buttonClass} onClick={() => setMultipartView(false)}>Objects</button>
      {!uploads.length && !busy && <p>No multipart uploads.</p>}
      {uploads.map(upload => <div key={upload.id} onKeyDown={resourceMenuKey} onContextMenu={event => resourceMenu.open(event, { bucket, key: upload.key, uploadId: upload.id })} className="tw-flex tw-gap-3 tw-text-sm"><button aria-label={`Inspect parts ${upload.id}`} onClick={() => void run(() => inspectParts(upload), 'Parts inspected')}>{upload.key} · {upload.id}</button></div>)}
      <button className={buttonClass} disabled={busy || !uploadsNext} onClick={() => void run(() => listUploads(uploadsNext!), 'Next upload page loaded')}>Next uploads</button>
    </section>}
    {error && <p role="alert" className="tw-text-sm tw-text-failed tw-break-all">{error}</p>}
    {originError && <div><p role="alert" className="tw-text-sm tw-text-failed">{originError}</p><button className={buttonClass} disabled={originLoading || busy} onClick={retryOrigin}>Retry cluster S3</button></div>}
    {!bucket && <>
      <PrefixList resource="Bucket" value={bucketPrefix} change={setBucketPrefix} busy={!connected || busy} submit={() => { checkpoint(); void run(() => listBuckets(), 'List buckets'); }} />
      <Records label="S3 bucket list" headings={['Bucket']} rows={buckets.map(name => [<button className="tw-text-accent" onKeyDown={resourceMenuKey} onContextMenu={event => resourceMenu.open(event, { bucket: name })} disabled={busy} onClick={() => chooseBucket(name)}>{name}</button>])} />
      {paging}
    </>}
    {bucket && !selected && !multipartView && <>
      <PrefixList resource="Object" value={prefix} change={setPrefix} busy={busy} submit={() => { checkpoint(); void run(() => loadObjects(bucket), 'List objects'); }} />
      <Records label="S3 objects" headings={['Key', 'Size (bytes)', 'Modified']} rows={rows.map(row => [<button className="tw-text-accent tw-break-all tw-text-left" onKeyDown={resourceMenuKey} onContextMenu={event => resourceMenu.open(event, { bucket, key: row.key })} disabled={busy} onClick={() => inspect(row)}>{row.key}</button>, row.size, row.modified])} />
      {!rows.length && !busy && <p className="tw-text-xs tw-text-muted">No objects in this prefix.</p>}
      <nav aria-label="Object pages" className="tw-flex tw-gap-2"><button className={buttonClass} disabled={busy || !previous.length} onClick={() => { checkpoint(); void run(async () => { const trail = previous.slice(0, -1); await loadObjects(bucket, previous.at(-1), listedPrefix); setPrevious(trail); }, 'Previous page loaded'); }}>Previous</button><button className={buttonClass} disabled={busy || !next} onClick={() => { checkpoint(); void run(async () => { const trail = [...previous, cursor].slice(-32); await loadObjects(bucket, next!, listedPrefix); setPrevious(trail); }, 'Next page loaded'); }}>Next</button></nav>
    </>}
    {selected && <section onContextMenu={event => { if (selected.size !== 'pending') resourceMenu.open(event, { bucket, key: selected.key }); }} aria-label="Object metadata" className="tw-space-y-3"><h2 className="tw-font-semibold">{selected.size === 'pending' ? 'Multipart parts' : 'HEAD metadata'}</h2>{busy && !detail ? <p>Loading metadata…</p> : selected.size === 'pending' ? <Records label="Multipart parts" headings={['Part', 'ETag', 'Size']} rows={((detail as { parts?: Array<{ number: string; etag: string; size: string }> } | null)?.parts ?? []).map(part => [part.number, part.etag, part.size])} /> : <Fields values={(detail ?? {}) as Record<string, unknown>} />}
      {selected.size === 'pending' && selectedUpload && <button className={buttonClass} disabled={busy} onClick={() => void run(() => inspectParts(selectedUpload, undefined, bucket, false), 'Parts refreshed')}>Refresh parts</button>}
      {partsNext && <button className={buttonClass} disabled={busy} onClick={() => void run(() => inspectParts(partsNext, partsNext.marker), 'Next parts page loaded')}>Next parts page</button>}
      {selected.size !== 'pending' && <StorageLocations inspection={locationInspection} onChunk={onChunk} />}
      {!!preview && <><h2>Preview · first 4 KiB</h2><pre aria-label="Object preview" className="tw-whitespace-pre-wrap tw-break-all tw-text-xs">{preview}</pre></>}
    </section>}
  </Workbench>;
}
