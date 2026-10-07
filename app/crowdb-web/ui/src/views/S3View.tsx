// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

import { randomUUID } from '../utils/randomUUID';
import { useState, useRef, useEffect } from 'react';
import { DocsHelp, Workbench, inputClass, buttonClass } from '../access/Workbench';
import { s3, xml, xmlText, objectPath, previewBytes } from '../access/native';
import { uploadObject, downloadObject } from '../access/s3Transfer';
import { useDomain, useNavigationSnapshot } from '../contexts/DomainContext';
import { Domain } from '../types';
import { useActivity } from '../contexts/ActivityContext';
import { ResourceActions } from '../access/ResourceActions';
import { Fields, Records } from '../iceberg/Fields';
import { Tree, type TreeNode } from '../components/Tree';
import { Database, Folder } from 'lucide-react';
import { useObjectLocations, type LocationQuery } from '../s3/useObjectLocations';
import { StorageLocations } from '../s3/StorageLocations';
import { useClusterOrigin } from '../s3/useClusterOrigin';

interface ObjectRow { key: string; size: string; etag: string; modified: string }
interface Upload { key: string; id: string }
interface S3Query {
  bucket: string; bucketStart: number; bucketFilter: string; prefix: string;
  listedPrefix: string; cursor?: string; previous: Array<string | undefined>;
  locations: LocationQuery | null;
  selected: ObjectRow | null; upload: { key: string; id: string; marker?: string } | null;
}
const MAX_ROWS = 1000;
const BUCKET_PAGE = 20;
export function S3View({ active, readonly, onChunk }: { active: boolean; readonly: boolean; onChunk: (id: string) => void }) {
  const { log } = useActivity();
  const { checkpoint } = useDomain();
  const restoreQuery = useRef<(query: S3Query) => void>(() => {});
  const [demoBucket, setDemoBucket] = useState<string | null>(null);
  const [uploadsNext, setUploadsNext] = useState<{ key: string; id: string } | null>(null);
  const [partsNext, setPartsNext] = useState<{ key: string; id: string; marker: string } | null>(null);
  const { origin, error: originError, loading: originLoading, retry: retryOrigin } = useClusterOrigin(active);
  const [buckets, setBuckets] = useState<string[]>([]);
  const [bucketStart, setBucketStart] = useState(0);
  const [bucketFilter, setBucketFilter] = useState('');
  const [bucket, setBucket] = useState('');
  const [newBucket, setNewBucket] = useState('');
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
  const [uploads, setUploads] = useState<Upload[]>([]);
  const [selectedUpload, setSelectedUpload] = useState<{ key: string; id: string; marker?: string } | null>(null);
  const [key, setKey] = useState('');
  const [file, setFile] = useState<File | null>(null);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState('');
  const [outcome, setOutcome] = useState('');
  const [progress, setProgress] = useState<{ bytes: number; id: string | null }>({ bytes: 0, id: null });
  const operationVersion = useRef(0);
  const controller = useRef<AbortController | null>(null);
  useEffect(() => {
    ++operationVersion.current;
    setBuckets([]); setBucketStart(0); setBucket(''); setRows([]); setNext(null); setSelected(null); setDetail(null); setUploads([]); setUploadsNext(null); setPartsNext(null); setDemoBucket(null); setError(''); setOutcome('');
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
    setBusy(true); setError(''); setOutcome('');
    try { await operation(); if (version !== operationVersion.current) return; setOutcome(label); log({ action: label, target: `S3 / ${bucket || 'buckets'}`, status: 'Success' }); }
    catch (error) { if (version !== operationVersion.current) return; setError(`${String(error)}. Refresh the resource before retrying a mutation.`); log({ action: label, target: `S3 / ${bucket || 'buckets'}`, status: 'Failed', message: 'Native request failed. Refresh resource state before retrying.' }); }
    finally { if (version === operationVersion.current) { setBusy(false); controller.current = null; } }
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
  const chooseBucket = (value: string) => { checkpoint(); setBucket(value); setPrevious([]); setCursor(undefined); setPreview(''); setSelected(null); setDetail(null); setRows([]); setUploads([]); setUploadsNext(null); setPartsNext(null); setNext(null); void run(() => loadObjects(value), `Loaded ${value}`); };
  const listBuckets = async () => {
    const document = await readXml(request('GET', '/'));
    setBuckets(Array.from(document.querySelectorAll('Bucket')).map(element => xmlText(element, 'Name')));
    setBucketStart(0);
  };
  const listUploads = async (after?: { key: string; id: string }) => {
    const document = await readXml(request('GET', objectPath(bucket), { uploads: '', 'max-uploads': '100', ...(after ? { 'key-marker': after.key, 'upload-id-marker': after.id } : {}) }));
    const rows = Array.from(document.querySelectorAll('Upload')).map(element => ({ key: xmlText(element, 'Key'), id: xmlText(element, 'UploadId') }));
    if (rows.length > 100) throw new Error('S3 returned more than the requested 100 uploads');
    setUploads(previous => (after ? Array.from(new Map([...previous, ...rows].map(row => [row.id, row])).values()) : rows).slice(0, MAX_ROWS));
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
  useEffect(() => { if (origin) void run(listBuckets, 'Buckets refreshed'); }, [origin]);
  useNavigationSnapshot(Domain.S3, 'object-query', () => {
    const state = { bucket, bucketStart, bucketFilter, prefix, listedPrefix, cursor,
      previous: [...previous], selected: selected ? { ...selected } : null,
      locations: locationInspection.snapshot(),
      upload: selected?.size === 'pending' && selectedUpload ? { ...selectedUpload } : null };
    return () => restoreQuery.current(state);
  });
  restoreQuery.current = state => {
    setBucket(state.bucket); setBucketStart(state.bucketStart); setBucketFilter(state.bucketFilter);
    setPrefix(state.prefix); setListedPrefix(state.listedPrefix); setPrevious(state.previous);
    setCursor(state.cursor); setSelected(state.selected); setDetail(null); setPreview('');
    setPartsNext(null); setUploads([]); setUploadsNext(null); locationInspection.restore(state.locations);
    if (!state.bucket) return;
    void run(async () => {
      await loadObjects(state.bucket, state.cursor, state.listedPrefix);
      setPrevious(state.previous);
      if (state.upload) {
        await inspectParts(state.upload, state.upload.marker, state.bucket, false);
      } else if (state.selected) {
        const response = await request('HEAD', objectPath(state.bucket, state.selected.key));
        setDetail(Object.fromEntries(response.headers.entries()));
      }
    }, `Restored ${state.bucket}`);
  };
  const visibleBuckets = buckets.filter(name => name.includes(bucketFilter));
  const root = () => { if (busy) return; ++operationVersion.current; checkpoint(); setBucket(''); setSelected(null); setDetail(null); setPreview(''); setOutcome(''); };
  const tree: TreeNode[] = [{ id: 's3-root', type: 'S3', label: 'S3', selected: !bucket, icon: <Database className="tw-h-4 tw-w-4 tw-text-muted" />, children: visibleBuckets.slice(bucketStart, bucketStart + BUCKET_PAGE).map(name => ({ id: name, type: 'S3', label: name, selected: name === bucket, icon: <Folder className="tw-h-4 tw-w-4 tw-text-muted" /> })) }];
  const scope = selected ? 'Object' : bucket ? 'Bucket' : 'S3';
  const paging = <nav aria-label="Bucket pages" className="tw-flex tw-gap-2"><button className={buttonClass} disabled={!bucketStart} onClick={() => { checkpoint(); setBucketStart(value => Math.max(0, value - BUCKET_PAGE)); }}>Previous buckets</button><button className={buttonClass} disabled={bucketStart + BUCKET_PAGE >= visibleBuckets.length} onClick={() => { checkpoint(); setBucketStart(value => value + BUCKET_PAGE); }}>Next buckets</button></nav>;
  return <Workbench resizableSidebar showActivity={false} help={<DocsHelp href="https://crowdb.dev/docs/manual/s3/" title="S3 Objects" description="Browse buckets, inspect objects, upload files, and review multipart state." auth="The Console signs requests with server-side AWS credentials. External clients use AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY; there is no separate username or password." />} sidebar={<>
    {originLoading && <p role="status">Loading cluster S3 endpoint…</p>}
    <nav aria-label="S3 buckets" className="-tw-mx-4"><Tree nodes={tree} defaultExpandedIds={['s3-root']} onNodeClick={node => { if (!busy) { if (node.id === 's3-root') root(); else chooseBucket(node.id); } }} /></nav>
  </>} detail={<div aria-label="S3 properties"><h2 className="tw-font-semibold tw-mb-4">Properties</h2><Fields stacked values={selected ? { Type: 'Object', Bucket: bucket, Key: selected.key, Size: selected.size, ETag: selected.etag, Modified: selected.modified } : { Type: scope, Name: bucket || 'S3' }} />{selected && locationInspection.selected && <section aria-label="Storage extent properties"><h3 className="tw-font-semibold tw-mt-4 tw-mb-3">Storage extent</h3><Fields stacked values={{ 'Extent index': locationInspection.selected.index, 'Chunk ID': locationInspection.selected.chunk_id, 'Logical offset (bytes)': locationInspection.selected.logical_offset, 'Logical length (bytes)': locationInspection.selected.logical_length, 'Chunk offset (bytes)': locationInspection.selected.offset, 'Physical length (bytes)': locationInspection.selected.length, Generation: locationInspection.page?.generation }} /></section>}</div>}>
    <nav aria-label="S3 breadcrumbs" className="tw-flex tw-gap-2 tw-text-xs tw-text-muted"><button disabled={busy} onClick={root}>S3</button>{bucket && <><span>/</span><button disabled={busy} onClick={() => { checkpoint(); setSelected(null); setDetail(null); setPreview(''); }}>{bucket}</button></>}{selected && <><span>/</span><span className="tw-break-all">{selected.key}</span></>}</nav>
    {bucket && <><p className="tw-text-xs tw-text-muted">{scope}</p><h1 className="tw-text-lg tw-font-semibold tw-break-all">{selected?.key || bucket}</h1></>}
    <ResourceActions key={`${bucket}/${selected?.key ?? ''}`} label={`${scope} actions`}>
      <p className="tw-text-xs tw-text-muted tw-break-all">Target: {bucket || 'S3'}{selected ? ` / ${selected.key}` : ''}</p>
      {!bucket && <>
    {!readonly && <><button className={buttonClass} disabled={!connected || busy || !!demoBucket} onClick={() => void run(async () => {
      const demo = `console-demo-${randomUUID().replace(/-/g, '').slice(0, 24)}`;
      await request('PUT', objectPath(demo)); setDemoBucket(demo);
      await request('PUT', objectPath(demo, 'example.txt'), {}, 'CROWDB console demo\n'); await listBuckets();
    }, 'Demo bucket and object created')}>Create object demo</button>
    {demoBucket && <><p className="tw-text-xs tw-break-all">Demo scope: {demoBucket} / example.txt</p><button className={`${buttonClass} tw-text-failed tw-border-failed/30`} disabled={busy} onClick={() => { if (confirm(`Remove only ${demoBucket}/example.txt and its empty demo bucket?`)) void run(async () => {
      await request('DELETE', objectPath(demoBucket, 'example.txt')); await request('DELETE', objectPath(demoBucket)); if (bucket === demoBucket) { setBucket(''); setRows([]); setSelected(null); } setDemoBucket(null); await listBuckets();
    }, 'Demo resources removed'); }}>Clean object demo</button></>}</>}
    {!readonly && <form className="tw-space-y-2" onSubmit={event => { event.preventDefault(); void run(async () => { await request('PUT', objectPath(newBucket)); await listBuckets(); }, `Created ${newBucket}`); }}>
      <label className="tw-block tw-text-xs">New bucket<input className={`${inputClass} tw-w-full`} value={newBucket} onChange={event => setNewBucket(event.target.value)} required /></label>
      <button className={buttonClass} disabled={!connected || busy}>Create bucket</button>
    </form>}
      </>}
      {bucket && !selected && <>
      {bucket && !readonly && <button className={`${buttonClass} tw-text-failed tw-border-failed/30`} disabled={busy} onClick={() => { if (confirm(`Delete empty bucket ${bucket}?`)) void run(async () => { await request('DELETE', objectPath(bucket)); setBucket(''); setRows([]); setSelected(null); await listBuckets(); }, 'Bucket deleted'); }}>Delete bucket</button>}      {!readonly && <form className="tw-space-y-2 tw-rounded tw-border tw-border-border tw-p-4" onSubmit={event => { event.preventDefault(); if (!file) return; controller.current = new AbortController(); const signal = controller.current.signal; void run(async () => { await uploadObject( bucket, key, file, signal, (bytes, id) => setProgress({ bytes, id })); await loadObjects(bucket); }, 'Upload completed'); }}>
        <h2 className="tw-font-semibold">Upload / replace object</h2><label className="tw-text-xs">Object key <input className={inputClass} required value={key} disabled={busy} onChange={event => setKey(event.target.value)} /></label>
        <input aria-label="Object file" type="file" disabled={busy} onChange={event => { const file = event.target.files?.[0] ?? null; setFile(file); if (file && !key) setKey(file.name); }} />
        <button className={buttonClass} disabled={busy || !file}>Upload</button>
        {busy && controller.current && <button type="button" className={buttonClass} onClick={() => controller.current?.abort()}>Stop upload</button>}
        <p className="tw-text-xs">{progress.bytes.toLocaleString()} bytes sent {progress.id && `· UploadId ${progress.id}`}</p>
        <p className="tw-text-xs tw-text-muted">Large files use 8 MiB parts. Stopped or failed uploads remain inspectable below.</p>
      </form>}
      <section className="tw-space-y-2"><button className={buttonClass} disabled={busy} onClick={() => void run(listUploads, 'Multipart state refreshed')}>List multipart uploads</button>
        {uploadsNext && uploads.length < MAX_ROWS && <button className={buttonClass} disabled={busy} onClick={() => void run(() => listUploads(uploadsNext), 'Next upload page loaded')}>More multipart uploads</button>}
        {uploadsNext && uploads.length >= MAX_ROWS && <p role="status" className="tw-text-xs">Showing 1,000 multipart uploads. Use the native S3 client to inspect later uploads.</p>}
        {uploads.map(upload => <div key={upload.id} className="tw-border tw-border-border tw-rounded tw-p-3 tw-text-xs tw-space-x-2"><span>{upload.key} · {upload.id}</span>
          <button className={buttonClass} disabled={busy} onClick={() => void run(() => inspectParts(upload), 'Parts inspected')}>Inspect parts</button>
          {!readonly && <button className={`${buttonClass} tw-text-failed tw-border-failed/30`} disabled={busy} onClick={() => { if (confirm(`Abort multipart upload ${upload.id}?`)) void run(async () => { await request('DELETE', objectPath(bucket, upload.key), { uploadId: upload.id }); await listUploads(); }, 'Upload aborted'); }}>Abort</button>}</div>)}
      </section>
      </>}
      {selected && <>
    <button className={buttonClass} disabled={busy} onClick={() => void run(async () => {
      const response = await s3( 'GET', objectPath(bucket, selected.key), {}, undefined, { range: 'bytes=0-4095' });
      const bytes = await previewBytes(response); setPreview(new TextDecoder('utf-8', { fatal: false }).decode(bytes));
    }, 'Loaded first 4 KiB')}>Preview first 4 KiB</button>
    <button className={buttonClass} disabled={busy} onClick={() => void run(() => downloadObject( bucket, selected.key), 'Downloaded object')}>Download</button>
    {!readonly && <button className={`${buttonClass} tw-text-failed tw-border-failed/30`} disabled={busy} onClick={() => { if (confirm(`Delete ${bucket}/${selected.key}?`)) void run(async () => { await request('DELETE', objectPath(bucket, selected.key)); setSelected(null); await loadObjects(bucket); }, 'Object deleted'); }}>Delete object</button>}
      </>}
    </ResourceActions>
    {error && <p role="alert" className="tw-text-sm tw-text-failed tw-break-all">{error}</p>}
    {originError && <div><p role="alert" className="tw-text-sm tw-text-failed">{originError}</p><button className={buttonClass} disabled={originLoading || busy} onClick={retryOrigin}>Retry cluster S3</button></div>}
    {outcome && <p role="status" className="tw-text-xs tw-text-muted">{outcome}</p>}
    {!bucket && <>
      <div className="tw-flex tw-gap-2"><button className={buttonClass} disabled={!connected || busy} onClick={() => void run(listBuckets, 'Buckets refreshed')}>List buckets</button><label className="tw-text-xs">Filter loaded buckets<input className={inputClass} value={bucketFilter} onChange={event => { setBucketFilter(event.target.value); setBucketStart(0); }} /></label></div>
      <Records label="S3 bucket list" headings={['Bucket']} rows={visibleBuckets.slice(bucketStart, bucketStart + BUCKET_PAGE).map(name => [<button className="tw-text-accent" disabled={busy} onClick={() => chooseBucket(name)}>{name}</button>])} />
      {paging}
    </>}
    {bucket && !selected && <>
      <form className="tw-flex tw-gap-2" onSubmit={event => { event.preventDefault(); void run(() => loadObjects(bucket), 'Objects refreshed'); }}><label className="tw-text-xs">Object prefix <input className={inputClass} value={prefix} onChange={event => setPrefix(event.target.value)} /></label><button className={buttonClass} disabled={busy}>List objects</button></form>
      <Records label="S3 objects" headings={['Key', 'Size (bytes)', 'Modified']} rows={rows.map(row => [<button className="tw-text-accent tw-break-all tw-text-left" disabled={busy} onClick={() => inspect(row)}>{row.key}</button>, row.size, row.modified])} />
      {!rows.length && !busy && <p className="tw-text-xs tw-text-muted">No objects in this prefix.</p>}
      <nav aria-label="Object pages" className="tw-flex tw-gap-2"><button className={buttonClass} disabled={busy || !previous.length} onClick={() => { checkpoint(); void run(async () => { const trail = previous.slice(0, -1); await loadObjects(bucket, previous.at(-1), listedPrefix); setPrevious(trail); }, 'Previous page loaded'); }}>Previous</button><button className={buttonClass} disabled={busy || !next} onClick={() => { checkpoint(); void run(async () => { const trail = [...previous, cursor].slice(-32); await loadObjects(bucket, next!); setPrevious(trail); }, 'Next page loaded'); }}>Next</button></nav>
    </>}
    {selected && <section aria-label="Object metadata" className="tw-space-y-3"><h2 className="tw-font-semibold">{selected.size === 'pending' ? 'Multipart parts' : 'HEAD metadata'}</h2>{busy && !detail ? <p>Loading metadata…</p> : selected.size === 'pending' ? <Records label="Multipart parts" headings={['Part', 'ETag', 'Size']} rows={((detail as { parts?: Array<{ number: string; etag: string; size: string }> } | null)?.parts ?? []).map(part => [part.number, part.etag, part.size])} /> : <Fields values={(detail ?? {}) as Record<string, unknown>} />}
      {selected.size === 'pending' && selectedUpload && <button className={buttonClass} disabled={busy} onClick={() => void run(() => inspectParts(selectedUpload, undefined, bucket, false), 'Parts refreshed')}>Refresh parts</button>}
      {partsNext && <button className={buttonClass} disabled={busy} onClick={() => void run(() => inspectParts(partsNext, partsNext.marker), 'Next parts page loaded')}>Next parts page</button>}
      {selected.size !== 'pending' && <StorageLocations inspection={locationInspection} onChunk={onChunk} />}
      {!!preview && <><h2>Preview · first 4 KiB</h2><pre aria-label="Object preview" className="tw-whitespace-pre-wrap tw-break-all tw-text-xs">{preview}</pre></>}
    </section>}
  </Workbench>;
}
