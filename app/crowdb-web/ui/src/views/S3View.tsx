// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

import { useState, useRef, useEffect } from 'react';
import { Workbench, JsonView, inputClass, buttonClass } from '../access/Workbench';
import { s3, xml, xmlText, objectPath, previewBytes, type S3Credentials } from '../access/native';
import { uploadObject, downloadObject } from '../access/s3Transfer';
import { useActivity } from '../contexts/ActivityContext';
import { useClusterOrigin } from '../s3/useClusterOrigin';

interface ObjectRow { key: string; size: string; etag: string; modified: string }
interface Upload { key: string; id: string }
const MAX_ROWS = 1000;
const BUCKET_PAGE = 100;
export function S3View({ active, readonly }: { active: boolean; readonly: boolean }) {
  const { log } = useActivity();
  const [demoBucket, setDemoBucket] = useState<string | null>(null);
  const [uploadsNext, setUploadsNext] = useState<{ key: string; id: string } | null>(null);
  const [partsNext, setPartsNext] = useState<{ key: string; id: string; marker: string } | null>(null);
  const { origin, error: originError, loading: originLoading, retry: retryOrigin } = useClusterOrigin(active);
  const [credentials, setCredentials] = useState<S3Credentials>({ accessKey: '', secretKey: '', sessionToken: '', region: 'us-east-1' });
  const [buckets, setBuckets] = useState<string[]>([]);
  const [bucketStart, setBucketStart] = useState(0);
  const [bucketFilter, setBucketFilter] = useState('');
  const [bucket, setBucket] = useState('');
  const [newBucket, setNewBucket] = useState('');
  const [prefix, setPrefix] = useState('');
  const [rows, setRows] = useState<ObjectRow[]>([]);
  const [next, setNext] = useState<string | null>(null);
  const [selected, setSelected] = useState<ObjectRow | null>(null);
  const [detail, setDetail] = useState<unknown>(null);
  const [uploads, setUploads] = useState<Upload[]>([]);
  const [key, setKey] = useState('');
  const [file, setFile] = useState<File | null>(null);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState('');
  const [outcome, setOutcome] = useState('');
  const [progress, setProgress] = useState<{ bytes: number; id: string | null }>({ bytes: 0, id: null });
  const controller = useRef<AbortController | null>(null);
  useEffect(() => {
    setBuckets([]); setBucketStart(0); setBucket(''); setRows([]); setNext(null); setSelected(null); setDetail(null); setUploads([]); setUploadsNext(null); setPartsNext(null); setDemoBucket(null); setError(''); setOutcome('');
  }, [origin, credentials]);
  const connected = !!origin && !!credentials.accessKey && !!credentials.secretKey;
  const request = (method: string, path: string, query: Record<string, string> = {}, body?: Blob | string) => {
    if (!origin) throw new Error('This cluster has no available S3 endpoint');
    return s3(origin, credentials, method, path, query, body);
  };
  const run = async (operation: () => Promise<void>, label: string) => {
    setBusy(true); setError(''); setOutcome('');
    try { await operation(); setOutcome(label); log({ action: label, target: `S3 / ${bucket || 'buckets'}`, status: 'Success' }); }
    catch (error) { setError(`${String(error)}. Refresh the resource before retrying a mutation.`); log({ action: label, target: `S3 / ${bucket || 'buckets'}`, status: 'Failed', message: 'Native request failed. Refresh resource state before retrying.' }); }
    finally { setBusy(false); controller.current = null; }
  };
  const loadObjects = async (scope: string, token?: string) => {
    const document = await xml(await request('GET', objectPath(scope), { 'list-type': '2', 'max-keys': '100', prefix, ...(token ? { 'continuation-token': token } : {}) }));
    const objects = Array.from(document.querySelectorAll('Contents')).map(element => ({ key: xmlText(element, 'Key'), size: xmlText(element, 'Size'), etag: xmlText(element, 'ETag'), modified: xmlText(element, 'LastModified') }));
    if (objects.length > 100) throw new Error('S3 returned more than the requested 100 objects');
    setRows(previous => (token ? Array.from(new Map([...previous, ...objects].map(row => [row.key, row])).values()) : objects).slice(0, MAX_ROWS));
    setNext(xmlText(document, 'IsTruncated') === 'true' ? xmlText(document, 'NextContinuationToken') : null);
  };
  const chooseBucket = (value: string) => { setBucket(value); setSelected(null); setDetail(null); setRows([]); setUploads([]); setUploadsNext(null); setPartsNext(null); setNext(null); void run(() => loadObjects(value), `Loaded ${value}`); };
  const listBuckets = async () => {
    const document = await xml(await request('GET', '/'));
    setBuckets(Array.from(document.querySelectorAll('Bucket')).map(element => xmlText(element, 'Name')));
    setBucketStart(0);
  };
  const listUploads = async (after?: { key: string; id: string }) => {
    const document = await xml(await request('GET', objectPath(bucket), { uploads: '', 'max-uploads': '100', ...(after ? { 'key-marker': after.key, 'upload-id-marker': after.id } : {}) }));
    const rows = Array.from(document.querySelectorAll('Upload')).map(element => ({ key: xmlText(element, 'Key'), id: xmlText(element, 'UploadId') }));
    if (rows.length > 100) throw new Error('S3 returned more than the requested 100 uploads');
    setUploads(previous => (after ? Array.from(new Map([...previous, ...rows].map(row => [row.id, row])).values()) : rows).slice(0, MAX_ROWS));
    setUploadsNext(xmlText(document, 'IsTruncated') === 'true' ? { key: xmlText(document, 'NextKeyMarker'), id: xmlText(document, 'NextUploadIdMarker') } : null);
  };
  const inspectParts = async (upload: Upload, marker?: string) => {
    const document = await xml(await request('GET', objectPath(bucket, upload.key), { uploadId: upload.id, 'max-parts': '100', ...(marker ? { 'part-number-marker': marker } : {}) }));
    setDetail({ parts: Array.from(document.querySelectorAll('Part')).map(part => ({ number: xmlText(part, 'PartNumber'), etag: xmlText(part, 'ETag'), size: xmlText(part, 'Size') })), is_truncated: xmlText(document, 'IsTruncated') === 'true' });
    setPartsNext(xmlText(document, 'IsTruncated') === 'true' ? { ...upload, marker: xmlText(document, 'NextPartNumberMarker') } : null);
    setSelected({ key: upload.key, size: 'pending', etag: '', modified: '' });
  };
  const inspect = (row: ObjectRow) => { setSelected(row); setPartsNext(null); void run(async () => {
    const response = await request('HEAD', objectPath(bucket, row.key));
    setDetail(Object.fromEntries(response.headers.entries()));
  }, `Inspected ${row.key}`); };
  const visibleBuckets = buckets.filter(name => name.includes(bucketFilter));
  return <Workbench sidebar={<>
    <h2 className="tw-font-semibold">S3</h2>
    <p className="tw-text-xs tw-text-muted">Buckets → object prefixes</p>
    <p className="tw-text-xs tw-text-muted">Current cluster · native S3</p>
    {originLoading && <p role="status" className="tw-text-xs">Loading cluster S3 endpoint…</p>}
    {(['accessKey', 'secretKey', 'region', 'sessionToken'] as const).map(name => <label className="tw-block tw-text-xs" key={name}>
      {{ accessKey: 'Access key', secretKey: 'Secret key', region: 'Region', sessionToken: 'Session token' }[name]}
      <input className={`${inputClass} tw-w-full tw-mt-1`} type={name === 'secretKey' || name === 'sessionToken' ? 'password' : 'text'} autoComplete="off" value={credentials[name]} disabled={busy} onChange={event => setCredentials(previous => ({ ...previous, [name]: event.target.value }))} />
    </label>)}
    <p className="tw-text-xs tw-text-muted">Credentials stay in this browser session.</p>
    <button className={buttonClass} disabled={!connected || busy} onClick={() => void run(listBuckets, 'Buckets refreshed')}>List buckets</button>
    {!readonly && <><button className={buttonClass} disabled={!connected || busy || !!demoBucket} onClick={() => void run(async () => {
      const demo = `console-demo-${crypto.randomUUID().replace(/-/g, '').slice(0, 24)}`;
      await request('PUT', objectPath(demo)); setDemoBucket(demo);
      await request('PUT', objectPath(demo, 'example.txt'), {}, 'CROWDB console demo\n'); await listBuckets();
    }, 'Demo bucket and object created')}>Create object demo</button>
    {demoBucket && <><p className="tw-text-xs tw-break-all">Demo scope: {demoBucket} / example.txt</p><button className={buttonClass} disabled={busy} onClick={() => { if (confirm(`Remove only ${demoBucket}/example.txt and its empty demo bucket?`)) void run(async () => {
      await request('DELETE', objectPath(demoBucket, 'example.txt')); await request('DELETE', objectPath(demoBucket)); if (bucket === demoBucket) { setBucket(''); setRows([]); setSelected(null); } setDemoBucket(null); await listBuckets();
    }, 'Demo resources removed'); }}>Clean object demo</button></>}</>}
    {!!buckets.length && <label className="tw-block tw-text-xs">Filter loaded buckets<input className={`${inputClass} tw-w-full`} value={bucketFilter} onChange={event => { setBucketFilter(event.target.value); setBucketStart(0); }} /></label>}
    <nav aria-label="S3 buckets" className="tw-flex tw-flex-col tw-gap-1">{visibleBuckets.slice(bucketStart, bucketStart + BUCKET_PAGE).map(name => <button key={name} className={`${buttonClass} tw-text-left ${bucket === name ? 'tw-text-accent' : ''}`} aria-pressed={bucket === name} disabled={busy} onClick={() => chooseBucket(name)}>{name}</button>)}</nav>
    {bucketStart > 0 && <button className={buttonClass} onClick={() => setBucketStart(value => Math.max(0, value - BUCKET_PAGE))}>Previous buckets</button>}
    {bucketStart + BUCKET_PAGE < visibleBuckets.length && <button className={buttonClass} onClick={() => setBucketStart(value => value + BUCKET_PAGE)}>Next buckets</button>}
    {!readonly && <form className="tw-space-y-2" onSubmit={event => { event.preventDefault(); void run(async () => { await request('PUT', objectPath(newBucket)); await listBuckets(); }, `Created ${newBucket}`); }}>
      <label className="tw-block tw-text-xs">New bucket<input className={`${inputClass} tw-w-full`} value={newBucket} onChange={event => setNewBucket(event.target.value)} required /></label>
      <button className={buttonClass} disabled={!connected || busy}>Create bucket</button>
    </form>}
  </>} detail={selected ? <>
    <h3 className="tw-font-semibold tw-break-all">{selected.key}</h3><p className="tw-text-xs">Bucket: {bucket}</p>
    <p className="tw-text-xs">Size: {selected.size} bytes · ETag: {selected.etag}</p><JsonView value={detail} />
    {partsNext && <button className={buttonClass} disabled={busy} onClick={() => void run(() => inspectParts(partsNext, partsNext.marker), 'Next parts page loaded')}>Next parts page</button>}
    <button className={buttonClass} disabled={busy} onClick={() => void run(async () => {
      const response = await s3(origin!, credentials, 'GET', objectPath(bucket, selected.key), {}, undefined, { range: 'bytes=0-4095' });
      const bytes = await previewBytes(response); setDetail({ ...selected, preview: new TextDecoder('utf-8', { fatal: false }).decode(bytes), range: response.headers.get('content-range') });
    }, 'Loaded first 4 KiB')}>Preview first 4 KiB</button>
    <button className={buttonClass} disabled={busy} onClick={() => void run(() => downloadObject(origin!, credentials, bucket, selected.key), 'Downloaded object')}>Download</button>
    {!readonly && <button className={buttonClass} disabled={busy} onClick={() => { if (confirm(`Delete ${bucket}/${selected.key}?`)) void run(async () => { await request('DELETE', objectPath(bucket, selected.key)); setSelected(null); await loadObjects(bucket); }, 'Object deleted'); }}>Delete object</button>}
  </> : undefined}>
    <div className="tw-flex tw-items-center tw-justify-between"><div><h1 className="tw-text-lg tw-font-semibold">{bucket ? `Bucket / ${bucket}` : 'S3 object browser'}</h1><p className="tw-text-xs tw-text-muted">Current cluster · {readonly ? 'Read only' : 'Credential permissions apply'}</p></div>
      {bucket && !readonly && <button className={buttonClass} disabled={busy} onClick={() => { if (confirm(`Delete empty bucket ${bucket}?`)) void run(async () => { await request('DELETE', objectPath(bucket)); setBucket(''); setRows([]); setSelected(null); await listBuckets(); }, 'Bucket deleted'); }}>Delete bucket</button>}</div>
    {error && <p role="alert" className="tw-text-sm tw-text-failed tw-break-all">{error}</p>}
    {originError && <div><p role="alert" className="tw-text-sm tw-text-failed">{originError}</p><button className={buttonClass} disabled={originLoading || busy} onClick={retryOrigin}>Retry cluster S3</button></div>}
    {outcome && <p role="status" className="tw-text-xs tw-text-muted">{outcome}</p>}
    {!bucket ? <p className="tw-text-muted">List buckets and select one to inspect or upload objects.</p> : <>
      <form className="tw-flex tw-gap-2" onSubmit={event => { event.preventDefault(); setSelected(null); void run(() => loadObjects(bucket), 'Objects refreshed'); }}><label className="tw-text-xs">Object prefix <input className={inputClass} value={prefix} onChange={event => setPrefix(event.target.value)} /></label><button className={buttonClass} disabled={busy}>List objects</button></form>
      <div className="tw-overflow-auto"><table className="tw-w-full tw-text-sm" aria-label="S3 objects"><thead><tr className="tw-text-left tw-text-muted"><th>Key</th><th>Size (bytes)</th><th>Modified</th></tr></thead><tbody>{rows.map(row => <tr key={row.key} className="tw-border-t tw-border-border"><td><button className="tw-py-2 tw-text-accent tw-break-all tw-text-left" disabled={busy} onClick={() => inspect(row)}>{row.key}</button></td><td>{row.size}</td><td>{row.modified}</td></tr>)}</tbody></table></div>
      {!rows.length && !busy && <p className="tw-text-xs tw-text-muted">No objects in this prefix.</p>}
      {next && rows.length < MAX_ROWS && <button className={buttonClass} disabled={busy} onClick={() => void run(() => loadObjects(bucket, next), 'Next page loaded')}>Load more</button>}
      {next && rows.length >= MAX_ROWS && <p role="status" className="tw-text-xs">Showing 1,000 objects. Narrow the object prefix to browse more.</p>}
      {!readonly && <form className="tw-space-y-2 tw-rounded tw-border tw-border-border tw-p-4" onSubmit={event => { event.preventDefault(); if (!file) return; controller.current = new AbortController(); const signal = controller.current.signal; void run(async () => { await uploadObject(origin!, credentials, bucket, key, file, signal, (bytes, id) => setProgress({ bytes, id })); await loadObjects(bucket); }, 'Upload completed'); }}>
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
          {!readonly && <button className={buttonClass} disabled={busy} onClick={() => { if (confirm(`Abort multipart upload ${upload.id}?`)) void run(async () => { await request('DELETE', objectPath(bucket, upload.key), { uploadId: upload.id }); await listUploads(); }, 'Upload aborted'); }}>Abort</button>}</div>)}
      </section>
    </>}
  </Workbench>;
}
