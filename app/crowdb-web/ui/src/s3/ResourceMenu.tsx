// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
import { randomUUID } from '../utils/randomUUID';
import { useState, useRef, type MouseEvent } from 'react';
import { ContextMenu, useContextMenu, type MenuItem } from '../components/ContextMenu';
import { Dialog } from '../components/Dialog';
import { inputClass } from '../access/Workbench';
import { s3, objectPath, previewBytes } from '../access/native';
import { uploadObject, downloadObject } from '../access/s3Transfer';
export interface S3Target { bucket?: string; key?: string; uploadId?: string }
type Action = 'create' | 'upload' | 'replace' | 'delete-bucket' | 'delete-object' | 'abort';
const labels: Record<Action, string> = { abort: 'Abort multipart upload', create: 'Create bucket', upload: 'Upload object', replace: 'Replace object', 'delete-bucket': 'Delete empty bucket', 'delete-object': 'Delete object' };
const defaultBucketName = () => `bucket-${randomUUID().replaceAll('-', '').slice(0, 8)}`;
export function useS3Menu(readonly: boolean, connected: boolean, changed: (target: S3Target, action: string) => Promise<void>, preview: (target: S3Target, text: string) => void, multipart: (bucket: string) => void) {
  const menu = useContextMenu(); const controller = useRef<AbortController>();
  const [dialog, setDialog] = useState<{ target: S3Target; action: Action }>();
  const [name, setName] = useState(''); const [files, setFiles] = useState<File[]>([]);
  const [results, setResults] = useState<Array<{ name: string; status: 'uploading' | 'success' | 'failed' | 'unknown'; error?: string }>>([]);
  const [busy, setBusy] = useState(false); const [error, setError] = useState('');
  const [progress, setProgress] = useState('');
  const run = async (work: () => Promise<void>) => { setBusy(true); setError(''); try { await work(); } catch (reason) { setError(`${String(reason)}. Refresh the resource before retrying.`); } finally { setBusy(false); } };
  const open = (event: MouseEvent, target: S3Target) => {
    const items: MenuItem[] = [];
    const add = (action: Action) => items.push({ id: action, label: labels[action] + '…', disabled: readonly || busy || !connected, destructive: (action.startsWith('delete-') || action === 'abort'), onSelect: () => { setName(action === 'create' ? defaultBucketName() : target.key ?? ''); setFiles([]); setResults([]); setError(''); setProgress(''); setDialog({ target: { ...target }, action }); } });
    if (target.uploadId) add('abort');
    else if (!target.bucket) add('create');
    else if (target.key === undefined) {
      add('upload'); add('delete-bucket');
      items.push({ id: 'multipart', label: 'Multipart uploads', disabled: busy || !connected, onSelect: () => multipart(target.bucket!) });
    } else {
      items.push({ id: 'preview', label: 'Preview first 4 KiB', disabled: busy || !connected, onSelect: () => run(async () => {
        const response = await s3('GET', objectPath(target.bucket!, target.key!), {}, undefined, { range: 'bytes=0-4095' });
        preview(target, new TextDecoder().decode(await previewBytes(response)));
      }) }, { id: 'download', label: 'Download', disabled: busy || !connected, onSelect: () => run(() => downloadObject(target.bucket!, target.key!)) });
      add('replace'); add('delete-object');
    }
    menu.openMenu(event, items);
  };
  const submit = () => run(async () => {
    if (!dialog || readonly) return;
    const { action, target } = dialog;
    if (action === 'abort') await s3('DELETE', objectPath(target.bucket!, target.key!), { uploadId: target.uploadId! });
    if (action === 'create') await s3('PUT', objectPath(name));
    if (action === 'delete-bucket') await s3('DELETE', objectPath(target.bucket!));
    if (action === 'delete-object') await s3('DELETE', objectPath(target.bucket!, target.key!));
    if (action === 'upload' || action === 'replace') {
      if (!files.length) throw new Error('Choose at least one object file.');
      controller.current = new AbortController();
      const statuses: Array<{ name: string; status: 'success' | 'failed' | 'unknown'; error?: string }> = [];
      for (const file of files) {
        const key = action === 'replace' ? target.key! : file.name;
        setResults(previous => [...previous.filter(result => result.name !== file.name), { name: file.name, status: 'uploading' }]);
        setProgress(`${file.name} · uploading…`);
        try {
          await uploadObject(target.bucket!, key, file, controller.current.signal, (bytes, id) => setProgress(`${file.name} · ${bytes.toLocaleString()} bytes sent${id ? ` · UploadId ${id}` : ''}`));
          statuses.push({ name: file.name, status: 'success' });
          setResults(previous => previous.map(result => result.name === file.name ? { ...result, status: 'success' } : result));
        } catch (reason) {
          const status = reason instanceof DOMException && reason.name === 'AbortError' ? 'unknown' : 'failed';
          statuses.push({ name: file.name, status, error: String(reason) });
          setResults(previous => previous.map(result => result.name === file.name ? { ...result, status, error: String(reason) } : result));
        }
      }
      const failures = statuses.filter(result => result.status !== 'success');
      if (failures.length) throw new Error(`${failures.length} object upload(s) failed or are unknown`);
    }
    await changed(target, action); setDialog(undefined);
  });
  const upload = dialog?.action === 'upload' || dialog?.action === 'replace';
  return { open, overlay: <>
    {menu.menuState && <ContextMenu {...menu.menuState} onClose={menu.closeMenu} />}
    {!dialog && error && <p role="alert">{error}</p>}
    {dialog && <Dialog isOpen title={labels[dialog.action]} description={`Target: ${dialog.target.bucket ?? 'S3'}${dialog.target.key !== undefined ? ` / ${dialog.target.key}` : ''}`} onClose={() => { if (!busy) setDialog(undefined); }} onConfirm={submit} confirmLabel={labels[dialog.action]} confirmLoading={busy} confirmDisabled={readonly || dialog.action === 'create' && !name || upload && !files.length} destructive={(dialog.action.startsWith('delete-') || dialog.action === 'abort')}>
      <div className="tw-space-y-3">
        {dialog.action === 'create' && <label className="tw-block">New bucket<input className={`${inputClass} tw-w-full`} disabled={busy} value={name} onChange={event => setName(event.target.value)} /></label>}
        {upload && <><label className="tw-block">{dialog.action === 'replace' ? 'Object file' : 'Object files'}<input aria-label={dialog.action === 'replace' ? 'Object file' : 'Object files'} type="file" multiple={dialog.action === 'upload'} disabled={busy} onChange={event => { setFiles(Array.from(event.target.files ?? [])); setResults([]); event.target.value = ''; }} /></label><p className="tw-text-xs">Each file uses its filename as the object key; an existing key is overwritten.</p>{results.length > 0 && <ul aria-label="Object upload results" className="tw-text-xs">{results.map(result => <li key={result.name}>{result.name}: {result.status}{result.error ? ` · ${result.error}` : ''}</li>)}</ul>}<p>{progress}</p>{busy && <button onClick={() => controller.current?.abort()}>Stop upload</button>}<p className="tw-text-xs">Stopped or failed multipart uploads remain inspectable through the Bucket menu.</p></>}
        {dialog.action === 'abort' && <p>Abort upload {dialog.target.uploadId} and discard its pending parts?</p>}
        {dialog.action === 'delete-bucket' && <p>Only an empty bucket can be deleted.</p>}
        {dialog.action === 'delete-object' && <p>Delete this exact Object and its stored content?</p>}
        {error && <p role="alert">{error}</p>}
      </div>
    </Dialog>}
  </> };
}
