// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
import { useState, useRef, type MouseEvent } from 'react';
import { ContextMenu, useContextMenu, type MenuItem } from '../components/ContextMenu';
import { Dialog } from '../components/Dialog';
import { inputClass } from '../access/Workbench';
import { s3, objectPath, previewBytes } from '../access/native';
import { uploadObject, downloadObject } from '../access/s3Transfer';
export interface S3Target { bucket?: string; key?: string; uploadId?: string }
type Action = 'create' | 'upload' | 'replace' | 'delete-bucket' | 'delete-object' | 'abort';
const labels: Record<Action, string> = { abort: 'Abort multipart upload', create: 'Create bucket', upload: 'Upload object', replace: 'Replace object', 'delete-bucket': 'Delete empty bucket', 'delete-object': 'Delete object' };
export function useS3Menu(readonly: boolean, connected: boolean, changed: (target: S3Target, action: string) => Promise<void>, preview: (target: S3Target, text: string) => void, multipart: (bucket: string) => void) {
  const menu = useContextMenu(); const controller = useRef<AbortController>();
  const [dialog, setDialog] = useState<{ target: S3Target; action: Action }>();
  const [name, setName] = useState(''); const [file, setFile] = useState<File>();
  const [busy, setBusy] = useState(false); const [error, setError] = useState('');
  const [progress, setProgress] = useState('');
  const run = async (work: () => Promise<void>) => { setBusy(true); setError(''); try { await work(); } catch (reason) { setError(`${String(reason)}. Refresh the resource before retrying.`); } finally { setBusy(false); } };
  const open = (event: MouseEvent, target: S3Target) => {
    const items: MenuItem[] = [];
    const add = (action: Action) => items.push({ id: action, label: labels[action] + '…', disabled: readonly || busy || !connected, destructive: (action.startsWith('delete-') || action === 'abort'), onSelect: () => { setName(target.key ?? ''); setFile(undefined); setError(''); setProgress(''); setDialog({ target: { ...target }, action }); } });
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
    if ((action === 'upload' || action === 'replace') && file) {
      controller.current = new AbortController();
      await uploadObject(target.bucket!, name, file, controller.current.signal, (bytes, id) => setProgress(`${bytes.toLocaleString()} bytes sent${id ? ` · UploadId ${id}` : ''}`));
    }
    await changed(target, action); setDialog(undefined);
  });
  const upload = dialog?.action === 'upload' || dialog?.action === 'replace';
  return { open, overlay: <>
    {menu.menuState && <ContextMenu {...menu.menuState} onClose={menu.closeMenu} />}
    {!dialog && error && <p role="alert">{error}</p>}
    {dialog && <Dialog isOpen title={labels[dialog.action]} description={`Target: ${dialog.target.bucket ?? 'S3'}${dialog.target.key !== undefined ? ` / ${dialog.target.key}` : ''}`} onClose={() => { if (!busy) setDialog(undefined); }} onConfirm={submit} confirmLabel={labels[dialog.action]} confirmLoading={busy} confirmDisabled={readonly || (dialog.action === 'create' || upload) && !name || upload && !file} destructive={(dialog.action.startsWith('delete-') || dialog.action === 'abort')}>
      <div className="tw-space-y-3">
        {(dialog.action === 'create' || upload) && <label className="tw-block">{upload ? 'Object key' : 'New bucket'}<input className={`${inputClass} tw-w-full`} disabled={busy || dialog.action === 'replace'} value={name} onChange={event => setName(event.target.value)} /></label>}
        {upload && <><input aria-label="Object file" type="file" disabled={busy} onChange={event => { const file = event.target.files?.[0]; setFile(file); if (file && !name) setName(file.name); }} /><p>{progress}</p>{busy && <button onClick={() => controller.current?.abort()}>Stop upload</button>}<p className="tw-text-xs">Stopped or failed multipart uploads remain inspectable through the Bucket menu.</p></>}
        {dialog.action === 'abort' && <p>Abort upload {dialog.target.uploadId} and discard its pending parts?</p>}
        {dialog.action === 'delete-bucket' && <p>Only an empty bucket can be deleted.</p>}
        {dialog.action === 'delete-object' && <p>Delete this exact Object and its stored content?</p>}
        {error && <p role="alert">{error}</p>}
      </div>
    </Dialog>}
  </> };
}
