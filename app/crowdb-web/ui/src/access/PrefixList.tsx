// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
import { buttonClass, inputClass } from './Workbench';

export function PrefixList({ resource, value, change, submit, busy }: {
  resource: 'Bucket' | 'Object'; value: string; change: (value: string) => void;
  submit: () => void; busy: boolean;
}) {
  return <form aria-label={`${resource} list query`} className="tw-flex tw-items-end tw-gap-2" onSubmit={event => { event.preventDefault(); submit(); }}>
    <label className="tw-text-xs">Prefix<input aria-label={`${resource} prefix`} className={`${inputClass} tw-block tw-mt-1`} value={value} onChange={event => change(event.target.value)} /></label>
    <button className={buttonClass} disabled={busy}>List</button>
  </form>;
}
