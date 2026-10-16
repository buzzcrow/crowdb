// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

import { useState } from 'react';
import { buttonClass } from '../access/Workbench';
import * as api from './api';
import { forgetOperation, operationPath, retryIdentical, type Operation } from './operation';
import type { CompositionDraft, SplitDraft } from './types';

/** Creation may commit before its response reaches the browser. Keep its identity. */
export function BeginRecovery({ operation, busy, run, loaded, discarded }: {
  operation: Operation; busy: boolean; run: (action: () => Promise<void>) => void;
  discarded: () => void;
  loaded: (draft: CompositionDraft | SplitDraft) => void;
}) {
  const [missing, setMissing] = useState(false);
  return <section aria-label="Recover draft creation" className="tw-space-y-2">
    <p>Draft creation needs inspection. A missing draft can be created by replaying the saved request with the same operation ID.</p>
    <button className={buttonClass} disabled={busy} onClick={() => run(async () => { try { loaded(await api.control(operationPath(operation))); } catch (reason) { if (reason instanceof api.DatasetError && reason.status === 404 && !operation.pending) setMissing(true); throw reason; } })}>Inspect created draft</button>
    {missing && !operation.pending && <button className={buttonClass} disabled={busy} onClick={() => { forgetOperation(operation); discarded(); }}>Discard rejected creation</button>}
    {operation.pending && <button className={buttonClass} disabled={busy} onClick={() => run(async () => {
      await retryIdentical(operation);
      loaded(await api.control(operationPath(operation)));
    })}>Retry identical creation</button>}
  </section>;
}
