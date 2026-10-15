// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

import { buttonClass } from '../access/Workbench';
import * as api from './api';
import { operationPath, retryIdentical, type Operation } from './operation';
import type { CompositionDraft, SplitDraft } from './types';

/** Creation may commit before its response reaches the browser. Keep its identity. */
export function BeginRecovery({ operation, busy, run, loaded }: {
  operation: Operation; busy: boolean; run: (action: () => Promise<void>) => void;
  loaded: (draft: CompositionDraft | SplitDraft) => void;
}) {
  return <section aria-label="Recover draft creation" className="tw-space-y-2">
    <p>Draft creation needs inspection. A missing draft can be created by replaying the saved request with the same operation ID.</p>
    <button className={buttonClass} disabled={busy} onClick={() => run(async () => loaded(await api.control(operationPath(operation))))}>Inspect created draft</button>
    {operation.pending && <button className={buttonClass} disabled={busy} onClick={() => run(async () => {
      await retryIdentical(operation);
      loaded(await api.control(operationPath(operation)));
    })}>Retry identical creation</button>}
  </section>;
}
