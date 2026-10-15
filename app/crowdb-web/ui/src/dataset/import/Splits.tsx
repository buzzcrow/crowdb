// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
import { useRef, useState } from 'react';
import { buttonClass } from '../../access/Workbench';
import { Definitions, validateEditor } from '../splits/Definitions';
import { operationPath, request, type Operation } from '../operation';
import type { CompositionDraft } from '../types';
import { newId } from './upload';

export function CompositionSplits({ operation, draft, busy, run, saved, close }: {
  operation: Operation; draft: CompositionDraft; busy: boolean; run: (action: () => Promise<void>) => void;
  saved: (value: CompositionDraft) => void; close: () => void;
}) {
  const element = useRef<HTMLDivElement>(null);
  const [splits, setSplits] = useState(draft.splits); const [policy, setPolicy] = useState(draft.policy);
  return <div ref={element} aria-label="Initial Split set editor">
    <Definitions splits={splits} policy={policy} disabled={busy || !!operation.pending} change={(values, rules) => { setSplits(values); setPolicy(rules); }} />
    <button className={buttonClass} disabled={busy || !!operation.pending} onClick={() => run(async () => {
      validateEditor(element.current);
      saved(await request<CompositionDraft>(operation, operationPath(operation), 'PUT', { edit: newId(), expected_revision: draft.revision, samples: [], splits, policy }));
    })}>Save initial Split set</button>
    <button className={buttonClass} disabled={busy} onClick={close}>Cancel Split edits</button>
  </div>;
}
