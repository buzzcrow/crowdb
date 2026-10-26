// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

import { buttonClass, JsonView } from '../access/Workbench';
import type { PublicationResult } from './types';
import { forgetOperation, prepare, publish, retryIdentical, useObservedHead, type Operation } from './operation';

export function Publication({ operation: op, update, complete, close, busy, run }: {
  operation: Operation; update: (op: Operation) => void; complete: (result: PublicationResult) => void;
  close: () => void; busy: boolean; run: (action: () => Promise<void>) => void;
}) {
  const finish = async (result?: PublicationResult) => {
    if (result) { await forgetOperation(op); complete(result); }
    else update({ ...op });
  };
  return <section role="region" aria-label="Resource publication" className="tw-space-y-3 tw-border-t tw-border-border tw-pt-3">
    <h3 className="tw-font-semibold">{op.prepared ? 'Complete result' : 'Complete input ready'}</h3>
    <p className="tw-text-xs tw-break-all">{op.kind === 'snapshot' ? 'Snapshot' : 'Split revision'} <span data-testid="prepared-resource-id">{op.resource}</span></p>
    <p className="tw-text-xs">{op.kind === 'snapshot' ? 'Publishing selects this Snapshot as latest. Stable and the parent keep their current values.' : 'Publishing replaces the current Split set. Historical revisions remain available.'}</p>
    {op.prepared && <JsonView value={op.prepared} />}
    {op.pending ? <div className="tw-space-y-2">
      <p role="status">Reply unavailable. The complete request is saved in this browser.</p>
      <button className={buttonClass} disabled={busy} onClick={() => run(async () => finish(await retryIdentical(op)))}>Retry identical request</button>
    </div> : <div className="tw-flex tw-flex-wrap tw-gap-2">
      {!op.prepared && op.input && <button className={buttonClass} disabled={busy} onClick={() => run(async () => { await prepare(op); update({ ...op }); })}>Prepare complete result</button>}
      <button className={buttonClass} disabled={busy || !op.prepared || !!op.conflict} onClick={() => run(async () => finish(await publish(op)))}>Publish {op.kind === 'snapshot' ? 'Snapshot' : 'Split revision'}</button>
      {op.conflict && <div><p>The head changed. Review the returned values before replacing it with this complete result.</p><JsonView value={op.conflict} />
        <button className={buttonClass} disabled={busy} onClick={() => run(async () => { await useObservedHead(op); update({ ...op }); })}>Use reviewed head</button>
      </div>}
      <button className={buttonClass} disabled={busy} onClick={() => run(async () => { await forgetOperation(op); close(); })}>Discard local request</button>
    </div>}
    <p className="tw-text-xs">Complete results can be read by ID before publication. Discarding local input leaves unreferenced results for GC.</p>
  </section>;
}
