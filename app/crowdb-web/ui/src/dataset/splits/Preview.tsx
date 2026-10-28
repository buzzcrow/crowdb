// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
import { useContext, useEffect, useState } from 'react';
import { Matrix, emptyWindow } from '../Matrix';
import { EditorDetails } from '../EditorDetails';
import { Properties } from '../Properties';
import type { DatasetSelection, DraftSplit } from '../types';

export function SplitPreview({ dataset, snapshot, split }: { dataset: string; snapshot: string; split: DraftSplit }) {
  const [window, setWindow] = useState(emptyWindow);
  const [selected, setSelected] = useState<DatasetSelection>({ kind: 'snapshot', dataset, snapshot });
  const details = useContext(EditorDetails);
  useEffect(() => { details(<Properties selected={selected} active />); return () => details(null); }, [selected, details]);
  return <section aria-label="Split preview" className="tw-space-y-3">
    <h3 className="tw-font-semibold">View · {split.name}</h3>
    <p className="tw-text-xs tw-text-muted">Draft preview on the complete Snapshot. Pages scan up to 20 Samples; Commit freezes membership.</p>
    <Matrix scope={{ dataset, snapshot }} preview={split} active refresh={0} window={window} updateWindow={setWindow} select={setSelected} selectedId={'sample' in selected ? selected.sample : undefined} />
  </section>;
}
