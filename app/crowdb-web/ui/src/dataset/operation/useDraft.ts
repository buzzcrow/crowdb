// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
import { useRef, useState } from 'react';
import { saveOperation, type Operation } from '../operation';

/** Serialize saves while keeping edits and upload callbacks on one current input. */
export function useDraft(initial?: Operation) {
  const [op, setOp] = useState(initial); const current = useRef(initial);
  const version = useRef(initial?.localRevision ?? 0); const saving = useRef(Promise.resolve());
  const [error, setError] = useState(''); const [dirty, setDirty] = useState(false);
  const install = (value: Operation) => { current.current = value; version.current = value.localRevision ?? 0; setOp(value); };
  const persist = (value = current.current) => {
    if (!value) return Promise.resolve();
    setDirty(true);
    saving.current = saving.current.catch(() => {}).then(async () => {
      value.localRevision = version.current;
      await saveOperation(value); version.current = value.localRevision ?? 0;
      if (current.current?.resource === value.resource) current.current.localRevision = version.current;
      if (current.current === value) setDirty(false);
    });
    void saving.current.catch(reason => setError(String(reason)));
    return saving.current;
  };
  const change = (update: (value: Operation) => Operation) => {
    if (!current.current) return;
    const next = update(current.current); current.current = next; setOp(next); setError('');
    void persist(next);
  };
  return { op, current, install, change, persist, saving, error, setError, dirty };
}
