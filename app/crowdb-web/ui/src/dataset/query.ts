// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

import { useEffect, useState } from 'react';

/** Each request belongs to one immutable scope. Hidden/unmounted work is cancelled. */
export function useQuery<T>(key: string, enabled: boolean, read: (signal: AbortSignal) => Promise<T>) {
  const [result, setResult] = useState<{ key: string; value?: T; error?: string }>({ key: '' });
  useEffect(() => {
    if (!enabled) return;
    const controller = new AbortController();
    setResult({ key });
    void read(controller.signal).then(value => {
      if (!controller.signal.aborted) setResult({ key, value });
    }, reason => {
      if (!controller.signal.aborted) setResult({ key, error: String(reason) });
    });
    return () => controller.abort();
    // The key includes every input to the request; callback identity is intentionally irrelevant.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [key, enabled]);
  const current = result.key === key ? result : { key };
  return { ...current, loading: enabled && current.value === undefined && !current.error };
}
