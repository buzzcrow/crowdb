// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

import { useEffect, useState } from 'react';
import { connections } from '../access/native';

export function useClusterOrigin(active: boolean) {
  const [origin, setOrigin] = useState<string | null>(null);
  const [error, setError] = useState('');
  const [loading, setLoading] = useState(false);
  const [attempt, setAttempt] = useState(0);
  useEffect(() => {
    if (!active) return;
    const controller = new AbortController();
    setLoading(true);
    setError('');
    void connections(controller.signal).then(result => {
      if (controller.signal.aborted) return;
      if (result.s3) setOrigin(result.s3);
      if (!result.s3) setError('This cluster has no S3 endpoint. Deploy Access Server in Cluster, then retry.');
    }).catch(error => {
      if (!controller.signal.aborted) { setError(String(error)); }
    }).finally(() => { if (!controller.signal.aborted) setLoading(false); });
    return () => controller.abort();
  }, [active, attempt]);
  return { origin, error, loading, retry: () => setAttempt(value => value + 1) };
}
