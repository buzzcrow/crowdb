// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
import { useEffect, useState } from 'react';
import { getApiBase } from '../api';
import { readJson } from '../access/native';
export interface DeploymentDefaults { instance_id: string; http_port?: number; rpc_port?: number; s3_port?: number; health_port?: number }
export function useDeploymentDefaults(active: boolean) {
  const [values, setValues] = useState<Record<string, DeploymentDefaults> | null>(null);
  const [error, setError] = useState('');
  useEffect(() => {
    if (!active) { setValues(null); return; }
    const controller = new AbortController();
    setValues(null); setError('');
    fetch(`${getApiBase()}/deployment-defaults`, { signal: controller.signal })
      .then(readJson<Record<string, DeploymentDefaults>>)
      .then(values => { if (!controller.signal.aborted) setValues(values); })
      .catch(error => { if (!controller.signal.aborted) setError(String(error)); });
    return () => controller.abort();
  }, [active]);
  return { values, error };
}
