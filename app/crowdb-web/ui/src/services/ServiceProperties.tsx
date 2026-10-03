// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

import type { ServerSummary } from '../api';

export function ServiceProperties({ service }: { service?: ServerSummary }) {
  if (!service) return <p className="tw-p-4 tw-text-sm">This service is no longer in the current deployment inventory.</p>;
  return <div className="tw-p-4 tw-space-y-3 tw-text-sm">
    <dl className="tw-space-y-3">{Object.entries({ 'Service ID': service.id, Type: service.service_type, Node: service.node_id,
      PID: service.pid ?? 'Stopped / unobserved', Health: service.health, 'Management endpoint': service.mgmt_url ?? 'None',
      'Service endpoint': service.endpoint ?? service.rpc_url ?? 'None',
    }).map(([label, value]) => <div key={label}><dt className="tw-text-muted">{label}</dt><dd className="tw-break-all tw-font-mono">{value}</dd></div>)}</dl>
    <p className="tw-text-xs tw-text-muted">Process presence does not establish service readiness. Use the service's domain for runtime diagnostics.</p>
  </div>;
}
