// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
import { Input } from '../components/ui/Input';
import { serviceLabels, type ServiceKind, type ServiceOverrides } from './useNodeServicePlans';

type Ports = Omit<NonNullable<ServiceOverrides[ServiceKind]>, 'dynamic_ownership'>;
export function listenerFields(kind: ServiceKind): [keyof Ports, string][] {
  return kind === 'access-server'
    ? [['http_port', 'Iceberg'], ['s3_port', 'S3'], ['health_port', 'Health']]
    : [['rpc_port', 'RPC']];
}
export function ServicePlanCard({ kind, enabled, ports, onToggle, onChange }: {
  kind: ServiceKind; enabled: boolean; ports: Ports; onToggle: () => void;
  onChange: (field: keyof Ports, value: number) => void;
}) {
  return <li data-testid={`service-card-${kind}`} className="tw-rounded tw-border tw-border-border tw-p-3 tw-text-sm">
    <label className="tw-flex tw-items-center tw-gap-2"><input type="checkbox" aria-label={serviceLabels[kind]}
      checked={enabled} onChange={onToggle} /><span>{serviceLabels[kind]}</span></label>
    <fieldset disabled={!enabled} className="tw-mt-2 tw-space-y-2">
      {listenerFields(kind).map(([field, label]) => <Input key={field} label={`${serviceLabels[kind]} ${label} port`}
        inputMode="numeric" value={Number.isNaN(ports[field]) ? '' : ports[field] ?? ''}
        onChange={event => onChange(field, event.target.value === '' ? NaN : Number(event.target.value))} />)}
    </fieldset>
  </li>;
}
