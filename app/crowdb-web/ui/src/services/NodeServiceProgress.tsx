// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
import { serviceLabels, serviceOrder, type NodeServicePlan } from './useNodeServicePlans';
export function NodeServiceProgress({ plan }: { plan: NodeServicePlan }) {
  return <div>
    <ul aria-label="Node service progress" className="tw-space-y-2 tw-my-4">{serviceOrder.map(kind => <li key={kind} className="tw-rounded tw-border tw-border-border tw-p-3 tw-text-sm">
      <div className="tw-flex tw-justify-between tw-gap-3"><span>{serviceLabels[kind]}</span><span className={plan[kind].state === 'failed' ? 'tw-text-failed' : 'tw-text-muted'}>{plan[kind].state}</span></div>
      {plan[kind].detail && <p role="status" className="tw-mt-1 tw-text-xs tw-text-muted">{plan[kind].detail}</p>}
    </li>)}</ul>
    <p className="tw-text-xs tw-text-muted">Waiting services resume automatically while this console page stays open. You can close this dialog to prepare KV and Capacity or create another node. Reopen the plan from the Node menu.</p>
  </div>;
}
