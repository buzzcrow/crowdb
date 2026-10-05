// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
import { Dialog } from '../components/Dialog';
import { NodeServiceProgress } from './NodeServiceProgress';
import type { NodeServicePlan } from './useNodeServicePlans';
export function NodeServicesDialog({ nodeId, plan, onClose, onStart }: { nodeId: number; plan?: NodeServicePlan; onClose: () => void; onStart: (id: number) => void }) {
  const failed = plan && Object.values(plan).some(step => step.state === 'failed');
  return <Dialog isOpen onClose={onClose} title={`Node ${nodeId} services`} confirmLabel={!plan ? 'Deploy missing services' : failed ? 'Retry failed services' : 'Done'} onConfirm={() => !plan || failed ? onStart(nodeId) : onClose()}>
    <p className="tw-text-sm">One instance of each selected service per node. Queued services remain in the plan and deploy automatically when their prerequisites are ready; existing deployments are retained.</p>
    {plan && <NodeServiceProgress plan={plan} />}
  </Dialog>;
}
