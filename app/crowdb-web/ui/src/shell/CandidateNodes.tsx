// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
import { useEffect, useRef, useState } from 'react';
import { createPortal } from 'react-dom';
import { Monitor, Zap, CircleHelp } from 'lucide-react';
import { cancelNodeAdmission, discardCandidateReview, type NodeDeploymentStatus } from '../api';
import { AdmitCandidateDialog } from './AdmitCandidateDialog';
import { Tree } from '../components/Tree';
import { ContextMenu, useContextMenu, type MenuItem } from '../components/ContextMenu';
import { useSelection } from '../contexts/SelectionContext';
import { Domain, type Rack } from '../types';
import { useNodeCandidates } from './NodeCandidatesContext';

export function CandidateNodes({ racks, readonly, onChange, addNodeRequest, deployment }: { deployment?: NodeDeploymentStatus; addNodeRequest?: { rackId: number; nonce: number }; racks: Rack[]; readonly?: boolean; onChange?: () => void }) {
  const section = useRef<HTMLElement>(null);
  const { snapshot, candidates, admissions, policy, saving, unavailable, policyUnavailable, actionError, setActionError, changeAutoJoin } = useNodeCandidates();
  const { selectedEntity, inspectEntity } = useSelection();
  const [admitting, setAdmitting] = useState<string>();
  const [menuTarget, setMenuTarget] = useState<string>();
  const { menuState, openMenu, closeMenu } = useContextMenu();
  const inspect = (id: string) => inspectEntity({ type: 'Node', section: 'discovery', id, name: `node-${id.slice(0, 8)}`, domain: Domain.Cluster });
  useEffect(() => { if (addNodeRequest) { section.current?.focus(); section.current?.scrollIntoView({ block: 'nearest' }); } }, [addNodeRequest]);
  const run = async (work: () => Promise<unknown>) => {
    setActionError('');
    try { await work(); onChange?.(); } catch (error) { setActionError(String(error)); }
  };
  useEffect(() => {
    if (menuState && menuTarget && !candidates.some(node => node.advertisement.discovery_id === menuTarget)) closeMenu();
  }, [candidates, menuTarget, menuState, closeMenu]);
  const menuItems = (id: string): MenuItem[] => {
    const enabled = policy ? policy.candidates?.[id] ?? policy.enabled : undefined;
    const record = admissions.find(entry => entry.discovery_id === id);
    const candidate = candidates.find(entry => entry.advertisement.discovery_id === id);
    const disabled = readonly || unavailable || saving !== undefined || !candidate;
    const items: MenuItem[] = [{ id: 'auto', label: enabled ? 'Disable Auto join' : 'Enable Auto join', disabled: disabled || policyUnavailable || enabled === undefined || candidate?.state !== 'unbound', onSelect: () => changeAutoJoin(id, !enabled) }];
    if (!record || record.cancelled) items.push({ id: 'join', label: 'Join node…', disabled: disabled || !racks.length || !deployment || !['unbound_draft', 'active'].includes(deployment.phase), onSelect: () => setAdmitting(id) });
    if (record && !record.confirmed && !record.cancelled) items.push({ id: 'cancel', label: 'Cancel admission', disabled, onSelect: () => run(async () => { const result = await cancelNodeAdmission(id, record.operation_id); if (result.pending.length) setActionError(`Cancellation pending: ${result.pending.join('; ')}`); }) });
    for (const prepared of snapshot?.prepared_access?.filter(entry => entry.discovery_id === id) ?? []) items.push({ id: prepared.operation_id, label: 'Discard access review', disabled, onSelect: () => run(() => discardCandidateReview(id, prepared.operation_id)) });
    return items;
  };
  return <section ref={section} tabIndex={-1} aria-label="Candidate nodes" className="tw-pl-8 tw-text-xs">
    {actionError && <p role="alert">{actionError}</p>}
    {unavailable && <p role="status">Candidate status unavailable{snapshot ? ' — last observations shown' : ''}.</p>}
    {policyUnavailable && <p role="status">Auto join settings unavailable.</p>}
    {!snapshot && !unavailable && <p>Discovering nodes…</p>}
    {snapshot && !candidates.length && <p>No candidate nodes</p>}
    <Tree nodes={candidates.map(node => {
      const id = node.advertisement.discovery_id;
      const enabled = policy ? policy.candidates?.[id] ?? policy.enabled : undefined;
      return { id: `candidate-${id}`, rawId: id, type: 'Node', section: 'discovery', label: `node-${id.slice(0, 8)}`,
        title: id, icon: <Monitor className="tw-h-4 tw-w-4 tw-text-muted" />,
        selected: selectedEntity?.section === 'discovery' && selectedEntity.id === id,
        activate: () => inspect(id),
        suffix: <span title={enabled === undefined ? 'Auto join unknown' : enabled ? 'Auto join enabled' : 'Auto join disabled'} aria-label={enabled === undefined ? 'Auto join unknown' : enabled ? 'Auto join enabled' : 'Auto join disabled'}>
          {enabled === undefined ? <CircleHelp className="tw-h-3 tw-w-3" /> : <Zap className={`tw-h-3 tw-w-3 ${enabled ? 'tw-text-accent' : 'tw-text-muted tw-opacity-40'}`} />}
        </span> };
    })} onNodeContextMenu={(node, event) => {
      const id = String(node.rawId); inspect(id);
      setMenuTarget(id); openMenu(event, []);
    }} />
    {menuState && menuTarget && createPortal(<ContextMenu {...menuState} items={menuItems(menuTarget)} onClose={closeMenu} />, section.current?.closest('.crowdb-console') ?? document.body)}
    {admitting && section.current?.closest('.crowdb-console') && createPortal(<AdmitCandidateDialog initialRackId={addNodeRequest?.rackId} discoveryId={admitting} racks={racks} preparedNode={snapshot?.prepared_access?.find(entry => entry.discovery_id === admitting)} onClose={() => setAdmitting(undefined)} onSuccess={onChange} />, section.current.closest('.crowdb-console')!)}
  </section>;
}
