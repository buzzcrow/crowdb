// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
import { createContext, useContext, useEffect, useRef, useState, type ReactNode } from 'react';
import { useDomain } from '../contexts/DomainContext';
import { Domain } from '../types';
import { listNodeCandidates, nodeAdmissions, nodeAutoJoin, setNodeAutoJoin, type AutoJoinStatus, type NodeAdmission, type NodeCandidates } from '../api';

function useCandidates(active: boolean) {
  const [snapshot, setSnapshot] = useState<NodeCandidates>();
  const [actionError, setActionError] = useState('');
  const [unavailable, setUnavailable] = useState(false);
  const [policyUnavailable, setPolicyUnavailable] = useState(false);
  const [admissions, setAdmissions] = useState<NodeAdmission[]>([]);
  const [policy, setPolicy] = useState<AutoJoinStatus>();
  const [saving, setSaving] = useState<string>();
  const revision = useRef(0);
  useEffect(() => {
    if (!active) return;
    const controller = new AbortController();
    const failed = [false, false, false];
    const poll = <T,>(index: number, request: () => Promise<T>, update: (value: T) => void) => {
      let timer: ReturnType<typeof setTimeout>;
      const refresh = async () => {
        try {
          const observed = revision.current;
          const value = await request();
          if (!controller.signal.aborted) {
            if (index !== 2 || (observed % 2 === 0 && observed === revision.current)) update(value);
            failed[index] = false;
          }
        } catch {
          if (!controller.signal.aborted) failed[index] = true;
        } finally {
          if (!controller.signal.aborted) {
            setUnavailable(failed[0] || failed[1]);
            setPolicyUnavailable(failed[2]);
            timer = setTimeout(refresh, 2000);
          }
        }
      };
      void refresh();
      return () => clearTimeout(timer);
    };
    const stop = [
      poll(0, () => listNodeCandidates({ signal: controller.signal }), setSnapshot),
      poll(1, () => nodeAdmissions({ signal: controller.signal }), setAdmissions),
      poll(2, () => nodeAutoJoin({ signal: controller.signal }), setPolicy),
    ];
    return () => { controller.abort(); stop.forEach(cancel => cancel()); };
  }, [active]);

  const candidates = snapshot?.candidates.filter(node =>
    (node.state === 'unbound' || node.state === 'same_cluster')
    && !admissions.some(entry => entry.discovery_id === node.advertisement.discovery_id && entry.confirmed && !entry.cancelled)
    && (node.state !== 'same_cluster' || admissions.some(entry => entry.discovery_id === node.advertisement.discovery_id && !entry.confirmed && !entry.cancelled))) ?? [];
  const changeAutoJoin = async (id: string, enabled: boolean) => {
    revision.current += 1;
    setSaving(id); setActionError('');
    try { setPolicy(await setNodeAutoJoin(enabled, undefined, id)); }
    catch (error) { setActionError(String(error)); }
    finally { revision.current += 1; setSaving(undefined); }
  };
  return { snapshot, candidates, admissions, policy, saving, unavailable, policyUnavailable, actionError, setActionError, changeAutoJoin };
}
const CandidateContext = createContext<ReturnType<typeof useCandidates> | null>(null);
export function CandidateProvider({ active, children }: { active: boolean; children: ReactNode }) {
  const { domain } = useDomain();
  const value = useCandidates(active && domain === Domain.Cluster);
  return <CandidateContext.Provider value={value}>{children}</CandidateContext.Provider>;
}
export function useNodeCandidates() {
  const value = useContext(CandidateContext);
  if (!value) throw new Error('CandidateProvider is required');
  return value;
}
