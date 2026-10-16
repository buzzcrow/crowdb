// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

import { randomUUID } from '../utils/randomUUID';
import { createContext, useContext, useState, useRef, useCallback, useEffect, ReactNode } from 'react';
import { Domain } from '../types';
import { captureScroll, cancelScrollRestore } from './navigationScroll';
import { readVisit, writeVisit } from './browserVisits';

type Capture = () => () => void;
export type ClusterViewMode = 'topology' | 'capacity';
export const canonicalDomain = (domain: Domain) => domain === Domain.Capacity ? Domain.Cluster : domain;
interface Visit { key: number; domain: Domain; clusterView: ClusterViewMode; restore: Array<() => void> }
interface DomainContextType {
  domain: Domain;
  setDomain: (domain: Domain) => void;
  clusterView: ClusterViewMode;
  setClusterView: (view: ClusterViewMode) => void;
  checkpoint: () => void;
  back: () => void;
  forward: () => void;
  canBack: boolean;
  canForward: boolean;
  returning: boolean;
  register: (domain: Domain, id: string, capture: Capture) => () => void;
}
const DomainContext = createContext<DomainContextType | undefined>(undefined);

export function DomainProvider({ children, initialDomain }: { children: ReactNode; initialDomain?: Domain }) {
  const [domain, updateDomain] = useState<Domain>(canonicalDomain(initialDomain ?? Domain.Cluster));
  const [clusterView, updateClusterView] = useState<ClusterViewMode>(initialDomain === Domain.Capacity ? 'capacity' : 'topology');
  const currentView = useRef(clusterView); currentView.current = clusterView;
  const [returning, setReturning] = useState(false);
  const session = useRef(randomUUID());
  const visitKey = useRef(0);
  const sequence = useRef(0);
  const current = useRef(domain); current.current = domain;
  const captures = useRef(new Map<Domain, Map<string, Capture>>());
  const previous = useRef<Visit[]>([]);
  const next = useRef<Visit[]>([]);
  const sameEvent = useRef(false);
  const [, setVersion] = useState(0);
  const changed = useCallback(() => setVersion(value => value + 1), []);
  const capture = useCallback((): Visit => ({
    key: visitKey.current, domain: current.current, clusterView: currentView.current,
    restore: [...[...(captures.current.get(current.current)?.values() ?? [])].map(read => read()), captureScroll()],
  }), []);
  const checkpoint = useCallback(() => {
    // Tree selection and its page handler describe one synchronous navigation.
    if (sameEvent.current) return;
    sameEvent.current = true;
    queueMicrotask(() => { sameEvent.current = false; });
    cancelScrollRestore();
    setReturning(false);
    previous.current = [...previous.current, capture()].slice(-32);
    next.current = [];
    visitKey.current = ++sequence.current;
    writeVisit({ session: session.current, key: visitKey.current }, current.current, true, currentView.current);
    changed();
  }, [capture, changed]);
  const setDomain = useCallback((target: Domain) => {
    const destination = canonicalDomain(target);
    const view = target === Domain.Capacity ? 'capacity' : currentView.current;
    if (destination === current.current && view === currentView.current) return;
    checkpoint();
    current.current = destination;
    currentView.current = view;
    updateDomain(destination);
    updateClusterView(view);
  }, [checkpoint]);
  const setClusterView = useCallback((view: ClusterViewMode) => {
    if (current.current === Domain.Cluster && currentView.current === view) return;
    checkpoint();
    current.current = Domain.Cluster;
    currentView.current = view;
    updateDomain(Domain.Cluster);
    updateClusterView(view);
  }, [checkpoint]);
  useEffect(() => {
    writeVisit({ session: session.current, key: visitKey.current }, current.current, false, currentView.current);
    const travel = (event: PopStateEvent) => {
      const target = readVisit(event.state);
      if (!target || target.session !== session.current || target.key === visitKey.current) return;
      const source = target.key < visitKey.current ? previous.current : next.current;
      const destination = target.key < visitKey.current ? next.current : previous.current;
      const index = source.findIndex(visit => visit.key === target.key);
      if (index < 0) { window.location.reload(); return; }
      const visit = source[index];
      destination.push(capture(), ...source.slice(index + 1).reverse());
      if (destination.length > 32) destination.splice(0, destination.length - 32);
      source.splice(index);
      setReturning(true);
      visitKey.current = visit.key;
      current.current = visit.domain;
      updateDomain(visit.domain);
      currentView.current = visit.clusterView;
      updateClusterView(visit.clusterView);
      for (const restore of visit.restore) restore();
      changed();
    };
    window.addEventListener('popstate', travel);
    return () => { window.removeEventListener('popstate', travel); cancelScrollRestore(); };
  }, [capture, changed]);
  useEffect(() => { writeVisit({ session: session.current, key: visitKey.current }, domain, false, clusterView); }, [domain, clusterView]);
  const back = useCallback(() => { if (previous.current.length) window.history.back(); }, []);
  const forward = useCallback(() => { if (next.current.length) window.history.forward(); }, []);
  const register = useCallback((scope: Domain, id: string, read: Capture) => {
    scope = canonicalDomain(scope);
    const entries = captures.current.get(scope) ?? new Map<string, Capture>();
    entries.set(id, read); captures.current.set(scope, entries);
    return () => { if (entries.get(id) === read) entries.delete(id); };
  }, []);
  return <DomainContext.Provider value={{ domain, setDomain, clusterView, setClusterView, checkpoint, back, forward,
    canBack: previous.current.length > 0, canForward: next.current.length > 0, returning, register }}>
    {children}
  </DomainContext.Provider>;
}

// Capture only bounded query state and identities, never resource payloads.
export function useNavigationSnapshot(scope: Domain, id: string, capture: Capture) {
  const { register } = useDomain();
  const latest = useRef(capture); latest.current = capture;
  useEffect(() => register(scope, id, () => latest.current()), [register, scope, id]);
}

export function useDomain() {
  const context = useContext(DomainContext);
  if (context === undefined) throw new Error('useDomain must be used within a DomainProvider');
  return context;
}
