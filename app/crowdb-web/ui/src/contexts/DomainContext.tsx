// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

import { createContext, useContext, useState, useRef, useCallback, useEffect, ReactNode } from 'react';
import { Domain } from '../types';

type Capture = () => () => void;
interface Visit { domain: Domain; restore: Array<() => void> }
function captureScroll(): () => void {
  const positions = [...document.querySelectorAll<HTMLElement>('[data-navigation-scroll]')]
    .filter(element => element.getClientRects().length > 0).slice(0, 8)
    .map(element => ({ id: element.dataset.navigationScroll!, top: element.scrollTop, left: element.scrollLeft }));
  return () => requestAnimationFrame(() => {
    for (const position of positions) {
      const element = [...document.querySelectorAll<HTMLElement>('[data-navigation-scroll]')]
        .find(candidate => candidate.dataset.navigationScroll === position.id && candidate.getClientRects().length > 0);
      if (element) { element.scrollTop = position.top; element.scrollLeft = position.left; }
    }
  });
}
interface DomainContextType {
  domain: Domain;
  setDomain: (domain: Domain) => void;
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
  const [domain, updateDomain] = useState<Domain>(initialDomain ?? Domain.Cluster);
  const [returning, setReturning] = useState(false);
  const current = useRef(domain); current.current = domain;
  const captures = useRef(new Map<Domain, Map<string, Capture>>());
  const previous = useRef<Visit[]>([]);
  const next = useRef<Visit[]>([]);
  const sameEvent = useRef(false);
  const [, setVersion] = useState(0);
  const changed = useCallback(() => setVersion(value => value + 1), []);
  const capture = useCallback((): Visit => ({
    domain: current.current,
    restore: [...[...(captures.current.get(current.current)?.values() ?? [])].map(read => read()), captureScroll()],
  }), []);
  const checkpoint = useCallback(() => {
    // Tree selection and its page handler describe one synchronous navigation.
    if (sameEvent.current) return;
    sameEvent.current = true;
    queueMicrotask(() => { sameEvent.current = false; });
    setReturning(false);
    previous.current = [...previous.current, capture()].slice(-32);
    next.current = [];
    changed();
  }, [capture, changed]);
  const setDomain = useCallback((target: Domain) => {
    if (target === current.current) return;
    checkpoint();
    current.current = target;
    updateDomain(target);
  }, [checkpoint]);
  const travel = useCallback((source: Visit[], destination: Visit[]) => {
    const visit = source.pop();
    if (!visit) return;
    destination.push(capture());
    if (destination.length > 32) destination.shift();
    setReturning(true);
    current.current = visit.domain;
    updateDomain(visit.domain);
    for (const restore of visit.restore) restore();
    changed();
  }, [capture, changed]);
  const back = useCallback(() => travel(previous.current, next.current), [travel]);
  const forward = useCallback(() => travel(next.current, previous.current), [travel]);
  const register = useCallback((scope: Domain, id: string, read: Capture) => {
    const entries = captures.current.get(scope) ?? new Map<string, Capture>();
    entries.set(id, read); captures.current.set(scope, entries);
    return () => { if (entries.get(id) === read) entries.delete(id); };
  }, []);
  return <DomainContext.Provider value={{ domain, setDomain, checkpoint, back, forward,
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
