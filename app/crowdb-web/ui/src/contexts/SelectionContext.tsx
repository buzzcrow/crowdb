// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

import { createContext, useContext, useState, useCallback, useRef, ReactNode } from 'react';
import { Domain, type ServiceKind } from '../types';
import { canonicalDomain, useDomain, useNavigationSnapshot } from './DomainContext';

export type EntityType = 'Datacenter' | 'Rack' | 'Node' | 'Server' | 'Store' | 'Group' | 'Replica' | 'DiskGroup' | 'Disk' | 'Partition' | 'Iceberg' | 'S3' | 'Dataset';

/**
 * The single selected entity. `parentIds` carries the ancestor chain in
 * snake_case (`rack_id`, `store_id`, `group_id`, `node_id`) so API calls and
 * cross-jumps can resolve the full path.
 */
export interface SelectedEntity {
  allocationBlock?: { index: number; zone: number; state: string; unitBytes?: number; zoneOffset?: number; diskOffset?: number; stale?: boolean };
  type: EntityType;
  id: string;
  parentIds?: Record<string, string | number>;
  domain: Domain;
  name?: string;
  section?: 'services' | 'storage' | 'candidates' | 'discovery';
  /** Service flavor for `Server` entities: KV vs DiskDB. */
  serviceType?: ServiceKind;
}

interface SelectionContextType {
  selectedEntity: SelectedEntity | null;
  scopeEntity: SelectedEntity | null;
  scopeForDomain: (domain: Domain) => SelectedEntity | null;
  inspectEntity: (entity: SelectedEntity) => void;
  selectionForDomain: (domain: Domain) => SelectedEntity | null;
  selectEntity: (entity: SelectedEntity | null, record?: boolean) => void;
  clearSelection: () => void;
  isSelected: (entityId: string) => boolean;
}

const SelectionContext = createContext<SelectionContextType | undefined>(undefined);

export function SelectionProvider({ children }: { children: ReactNode }) {
  const { domain, checkpoint } = useDomain();
  const [scopes, setScopes] = useState<Partial<Record<Domain, SelectedEntity | null>>>({});
  const [navigationScopes, setNavigationScopes] = useState<Partial<Record<Domain, SelectedEntity | null>>>({});
  const scopeEntity = navigationScopes[domain] ?? null;
  const scopeForDomain = useCallback((scope: Domain) => navigationScopes[canonicalDomain(scope)] ?? null, [navigationScopes]);
  const latestNavigation = useRef(navigationScopes); latestNavigation.current = navigationScopes;
  const latestScopes = useRef(scopes); latestScopes.current = scopes;
  const selectedEntity = scopes[domain] ?? null;
  const selectionForDomain = useCallback((scope: Domain) => scopes[canonicalDomain(scope)] ?? null, [scopes]);

  useNavigationSnapshot(domain, 'selection', () => {
    const selection = scopes[domain] ?? null;
    const scope = navigationScopes[domain] ?? null;
    return () => {
      setScopes(previous => ({ ...previous, [domain]: selection }));
      setNavigationScopes(previous => ({ ...previous, [domain]: scope }));
    };
  });
  const selectEntity = useCallback((entity: SelectedEntity | null, record = true) => {
    entity = entity ? { ...entity, domain: canonicalDomain(entity.domain) } : null;
    const scope = entity?.domain ?? domain;
    if (record && (JSON.stringify(latestScopes.current[scope] ?? null) !== JSON.stringify(entity)
      || JSON.stringify(latestNavigation.current[scope] ?? null) !== JSON.stringify(entity))) checkpoint();
    setScopes(previous => ({ ...previous, [scope]: entity }));
    setNavigationScopes(previous => ({ ...previous, [scope]: entity }));
  }, [domain, checkpoint]);
  const inspectEntity = useCallback((entity: SelectedEntity) => {
    const scope = canonicalDomain(entity.domain);
    setScopes(previous => ({ ...previous, [scope]: { ...entity, domain: scope } }));
  }, []);

  const clearSelection = useCallback(() => setScopes(previous => ({ ...previous, [domain]: null })), [domain]);

  const isSelected = useCallback(
    (entityId: string) => selectedEntity?.id === entityId,
    [selectedEntity],
  );

  return (
    <SelectionContext.Provider value={{ selectedEntity, scopeEntity, scopeForDomain, inspectEntity, selectionForDomain, selectEntity, clearSelection, isSelected }}>
      {children}
    </SelectionContext.Provider>
  );
}

export function useSelection() {
  const context = useContext(SelectionContext);
  if (context === undefined) {
    throw new Error('useSelection must be used within a SelectionProvider');
  }
  return context;
}
