// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

import { createContext, useContext, useState, useCallback, useRef, ReactNode } from 'react';
import { Domain, type ServiceKind } from '../types';
import { useDomain, useNavigationSnapshot } from './DomainContext';

export type EntityType = 'Datacenter' | 'Rack' | 'Node' | 'Server' | 'Store' | 'Group' | 'Replica' | 'DiskGroup' | 'Disk' | 'Partition' | 'Iceberg' | 'S3';

/**
 * The single selected entity. `parentIds` carries the ancestor chain in
 * snake_case (`rack_id`, `store_id`, `group_id`, `node_id`) so API calls and
 * cross-jumps can resolve the full path.
 */
export interface SelectedEntity {
  type: EntityType;
  id: string;
  parentIds?: Record<string, string | number>;
  domain: Domain;
  name?: string;
  /** Service flavor for `Server` entities: KV vs DiskDB. */
  serviceType?: ServiceKind;
}

interface SelectionContextType {
  selectedEntity: SelectedEntity | null;
  selectionForDomain: (domain: Domain) => SelectedEntity | null;
  selectEntity: (entity: SelectedEntity | null, record?: boolean) => void;
  clearSelection: () => void;
  isSelected: (entityId: string) => boolean;
}

const SelectionContext = createContext<SelectionContextType | undefined>(undefined);

export function SelectionProvider({ children }: { children: ReactNode }) {
  const { domain, checkpoint } = useDomain();
  const [scopes, setScopes] = useState<Partial<Record<Domain, SelectedEntity | null>>>({});
  const latestScopes = useRef(scopes); latestScopes.current = scopes;
  const selectedEntity = scopes[domain] ?? null;
  const selectionForDomain = useCallback((scope: Domain) => scopes[scope] ?? null, [scopes]);

  useNavigationSnapshot(domain, 'selection', () => {
    const selection = scopes[domain] ?? null;
    return () => setScopes(previous => ({ ...previous, [domain]: selection }));
  });
  const selectEntity = useCallback((entity: SelectedEntity | null, record = true) => {
    const scope = entity?.domain ?? domain;
    if (record && JSON.stringify(latestScopes.current[scope] ?? null) !== JSON.stringify(entity)) checkpoint();
    setScopes(previous => ({ ...previous, [scope]: entity }));
  }, [domain, checkpoint]);

  const clearSelection = useCallback(() => setScopes(previous => ({ ...previous, [domain]: null })), [domain]);

  const isSelected = useCallback(
    (entityId: string) => selectedEntity?.id === entityId,
    [selectedEntity],
  );

  return (
    <SelectionContext.Provider value={{ selectedEntity, selectionForDomain, selectEntity, clearSelection, isSelected }}>
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
