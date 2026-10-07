// Copyright 2026-present Gian <crow.db@outlook.com>

import { RefreshCw, RotateCcw, Info } from 'lucide-react';
import { useDomain } from '../contexts/DomainContext';
import { Domain } from '../types';
import { cn } from '../utils/cn';
import { domainTabs } from './domainTabs';

export type ClusterHealth = 'Healthy' | 'Degraded' | 'Failed' | 'Unknown';

export type CenterPanelMode = 'topology' | 'kv' | 'capacity' | 'chunk';

interface HeaderProps {
  clusterHealth: ClusterHealth;
  onRefresh: () => void;
  refreshing?: boolean;
  onShowTopology?: () => void;
  onShowCapacity?: () => void;
  onResetCluster?: () => void;
}

const healthPill: Record<ClusterHealth, string> = {
  Healthy: 'tw-bg-healthy/15 tw-text-healthy tw-border-healthy/30',
  Degraded: 'tw-bg-degraded/15 tw-text-degraded tw-border-degraded/30',
  Failed: 'tw-bg-failed/15 tw-text-failed tw-border-failed/30',
  Unknown: 'tw-bg-unknown/15 tw-text-muted tw-border-unknown/30',
};

const healthGlyph: Record<ClusterHealth, string> = {
  Healthy: '✓',
  Degraded: '!',
  Failed: '✕',
  Unknown: '?',
};

export function Header({
  clusterHealth,
  onRefresh,
  refreshing,
  onShowTopology,
  onShowCapacity,
  onResetCluster,
}: HeaderProps) {
  const { domain, setDomain, back, forward, canBack, canForward } = useDomain();

  return (
    <header className="tw-fixed tw-top-0 tw-left-0 tw-right-0 tw-z-40 tw-h-14 tw-bg-panel tw-border-b tw-border-border tw-flex tw-items-center tw-gap-4 tw-px-4">
      {/* The website crow mark uses the console's fixed dark palette. */}
      <div className="tw-flex tw-shrink-0 tw-items-center tw-gap-2.5 tw-whitespace-nowrap" aria-label="CrowDB Console">
        <img src={new URL('../assets/crowdb-mark.svg', import.meta.url).href} alt="" width={30} height={30} />
        <span className="tw-text-[17px] tw-font-bold tw-tracking-wide">Crow<span className="tw-text-brand">DB</span></span>
        <span className="tw-border-l tw-border-border tw-pl-3 tw-text-sm tw-text-muted">Console</span>
      </div>
      <nav aria-label="Navigation history" className="tw-flex tw-gap-1">
        <button aria-label="Back" title="Back" disabled={!canBack} onClick={back} className="tw-rounded tw-border tw-border-border tw-px-2 tw-py-1 disabled:tw-opacity-40">←</button>
        <button aria-label="Forward" title="Forward" disabled={!canForward} onClick={forward} className="tw-rounded tw-border tw-border-border tw-px-2 tw-py-1 disabled:tw-opacity-40">→</button>
      </nav>
      {/* Health pill */}
      <span
        className={cn(
          'tw-inline-flex tw-items-center tw-gap-1.5 tw-px-2.5 tw-py-1 tw-rounded-full tw-text-xs tw-font-medium tw-border',
          healthPill[clusterHealth],
        )}
        title={`Cluster health: ${clusterHealth}`}
      >
        <span aria-hidden>{healthGlyph[clusterHealth]}</span>
        {clusterHealth}
      </span>

      <nav aria-label="Console domains" className="console-domains tw-flex tw-items-center tw-self-stretch">
        {domainTabs.map(({ domain: target, id, label, description, docs, Icon }) => (
          <div key={id} className="tw-flex tw-items-center tw-self-stretch">
            <button data-testid={`domain-${id}`} onClick={() => { setDomain(target); if (target === Domain.Cluster || target === Domain.KV) onShowTopology?.(); if (target === Domain.Capacity) onShowCapacity?.(); }}
              className={cn('tw-flex tw-items-center tw-gap-1.5 tw-px-3 tw-py-1.5 tw-text-xs tw-transition-colors', domain === target ? 'tw-bg-accent/15 tw-text-accent' : 'tw-text-muted hover:tw-bg-bg')}
              aria-pressed={domain === target} title={description}>
              <Icon className="tw-h-3.5 tw-w-3.5" /> {label}
            </button>
            <a href={docs} target="_blank" rel="noreferrer" aria-label={`${label} documentation`} title={`${description} · Open documentation`}
              onClick={event => event.stopPropagation()} className="tw-p-1 tw-text-muted hover:tw-text-accent">
              <Info className="tw-h-3.5 tw-w-3.5" />
            </a>
          </div>
        ))}
      </nav>

      <div className="tw-flex-1" />

      {onResetCluster && domain === Domain.Cluster && (
        <button
          onClick={onResetCluster}
          className="tw-flex tw-items-center tw-gap-1.5 tw-px-2.5 tw-py-1.5 tw-rounded-md tw-text-xs tw-border tw-border-failed/30 tw-text-failed hover:tw-bg-failed/10 tw-transition-colors"
          title="Reset entire cluster: tear down all stores, groups, servers, nodes, and racks"
        >
          <RotateCcw className="tw-h-3.5 tw-w-3.5" /> Reset
        </button>
      )}

      <button
        onClick={onRefresh}
        className="tw-p-2 tw-rounded-md tw-text-muted hover:tw-text-text hover:tw-bg-bg tw-transition-colors"
        aria-label="Refresh"
        title="Refresh now"
      >
        <RefreshCw className={cn('tw-h-4 tw-w-4', refreshing && 'tw-animate-spin')} />
      </button>
    </header>
  );
}
