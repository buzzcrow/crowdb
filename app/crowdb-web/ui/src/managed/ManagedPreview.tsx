import { useEffect, useState } from 'react';

interface ServiceStatus {
  pid: number | null;
  generation: number;
  healthy: boolean;
  restart_attempts: number;
}

interface ServiceView {
  kind: string;
  instance_id: string;
  endpoint: string;
  monitor: ServiceStatus | null;
}

interface ManagedSnapshot {
  source: string;
  racks: Array<{ id: number; status: number; node_ids: number[] }>;
  nodes: Array<{ id: number; rack_id: number; status: number }>;
  disk_groups: Array<{ dg_id: number; node_id: number }>;
  disks: Array<{ disk_group_id: number; disk_id: unknown }>;
  stores: Array<{ store_id: number; node_ids: number[] }>;
  groups: Array<{ store_id: number; group_id: number }>;
  replicas: Array<{ store_id: number; group_id: number; replica_id: number }>;
  services: ServiceView[];
  monitor: {
    phase: string;
    revision: number;
    updated_at_ms: number;
    services: Record<string, ServiceStatus>;
  };
}

const reasonLabel: Record<string, string> = {
  group0_unavailable: 'Group 0 is unavailable or its topology is incomplete.',
  monitor_unavailable: 'The monitor status is missing or stale.',
};

export function ManagedPreview({ apiPrefix }: { apiPrefix: string }) {
  const [snapshot, setSnapshot] = useState<ManagedSnapshot | null>(null);
  const [reason, setReason] = useState<string | null>(null);

  useEffect(() => {
    let disposed = false;
    let timer: ReturnType<typeof setTimeout> | undefined;
    let controller: AbortController | undefined;
    const refresh = async () => {
      controller = new AbortController();
      try {
        const response = await fetch(`${apiPrefix}/preview`, {
          cache: 'no-store',
          signal: controller.signal,
        });
        const body = await response.json();
        if (!response.ok || body.source !== 'group0') {
          throw new Error(body.reason || 'group0_unavailable');
        }
        if (!disposed) {
          setSnapshot(body as ManagedSnapshot);
          setReason(null);
        }
      } catch (error) {
        if (!disposed) {
          setSnapshot(null);
          setReason(error instanceof Error ? error.message : 'group0_unavailable');
        }
      } finally {
        if (!disposed) timer = setTimeout(refresh, 3000);
      }
    };
    void refresh();
    return () => {
      disposed = true;
      if (timer) clearTimeout(timer);
      controller?.abort();
    };
  }, [apiPrefix]);

  return (
    <main className="tw-min-h-full tw-bg-bg tw-p-6 tw-text-text" data-testid="managed-preview">
      <div className="tw-mx-auto tw-max-w-6xl tw-space-y-6">
        <header className="tw-flex tw-flex-wrap tw-items-start tw-justify-between tw-gap-4">
          <div>
            <h1 className="tw-text-2xl tw-font-semibold">CROWDB Single-Node Preview</h1>
            <p className="tw-mt-1 tw-text-sm tw-text-muted">Live topology from Group 0 and process state from crowdb-monitor.</p>
          </div>
          <div className="tw-flex tw-gap-2 tw-text-xs">
            <span className="tw-rounded tw-border tw-border-border tw-px-3 tw-py-1" data-testid="managed-source">Source: Group 0</span>
            <span className="tw-rounded tw-border tw-border-border tw-px-3 tw-py-1" data-testid="managed-readonly">Hardware topology is read-only</span>
          </div>
        </header>

        {!snapshot && (
          <div role="alert" className="tw-rounded tw-border tw-border-border tw-bg-panel tw-p-5" data-testid="managed-unavailable">
            <h2 className="tw-font-semibold">Live status unavailable</h2>
            <p className="tw-mt-2 tw-text-sm tw-text-muted">{reason ? (reasonLabel[reason] || 'The live authority cannot be reached.') : 'Loading live status…'}</p>
            <p className="tw-mt-2 tw-text-xs tw-text-muted">No cached topology is displayed while the source is unavailable.</p>
          </div>
        )}

        {snapshot && (
          <>
            <section aria-label="Preview summary" className="tw-grid tw-grid-cols-2 tw-gap-3 md:tw-grid-cols-4">
              {([
                ['Racks', snapshot.racks.length],
                ['Nodes', snapshot.nodes.length],
                ['Disks', snapshot.disks.length],
                ['Stores', snapshot.stores.length],
              ] as const).map(([label, count]) => (
                <div key={label} className="tw-rounded tw-border tw-border-border tw-bg-panel tw-p-4">
                  <div className="tw-text-sm tw-text-muted">{label}</div>
                  <div className="tw-mt-2 tw-text-2xl tw-font-semibold">{count}</div>
                </div>
              ))}
            </section>

            <section className="tw-rounded tw-border tw-border-border tw-bg-panel tw-p-5" aria-label="Monitor status">
              <h2 className="tw-text-lg tw-font-semibold">Monitor status</h2>
              <p className="tw-mt-2 tw-text-sm tw-text-muted" data-testid="managed-monitor-phase">Phase: {snapshot.monitor.phase} · revision {snapshot.monitor.revision}</p>
              <div className="tw-mt-4 tw-grid tw-gap-2 md:tw-grid-cols-2">
                {Object.entries(snapshot.monitor.services).map(([name, service]) => (
                  <div key={name} className="tw-rounded tw-border tw-border-border tw-p-3 tw-text-sm">
                    <span className="tw-font-medium">{name}</span>
                    <span className="tw-ml-2 tw-text-muted">PID {service.pid ?? '—'} · generation {service.generation} · restarts {service.restart_attempts} · {service.healthy ? 'healthy' : 'unhealthy'}</span>
                  </div>
                ))}
              </div>
            </section>

            <section className="tw-rounded tw-border tw-border-border tw-bg-panel tw-p-5" aria-label="Group 0 topology">
              <h2 className="tw-text-lg tw-font-semibold">Group 0 topology</h2>
              <p className="tw-mt-2 tw-text-sm tw-text-muted">{snapshot.groups.length} groups · {snapshot.replicas.length} replicas · {snapshot.disk_groups.length} disk groups</p>
              <div className="tw-mt-4 tw-grid tw-gap-4 md:tw-grid-cols-2">
                <div>
                  <h3 className="tw-font-medium">Nodes</h3>
                  <ul className="tw-mt-2 tw-space-y-1 tw-text-sm tw-text-muted">
                    {snapshot.nodes.map((node) => <li key={node.id}>Node {node.id} · rack {node.rack_id} · status {node.status}</li>)}
                  </ul>
                </div>
                <div>
                  <h3 className="tw-font-medium">Stores</h3>
                  <ul className="tw-mt-2 tw-space-y-1 tw-text-sm tw-text-muted">
                    {snapshot.stores.map((store) => <li key={store.store_id}>Store {store.store_id} · nodes {store.node_ids.join(', ')}</li>)}
                  </ul>
                </div>
              </div>
            </section>

            <section className="tw-rounded tw-border tw-border-border tw-bg-panel tw-p-5" aria-label="Group 0 services">
              <h2 className="tw-text-lg tw-font-semibold">Group 0 services</h2>
              <ul className="tw-mt-3 tw-space-y-2 tw-text-sm">
                {snapshot.services.map((service) => (
                  <li key={`${service.kind}-${service.instance_id}`} className="tw-rounded tw-border tw-border-border tw-p-3">
                    <span className="tw-font-medium">{service.kind} #{service.instance_id}</span>
                    <span className="tw-ml-2 tw-text-muted">{service.endpoint}</span>
                    <div className="tw-mt-1 tw-text-xs tw-text-muted">
                      {service.monitor
                        ? `Monitor PID ${service.monitor.pid ?? '—'} · generation ${service.monitor.generation} · restarts ${service.monitor.restart_attempts}`
                        : 'Monitor mapping unavailable'}
                    </div>
                  </li>
                ))}
              </ul>
            </section>
          </>
        )}
      </div>
    </main>
  );
}
