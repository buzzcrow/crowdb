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
  const [monitor, setMonitor] = useState<ManagedSnapshot['monitor'] | null>(null);
  const [managementToken, setManagementToken] = useState('');
  const [storeId, setStoreId] = useState('');
  const [groupStoreId, setGroupStoreId] = useState('');
  const [groupId, setGroupId] = useState('');
  const [replicaId, setReplicaId] = useState('1');
  const [nodeId, setNodeId] = useState('');
  const [groupNodeId, setGroupNodeId] = useState('');
  const [replicaGroup, setReplicaGroup] = useState('');
  const [replicaNodeId, setReplicaNodeId] = useState('');
  const [newReplicaId, setNewReplicaId] = useState('');
  const [writeError, setWriteError] = useState<string | null>(null);
  const [writeBusy, setWriteBusy] = useState(false);
  const [refreshRevision, setRefreshRevision] = useState(0);

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
        if (!disposed) {
          const available = response.ok && body.source === 'group0';
          setSnapshot(available ? body as ManagedSnapshot : null);
          setMonitor(body.source === 'group0' ? body.monitor ?? null : null);
          setReason(available ? null : body.reason || 'group0_unavailable');
        }
      } catch (error) {
        if (!disposed) {
          setSnapshot(null);
          setMonitor(null);
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
  }, [apiPrefix, refreshRevision]);

  const write = async (path: string, method: 'POST' | 'DELETE', body?: object) => {
    setWriteBusy(true);
    setWriteError(null);
    try {
      const response = await fetch(`${apiPrefix}${path}`, {
        method,
        headers: {
          Authorization: `Bearer ${managementToken}`,
          ...(body ? { 'Content-Type': 'application/json' } : {}),
        },
        ...(body ? { body: JSON.stringify(body) } : {}),
      });
      if (!response.ok) {
        const error = await response.json().catch(() => null);
        throw new Error(error?.error || `Request failed (${response.status})`);
      }
      setRefreshRevision((revision) => revision + 1);
    } catch (error) {
      setWriteError(error instanceof Error ? error.message : 'Request failed');
    } finally {
      setWriteBusy(false);
    }
  };

  return (
    <main className="tw-min-h-full tw-bg-bg tw-p-6 tw-text-text" data-testid="managed-preview">
      <div className="tw-mx-auto tw-max-w-6xl tw-space-y-6">
        <header className="tw-flex tw-flex-wrap tw-items-start tw-justify-between tw-gap-4">
          <div>
            <h1 className="tw-text-2xl tw-font-semibold">CROWDB Single-Node Container</h1>
            <p className="tw-mt-1 tw-text-sm tw-text-muted">Non-production preview · one host, no fault tolerance or upgrade guarantee. Space may remain unreclaimed.</p>
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

        {monitor && (
            <section className="tw-rounded tw-border tw-border-border tw-bg-panel tw-p-5" aria-label="Monitor status">
              <h2 className="tw-text-lg tw-font-semibold">Monitor status</h2>
              <p className="tw-mt-2 tw-text-sm tw-text-muted" data-testid="managed-monitor-phase">Phase: {monitor.phase} · revision {monitor.revision}</p>
              <div className="tw-mt-4 tw-grid tw-gap-2 md:tw-grid-cols-2">
                {Object.entries(monitor.services).map(([name, service]) => (
                  <div key={name} data-testid={`managed-process-${name}`} className="tw-rounded tw-border tw-border-border tw-p-3 tw-text-sm">
                    <span className="tw-font-medium">{name}</span>
                    <span className="tw-ml-2 tw-text-muted">PID {service.pid ?? '—'} · generation {service.generation} · restarts {service.restart_attempts} · {service.healthy ? 'healthy' : 'unhealthy'}</span>
                  </div>
                ))}
              </div>
            </section>
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

            <section className="tw-rounded tw-border tw-border-border tw-bg-panel tw-p-5" aria-label="Logical topology management">
              <h2 className="tw-text-lg tw-font-semibold">Logical topology management</h2>
              <p className="tw-mt-2 tw-text-sm tw-text-muted">Store, group, and replica changes use Group 0. Hardware and process controls remain disabled.</p>
              <label className="tw-mt-4 tw-block tw-text-sm">
                Management token
                <input
                  className="tw-ml-2 tw-rounded tw-border tw-border-border tw-bg-bg tw-px-2 tw-py-1"
                  type="password"
                  autoComplete="off"
                  value={managementToken}
                  onChange={(event) => setManagementToken(event.target.value)}
                />
              </label>
              {writeError && <p role="alert" className="tw-mt-2 tw-text-sm">{writeError}</p>}
              <form
                className="tw-mt-4 tw-flex tw-flex-wrap tw-items-end tw-gap-2"
                onSubmit={(event) => {
                  event.preventDefault();
                  void write('/stores', 'POST', { store_id: Number(storeId), nodes: [Number(nodeId)] });
                }}
              >
                <label className="tw-text-sm">Store ID <input className="tw-ml-1 tw-w-20 tw-rounded tw-border tw-border-border tw-bg-bg tw-px-2 tw-py-1" type="number" min="1" required value={storeId} onChange={(event) => setStoreId(event.target.value)} /></label>
                <label className="tw-text-sm">Node <select aria-label="Store node" className="tw-ml-1 tw-rounded tw-border tw-border-border tw-bg-bg tw-px-2 tw-py-1" required value={nodeId} onChange={(event) => setNodeId(event.target.value)}><option value="">Select node</option>{snapshot.nodes.map((node) => <option key={node.id} value={node.id}>{node.id}</option>)}</select></label>
                <button className="tw-rounded tw-border tw-border-border tw-px-3 tw-py-1 tw-text-sm" type="submit" disabled={!managementToken || writeBusy}>Create store</button>
              </form>
              <form
                className="tw-mt-3 tw-flex tw-flex-wrap tw-items-end tw-gap-2"
                onSubmit={(event) => {
                  event.preventDefault();
                  void write(`/stores/${groupStoreId}/groups`, 'POST', { group_id: Number(groupId), replica_id: Number(replicaId), nodes: [Number(groupNodeId)] });
                }}
              >
                <label className="tw-text-sm">Store <select aria-label="Group store" className="tw-ml-1 tw-rounded tw-border tw-border-border tw-bg-bg tw-px-2 tw-py-1" required value={groupStoreId} onChange={(event) => setGroupStoreId(event.target.value)}><option value="">Select store</option>{snapshot.stores.map((store) => <option key={store.store_id} value={store.store_id}>{store.store_id}</option>)}</select></label>
                <label className="tw-text-sm">Group ID <input className="tw-ml-1 tw-w-20 tw-rounded tw-border tw-border-border tw-bg-bg tw-px-2 tw-py-1" type="number" min="0" required value={groupId} onChange={(event) => setGroupId(event.target.value)} /></label>
                <label className="tw-text-sm">Replica ID <input className="tw-ml-1 tw-w-20 tw-rounded tw-border tw-border-border tw-bg-bg tw-px-2 tw-py-1" type="number" min="1" required value={replicaId} onChange={(event) => setReplicaId(event.target.value)} /></label>
                <label className="tw-text-sm">Group node <select aria-label="Group node" className="tw-ml-1 tw-rounded tw-border tw-border-border tw-bg-bg tw-px-2 tw-py-1" required value={groupNodeId} onChange={(event) => setGroupNodeId(event.target.value)}><option value="">Select node</option>{snapshot.nodes.map((node) => <option key={node.id} value={node.id}>{node.id}</option>)}</select></label>
                <button className="tw-rounded tw-border tw-border-border tw-px-3 tw-py-1 tw-text-sm" type="submit" disabled={!managementToken || writeBusy}>Create group</button>
              </form>
              <form
                className="tw-mt-3 tw-flex tw-flex-wrap tw-items-end tw-gap-2"
                onSubmit={(event) => {
                  event.preventDefault();
                  const [selectedStore, selectedGroup] = replicaGroup.split('/');
                  void write(`/stores/${selectedStore}/groups/${selectedGroup}/replicas`, 'POST', {
                    node_id: Number(replicaNodeId),
                    ...(newReplicaId ? { replica_id: Number(newReplicaId) } : {}),
                  });
                }}
              >
                <label className="tw-text-sm">Group <select aria-label="Replica group" className="tw-ml-1 tw-rounded tw-border tw-border-border tw-bg-bg tw-px-2 tw-py-1" required value={replicaGroup} onChange={(event) => setReplicaGroup(event.target.value)}><option value="">Select group</option>{snapshot.groups.filter((group) => group.store_id !== 0 || group.group_id !== 0).map((group) => <option key={`${group.store_id}/${group.group_id}`} value={`${group.store_id}/${group.group_id}`}>{group.store_id}/{group.group_id}</option>)}</select></label>
                <label className="tw-text-sm">Replica node <select aria-label="Replica node" className="tw-ml-1 tw-rounded tw-border tw-border-border tw-bg-bg tw-px-2 tw-py-1" required value={replicaNodeId} onChange={(event) => setReplicaNodeId(event.target.value)}><option value="">Select node</option>{snapshot.nodes.map((node) => <option key={node.id} value={node.id}>{node.id}</option>)}</select></label>
                <label className="tw-text-sm">New replica ID <input className="tw-ml-1 tw-w-20 tw-rounded tw-border tw-border-border tw-bg-bg tw-px-2 tw-py-1" type="number" min="1" value={newReplicaId} onChange={(event) => setNewReplicaId(event.target.value)} /></label>
                <button className="tw-rounded tw-border tw-border-border tw-px-3 tw-py-1 tw-text-sm" type="submit" disabled={!managementToken || writeBusy}>Add replica</button>
              </form>
              <ul className="tw-mt-4 tw-space-y-2 tw-text-sm" aria-label="Logical stores">
                {snapshot.stores.filter((store) => store.store_id !== 0).map((store) => (
                  <li key={store.store_id} className="tw-flex tw-flex-wrap tw-items-center tw-gap-2">
                    <span>Store {store.store_id} · nodes {store.node_ids.join(', ')}</span>
                    <button className="tw-rounded tw-border tw-border-border tw-px-2 tw-py-1" type="button" disabled={!managementToken || writeBusy} onClick={() => { if (window.confirm(`Delete store ${store.store_id}?`)) void write(`/stores/${store.store_id}`, 'DELETE'); }}>Delete store</button>
                  </li>
                ))}
              </ul>
              <ul className="tw-mt-4 tw-space-y-2 tw-text-sm" aria-label="Logical groups">
                {snapshot.groups.map((group) => (
                  <li key={`${group.store_id}-${group.group_id}`} className="tw-flex tw-flex-wrap tw-items-center tw-gap-2">
                    <span>Store {group.store_id} · group {group.group_id}</span>
                    {!(group.store_id === 0 && group.group_id === 0) && (
                      <button className="tw-rounded tw-border tw-border-border tw-px-2 tw-py-1" type="button" disabled={!managementToken || writeBusy} onClick={() => { if (window.confirm(`Delete group ${group.store_id}/${group.group_id}?`)) void write(`/stores/${group.store_id}/groups/${group.group_id}`, 'DELETE'); }}>Delete group</button>
                    )}
                  </li>
                ))}
              </ul>
              <ul className="tw-mt-4 tw-space-y-2 tw-text-sm" aria-label="Logical replicas">
                {snapshot.replicas.filter((replica) => replica.store_id !== 0 || replica.group_id !== 0).map((replica) => (
                  <li key={`${replica.store_id}-${replica.group_id}-${replica.replica_id}`} className="tw-flex tw-flex-wrap tw-items-center tw-gap-2">
                    <span>Store {replica.store_id} · group {replica.group_id} · replica {replica.replica_id}</span>
                    <button className="tw-rounded tw-border tw-border-border tw-px-2 tw-py-1" type="button" disabled={!managementToken || writeBusy} onClick={() => { if (window.confirm(`Delete replica ${replica.replica_id}?`)) void write(`/stores/${replica.store_id}/groups/${replica.group_id}/replicas/${replica.replica_id}`, 'DELETE'); }}>Delete replica</button>
                  </li>
                ))}
              </ul>
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
