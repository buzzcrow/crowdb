import { Database, HardDrive, Layers, Network, Package, type LucideIcon } from 'lucide-react';
import { Domain } from '../types';

/**
 * Shared contract for the header domain navigation.
 *
 * Compatibility rules:
 * - `domain` must remain the corresponding Domain enum value; it drives routing.
 * - `id` is a stable DOM/test identifier (`domain-${id}`), so do not rename it
 *   without updating deep links and UI tests.
 * - `label` and `description` are presentation text; `docs` is the durable
 *   documentation entry point for the tab.
 * Keep this list as the single source of truth; Header only renders it.
 * Console documentation is partitioned under /docs/manual/console/; future CLI
 * pages should live beside this UI section under /docs/manual/console/cli/.
 * The URLs map to pages in the separate crowdb-web site. When that site
 * renames a page, update this registry in the same change and verify the
 * target under crowdb-web/site/docs or crowdb-web/site/demo.
 */
export interface DomainTab {
  domain: Domain;
  id: string;
  label: string;
  description: string;
  docs: string;
  Icon: LucideIcon;
}

export const domainTabs: readonly DomainTab[] = [
  { domain: Domain.Cluster, id: 'cluster', label: 'Cluster', description: 'Nodes, services, and physical topology', docs: 'https://crowdb.dev/docs/manual/console/ui/tab/cluster/', Icon: Network },
  { domain: Domain.KV, id: 'kv', label: 'PaxosKV', description: 'Consensus groups and key-value data', docs: 'https://crowdb.dev/docs/manual/console/ui/tab/kv/', Icon: Database },
  { domain: Domain.Capacity, id: 'capacity', label: 'Capacity', description: 'Disk groups, disks, and hardware health', docs: 'https://crowdb.dev/docs/manual/console/ui/tab/capacity/', Icon: HardDrive },
  { domain: Domain.Chunk, id: 'chunk', label: 'Chunk', description: 'User-data chunks and placement', docs: 'https://crowdb.dev/docs/manual/console/ui/tab/chunk/', Icon: Package },
  { domain: Domain.ChunkKV, id: 'chunk-kv', label: 'ChunkKV', description: 'Chunk partition catalog and serving layout', docs: 'https://crowdb.dev/docs/manual/console/ui/tab/chunkkv/', Icon: Database },
  { domain: Domain.Iceberg, id: 'iceberg', label: 'Iceberg', description: 'Tables, snapshots, manifests, and files', docs: 'https://crowdb.dev/docs/manual/console/ui/tab/iceberg/', Icon: Layers },
  { domain: Domain.S3, id: 's3', label: 'S3', description: 'Buckets, objects, and multipart uploads', docs: 'https://crowdb.dev/docs/manual/console/ui/tab/s3/', Icon: Package },
];
