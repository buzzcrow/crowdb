// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

import { useEffect, useRef, useState } from 'react';
import { Dialog } from '../Dialog';
import { useToast } from '../../contexts/ToastContext';
import { initCluster } from '../../api';
import { Node, CrowdbKVServerView } from '../../types';
import { isCrowdbKVServerAvailable } from '../../data/crowdbKvServers';

export interface InitClusterDialogProps {
  isOpen: boolean;
  onClose: () => void;
  nodes: Node[];
  servers?: CrowdbKVServerView[];
  preparedNodes?: boolean;
  defaultNodeIds?: number[];
  onSuccess?: () => void | Promise<void>;
}

/**
 * Initialize system Group 0 and ordinary data Group 1 in Store 0.
 */
export function InitClusterDialog({
  isOpen,
  onClose,
  nodes,
  servers = [],
  preparedNodes = false,
  defaultNodeIds = [],
  onSuccess,
}: InitClusterDialogProps) {
  const availableNodes = preparedNodes ? nodes : nodes.filter((node) =>
    servers.some((server) => server.node_id === node.id && isCrowdbKVServerAvailable(server)),
  );
  const defaultSelectedNodeIds = defaultNodeIds.filter((id) => availableNodes.some((n) => n.id === id));
  const [selectedNodeIds, setSelectedNodeIds] = useState<number[]>(
    defaultSelectedNodeIds.length > 0 ? defaultSelectedNodeIds : availableNodes.map((n) => n.id),
  );
  const [isLoading, setIsLoading] = useState(false);
  const [submitError, setSubmitError] = useState('');
  const wasOpenRef = useRef(false);
  const { success } = useToast();

  const valid = selectedNodeIds.length > 0;

  const reset = () => {
    setSubmitError('');
    setSelectedNodeIds(
      defaultSelectedNodeIds.length > 0 ? defaultSelectedNodeIds : availableNodes.map((n) => n.id),
    );
  };

  useEffect(() => {
    if (isOpen && !wasOpenRef.current) reset();
    wasOpenRef.current = isOpen;
  }, [defaultSelectedNodeIds, isOpen]);

  const handleSubmit = async () => {
    if (!valid) return;
    setIsLoading(true);
    setSubmitError('');
    try {
      await initCluster({ nodes: selectedNodeIds, create_data_group: true });
      success('Cluster initialized successfully');
      reset();
      onClose();
      await onSuccess?.();
    } catch (err) {
      const message = err instanceof Error ? err.message : 'Failed to initialize cluster';
      setSubmitError(message);
    } finally {
      setIsLoading(false);
    }
  };

  const handleClose = () => {
    reset();
    onClose();
  };

  const toggleNode = (nodeId: number) => {
    setSelectedNodeIds((prev) =>
      prev.includes(nodeId) ? prev.filter((id) => id !== nodeId) : [...prev, nodeId],
    );
  };

  return (
    <Dialog
      isOpen={isOpen}
      onClose={handleClose}
      title="Initialize Cluster"
      description="Create system Group 0 and data Group 1 in Store 0 on the selected nodes. Group 1 is the initial data destination for DiskGroups."
      confirmLabel="Initialize Cluster"
      onConfirm={handleSubmit}
      confirmDisabled={!valid || isLoading}
      confirmLoading={isLoading}
    >
      <div className="tw-space-y-4">
        {submitError && <p role="alert" className="tw-text-sm tw-text-failed">{submitError}</p>}
        <div className="tw-space-y-2">
          <label className="tw-text-xs tw-font-medium tw-text-text">
            CrowDB Storage Nodes (select at least one)
          </label>
          {availableNodes.length === 0 ? (
            <div className="tw-text-sm tw-text-muted">
              No reachable CrowDB Storage nodes available. Deploy a CrowDB Storage server and wait until it is Running and Up.
            </div>
          ) : (
            <div className="tw-max-h-40 tw-overflow-y-auto tw-border tw-border-border tw-rounded-md tw-p-2 tw-space-y-1">
              {availableNodes.map((node) => (
                <label
                  key={node.id}
                  className="tw-flex tw-items-center tw-gap-2 tw-p-2 tw-rounded tw-cursor-pointer hover:tw-bg-bg"
                >
                  <input
                    type="checkbox"
                    checked={selectedNodeIds.includes(node.id)}
                    onChange={() => toggleNode(node.id)}
                    className="tw-h-4 tw-w-4 tw-rounded tw-border-border tw-text-accent focus:tw-ring-accent"
                  />
                  <span className="tw-text-sm tw-text-text">
                    {node.id}
                    <span className="tw-text-xs tw-text-muted tw-ml-1">({node.host})</span>
                  </span>
                </label>
              ))}
            </div>
          )}
        </div>
      </div>
    </Dialog>
  );
}
