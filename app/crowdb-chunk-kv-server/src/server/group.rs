// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Partition-local grouped point execution.

use super::{
    not_my_range_failure, rpc_failure, BatchMutationRequest, BatchMutationResponse, BatchMutationResult,
    ChunkKvRpcErrorCode, ChunkKvService, MultiGetRequest, MultiGetResponse, OperationResult, PointOperation,
    PointRequest, RequestRouting,
};

mod authority;

impl ChunkKvService {
    /// Executes a fully range-validated partition-local multi-get.
    pub async fn handle_multi_get(
        &self,
        request: MultiGetRequest,
        now_wall_ms: u64,
        now_monotonic_ms: u64,
    ) -> MultiGetResponse {
        let catalog = self.catalog.load_full();
        let Some(range) = self.logical_range_for_request(request.routing.partition_id, &catalog) else {
            return MultiGetResponse {
                map_revision: catalog.generation,
                result: Err(not_my_range_failure(catalog.generation, None)),
            };
        };
        if request.validate_for_range(&range).is_err() {
            return MultiGetResponse {
                map_revision: catalog.generation,
                result: Err(rpc_failure(
                    ChunkKvRpcErrorCode::InvalidRequest,
                    "multi-get group contains an out-of-range key",
                )),
            };
        }
        let group = match self.capture_point_group(
            &catalog,
            request.keys.iter().map(Vec::as_slice),
            now_wall_ms,
            now_monotonic_ms,
        ) {
            Ok(group) => group,
            Err(error) => {
                return MultiGetResponse {
                    map_revision: catalog.generation,
                    result: Err(error),
                }
            }
        };
        let mut values = Vec::with_capacity(request.keys.len());
        for key in request.keys {
            let response = self
                .execute_group_point(
                    &group,
                    PointRequest {
                        routing: request.routing.clone(),
                        operation: PointOperation::Get { key },
                    },
                )
                .await;
            match response.result {
                Ok(OperationResult::Value(value)) => values.push(value),
                Ok(_) => {
                    return MultiGetResponse {
                        map_revision: response.map_revision,
                        result: Err(rpc_failure(
                            ChunkKvRpcErrorCode::Internal,
                            "multi-get produced a non-value result",
                        )),
                    };
                }
                Err(error) => {
                    return MultiGetResponse {
                        map_revision: response.map_revision,
                        result: Err(error),
                    };
                }
            }
        }
        MultiGetResponse {
            map_revision: catalog.generation,
            result: Ok(values),
        }
    }

    /// Executes a fully range-validated mutation group in input order.
    pub async fn handle_batch_mutation(
        &self,
        request: BatchMutationRequest,
        now_wall_ms: u64,
        now_monotonic_ms: u64,
    ) -> BatchMutationResponse {
        let catalog = self.catalog.load_full();
        let Some(range) = self.logical_range_for_request(request.routing.partition_id, &catalog) else {
            return BatchMutationResponse {
                map_revision: catalog.generation,
                result: Err(not_my_range_failure(catalog.generation, None)),
            };
        };
        if request.validate_for_range(&range).is_err() {
            return BatchMutationResponse {
                map_revision: catalog.generation,
                result: Err(rpc_failure(
                    ChunkKvRpcErrorCode::InvalidRequest,
                    "batch group contains an invalid or out-of-range mutation",
                )),
            };
        }
        let group = match self.capture_point_group(
            &catalog,
            request.operations.iter().map(|item| item.operation.key()),
            now_wall_ms,
            now_monotonic_ms,
        ) {
            Ok(group) => group,
            Err(error) => {
                return BatchMutationResponse {
                    map_revision: catalog.generation,
                    result: Err(error),
                }
            }
        };
        let first_id = request.operations[0].request_id;
        let routing = RequestRouting {
            request_id: first_id,
            map_revision: request.routing.map_revision,
            partition_id: request.routing.partition_id,
            owner_epoch: request.routing.owner_epoch,
            min_journal_position: None,
            deadline_ms: request.routing.deadline_ms,
        };
        let mut results = Vec::with_capacity(request.operations.len());
        for item in request.operations {
            let response = self
                .execute_group_point(
                    &group,
                    PointRequest {
                        routing: RequestRouting {
                            request_id: item.request_id,
                            ..routing.clone()
                        },
                        operation: item.operation,
                    },
                )
                .await;
            results.push(BatchMutationResult {
                request_id: item.request_id,
                journal_position: response.journal_position,
                result: response.result,
            });
        }
        BatchMutationResponse {
            map_revision: catalog.generation,
            result: Ok(results),
        }
    }
}
