use crowdb_access_iceberg::catalog::CatalogContext;
use crowdb_access_iceberg::file::{
    AssemblyProgress, FileIdentity, MultipartCompletion, MultipartLimits, MultipartPhase, MultipartSession,
    TableLocation,
};
use crowdb_access_iceberg::key::{CatalogId, FileId, OperationId, TableId};
use crowdb_access_iceberg::operation::PayloadReference;

pub fn session() -> MultipartSession {
    let table = TableLocation {
        catalog: CatalogId::random(),
        table: TableId::random(),
    };
    MultipartSession {
        context: CatalogContext {
            catalog: table.catalog,
            activation_epoch: 1,
        },
        upload: OperationId::random(),
        owner: FileIdentity {
            table,
            file: FileId::random(),
        },
        location: table.file("file").unwrap(),
        principal: [1; 32],
        revision: 1,
        created_ms: 100,
        expires_ms: 1100,
        limits: MultipartLimits {
            max_parts: 10,
            max_part_bytes: 100,
            max_file_bytes: 1000,
            max_staged_bytes: 1500,
            ttl_ms: 1000,
        },
        phase: MultipartPhase::Open,
        part_count: 0,
        staged_bytes: 0,
        completion: None,
        published: None,
        pending: None,
    }
}

pub fn completion(session: &MultipartSession) -> MultipartCompletion {
    MultipartCompletion {
        selection: PayloadReference {
            catalog: session.context.catalog,
            operation: session.upload,
            digest: [3; 32],
            length: 100,
        },
        selected_parts: 1,
        progress: AssemblyProgress {
            selection: [3; 32],
            next_part: 0,
            part_offset: 0,
            completed_bytes: 0,
            writer: None,
            active: None,
            part_digest: None,
        },
        candidate: None,
    }
}
