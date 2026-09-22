#![cfg(feature = "iceberg")]

use std::process::Command;

#[test]
fn writer_configuration_fails_before_backend_connection() {
    for writer in [
        None,
        Some("short".into()),
        Some("r".repeat(32)),
        Some("m".repeat(32)),
        Some("c".repeat(32)),
    ] {
        let mut command = Command::new(env!("CARGO_BIN_EXE_crowdb-iceberg"));
        command
            .env("CROWDB_MANAGEMENT_SEEDS", "127.0.0.1:1")
            .env("CROWDB_ICEBERG_READ_TOKEN", "r".repeat(32))
            .env("CROWDB_ICEBERG_MANAGE_TOKEN", "m".repeat(32))
            .env("CROWDB_ICEBERG_CLEAR_TOKEN", "c".repeat(32))
            .env_remove("CROWDB_ICEBERG_WRITE_TOKEN")
            .arg("serve");
        if let Some(token) = &writer {
            command.env("CROWDB_ICEBERG_WRITE_TOKEN", token);
        }
        let output = command.output().unwrap();
        assert!(!output.status.success());
        let error = String::from_utf8_lossy(&output.stderr);
        if writer.is_none() {
            assert!(
                error.contains("CROWDB_ICEBERG_WRITE_TOKEN must be set"),
                "{error}"
            );
        } else {
            assert!(error.contains("invalid or oversized text field"), "{error}");
        }
    }
}
