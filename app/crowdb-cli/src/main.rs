// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! `crowdb-cli` CLI entrypoint.
//!
//! Cluster operations, logical KV operations and attached workloads. The CLI
//! resolves a confirmed application target, then uses Group 0 and native
//! service clients for metadata, data and attached workloads.

macro_rules! eprintln {
    () => {
        std::eprintln!()
    };
    ($($arg:tt)*) => {{
        let message = format!($($arg)*);
        if let Some(detail) = message.strip_prefix("error: ") {
            std::eprintln!("\x1b[1;31mERROR\x1b[0m {detail}");
        } else if let Some(detail) = message.strip_prefix("warn: ") {
            std::eprintln!("\x1b[1;33mWARNING\x1b[0m {detail}");
        } else if let Some(detail) = message.strip_prefix("warning: ") {
            std::eprintln!("\x1b[1;33mWARNING\x1b[0m {detail}");
        } else {
            std::eprintln!("{message}");
        }
    }};
}

mod commands;

use std::path::PathBuf;
use std::process::ExitCode;

use clap::{Parser, Subcommand};

use commands::{
    run_bench_verb, run_cluster_verb, run_group_verb, run_kv_data_verb, run_port_alloc, run_replica_verb,
    run_s3_verb, run_store_verb, BenchVerb, ClusterVerb, GroupVerb, KvDataVerb, PortAllocArgs, ReplicaVerb,
    S3Verb, StoreVerb,
};

#[derive(Parser, Debug)]
#[command(name = "crowdb-cli", version, about = "CrowDB cluster console (CLI)")]
struct Cli {
    /// Select a saved application cluster binding.
    #[arg(long, global = true, env = "CROWDB_CLUSTER")]
    cluster: Option<String>,

    /// Console management origin; repeat to verify multiple cluster members.
    #[arg(long = "management-endpoint", global = true)]
    management_endpoints: Vec<String>,

    #[arg(skip)]
    target: Option<crowdb_console_shared::deployment::target::ResolvedClusterTarget>,

    /// Latest-run benchmark log directory, computed in `main()` after parse.
    /// This is not a CLI flag and remains empty for non-benchmark commands.
    #[arg(skip)]
    log_dir: PathBuf,

    #[command(subcommand)]
    command: Domain,
}

impl Cli {
    /// Stable folder name for the latest run of each benchmark family.
    fn benchmark_slug(&self) -> Option<&'static str> {
        match &self.command {
            Domain::Bench { verb } => Some(match verb {
                BenchVerb::Kv(_) => "bench-kv",
                BenchVerb::Diskdb(_) => "bench-diskdb",
                BenchVerb::Chunkdb(_) => "bench-chunkdb",
                BenchVerb::Chunkio(_) => "bench-chunkio",
                BenchVerb::S3(_) => "bench-s3",
            }),
            _ => None,
        }
    }
}

#[derive(Subcommand, Debug)]
enum Domain {
    /// Hardware topology + cluster-level ops.
    #[command(alias = "cls")]
    Cluster {
        #[command(subcommand)]
        verb: ClusterVerb,
    },
    /// Logical KV topology and data operations.
    Kv {
        #[command(subcommand)]
        verb: KvVerb,
    },
    /// Load injection only.
    Bench {
        #[command(subcommand)]
        verb: BenchVerb,
    },
    /// Signed bucket and object operations on the selected cluster.
    S3 {
        #[command(subcommand)]
        verb: S3Verb,
    },
    /// Flock-coordinated port allocation for tests and cluster
    /// bootstrap. No tokio, no RPC — handled before the runtime is
    /// built in `main()`.
    PortAlloc {
        #[command(flatten)]
        args: PortAllocArgs,
    },
}

#[derive(Subcommand, Debug)]
enum KvVerb {
    #[command(subcommand)]
    Store(StoreVerb),
    #[command(subcommand)]
    Group(GroupVerb),
    #[command(subcommand)]
    Replica(ReplicaVerb),
    #[command(subcommand)]
    Data(KvDataVerb),
}

fn main() -> ExitCode {
    let mut cli = Cli::parse();
    print_command();

    // Port-alloc is a synchronous bootstrap tool: no tokio runtime,
    // no RPC, no log files. Short-circuit before the heavy init so
    // the E2E fixture (which calls it many times) stays fast and
    // doesn't create invocation log directories.
    if let Domain::PortAlloc { args } = &cli.command {
        return finish(run_port_alloc(args));
    }

    let _log_guards = match cli.benchmark_slug() {
        None => {
            let _ = crowdb_common::logging::init_console_logging("warn");
            crowdb_rpc_ffi::init_logging("", "warn", 30, 5, "crowdb-cli-rpc");
            None
        }
        Some(benchmark_slug) => {
            // Each benchmark family keeps only its latest run. The fixed,
            // command-derived path is safe to replace and avoids unbounded
            // accumulation of timestamped run directories.
            let log_root = crowdb_protocol::port::namespace::runtime_root()
                .join("artifacts")
                .join("cli");
            let invocation_dir = log_root.join(benchmark_slug);
            if let Err(error) = std::fs::remove_dir_all(&invocation_dir) {
                if error.kind() != std::io::ErrorKind::NotFound {
                    eprintln!("warning: cannot replace previous benchmark log: {error}");
                }
            }
            let _ = std::fs::create_dir_all(&invocation_dir);
            cli.log_dir.clone_from(&invocation_dir);
            eprintln!("log dir: {}", invocation_dir.display());

            let guards = crowdb_common::logging::init_file_and_console_logging_split(
                &invocation_dir,
                "crowdb-cli",
                30,
                1,
                "warn,crowdb_cli=info,crowdb_console_shared=info,crowdb_kv_client=info",
                "warn,crowdb_cli=info,crowdb_console_shared=info,crowdb_kv_client=info",
            )
            .ok();
            crowdb_rpc_ffi::init_logging(
                invocation_dir.to_str().unwrap_or(".crowdb-runtime/artifacts/cli"),
                "warn",
                30,
                1,
                "crowdb-cli-rpc",
            );
            crowdb_rpc_ffi::add_log_stderr("warn");
            guards
        }
    };

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");

    let cid = crowdb_console_shared::corr_id::generate();
    finish(
        runtime.block_on(
            async move { Box::pin(crowdb_console_shared::corr_id::scope(cid, dispatch(cli))).await },
        ),
    )
}

fn print_command() {
    let command = std::env::args_os()
        .map(|value| shell_quote(&value.to_string_lossy()))
        .collect::<Vec<_>>()
        .join(" ");
    eprintln!("\x1b[1;36mCOMMAND\x1b[0m {command}");
}

fn finish(code: ExitCode) -> ExitCode {
    use std::io::Write as _;

    let _ = std::io::stdout().flush();
    if code == ExitCode::SUCCESS {
        eprintln!("\x1b[1;32mRESULT\x1b[0m success");
    } else {
        eprintln!("\x1b[1;31mRESULT\x1b[0m failed");
    }
    code
}

fn shell_quote(value: &str) -> String {
    if !value.is_empty()
        && value.bytes().all(|byte| {
            byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.' | b'/' | b':' | b'=')
        })
    {
        value.to_string()
    } else {
        format!("'{}'", value.replace('\'', "'\\''"))
    }
}

async fn dispatch(mut cli: Cli) -> ExitCode {
    cli.target = match commands::cluster::target::resolve(&cli).await {
        Ok(target) => target,
        Err(code) => return code,
    };
    let command = std::mem::replace(
        &mut cli.command,
        Domain::Cluster {
            verb: ClusterVerb::Status {
                json: false,
                timeout: 10,
            },
        },
    );
    match command {
        Domain::Cluster { verb } => run_cluster_verb(&cli, verb).await,
        Domain::Kv { verb } => match verb {
            KvVerb::Store(sv) => run_store_verb(&cli, sv).await,
            KvVerb::Group(gv) => run_group_verb(&cli, gv).await,
            KvVerb::Replica(rv) => run_replica_verb(&cli, rv).await,
            KvVerb::Data(dv) => run_kv_data_verb(&cli, dv).await,
        },
        Domain::Bench { verb } => run_bench_verb(&cli, verb).await,
        Domain::S3 { verb } => run_s3_verb(&cli, verb).await,
        Domain::PortAlloc { args } => run_port_alloc(&args),
    }
}
