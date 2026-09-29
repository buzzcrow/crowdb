// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Hardware command handlers: rack, node, disk-group, disk.
//! Delegates to `ops::hardware`.

use std::process::ExitCode;

use clap::Subcommand;
use crowdb_console_shared::config::NodeEntry;
use crowdb_protocol::{NodeId, RackId};

use crate::commands::authority_context;
use crate::Cli;

// ── rack ─────────────────────────────────────────────────────────

#[derive(Subcommand, Debug)]
pub enum RackVerb {
    Add {
        #[arg(short = 'I', long)]
        id: String,
        #[arg(short = 'n', long, default_value = "")]
        name: String,
    },
    Remove {
        #[arg(short = 'I', long)]
        id: String,
    },
    List,
}

pub async fn run_rack_verb(cli: &Cli, verb: RackVerb) -> ExitCode {
    match verb {
        RackVerb::Add { id, name } => {
            let rack_id: RackId = match id.parse() {
                Ok(r) => r,
                Err(e) => {
                    eprintln!("error: invalid rack id {id:?}: {e}");
                    return ExitCode::from(1);
                }
            };
            let ctx = match authority_context(cli).await {
                Ok(c) => c,
                Err(c) => return c,
            };
            let result = crowdb_console_shared::ops::hardware::add_rack_to_group0(&ctx, rack_id, &name).await;
            match result {
                Ok(entry) => {
                    println!("added rack {}", entry.id);
                    ExitCode::SUCCESS
                }
                Err(e) => {
                    eprintln!("error: add rack {id}: {e}");
                    ExitCode::from(2)
                }
            }
        }
        RackVerb::Remove { id } => {
            let rack_id: RackId = match id.parse() {
                Ok(r) => r,
                Err(e) => {
                    eprintln!("error: invalid rack id {id:?}: {e}");
                    return ExitCode::from(1);
                }
            };
            let ctx = match authority_context(cli).await {
                Ok(c) => c,
                Err(c) => return c,
            };
            let result = crowdb_console_shared::ops::hardware::remove_rack_from_group0(&ctx, rack_id).await;
            match result {
                Ok(()) => {
                    println!("removed rack {id}");
                    ExitCode::SUCCESS
                }
                Err(e) => {
                    eprintln!("error: remove rack {id}: {e}");
                    ExitCode::from(2)
                }
            }
        }
        RackVerb::List => {
            let ctx = match authority_context(cli).await {
                Ok(c) => c,
                Err(c) => return c,
            };
            let racks = match crowdb_console_shared::ops::hardware::list_racks_from_group0(&ctx).await {
                Ok(racks) => racks,
                Err(error) => {
                    eprintln!("error: list racks: {error}");
                    return ExitCode::from(2);
                }
            };
            if racks.is_empty() {
                println!("(no racks)");
                return ExitCode::SUCCESS;
            }
            println!("{:<16}  NAME", "ID");
            for r in &racks {
                println!("{:<16}  {}", r.id, r.name);
            }
            ExitCode::SUCCESS
        }
    }
}

// ── node ─────────────────────────────────────────────────────────

#[derive(Subcommand, Debug)]
pub enum NodeVerb {
    Add {
        #[arg(short = 'I', long)]
        id: String,
        #[arg(short = 'r', long)]
        rack: String,
        #[arg(short = 'H', long, default_value = "127.0.0.1")]
        host: String,
        #[arg(short = 'P', long, default_value_t = 22)]
        ssh_port: u16,
        #[arg(short = 'u', long, default_value = "")]
        ssh_user: String,
        #[arg(short = 'k', long)]
        ssh_key: Option<String>,
        #[arg(long)]
        ssh_credential_ref: Option<String>,
    },
    Remove {
        #[arg(short = 'I', long)]
        id: String,
    },
    List,
    #[command(alias = "ls")]
    ListRack {
        #[arg(short = 'r', long)]
        rack: String,
    },
}

#[allow(clippy::too_many_lines)]
pub async fn run_node_verb(cli: &Cli, verb: NodeVerb) -> ExitCode {
    match verb {
        NodeVerb::Add {
            id,
            rack,
            host,
            ssh_port,
            ssh_user,
            ssh_key,
            ssh_credential_ref,
        } => {
            if ssh_key.is_some() {
                eprintln!("error: --ssh-key is local secret material; use --ssh-credential-ref");
                return ExitCode::from(1);
            }
            let node_id: NodeId = match id.parse() {
                Ok(n) => n,
                Err(e) => {
                    eprintln!("error: invalid node id {id:?}: {e}");
                    return ExitCode::from(1);
                }
            };
            let rack_id: RackId = match rack.parse() {
                Ok(r) => r,
                Err(e) => {
                    eprintln!("error: invalid rack id {rack:?}: {e}");
                    return ExitCode::from(1);
                }
            };
            let entry = NodeEntry {
                id: node_id,
                rack_id,
                host,
                ssh_port,
                ssh_user,
                ssh_key,
                ssh_password: None,
                ssh_credential_ref,
            };
            let ctx = match authority_context(cli).await {
                Ok(c) => c,
                Err(c) => return c,
            };
            let result = crowdb_console_shared::ops::hardware::add_node_to_group0(&ctx, entry.clone()).await;
            match result {
                Ok(e) => {
                    println!("added node {} (rack {})", e.id, e.rack_id);
                    ExitCode::SUCCESS
                }
                Err(err) => {
                    eprintln!("error: add node {id}: {err}");
                    ExitCode::from(2)
                }
            }
        }
        NodeVerb::Remove { id } => {
            let node_id: NodeId = match id.parse() {
                Ok(n) => n,
                Err(e) => {
                    eprintln!("error: invalid node id {id:?}: {e}");
                    return ExitCode::from(1);
                }
            };
            let ctx = match authority_context(cli).await {
                Ok(c) => c,
                Err(c) => return c,
            };
            let result = crowdb_console_shared::ops::hardware::remove_node_from_group0(&ctx, node_id).await;
            match result {
                Ok(()) => {
                    println!("removed node {id}");
                    ExitCode::SUCCESS
                }
                Err(e) => {
                    eprintln!("error: remove node {id}: {e}");
                    ExitCode::from(2)
                }
            }
        }
        NodeVerb::List => {
            let ctx = match authority_context(cli).await {
                Ok(c) => c,
                Err(c) => return c,
            };
            let nodes = match crowdb_console_shared::ops::hardware::list_nodes_from_group0(&ctx, None).await {
                Ok(nodes) => nodes,
                Err(error) => {
                    eprintln!("error: list nodes: {error}");
                    return ExitCode::from(2);
                }
            };
            print_node_table(&nodes)
        }
        NodeVerb::ListRack { rack } => {
            let rack_id: RackId = match rack.parse() {
                Ok(r) => r,
                Err(e) => {
                    eprintln!("error: invalid rack id {rack:?}: {e}");
                    return ExitCode::from(1);
                }
            };
            let ctx = match authority_context(cli).await {
                Ok(c) => c,
                Err(c) => return c,
            };
            let nodes =
                match crowdb_console_shared::ops::hardware::list_nodes_from_group0(&ctx, Some(rack_id)).await
                {
                    Ok(nodes) => nodes,
                    Err(error) => {
                        eprintln!("error: list nodes: {error}");
                        return ExitCode::from(2);
                    }
                };
            print_node_table(&nodes)
        }
    }
}

fn print_node_table(nodes: &[NodeEntry]) -> ExitCode {
    if nodes.is_empty() {
        println!("(no nodes)");
        return ExitCode::SUCCESS;
    }
    println!("{:<8}  {:<8}  {:<16}  {:<8}  SSH", "ID", "RACK", "HOST", "PORT");
    for n in nodes {
        println!(
            "{:<8}  {:<8}  {:<16}  {:<8}  {}",
            n.id, n.rack_id, n.host, n.ssh_port, n.ssh_user
        );
    }
    ExitCode::SUCCESS
}

// ── disk-group ───────────────────────────────────────────────────

#[derive(Subcommand, Debug)]
pub enum DiskGroupVerb {
    Add {
        #[arg(short = 'I', long)]
        id: String,
        #[arg(short = 'r', long)]
        rack: String,
        #[arg(short = 'n', long)]
        node: String,
        #[arg(short = 'N', long, default_value = "")]
        name: String,
    },
    Remove {
        #[arg(short = 'I', long)]
        id: String,
    },
    List,
}

pub async fn run_disk_group_verb(cli: &Cli, verb: DiskGroupVerb) -> ExitCode {
    use crowdb_console_shared::ops::hardware;
    let ctx = match authority_context(cli).await {
        Ok(ctx) => ctx,
        Err(code) => return code,
    };
    let result = match verb {
        DiskGroupVerb::Add { id, rack, node, name } => {
            let (Ok(id), Ok(rack), Ok(node)) = (id.parse::<u64>(), rack.parse::<u64>(), node.parse::<u64>())
            else {
                eprintln!("error: disk-group, rack and node IDs must be integers");
                return ExitCode::from(1);
            };
            match hardware::list_nodes_from_group0(&ctx, Some(rack)).await {
                Ok(nodes) if nodes.iter().any(|entry| entry.id == node) => {}
                Ok(_) => {
                    eprintln!("error: node {node} is not in rack {rack}");
                    return ExitCode::from(2);
                }
                Err(error) => {
                    eprintln!("error: {error}");
                    return ExitCode::from(2);
                }
            }
            hardware::add_disk_group_to_group0(&ctx, node, id, &name)
                .await
                .map(|entry| {
                    println!("added disk group {} on node {}", entry.id, node);
                })
        }
        DiskGroupVerb::Remove { id } => {
            let id = match id.parse::<u64>() {
                Ok(id) => id,
                Err(error) => {
                    eprintln!("error: {error}");
                    return ExitCode::from(1);
                }
            };
            let groups = match ctx.sysmd().list_disk_groups().await {
                Ok(groups) => groups,
                Err(error) => {
                    eprintln!("error: {error}");
                    return ExitCode::from(2);
                }
            };
            let matches: Vec<_> = groups.into_iter().filter(|group| group.dg_id == id).collect();
            if matches.len() != 1 {
                eprintln!(
                    "error: disk group {id} has {} matches; specify a unique ID",
                    matches.len()
                );
                return ExitCode::from(2);
            }
            hardware::remove_disk_group_from_group0(&ctx, matches[0].node_id, id)
                .await
                .map(|()| println!("removed disk group {id}"))
        }
        DiskGroupVerb::List => match ctx.sysmd().list_disk_groups().await {
            Ok(mut groups) => {
                groups.sort_unstable_by_key(|group| (group.rack_id, group.node_id, group.dg_id));
                for group in groups {
                    println!(
                        "{}\t{}\t{}\t{}",
                        group.rack_id, group.node_id, group.dg_id, group.value.name
                    );
                }
                Ok(())
            }
            Err(error) => Err(error.into()),
        },
    };
    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("error: {error}");
            ExitCode::from(2)
        }
    }
}

// ── disk ─────────────────────────────────────────────────────────

#[derive(Subcommand, Debug)]
pub enum DiskVerb {
    Add {
        #[arg(short = 'I', long)]
        id: String,
        #[arg(short = 'r', long)]
        rack: String,
        #[arg(short = 'n', long)]
        node: String,
        #[arg(short = 'g', long)]
        group: String,
        #[arg(short = 't', long)]
        disk_type: String,
        #[arg(short = 'c', long)]
        capacity: String,
        #[arg(short = 'z', long)]
        zone_size: String,
        #[arg(short = 'u', long)]
        unit_size: String,
        #[arg(short = 'd', long, default_value = "")]
        device_path: String,
    },
    Remove {
        #[arg(short = 'I', long)]
        id: String,
    },
    List,
}

#[allow(clippy::too_many_lines)]
pub async fn run_disk_verb(cli: &Cli, verb: DiskVerb) -> ExitCode {
    use crowdb_console_shared::ops::hardware::{self, AddDiskInput};
    use crowdb_protocol::DiskIdExt;
    let ctx = match authority_context(cli).await {
        Ok(ctx) => ctx,
        Err(code) => return code,
    };
    let result = match verb {
        DiskVerb::Add {
            id,
            rack,
            node,
            group,
            disk_type,
            capacity,
            zone_size,
            unit_size,
            device_path,
        } => {
            let (Ok(rack), Ok(node), Ok(group), Ok(capacity_bytes), Ok(zone_size_bytes), Ok(unit_size_bytes)) = (
                rack.parse::<u64>(),
                node.parse::<u64>(),
                group.parse::<u64>(),
                capacity.parse::<u64>(),
                zone_size.parse::<u64>(),
                unit_size.parse::<u32>(),
            ) else {
                eprintln!("error: rack, node, group and size arguments must be integers");
                return ExitCode::from(1);
            };
            let nodes = match hardware::list_nodes_from_group0(&ctx, Some(rack)).await {
                Ok(nodes) => nodes,
                Err(error) => {
                    eprintln!("error: {error}");
                    return ExitCode::from(2);
                }
            };
            if !nodes.iter().any(|entry| entry.id == node) {
                eprintln!("error: node {node} is not in rack {rack}");
                return ExitCode::from(2);
            }
            let input = AddDiskInput {
                disk_id: id,
                disk_type,
                capacity_bytes,
                zone_size_bytes,
                unit_size_bytes,
                device_path,
            };
            hardware::add_disk_to_group0(&ctx, node, group, &input)
                .await
                .map(|entry| println!("added disk {}", entry.disk_id))
        }
        DiskVerb::Remove { id } => {
            let disk_id = match crowdb_protocol::common::DiskId::from_display_string(&id) {
                Ok(id) => id,
                Err(error) => {
                    eprintln!("error: {error}");
                    return ExitCode::from(1);
                }
            };
            let disks = match ctx.sysmd().list_all_disks().await {
                Ok(disks) => disks,
                Err(error) => {
                    eprintln!("error: {error}");
                    return ExitCode::from(2);
                }
            };
            let matches: Vec<_> = disks.into_iter().filter(|disk| disk.disk_id == disk_id).collect();
            if matches.len() != 1 {
                eprintln!(
                    "error: disk {id} has {} matches; specify a unique ID",
                    matches.len()
                );
                return ExitCode::from(2);
            }
            hardware::remove_disk_from_group0(&ctx, matches[0].node_id, matches[0].disk_group_id, &id)
                .await
                .map(|_| println!("removed disk {id}"))
        }
        DiskVerb::List => match ctx.sysmd().list_all_disks().await {
            Ok(mut disks) => {
                disks.sort_unstable_by_key(|disk| {
                    (
                        disk.rack_id,
                        disk.node_id,
                        disk.disk_group_id,
                        disk.disk_id.high,
                        disk.disk_id.low,
                    )
                });
                for disk in disks {
                    println!(
                        "{}\t{}\t{}\t{}",
                        disk.rack_id,
                        disk.node_id,
                        disk.disk_group_id,
                        disk.disk_id.to_display_string()
                    );
                }
                Ok(())
            }
            Err(error) => Err(error.into()),
        },
    };
    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("error: {error}");
            ExitCode::from(2)
        }
    }
}
