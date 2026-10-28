// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use crate::server_config::ConnectionArgs;
use anyhow::Result;
use clap::{Parser, Subcommand};

/// Public client commands. Runtime and administrative commands live in Enterprise.
#[derive(Parser, Debug)]
#[command(
    name = "lancedb",
    version,
    about = "LanceDB command-line client",
    args_conflicts_with_subcommands = true
)]
pub struct Args {
    #[command(subcommand)]
    pub command: Option<Commands>,
    /// SQL options used when no subcommand is given. These are local to the
    /// default SQL invocation, so other subcommands use their own options.
    #[command(flatten)]
    pub sql: SqlArgs,
}

#[derive(Subcommand, Debug)]
pub enum Commands {
    /// Manage OAuth login and cached interactive sessions
    Auth(crate::auth::AuthArgs),
    /// List, add, and delete access-control entries
    Acl(crate::acl::AclArgs),
    /// Manage connection profiles
    Profile(crate::profile::ProfileArgs),
    /// Run SQL or open an interactive shell against a remote server
    Sql(SqlArgs),
    /// Load data from external sources
    Load(Box<crate::load::LoadArgs>),
}

#[derive(clap::Args, Debug)]
pub struct SqlArgs {
    #[command(flatten)]
    pub connection: ConnectionArgs,
    /// Execute SQL and exit
    #[arg(short = 'c', long = "command", conflicts_with = "file")]
    pub command_string: Option<String>,
    /// Execute SQL commands from a file and exit
    #[arg(short = 'f', long)]
    pub file: Option<String>,
    /// Output format: table, csv, json, ndjson
    #[arg(long, default_value = "table")]
    pub format: String,
}

pub async fn run_sql(args: SqlArgs) -> Result<()> {
    let connection = args.connection.resolve(None)?;
    if let Some(file) = args.file {
        crate::client::run_client_script_with_writer(
            &connection,
            std::fs::read_to_string(file)?,
            args.format,
            None,
        )
        .await
    } else if let Some(sql) = args.command_string {
        crate::client::run_client_command_with_writer(&connection, sql, args.format, None).await
    } else {
        crate::client::run_client_with_writer(&connection, None).await
    }
}

pub async fn run(args: Args) -> Result<()> {
    match args.command {
        None => run_sql(args.sql).await,
        Some(Commands::Auth(args)) => crate::auth::run(args, None).await,
        Some(Commands::Acl(args)) => crate::acl::run_command(args).await,
        Some(Commands::Profile(args)) => crate::profile::run_profile_command(args).await,
        Some(Commands::Sql(args)) => run_sql(args).await,
        Some(Commands::Load(args)) => crate::load::run_load(*args).await,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn sql_is_the_default_command() {
        let args = Args::try_parse_from(["lancedb"]).unwrap();
        assert!(args.command.is_none());
        assert_eq!(args.sql.format, "table");
        assert!(args.sql.command_string.is_none());
        assert!(args.sql.file.is_none());
        assert!(matches!(
            Args::try_parse_from(["lancedb", "sql"]).unwrap().command,
            Some(Commands::Sql(_))
        ));
        let args = Args::try_parse_from([
            "lancedb",
            "--profile",
            "production",
            "--command",
            "SELECT 1",
            "--format",
            "json",
        ])
        .unwrap();
        assert!(args.command.is_none());
        assert_eq!(args.sql.connection.profile.as_deref(), Some("production"));
        assert_eq!(args.sql.command_string.as_deref(), Some("SELECT 1"));
        assert_eq!(args.sql.format, "json");
        let args = Args::try_parse_from(["lancedb", "--file", "queries.sql"]).unwrap();
        assert_eq!(args.sql.file.as_deref(), Some("queries.sql"));
    }

    #[test]
    fn default_sql_options_do_not_apply_to_subcommands() {
        for args in [
            vec!["lancedb", "--format", "json", "profile", "list"],
            vec!["lancedb", "--profile", "production", "profile", "list"],
            vec!["lancedb", "--command", "SELECT 1", "profile", "list"],
            vec!["lancedb", "profile", "list", "--command", "SELECT 1"],
            vec!["lancedb", "profile", "list", "--file", "queries.sql"],
            vec!["lancedb", "-c", "SELECT 1", "-f", "query.sql"],
            vec!["lancedb", "sql", "-c", "SELECT 1", "-f", "query.sql"],
        ] {
            assert!(Args::try_parse_from(&args).is_err(), "{args:?}");
        }
        assert!(Args::try_parse_from(["lancedb", "profile", "list"]).is_ok());
    }

    #[test]
    fn excluded_commands_are_rejected() {
        for command in [
            "server",
            "admin",
            "bench",
            "service",
            "pe",
            "agent",
            "management",
            "preflight",
            "run",
            "function-worker",
        ] {
            assert!(
                Args::try_parse_from(["lancedb", command]).is_err(),
                "{command}"
            );
        }
    }
}
