// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use std::process::Command;

#[test]
fn help_lists_commands_without_creating_configuration() {
    let home = tempfile::tempdir().unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_lancedb"))
        .arg("--help")
        .env("HOME", home.path())
        .env("LANCEDB_API_KEY", "secret-that-must-not-appear")
        .output()
        .unwrap();
    assert!(output.status.success());
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(stdout.contains("Usage:"));
    for command in ["auth", "acl", "profile", "sql", "load"] {
        assert!(stdout.contains(command));
    }
    assert!(!stdout.contains("secret-that-must-not-appear"));
    assert!(!home.path().join(".lancedb").exists());
}

#[test]
fn excluded_commands_and_mixed_sql_options_are_rejected() {
    for args in [
        vec!["server"],
        vec!["management"],
        vec!["job", "--help"],
        vec!["job", "list", "--help"],
        vec!["job", "usage"],
        vec!["--command", "SELECT 1", "profile", "list"],
        vec!["--file", "queries.sql", "profile", "list"],
        vec!["profile", "list", "--command", "SELECT 1"],
        vec!["profile", "list", "--file", "queries.sql"],
    ] {
        let output = Command::new(env!("CARGO_BIN_EXE_lancedb"))
            .args(args)
            .output()
            .unwrap();
        assert!(!output.status.success());
    }
}

#[test]
fn sql_options_do_not_appear_in_other_subcommand_help() {
    let output = Command::new(env!("CARGO_BIN_EXE_lancedb"))
        .args(["profile", "--help"])
        .output()
        .unwrap();
    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    for option in ["--command", "--file", "--format", "--sql-server-url"] {
        assert!(!stdout.contains(option), "{stdout}");
    }
}

#[test]
fn sql_environment_options_do_not_change_profile_commands() {
    let home = tempfile::tempdir().unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_lancedb"))
        .args(["profile", "list"])
        .env("HOME", home.path())
        .env("LANCEDB_CA_CERT", "/not-used-by-profile/ca.pem")
        .env("LANCEDB_CLIENT_CERT", "/not-used-by-profile/client.pem")
        .env("LANCEDB_CLIENT_KEY", "/not-used-by-profile/key.pem")
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("Configured profiles:"), "{stdout}");
}

#[test]
fn profile_updates_preserve_enterprise_data_directory() {
    use lancedb_cli::server_config::{Config, Profile};
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("config.toml");
    std::fs::write(
        &path,
        "data_dir = '/enterprise/data'\ndefault_profile = 'local'\n",
    )
    .unwrap();
    let mut config = Config::load_from_path(&path).unwrap();
    config.set_profile(
        "cloud",
        Profile {
            database: Some("analytics".into()),
            ..Default::default()
        },
    );
    config.save_to_path(&path).unwrap();
    let saved = Config::load_from_path(&path).unwrap();
    assert_eq!(saved.data_dir.as_deref(), Some("/enterprise/data"));
    assert_eq!(
        saved.get_profile(Some("cloud")).database.as_deref(),
        Some("analytics")
    );
}

#[cfg(unix)]
#[test]
fn api_key_profiles_are_private_under_permissive_umasks_and_after_edits() {
    use lancedb_cli::server_config::Config;
    use std::fs;
    use std::os::unix::fs::PermissionsExt;

    for mask in ["022", "000"] {
        let home = tempfile::tempdir().unwrap();
        let config_dir = home.path().join(".lancedb");
        let path = config_dir.join("config.toml");
        let run = |command: &str, key: &str| {
            // Set umask only in the child, leaving other test threads unaffected.
            let output = Command::new("/bin/sh")
                .args([
                    "-c",
                    r#"umask "$1"; shift; exec "$@""#,
                    "profile-permissions",
                    mask,
                    env!("CARGO_BIN_EXE_lancedb"),
                    "profile",
                    command,
                    "cloud",
                    "--api-key",
                    key,
                    "--database",
                    "analytics",
                ])
                .env("HOME", home.path())
                .output()
                .unwrap();
            assert!(
                output.status.success(),
                "{command} under umask {mask}: {}",
                String::from_utf8_lossy(&output.stderr)
            );
        };
        let assert_private = || {
            assert_eq!(
                fs::metadata(&config_dir).unwrap().permissions().mode() & 0o777,
                0o700,
                "config directory under umask {mask}"
            );
            assert_eq!(
                fs::metadata(&path).unwrap().permissions().mode() & 0o777,
                0o600,
                "config file under umask {mask}"
            );
        };

        run("create", "initial-key");
        assert_private();

        fs::set_permissions(&config_dir, fs::Permissions::from_mode(0o755)).unwrap();
        fs::set_permissions(&path, fs::Permissions::from_mode(0o644)).unwrap();
        run("edit", "replacement-key");
        assert_private();
        let saved = Config::load_from_path(&path).unwrap();
        assert_eq!(
            saved.get_profile(Some("cloud")).api_key.as_deref(),
            Some("replacement-key")
        );
        assert_eq!(
            saved.get_profile(Some("cloud")).database.as_deref(),
            Some("analytics")
        );
    }
}
