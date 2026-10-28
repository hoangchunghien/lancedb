# LanceDB CLI

A command-line client for LanceDB. SQL commands connect to an existing
LanceDB Cloud or Enterprise deployment; this package does not include a server.

Build and install from the LanceDB repository root:

```sh
cargo install --locked --path rust/lancedb/cli
```

The executable is `lancedb`. Running it without arguments prints help.

```sh
lancedb profile create production \
  --http-server-url https://query.example.com \
  --sql-server-url grpcs://sql.example.com:10025 \
  --database analytics
lancedb auth login --profile production
lancedb sql --profile production
lancedb sql --profile production --command 'SELECT 1' --format json
lancedb sql --profile production --file queries.sql
lancedb sql --profile production --command 'SHOW JOBS'
lancedb sql --profile production --command "SHOW ERRORS ON documents FOR JOB 'JOB_ID'"
lancedb sql --profile production --command "CANCEL JOB 'JOB_ID'"
lancedb acl list --profile production --object 'table:analytics:public$events' --format json
lancedb acl add --profile production --principal-name Alice --object 'table:analytics:public$events' --privilege SELECT
lancedb acl delete --profile production --principal-name Alice --object 'table:analytics:public$events' --privilege SELECT
lancedb load --profile production --source lance --dataset /data/events.lance --table events
lancedb load --profile production --source hf --dataset org/dataset --table documents
lancedb load --source lance --dataset /data/events.lance --table events --destination /data/my_lancedb
```

For `load`, `--destination` selects a remote HTTP endpoint or a local LanceDB
database path. `--http-server-url` remains an alias for existing load commands.
Without either flag, the selected profile supplies the endpoint.

For API-key authentication set `LANCEDB_API_KEY`. OAuth login requires an `oidc`
configuration in the selected profile. Existing Enterprise profiles in
`~/.lancedb/config.toml` and cached OAuth sessions are shared without migration.
Profile updates preserve the Enterprise `data_dir` setting. TLS and mutual TLS use
`--ca-cert`, `--client-cert`, and `--client-key`.

Commands are `auth`, `acl`, `profile`, `sql`, and `load`. ACLs support
`list`, `add`, and `delete` through the remote catalog API. `acl list` accepts
`--object`, one subject selector, `--limit`, and `--page-token`; it prints TSV by
default or JSON with `--format json`. The server determines which grants the
selected credentials may inspect or modify. Inspect and manage jobs through SQL
with `SHOW JOBS`, `SHOW ERRORS ON <table>`, and `CANCEL JOB '<job-id>'`.
Job access remains subject to the deployment's authorization rules.

The SQL command connects to the server's Flight SQL API.
The CLI uses the Rust SDK for SQL and ACLs. The Enterprise executable shares the client
library while retaining its runtime/admin commands and legacy SQL
entry points (`lancedb`, `lancedb --command`, and `lancedb --file`). Both executables
have the same name: installing one replaces the other in that installation path.

## Development

```sh
cargo test --quiet -p lancedb-cli
cargo clippy -p lancedb-cli --all-targets --all-features -- -D warnings
cargo fmt --all
```

Tests use local fixtures and mock HTTP/Flight services. No private Sophon source
or running Enterprise deployment is required. Release binaries and registry
publication are not configured by this extraction.
