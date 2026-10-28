// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use arrow_array::{types::Float32Type, Int32Array, ListArray, RecordBatch, RecordBatchIterator};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use async_trait::async_trait;
use clap::Parser;
use futures::stream::BoxStream;
use lancedb_cli::load::sources::DataSource;
use lancedb_cli::load::writer::load_to_lancedb;
use lancedb_cli::load::{run_load, LoadArgs};
use std::collections::HashMap;
use std::sync::Arc;

struct UnreadableSource;

struct RaggedVectorSource;

#[async_trait]
impl DataSource for RaggedVectorSource {
    async fn schema(&self) -> anyhow::Result<SchemaRef> {
        Ok(ragged_vector_batch().schema())
    }

    async fn total_rows(&self) -> anyhow::Result<Option<u64>> {
        Ok(Some(3))
    }

    fn stream_batches(&self, _batch_size: usize) -> BoxStream<'_, anyhow::Result<RecordBatch>> {
        Box::pin(futures::stream::once(async { Ok(ragged_vector_batch()) }))
    }
}

fn ragged_vector_batch() -> RecordBatch {
    let lists = ListArray::from_iter_primitive::<Float32Type, _, _>([
        Some(vec![Some(1.), Some(2.)]),
        Some(vec![Some(3.)]),
        Some(vec![Some(4.), Some(5.), Some(6.)]),
    ]);
    RecordBatch::try_from_iter([("vector", Arc::new(lists) as arrow_array::ArrayRef)]).unwrap()
}

#[async_trait]
impl DataSource for UnreadableSource {
    async fn schema(&self) -> anyhow::Result<SchemaRef> {
        Ok(Arc::new(Schema::new(vec![Field::new(
            "id",
            DataType::Int32,
            true,
        )])))
    }

    async fn total_rows(&self) -> anyhow::Result<Option<u64>> {
        Ok(None)
    }

    async fn detect_vector_columns(
        &self,
    ) -> anyhow::Result<HashMap<String, lancedb_cli::load::schema::VectorColumnInfo>> {
        // Let preflight succeed so the failure comes from the import stream.
        Ok(HashMap::new())
    }

    fn stream_batches(&self, _batch_size: usize) -> BoxStream<'_, anyhow::Result<RecordBatch>> {
        Box::pin(futures::stream::once(async {
            anyhow::bail!("source read failed")
        }))
    }
}

#[tokio::test]
async fn load_lance_into_local_database_and_overwrite() {
    let dir = tempfile::tempdir().unwrap();
    let source = dir.path().join("source.lance");
    let destination = dir.path().join("db");
    let batch = RecordBatch::try_from_iter([(
        "id",
        Arc::new(Int32Array::from(vec![1, 2, 3])) as arrow_array::ArrayRef,
    )])
    .unwrap();
    lance::Dataset::write(
        RecordBatchIterator::new(vec![Ok(batch.clone())], batch.schema()),
        source.to_str().unwrap(),
        None,
    )
    .await
    .unwrap();
    let args = || {
        LoadArgs::try_parse_from([
            "load",
            "--source",
            "lance",
            "--dataset",
            source.to_str().unwrap(),
            "--table",
            "events",
            "--destination",
            destination.to_str().unwrap(),
        ])
        .unwrap()
    };
    run_load(args()).await.unwrap();
    let error = run_load(args()).await.unwrap_err();
    assert!(
        format!("{error:#}").contains("already exists. Use --overwrite"),
        "{error:#}"
    );
    let connection = lancedb::connect(destination.to_str().unwrap())
        .execute()
        .await
        .unwrap();
    let original = connection.open_table("events").execute().await.unwrap();
    let original_version = original.version().await.unwrap();

    let replacement = dir.path().join("replacement.lance");
    let batch = RecordBatch::try_from_iter([(
        "id",
        Arc::new(Int32Array::from(vec![4, 5, 6, 7, 8])) as arrow_array::ArrayRef,
    )])
    .unwrap();
    lance::Dataset::write(
        RecordBatchIterator::new(vec![Ok(batch.clone())], batch.schema()),
        replacement.to_str().unwrap(),
        None,
    )
    .await
    .unwrap();
    let mut overwrite = args();
    overwrite.dataset = Some(replacement.to_string_lossy().into_owned());
    overwrite.overwrite = true;
    overwrite.batch_size = 2;
    run_load(overwrite).await.unwrap();

    let table = connection.open_table("events").execute().await.unwrap();
    assert_eq!(table.count_rows(None).await.unwrap(), 5);
    assert!(table.version().await.unwrap() > original_version);
    table.checkout(original_version).await.unwrap();
    assert_eq!(table.count_rows(None).await.unwrap(), 3);
}

#[tokio::test]
async fn overwrite_source_failures_preserve_data_and_history() {
    for failure in ["open", "read", "vector conversion"] {
        let dir = tempfile::tempdir().unwrap();
        let destination = dir.path().join("db");
        let connection = lancedb::connect(destination.to_str().unwrap())
            .execute()
            .await
            .unwrap();
        let batch = RecordBatch::try_from_iter([(
            "id",
            Arc::new(Int32Array::from(vec![1, 2, 3])) as arrow_array::ArrayRef,
        )])
        .unwrap();
        let table = connection
            .create_table("events", batch.clone())
            .execute()
            .await
            .unwrap();
        let first_version = table.version().await.unwrap();
        table.add(batch).execute().await.unwrap();
        let original_version = table.version().await.unwrap();

        let missing_source = dir.path().join("missing.lance");
        let args = LoadArgs::try_parse_from([
            "load",
            "--source",
            "lance",
            "--dataset",
            missing_source.to_str().unwrap(),
            "--table",
            "events",
            "--destination",
            destination.to_str().unwrap(),
            "--overwrite",
        ])
        .unwrap();
        let error = if failure == "open" {
            run_load(args).await.unwrap_err()
        } else {
            let mut connection_args = args.connection.clone();
            connection_args.http_server_url = args.destination.clone();
            let conn = connection_args.resolve(None).unwrap();
            let source: &dyn DataSource = if failure == "read" {
                &UnreadableSource
            } else {
                &RaggedVectorSource
            };
            load_to_lancedb(&args, &conn, source).await.unwrap_err()
        };
        let expected_error = match failure {
            "open" => "failed to open Lance dataset",
            "read" => "source read failed",
            _ => "Column 'vector', row 1: expected vector dimension 2, got 1",
        };
        assert!(format!("{error:#}").contains(expected_error), "{error:#}");

        let table = connection.open_table("events").execute().await.unwrap();
        assert_eq!(table.version().await.unwrap(), original_version);
        assert_eq!(table.count_rows(None).await.unwrap(), 6);
        assert_eq!(table.list_versions().await.unwrap().len(), 2);
        table.checkout(first_version).await.unwrap();
        assert_eq!(table.count_rows(None).await.unwrap(), 3);
    }
}

#[test]
fn load_accepts_destination_and_http_server_url_alias() {
    let args = [
        "load",
        "--source",
        "lance",
        "--dataset",
        "source.lance",
        "--table",
        "events",
    ];
    let destination = LoadArgs::try_parse_from(
        args.into_iter()
            .chain(["--destination", "https://example.com"]),
    )
    .unwrap();
    let alias = LoadArgs::try_parse_from(
        args.into_iter()
            .chain(["--http-server-url", "https://example.com"]),
    )
    .unwrap();
    assert_eq!(
        destination.destination.as_deref(),
        Some("https://example.com")
    );
    assert_eq!(
        alias.connection.http_server_url.as_deref(),
        Some("https://example.com")
    );
    assert!(LoadArgs::try_parse_from(args.into_iter().chain([
        "--destination",
        "https://example.com",
        "--http-server-url",
        "https://example.com",
    ]))
    .is_err());
}
