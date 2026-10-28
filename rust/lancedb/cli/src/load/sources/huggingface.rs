// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! HuggingFace dataset source.
//!
//! Downloads and streams parquet files from HuggingFace Hub.

use anyhow::{Context, Result};
use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use async_trait::async_trait;
use futures::stream::BoxStream;
use futures::StreamExt;
use hf_hub::api::tokio::Api;
use hf_hub::Repo;
use parquet::arrow::ParquetRecordBatchStreamBuilder;
use std::sync::Arc;
use tokio::fs::File;
use tokio::sync::Mutex;
use tracing::info;

use super::DataSource;

/// Check if a filename matches the given split.
///
/// HuggingFace datasets use various naming conventions for parquet files:
/// - `data/train-00000.parquet` (data/{split}-*)
/// - `plain_text/train-00000.parquet` ({configuration}/{split}-*)
/// - `train/data.parquet` ({split}/*)
/// - `train-00000.parquet` ({split}-*)
/// - `some/path/train/file.parquet` (*/{split}/*)
/// - `some/path/train.parquet` (*/{split}.parquet)
fn matches_split(filename: &str, split: &str) -> bool {
    if split.is_empty() {
        return false;
    }
    let (parent, basename) = filename.rsplit_once('/').unwrap_or(("", filename));
    parent.split('/').any(|component| component == split)
        || basename
            .strip_prefix(split)
            .is_some_and(|suffix| suffix == ".parquet" || suffix.starts_with('-'))
}

/// Data source for HuggingFace datasets.
pub struct HuggingFaceSource {
    /// Dataset identifier (e.g., "KShivendu/dbpedia-entities-openai-1M")
    dataset_id: String,
    /// Dataset split (e.g., "train", "test")
    split: String,
    /// Cached list of parquet file paths
    parquet_files: Arc<Mutex<Option<Vec<std::path::PathBuf>>>>,
    /// Cached schema
    cached_schema: Arc<Mutex<Option<SchemaRef>>>,
}

impl HuggingFaceSource {
    /// Create a new HuggingFace data source.
    /// Discovery fails if no Parquet paths identify the requested split.
    pub fn new(dataset_id: String, split: String) -> Result<Self> {
        Ok(Self {
            dataset_id,
            split,
            parquet_files: Arc::new(Mutex::new(None)),
            cached_schema: Arc::new(Mutex::new(None)),
        })
    }

    /// Download dataset files from HuggingFace Hub.
    async fn ensure_files_downloaded(&self) -> Result<Vec<std::path::PathBuf>> {
        let mut cached = self.parquet_files.lock().await;
        if let Some(ref files) = *cached {
            return Ok(files.clone());
        }

        info!(
            "Downloading dataset {} from HuggingFace Hub...",
            self.dataset_id
        );

        let api = Api::new().context("Failed to create HuggingFace Hub API client")?;
        let repo = Repo::dataset(self.dataset_id.clone());
        let repo_api = api.repo(repo);

        let filenames = self.discover_parquet_files(&repo_api).await?;
        let downloaded = self.download_files(&repo_api, &filenames).await?;

        *cached = Some(downloaded.clone());
        Ok(downloaded)
    }

    /// Discover parquet files for the specified split. Return an error if the
    /// requested split cannot be identified; unrelated files must not be imported.
    async fn discover_parquet_files(
        &self,
        repo_api: &hf_hub::api::tokio::ApiRepo,
    ) -> Result<Vec<String>> {
        let files = repo_api
            .info()
            .await
            .context("Failed to get dataset info")?;

        // Find parquet files for the specified split
        let split_files: Vec<_> = files
            .siblings
            .iter()
            .filter(|s| {
                s.rfilename.ends_with(".parquet") && matches_split(&s.rfilename, &self.split)
            })
            .map(|s| s.rfilename.clone())
            .collect();

        if !split_files.is_empty() {
            info!(
                "Found {} parquet files for split '{}'",
                split_files.len(),
                self.split
            );
            return Ok(split_files);
        }

        let all_parquet: Vec<_> = files
            .siblings
            .iter()
            .filter(|s| s.rfilename.ends_with(".parquet"))
            .map(|s| s.rfilename.clone())
            .collect();

        if all_parquet.is_empty() {
            anyhow::bail!(
                "No parquet files found in dataset {}. Available files: {:?}",
                self.dataset_id,
                files
                    .siblings
                    .iter()
                    .map(|s| &s.rfilename)
                    .collect::<Vec<_>>()
            );
        }

        anyhow::bail!(
            "No parquet files found for requested split '{}' in dataset {}. \
             Available parquet files: {:?}",
            self.split,
            self.dataset_id,
            all_parquet
        )
    }

    /// Download the specified files from the repository.
    async fn download_files(
        &self,
        repo_api: &hf_hub::api::tokio::ApiRepo,
        filenames: &[String],
    ) -> Result<Vec<std::path::PathBuf>> {
        let mut downloaded = Vec::with_capacity(filenames.len());
        for filename in filenames {
            info!("Downloading {}...", filename);
            let path = repo_api
                .get(filename)
                .await
                .with_context(|| format!("Failed to download {}", filename))?;
            downloaded.push(path);
        }
        Ok(downloaded)
    }
}

#[async_trait]
impl DataSource for HuggingFaceSource {
    async fn schema(&self) -> Result<SchemaRef> {
        let mut cached = self.cached_schema.lock().await;
        if let Some(ref schema) = *cached {
            return Ok(schema.clone());
        }

        let files = self.ensure_files_downloaded().await?;
        let first_file = files
            .first()
            .ok_or_else(|| anyhow::anyhow!("No files found"))?;

        let file = File::open(first_file)
            .await
            .context("Failed to open parquet file")?;

        let builder = ParquetRecordBatchStreamBuilder::new(file)
            .await
            .context("Failed to read parquet metadata")?;

        let schema = builder.schema().clone();
        *cached = Some(schema.clone());
        Ok(schema)
    }

    async fn total_rows(&self) -> Result<Option<u64>> {
        let files = self.ensure_files_downloaded().await?;
        let mut total = 0u64;

        for file_path in &files {
            let file = File::open(file_path)
                .await
                .context("Failed to open parquet file")?;

            let builder = ParquetRecordBatchStreamBuilder::new(file)
                .await
                .context("Failed to read parquet metadata")?;

            let metadata = builder.metadata();
            for rg in metadata.row_groups() {
                total += rg.num_rows() as u64;
            }
        }

        Ok(Some(total))
    }

    fn stream_batches(&self, batch_size: usize) -> BoxStream<'_, Result<RecordBatch>> {
        let source = self.clone_for_stream();

        Box::pin(async_stream::try_stream! {
            let files = source.ensure_files_downloaded().await?;
            let mut total_rows = 0usize;

            for file_path in files {
                let file = File::open(&file_path)
                    .await
                    .context("Failed to open parquet file")?;

                let builder = ParquetRecordBatchStreamBuilder::new(file)
                    .await
                    .context("Failed to read parquet file")?;

                let mut stream = builder
                    .with_batch_size(batch_size)
                    .build()
                    .context("Failed to build parquet stream")?;

                while let Some(batch_result) = stream.next().await {
                    let batch = batch_result.context("Failed to read batch")?;
                    total_rows += batch.num_rows();
                    yield batch;
                }
            }

            info!("Streamed {} total rows", total_rows);
        })
    }
}

impl HuggingFaceSource {
    fn clone_for_stream(&self) -> Self {
        Self {
            dataset_id: self.dataset_id.clone(),
            split: self.split.clone(),
            parquet_files: self.parquet_files.clone(),
            cached_schema: self.cached_schema.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::TryStreamExt;

    #[test]
    fn split_matching_respects_path_and_name_boundaries() {
        for filename in [
            "data/train-00000.parquet",
            "plain_text/train-00000-of-00001.parquet",
            "some/configuration/data/train-00000.parquet",
            "train/data.parquet",
            "train-00000.parquet",
            "some/path/train/file.parquet",
            "train.parquet",
            "plain_text/train.parquet",
        ] {
            assert!(matches_split(filename, "train"), "{filename}");
        }
        for filename in [
            "plain_text/test-00000-of-00001.parquet",
            "plain_text/unsupervised-00000-of-00001.parquet",
            "plain_text/training-00000.parquet",
            "plain_text/pretrain-00000.parquet",
            "plain_text/train_extra.parquet",
            "training/file.parquet",
            "train-configuration/test-00000.parquet",
            "data/part-00000.parquet",
        ] {
            assert!(!matches_split(filename, "train"), "{filename}");
        }
        assert!(!matches_split("train.parquet", ""));
    }

    #[tokio::test]
    async fn unmatched_split_is_an_error() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let app = axum::Router::new().fallback(|| async {
            axum::Json(serde_json::json!({"sha":"fixture", "siblings":[
                {"rfilename":"README.md"},
                {"rfilename":"plain_text/test-00000.parquet"},
                {"rfilename":"plain_text/unsupervised-00000.parquet"},
                {"rfilename":"data/part-00000.parquet"}
            ]}))
        });
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let dir = tempfile::tempdir().unwrap();
        let api = hf_hub::api::tokio::ApiBuilder::new()
            .with_endpoint(endpoint)
            .with_cache_dir(dir.path().join("cache"))
            .with_token(None)
            .build()
            .unwrap();
        let repo = api.repo(Repo::dataset("org/dataset".into()));
        for split in ["train", "typo", ""] {
            let source = HuggingFaceSource::new("org/dataset".into(), split.into()).unwrap();
            let error = source.discover_parquet_files(&repo).await.unwrap_err();
            let message = error.to_string();
            assert!(
                message.contains(&format!("requested split '{split}'")),
                "{message}"
            );
            assert!(message.contains("org/dataset"), "{message}");
            assert!(
                message.contains("plain_text/test-00000.parquet"),
                "{message}"
            );
            assert!(message.contains("data/part-00000.parquet"), "{message}");
        }
        server.abort();
    }

    #[tokio::test]
    async fn discovers_configuration_prefixed_split_and_streams_cached_parquet() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let app = axum::Router::new().fallback(|| async {
            axum::Json(serde_json::json!({"sha":"fixture", "siblings":[
                {"rfilename":"README.md"},
                {"rfilename":"plain_text/test-00000-of-00001.parquet"},
                {"rfilename":"plain_text/train-00000-of-00002.parquet"},
                {"rfilename":"plain_text/unsupervised-00000-of-00001.parquet"},
                {"rfilename":"plain_text/train-00001-of-00002.parquet"},
                {"rfilename":"plain_text/training-00000.parquet"},
                {"rfilename":"plain_text/train-not-parquet.txt"}
            ]}))
        });
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let dir = tempfile::tempdir().unwrap();
        let api = hf_hub::api::tokio::ApiBuilder::new()
            .with_endpoint(endpoint)
            .with_cache_dir(dir.path().join("cache"))
            .with_token(None)
            .build()
            .unwrap();
        let source = HuggingFaceSource::new("org/dataset".into(), "train".into()).unwrap();
        assert_eq!(
            source
                .discover_parquet_files(&api.repo(Repo::dataset("org/dataset".into())))
                .await
                .unwrap(),
            [
                "plain_text/train-00000-of-00002.parquet",
                "plain_text/train-00001-of-00002.parquet"
            ]
        );
        server.abort();

        let batch = RecordBatch::try_from_iter([(
            "id",
            Arc::new(arrow_array::Int32Array::from(vec![1, 2, 3])) as arrow_array::ArrayRef,
        )])
        .unwrap();
        let path = dir.path().join("train.parquet");
        let mut writer = parquet::arrow::ArrowWriter::try_new(
            std::fs::File::create(&path).unwrap(),
            batch.schema(),
            None,
        )
        .unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        *source.parquet_files.lock().await = Some(vec![path]);
        assert_eq!(source.total_rows().await.unwrap(), Some(3));
        assert_eq!(source.schema().await.unwrap(), batch.schema());
        let batches: Vec<_> = source.stream_batches(2).try_collect().await.unwrap();
        assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 3);
        assert!(batches.iter().all(|batch| batch.num_rows() <= 2));
    }
}
