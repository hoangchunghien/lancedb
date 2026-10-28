// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! Schema inference and transformation utilities.

use anyhow::{Context, Result};
use arrow_array::{new_null_array, Array, ArrayRef, FixedSizeListArray, ListArray, RecordBatch};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use arrow_select::concat::concat;
use std::collections::HashMap;
use std::sync::Arc;
use tracing::info;

/// Information about detected vector columns that need transformation.
#[derive(Debug, Clone)]
pub struct VectorColumnInfo {
    pub name: String,
    pub dims: i32,
    pub inner_type: DataType,
}

/// Detect all List columns with float elements that look like vectors.
/// Returns a map of column name -> dimensions.
pub fn detect_vector_columns(batch: &RecordBatch) -> HashMap<String, VectorColumnInfo> {
    let mut vector_columns = HashMap::new();

    for (idx, field) in batch.schema().fields().iter().enumerate() {
        if let Some(info) = detect_vector_column_info(field, batch.column(idx)) {
            info!(
                "Auto-detected vector column '{}': {} dimensions, {:?}",
                info.name, info.dims, info.inner_type
            );
            vector_columns.insert(info.name.clone(), info);
        }
    }

    vector_columns
}

/// Detect vector columns using only the schema. Picks up `FixedSizeList<Float>`
/// columns (dims live in the data type) but skips variable-length `List<Float>`
/// columns since their dims aren't in the schema.
pub fn detect_vector_columns_from_schema(schema: &Schema) -> HashMap<String, VectorColumnInfo> {
    let mut vector_columns = HashMap::new();

    for field in schema.fields().iter() {
        if let DataType::FixedSizeList(inner, dims) = field.data_type() {
            if matches!(
                inner.data_type(),
                DataType::Float16 | DataType::Float32 | DataType::Float64
            ) {
                let info = VectorColumnInfo {
                    name: field.name().clone(),
                    dims: *dims,
                    inner_type: inner.data_type().clone(),
                };
                info!(
                    "Auto-detected vector column '{}' from schema: {} dimensions, {:?}",
                    info.name, info.dims, info.inner_type
                );
                vector_columns.insert(info.name.clone(), info);
            }
        }
    }

    vector_columns
}

/// Check if a field/column looks like a vector column.
fn detect_vector_column_info(field: &Field, column: &ArrayRef) -> Option<VectorColumnInfo> {
    match field.data_type() {
        DataType::List(inner) | DataType::LargeList(inner) => {
            // Check if inner type is float
            match inner.data_type() {
                DataType::Float16 | DataType::Float32 | DataType::Float64 => {
                    // Get dimensions from first non-null element
                    if let Some(list_array) = column.as_any().downcast_ref::<ListArray>() {
                        for i in 0..list_array.len() {
                            if !list_array.is_null(i) {
                                let dims = list_array.value(i).len() as i32;
                                if dims > 0 {
                                    return Some(VectorColumnInfo {
                                        name: field.name().clone(),
                                        dims,
                                        inner_type: inner.data_type().clone(),
                                    });
                                }
                            }
                        }
                    }
                }
                _ => {}
            }
        }
        DataType::FixedSizeList(inner, dims) => {
            // Already a FixedSizeList with float - still a vector column
            match inner.data_type() {
                DataType::Float16 | DataType::Float32 | DataType::Float64 => {
                    return Some(VectorColumnInfo {
                        name: field.name().clone(),
                        dims: *dims,
                        inner_type: inner.data_type().clone(),
                    });
                }
                _ => {}
            }
        }
        _ => {}
    }
    None
}

/// Transform a schema to convert List columns to FixedSizeList for all detected vector columns.
pub fn transform_schema(
    schema: &Schema,
    vector_columns: &HashMap<String, VectorColumnInfo>,
) -> Result<SchemaRef> {
    let fields: Vec<Field> = schema
        .fields()
        .iter()
        .map(|field| {
            if let Some(info) = vector_columns.get(field.name()) {
                // Convert to FixedSizeList
                let inner_field = Arc::new(Field::new("item", info.inner_type.clone(), true));
                Field::new(
                    field.name(),
                    DataType::FixedSizeList(inner_field, info.dims),
                    field.is_nullable(),
                )
            } else {
                field.as_ref().clone()
            }
        })
        .collect();

    Ok(Arc::new(Schema::new(fields)))
}

/// Transform a record batch to convert List columns to FixedSizeList for all vector columns.
/// Preserves row boundaries and nulls, and rejects non-null lists whose lengths
/// differ from the target dimension. Row numbers in errors are relative to this batch.
pub fn transform_batch(
    batch: &RecordBatch,
    target_schema: &SchemaRef,
    vector_columns: &HashMap<String, VectorColumnInfo>,
) -> Result<RecordBatch> {
    let columns: Vec<ArrayRef> = batch
        .schema()
        .fields()
        .iter()
        .zip(batch.columns())
        .map(|(field, col)| {
            if vector_columns.contains_key(field.name()) {
                convert_to_fixed_size_list(col, target_schema, field.name())
            } else {
                Ok(col.clone())
            }
        })
        .collect::<Result<_>>()?;

    RecordBatch::try_new(target_schema.clone(), columns)
        .context("Failed to create transformed batch")
}

/// Convert a List array to a FixedSizeList array.
fn convert_to_fixed_size_list(
    array: &ArrayRef,
    target_schema: &SchemaRef,
    column_name: &str,
) -> Result<ArrayRef> {
    let target_field = target_schema
        .field_with_name(column_name)
        .context("Column not found in target schema")?;

    let (inner_field, dims) = match target_field.data_type() {
        DataType::FixedSizeList(inner, d) => (inner, *d),
        _ => return Ok(array.clone()), // Not a FixedSizeList target, return as-is
    };

    match array.data_type() {
        DataType::List(_) => {
            let list_array = array
                .as_any()
                .downcast_ref::<ListArray>()
                .ok_or_else(|| anyhow::anyhow!("Failed to downcast to ListArray"))?;

            match list_array.values().data_type() {
                DataType::Float16 | DataType::Float32 | DataType::Float64 => {}
                other => {
                    anyhow::bail!("Unsupported vector element type: {:?}", other);
                }
            }

            let size = usize::try_from(dims).context("Vector dimension must be non-negative")?;
            for row in 0..list_array.len() {
                if list_array.is_valid(row) {
                    let actual = list_array.value_length(row) as usize;
                    if actual != size {
                        anyhow::bail!(
                            "Column '{column_name}', row {row}: expected vector dimension \
                             {dims}, got {actual}"
                        );
                    }
                }
            }

            let values = if list_array.null_count() == 0 {
                // A sliced list retains its original child buffer. Restrict the
                // values to the logical rows after validating their boundaries.
                let start = list_array.value_offsets()[0] as usize;
                list_array.values().slice(start, list_array.len() * size)
            } else {
                // Null lists can cover any number of child values. Give them
                // fixed-width null slots instead of using their hidden values.
                let rows: Vec<ArrayRef> = (0..list_array.len())
                    .map(|row| {
                        if list_array.is_null(row) {
                            new_null_array(list_array.values().data_type(), size)
                        } else {
                            list_array.value(row)
                        }
                    })
                    .collect();
                let rows: Vec<&dyn Array> = rows.iter().map(|row| row.as_ref()).collect();
                concat(&rows)?
            };

            let fsl = FixedSizeListArray::try_new_with_length(
                inner_field.clone(),
                dims,
                values,
                list_array.nulls().cloned(),
                list_array.len(),
            )
            .with_context(|| {
                format!("Failed to convert column '{column_name}' to FixedSizeList")
            })?;
            Ok(Arc::new(fsl))
        }
        DataType::FixedSizeList(_, _) => {
            // Already a FixedSizeList, return as-is
            Ok(array.clone())
        }
        other => {
            anyhow::bail!(
                "Cannot convert {:?} to FixedSizeList for column '{}'",
                other,
                column_name
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Float16Array, Float32Array, Float64Array, Int64Array, StringArray};
    use half::f16;

    struct VectorColumnTestCase {
        name: &'static str,
        inner_type: DataType,
        values: ArrayRef,
        expected_detected: bool,
    }

    fn make_list_array(inner_type: DataType, values: ArrayRef) -> ListArray {
        let offsets = arrow_buffer::OffsetBuffer::from_lengths([3, 3]);
        let field = Arc::new(Field::new("item", inner_type, true));
        ListArray::new(field, offsets, values, None)
    }

    fn vector_batch(lists: ListArray) -> RecordBatch {
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int64, false),
                Field::new("vector", lists.data_type().clone(), true),
            ])),
            vec![
                Arc::new(Int64Array::from_iter_values(0..lists.len() as i64)),
                Arc::new(lists),
            ],
        )
        .unwrap()
    }

    fn float_lists(lengths: &[usize], values: Vec<Option<f32>>) -> ListArray {
        ListArray::new(
            Arc::new(Field::new("item", DataType::Float32, true)),
            arrow_buffer::OffsetBuffer::from_lengths(lengths.iter().copied()),
            Arc::new(Float32Array::from(values)),
            None,
        )
    }

    fn transform_vectors(batch: &RecordBatch) -> Result<RecordBatch> {
        let vectors = detect_vector_columns(batch);
        let schema = transform_schema(&batch.schema(), &vectors)?;
        transform_batch(batch, &schema, &vectors)
    }

    #[test]
    fn rejects_ragged_and_empty_non_null_vectors() {
        for (lengths, values, expected) in [
            (vec![2, 1, 3], vec![1., 2., 3., 4., 5., 6.], 1),
            (vec![2, 0], vec![1., 2.], 0),
        ] {
            let batch = vector_batch(float_lists(
                &lengths,
                values.into_iter().map(Some).collect(),
            ));
            let error = transform_vectors(&batch).unwrap_err();
            assert_eq!(
                error.to_string(),
                format!("Column 'vector', row 1: expected vector dimension 2, got {expected}")
            );
        }
    }

    #[test]
    fn sliced_vectors_preserve_offsets_and_row_ids() {
        let batch = vector_batch(float_lists(
            &[1, 2, 2, 1],
            vec![Some(99.), Some(1.), None, Some(3.), Some(4.), Some(88.)],
        ))
        .slice(1, 2);
        let transformed = transform_vectors(&batch).unwrap();
        assert_eq!(transformed.column(0), batch.column(0));
        let vectors = transformed
            .column(1)
            .as_any()
            .downcast_ref::<FixedSizeListArray>()
            .unwrap();
        assert_eq!(vectors.len(), 2);
        assert_eq!(
            vectors.values().as_ref(),
            &Float32Array::from(vec![Some(1.), None, Some(3.), Some(4.)])
        );
    }

    #[test]
    fn sliced_nullable_vectors_preserve_parent_and_child_nulls() {
        for hidden_len in [0, 2, 3] {
            let mut values = vec![Some(99.), Some(1.), None];
            values.extend(vec![Some(123.); hidden_len]);
            values.extend([Some(3.), Some(4.), Some(88.)]);
            let lists = float_lists(&[1, 2, hidden_len, 2, 1], values);
            let lists = ListArray::new(
                Arc::new(Field::new("item", DataType::Float32, true)),
                lists.offsets().clone(),
                lists.values().clone(),
                Some(arrow_buffer::NullBuffer::from(vec![
                    false, true, false, true, false,
                ])),
            );
            let batch = vector_batch(lists).slice(1, 3);
            let transformed = transform_vectors(&batch).unwrap();
            assert_eq!(transformed.column(0), batch.column(0));
            let vectors = transformed
                .column(1)
                .as_any()
                .downcast_ref::<FixedSizeListArray>()
                .unwrap();
            assert_eq!(vectors.len(), 3);
            assert!(vectors.is_valid(0));
            assert!(vectors.is_null(1));
            assert!(vectors.is_valid(2));
            assert_eq!(
                vectors.values().as_ref(),
                &Float32Array::from(vec![Some(1.), None, None, None, Some(3.), Some(4.)])
            );
        }
    }

    #[test]
    fn validates_dimensions_in_later_batches() {
        let first = vector_batch(float_lists(&[2], vec![Some(1.), Some(2.)]));
        let vectors = detect_vector_columns(&first);
        let schema = transform_schema(&first.schema(), &vectors).unwrap();
        transform_batch(&first, &schema, &vectors).unwrap();

        let next = vector_batch(float_lists(&[3], vec![Some(3.), Some(4.), Some(5.)]));
        let error = transform_batch(&next, &schema, &vectors).unwrap_err();
        assert_eq!(
            error.to_string(),
            "Column 'vector', row 0: expected vector dimension 2, got 3"
        );
    }

    #[test]
    fn handles_all_null_and_empty_later_batches() {
        let first = vector_batch(float_lists(&[2], vec![Some(1.), Some(2.)]));
        let vectors = detect_vector_columns(&first);
        let schema = transform_schema(&first.schema(), &vectors).unwrap();
        let lists = float_lists(&[0, 3, 1], vec![Some(123.); 4]);
        let nulls = ListArray::new(
            Arc::new(Field::new("item", DataType::Float32, true)),
            lists.offsets().clone(),
            lists.values().clone(),
            Some(arrow_buffer::NullBuffer::new_null(3)),
        );
        for lists in [nulls, float_lists(&[], vec![])] {
            let batch = vector_batch(lists);
            let transformed = transform_batch(&batch, &schema, &vectors).unwrap();
            assert_eq!(transformed.num_rows(), batch.num_rows());
            assert_eq!(transformed.column(1).null_count(), batch.num_rows());
        }
    }

    #[test]
    fn test_detect_vector_columns() {
        let test_cases = vec![
            VectorColumnTestCase {
                name: "Float16 vector",
                inner_type: DataType::Float16,
                values: Arc::new(Float16Array::from(vec![
                    f16::from_f32(1.0),
                    f16::from_f32(2.0),
                    f16::from_f32(3.0),
                    f16::from_f32(4.0),
                    f16::from_f32(5.0),
                    f16::from_f32(6.0),
                ])),
                expected_detected: true,
            },
            VectorColumnTestCase {
                name: "Float32 vector",
                inner_type: DataType::Float32,
                values: Arc::new(Float32Array::from(vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0])),
                expected_detected: true,
            },
            VectorColumnTestCase {
                name: "Float64 vector",
                inner_type: DataType::Float64,
                values: Arc::new(Float64Array::from(vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0])),
                expected_detected: true,
            },
            VectorColumnTestCase {
                name: "String list (not a vector)",
                inner_type: DataType::Utf8,
                values: Arc::new(StringArray::from(vec!["a", "b", "c", "d", "e", "f"])),
                expected_detected: false,
            },
            VectorColumnTestCase {
                name: "Int64 list (not a vector)",
                inner_type: DataType::Int64,
                values: Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5, 6])),
                expected_detected: false,
            },
        ];

        for case in test_cases {
            let list_array = make_list_array(case.inner_type.clone(), case.values);

            let schema = Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int64, false),
                Field::new(
                    "vector",
                    DataType::List(Arc::new(Field::new("item", case.inner_type.clone(), true))),
                    false,
                ),
            ]));

            let batch = RecordBatch::try_new(
                schema,
                vec![Arc::new(Int64Array::from(vec![1, 2])), Arc::new(list_array)],
            )
            .unwrap();

            let vector_columns = detect_vector_columns(&batch);

            if case.expected_detected {
                assert_eq!(
                    vector_columns.len(),
                    1,
                    "{}: expected 1 vector column",
                    case.name
                );
                assert!(
                    vector_columns.contains_key("vector"),
                    "{}: expected 'vector' column",
                    case.name
                );
                assert_eq!(
                    vector_columns["vector"].dims, 3,
                    "{}: expected 3 dimensions",
                    case.name
                );
                assert_eq!(
                    vector_columns["vector"].inner_type, case.inner_type,
                    "{}: inner type mismatch",
                    case.name
                );
                let transformed = transform_vectors(&batch).unwrap();
                let vectors = transformed
                    .column(1)
                    .as_any()
                    .downcast_ref::<FixedSizeListArray>()
                    .unwrap();
                let original = batch
                    .column(1)
                    .as_any()
                    .downcast_ref::<ListArray>()
                    .unwrap();
                assert_eq!(vectors.len(), original.len());
                for row in 0..original.len() {
                    assert_eq!(vectors.value(row).as_ref(), original.value(row).as_ref());
                }
            } else {
                assert!(
                    vector_columns.is_empty(),
                    "{}: expected no vector columns detected",
                    case.name
                );
            }
        }
    }
}
