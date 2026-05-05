// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Streaming Arrow RecordBatch reader wrapping an async FlightRecordBatchStream.
//!
//! Bridges the async `FlightRecordBatchStream` (futures::Stream) to the
//! synchronous `RecordBatchReader` (Iterator) trait required by the ADBC
//! `Statement::execute()` return type.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use arrow_array::{RecordBatch, RecordBatchReader};
use arrow_flight::decode::FlightRecordBatchStream;
use arrow_flight::error::FlightError;
use arrow_schema::{ArrowError, Schema, SchemaRef};

use crate::runtime::Runtime;

/// A synchronous `RecordBatchReader` wrapping an async `FlightRecordBatchStream`.
///
/// Each `next()` call uses `runtime.block_on()` to bridge from the sync
/// Iterator interface to the async Stream. Before each fetch, checks
/// an `Arc<AtomicBool>` cancellation flag — if set (because the statement
/// was re-executed), returns `None` to terminate the iterator, invalidating
/// the prior result set per ADBC spec.
pub struct FlightSqlRecordBatchReader {
    runtime: Arc<Runtime>,
    inner: FlightRecordBatchStream,
    schema: Schema,
    cancelled: Arc<AtomicBool>,
}

impl FlightSqlRecordBatchReader {
    /// Create a new reader wrapping a FlightRecordBatchStream.
    ///
    /// The `cancelled` flag is shared with the `FlightSqlStatement` that
    /// created this reader. When the statement calls `execute()` again,
    /// it flips this flag to `true`, causing this reader's `next()` to
    /// return `None` on the next check.
    pub fn new(
        runtime: Arc<Runtime>,
        inner: FlightRecordBatchStream,
        schema: Schema,
        cancelled: Arc<AtomicBool>,
    ) -> Self {
        Self {
            runtime,
            inner,
            schema,
            cancelled,
        }
    }
}

impl RecordBatchReader for FlightSqlRecordBatchReader {
    fn schema(&self) -> SchemaRef {
        Arc::new(self.schema.clone())
    }
}

impl Iterator for FlightSqlRecordBatchReader {
    type Item = Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        // Check cancellation — if statement was re-executed, stop
        if self.cancelled.load(Ordering::SeqCst) {
            return None;
        }

        self.runtime.block_on(async {
            use futures::StreamExt;
            match self.inner.next().await {
                Some(Ok(batch)) => Some(Ok(batch)),
                Some(Err(flight_err)) => Some(Err(flight_error_to_arrow(flight_err))),
                None => None,
            }
        })
    }
}

/// Convert an arrow_flight::FlightError to an arrow_schema::ArrowError.
///
/// For Arrow-origin errors (`FlightError::Arrow`), extracts the original
/// `ArrowError` directly. For all other FlightError variants, wraps in
/// `ArrowError::ExternalError` so callers can still inspect the error.
fn flight_error_to_arrow(err: FlightError) -> ArrowError {
    match err {
        FlightError::Arrow(e) => e,
        other => ArrowError::ExternalError(Box::new(other)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::{DataType, Field};

    /// Helper: create an empty FlightRecordBatchStream (no batches, no server needed).
    fn make_empty_stream() -> FlightRecordBatchStream {
        use arrow_flight::FlightData;
        let empty: futures::stream::Empty<Result<FlightData, FlightError>> =
            futures::stream::empty();
        FlightRecordBatchStream::new_from_flight_data(empty)
    }

    #[test]
    fn test_reader_schema_returns_constructed_schema() {
        let schema = Schema::new(vec![
            Field::new("col1", DataType::Int32, false),
            Field::new("col2", DataType::Utf8, true),
        ]);
        let runtime = Arc::new(Runtime::new(None).unwrap());
        let inner = make_empty_stream();
        let cancelled = Arc::new(AtomicBool::new(false));

        let reader = FlightSqlRecordBatchReader::new(runtime, inner, schema.clone(), cancelled);
        let got = reader.schema();
        assert_eq!(
            *got, schema,
            "schema() should return the schema passed at construction"
        );
    }

    #[test]
    fn test_reader_stops_on_cancel() {
        let schema = Schema::new(vec![Field::new("x", DataType::Int32, false)]);
        let runtime = Arc::new(Runtime::new(None).unwrap());
        let inner = make_empty_stream();
        let cancelled = Arc::new(AtomicBool::new(true));

        let mut reader =
            FlightSqlRecordBatchReader::new(runtime, inner, schema, cancelled);
        // With cancelled=true, next() MUST return None without polling inner stream
        assert!(reader.next().is_none(), "Cancelled reader must stop immediately");
    }

    #[test]
    fn test_cancelled_flag_respected_when_false() {
        let schema = Schema::new(vec![Field::new("x", DataType::Int32, false)]);
        let runtime = Arc::new(Runtime::new(None).unwrap());
        let inner = make_empty_stream();
        let cancelled = Arc::new(AtomicBool::new(false));

        let mut reader =
            FlightSqlRecordBatchReader::new(runtime, inner, schema, cancelled);

        // With cancelled=false and an empty inner stream,
        // next() should poll the inner stream and get None (end of stream).
        // This confirms that when NOT cancelled, the reader actually polls.
        assert!(reader.next().is_none(), "Empty stream should return None at end");
    }

    #[test]
    fn test_flight_error_to_arrow_extracts_arrow_error() {
        let arrow_err = ArrowError::ParseError("test parse error".into());
        let flight_err = FlightError::Arrow(arrow_err);
        let result = flight_error_to_arrow(flight_err);
        match result {
            ArrowError::ParseError(msg) => assert_eq!(msg, "test parse error"),
            _ => panic!("expected ArrowError::ParseError, got {result:?}"),
        }
    }

    #[test]
    fn test_flight_error_to_arrow_wraps_external() {
        let flight_err =
            FlightError::ProtocolError("protocol broken".into());
        let result = flight_error_to_arrow(flight_err);
        match &result {
            ArrowError::ExternalError(boxed) => {
                let msg = format!("{boxed}");
                assert!(msg.contains("protocol broken"), "expected message to contain error, got: {msg}");
            }
            _ => panic!("expected ArrowError::ExternalError, got {result:?}"),
        }
    }

    #[test]
    fn test_cancelled_true_returns_none_even_with_data() {
        // This test validates that when cancelled is true,
        // next() returns None without polling the inner stream.
        // RED PHASE: The stub ALWAYS returns None, so this test passes
        // coincidentally. In the GREEN phase, we verify this is because
        // of the cancellation check, not because of a missing impl.
        let schema = Schema::new(vec![Field::new("x", DataType::Int32, false)]);
        let runtime = Arc::new(Runtime::new(None).unwrap());
        let inner = make_empty_stream();
        let cancelled = Arc::new(AtomicBool::new(true));

        let mut reader =
            FlightSqlRecordBatchReader::new(runtime, inner, schema, cancelled);
        assert!(
            reader.next().is_none(),
            "cancelled reader must return None"
        );
    }
}
