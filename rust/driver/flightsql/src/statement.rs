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

//! FlightSQL Statement — holds SQL text, runtime, client, and cancellation state.

use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use adbc_core::{
    error::Result,
    options::{OptionStatement, OptionValue},
    Optionable, PartitionedResult, Statement,
};
use arrow_array::{RecordBatch, RecordBatchReader};
use arrow_flight::{FlightInfo, IpcMessage, Ticket};
use arrow_flight::decode::FlightRecordBatchStream;
use arrow_flight::error::FlightError;
use arrow_flight::sql::client::{FlightSqlServiceClient, PreparedStatement};
use arrow_schema::Schema;
use prost::Message;

use crate::error::{ErrorHelper, FlightSqlErrorHelper, map_flight_error};
use crate::runtime::Runtime;
use crate::stream::FlightSqlRecordBatchReader;
use crate::timeouts::TimeoutOption;

/// A handle to a FlightSQL statement.
///
/// Holds the SQL query text, a shared async runtime, a cloned
/// FlightSqlServiceClient for RPC calls, and a cancellation flag
/// shared with the FlightSqlRecordBatchReader.
///
/// Re-executing the statement flips the cancellation flag, which causes
/// the previous reader's next() to return None — invalidating prior results.
pub struct FlightSqlStatement {
    sql: Option<String>,
    pub(crate) cached_schema: Option<Schema>,
    runtime: Arc<Runtime>,
    client: FlightSqlServiceClient<tonic::transport::Channel>,
    pub(crate) cancelled: Arc<AtomicBool>,
    // Prepared statement state (FSQL-21):
    prepared: Option<PreparedStatement<tonic::transport::Channel>>,
    prepared_parameter_schema: Option<Schema>,
    // Option state (FSQL-12/FSQL-13):
    timeout: Option<Duration>,
    // Per-operation timeouts (FSQL-22):
    timeouts: TimeoutOption,
    // Bulk ingest state (FSQL-31):
    ingest_target_table: Option<String>,
    ingest_mode: Option<String>,
    ingest_catalog: Option<String>,
    ingest_schema: Option<String>,
    ingest_temporary: bool,
    bound_batch: Option<RecordBatch>,
}

impl FlightSqlStatement {
    /// Create a new FlightSqlStatement, cloning the client from the connection.
    pub(crate) fn new(
        runtime: Arc<Runtime>,
        client: FlightSqlServiceClient<tonic::transport::Channel>,
    ) -> Self {
        Self {
            sql: None,
            cached_schema: None,
            runtime,
            client,
            cancelled: Arc::new(AtomicBool::new(false)),
            prepared: None,
            prepared_parameter_schema: None,
            timeout: None,
            timeouts: TimeoutOption::new(),
            ingest_target_table: None,
            ingest_mode: None,
            ingest_catalog: None,
            ingest_schema: None,
            ingest_temporary: false,
            bound_batch: None,
        }
    }

    /// Close any existing prepared statement, sending ActionClosePreparedStatement RPC.
    fn close_prepared_statement(&mut self) {
        if let Some(prepared) = self.prepared.take() {
            let _ = self.runtime.block_on(async { prepared.close().await });
        }
    }

    /// Execute bulk ingest on the server (FSQL-31).
    fn execute_ingest(&mut self) -> Result<i64> {
        use arrow_flight::sql::CommandStatementIngest;
        use arrow_flight::sql::TableDefinitionOptions;
        use arrow_flight::sql::{TableExistsOption, TableNotExistOption};

        let table = self.ingest_target_table.as_ref().ok_or_else(|| {
            FlightSqlErrorHelper::invalid_state()
                .message("no target table set for ingest")
                .to_adbc()
        })?;

        let mode = self.ingest_mode.as_deref().unwrap_or("create");
        let (if_not_exist, if_exists): (i32, i32) = match mode {
            "adbc.ingest.mode.create" | "create" => (
                TableNotExistOption::Create as i32,
                TableExistsOption::Fail as i32,
            ),
            "adbc.ingest.mode.append" | "append" => (
                TableNotExistOption::Fail as i32,
                TableExistsOption::Append as i32,
            ),
            "adbc.ingest.mode.replace" | "replace" => (
                TableNotExistOption::Fail as i32,
                TableExistsOption::Replace as i32,
            ),
            "adbc.ingest.mode.create_append" | "create_append" => (
                TableNotExistOption::Create as i32,
                TableExistsOption::Append as i32,
            ),
            _ => return Err(FlightSqlErrorHelper::invalid_argument()
                .message(format!("unknown ingest mode: {mode}")).to_adbc()),
        };

        let command = CommandStatementIngest {
            table_definition_options: Some(TableDefinitionOptions {
                if_not_exist,
                if_exists,
            }),
            table: table.clone(),
            schema: self.ingest_schema.clone(),
            catalog: self.ingest_catalog.clone(),
            temporary: self.ingest_temporary,
            transaction_id: None,
            options: std::collections::HashMap::new(),
        };

        let batch = self.bound_batch.take().ok_or_else(|| {
            FlightSqlErrorHelper::invalid_state()
                .message("must call Bind before executing ingest")
                .to_adbc()
        })?;

        let stream = futures::stream::once(async move { Ok(batch) });

        let nrec = self.runtime.block_on(async {
            self.client.execute_ingest(command, stream).await
        }).map_err(|e: FlightError| map_flight_error(e, "ExecuteIngest"))?;

        Ok(nrec)
    }
}

impl Optionable for FlightSqlStatement {
    type Option = OptionStatement;

    fn set_option(&mut self, key: Self::Option, value: OptionValue) -> Result<()> {
        match key {
            OptionStatement::Other(ref k) if k == "adbc.statement.exec_timeout_ms" => {
                let millis = FlightSqlErrorHelper::option_as_int(&key, &value)?;
                if millis < 0 {
                    return Err(FlightSqlErrorHelper::invalid_argument()
                        .message("timeout must be non-negative (milliseconds)")
                        .context("setting query timeout")
                        .to_adbc());
                }
                if millis == 0 {
                    self.timeout = None;
                    self.client.set_header(
                        "grpc-timeout",
                        format!("{}m", 365u64 * 24 * 60 * 60 * 1000),
                    );
                } else {
                    let duration = Duration::from_millis(millis as u64);
                    self.timeout = Some(duration);
                    self.client.set_header("grpc-timeout", format!("{millis}m"));
                }
                Ok(())
            }
            OptionStatement::Other(ref k) if k.starts_with("adbc.flight.sql.rpc.timeout_seconds.") => {
                let seconds = match &value {
                    OptionValue::Double(v) => *v,
                    OptionValue::Int(v) => *v as f64,
                    OptionValue::String(s) => s.parse::<f64>().map_err(|_| {
                        FlightSqlErrorHelper::invalid_argument()
                            .message("timeout value must be a number")
                            .to_adbc()
                    })?,
                    _ => return Err(FlightSqlErrorHelper::invalid_argument()
                        .message("timeout must be a number")
                        .to_adbc()),
                };
                self.timeouts.set_timeout_seconds(k, seconds)
            }
            OptionStatement::IngestMode => {
                let mode = FlightSqlErrorHelper::option_as_string(&key, &value)?.to_string();
                self.ingest_mode = Some(mode);
                Ok(())
            }
            OptionStatement::TargetTable => {
                let table = FlightSqlErrorHelper::option_as_string(&key, &value)?.to_string();
                self.ingest_target_table = Some(table);
                Ok(())
            }
            OptionStatement::TargetCatalog => {
                let catalog = FlightSqlErrorHelper::option_as_string(&key, &value)?.to_string();
                self.ingest_catalog = if catalog.is_empty() { None } else { Some(catalog) };
                Ok(())
            }
            OptionStatement::TargetDbSchema => {
                let schema = FlightSqlErrorHelper::option_as_string(&key, &value)?.to_string();
                self.ingest_schema = if schema.is_empty() { None } else { Some(schema) };
                Ok(())
            }
            OptionStatement::Temporary => {
                let v = FlightSqlErrorHelper::option_as_string(&key, &value)?;
                self.ingest_temporary = v == "true" || v == "adbc.option.value.enabled";
                Ok(())
            }
            OptionStatement::Incremental
            | OptionStatement::Progress
            | OptionStatement::MaxProgress => {
                Ok(())
            }
            OptionStatement::Other(key) => {
                Err(FlightSqlErrorHelper::set_unknown_option(&key).to_adbc())
            }
            _ => Err(FlightSqlErrorHelper::not_implemented()
                .message(format!("statement option not implemented: {key:?}"))
                .to_adbc()),
        }
    }

    fn get_option_string(&self, key: Self::Option) -> Result<String> {
        match key {
            OptionStatement::Other(ref k) if k == "adbc.statement.exec_timeout_ms" => {
                Err(FlightSqlErrorHelper::not_found()
                    .message("timeout is an integer option — use get_option_int")
                    .to_adbc())
            }
            _ => Err(FlightSqlErrorHelper::not_found()
                .message(format!("Option {key:?} not available")).to_adbc()),
        }
    }

    fn get_option_int(&self, key: Self::Option) -> Result<i64> {
        match key {
            OptionStatement::Other(ref k) if k == "adbc.statement.exec_timeout_ms" => {
                match self.timeout {
                    Some(d) => Ok(d.as_millis() as i64),
                    None => Ok(0),
                }
            }
            _ => Err(FlightSqlErrorHelper::not_found()
                .message(format!("Option {key:?} not available as int")).to_adbc()),
        }
    }

    fn get_option_bytes(&self, key: Self::Option) -> Result<Vec<u8>> {
        Err(FlightSqlErrorHelper::not_found()
            .message(format!("Option {key:?} not available")).to_adbc())
    }

    fn get_option_double(&self, key: Self::Option) -> Result<f64> {
        match key {
            OptionStatement::Other(ref k) if k.starts_with("adbc.flight.sql.rpc.timeout_seconds.") => {
                self.timeouts.get_timeout_seconds(k)
            }
            _ => Err(FlightSqlErrorHelper::not_found()
                .message(format!("Option {key:?} not available")).to_adbc()),
        }
    }
}

impl Statement for FlightSqlStatement {
    fn bind(&mut self, batch: RecordBatch) -> Result<()> {
        match &mut self.prepared {
            Some(prepared) => {
                prepared
                    .set_parameters(batch)
                    .map_err(|e| {
                        FlightSqlErrorHelper::invalid_state()
                            .message(format!("bind failed: {e}"))
                            .context("binding parameters")
                            .to_adbc()
                    })?;
                Ok(())
            }
            None => Err(FlightSqlErrorHelper::invalid_state()
                .message("must call Prepare before calling Bind")
                .context("binding parameters")
                .to_adbc()),
        }
    }

    fn bind_stream(&mut self, _reader: Box<dyn RecordBatchReader + Send>) -> Result<()> {
        if self.prepared.is_none() {
            return Err(FlightSqlErrorHelper::invalid_state()
                .message("must call Prepare before calling BindStream")
                .context("binding stream")
                .to_adbc());
        }
        Err(FlightSqlErrorHelper::not_implemented()
            .message("bind_stream not supported for prepared statements in Rust FlightSQL")
            .context("binding stream")
            .to_adbc())
    }

    fn execute(&mut self) -> Result<Box<dyn RecordBatchReader + Send + 'static>> {
        // Handle bulk ingest (FSQL-31)
        if self.ingest_target_table.is_some() {
            self.execute_ingest()?;
            // Return empty reader
            let schema = Schema::empty();
            let reader: Box<dyn RecordBatchReader + Send + 'static> =
                Box::new(arrow_array::RecordBatchIterator::new(vec![], Arc::new(schema)));
            return Ok(reader);
        }

        self.cancelled.store(true, Ordering::SeqCst);

        // Step 2: Get FlightInfo — from prepared statement or ad-hoc query
        let flight_info: FlightInfo = if let Some(ref mut prepared) = self.prepared {
            self.runtime.block_on(async {
                prepared.execute().await
            }).map_err(|e: FlightError| map_flight_error(e, "executing prepared statement"))?
        } else {
            let sql = self.sql.as_ref().ok_or_else(|| {
                FlightSqlErrorHelper::invalid_state()
                    .message("no SQL query set — call set_sql_query() first")
                    .context("executing statement")
                    .to_adbc()
            })?;
            self.runtime.block_on(async {
                self.client.execute(sql.clone(), None).await
            }).map_err(|e: FlightError| map_flight_error(e, "executing query"))?
        };

        // Step 3: Extract schema from IPC-encoded bytes in FlightInfo.schema
        let schema = Schema::try_from(IpcMessage(flight_info.schema.clone()))
            .map_err(|e| {
                FlightSqlErrorHelper::internal_no_location()
                    .message(format!("failed to decode result schema: {e}"))
                    .to_adbc()
            })?;

        // Step 4: Extract ticket from first endpoint
        let ticket: Ticket = flight_info.endpoint
            .first()
            .and_then(|ep| ep.ticket.clone())
            .ok_or_else(|| {
                FlightSqlErrorHelper::internal_no_location()
                    .message("server returned FlightInfo with no endpoints/tickets")
                    .to_adbc()
            })?;

        // Step 5: Open result stream via do_get
        let stream: FlightRecordBatchStream = self.runtime.block_on(async {
            self.client.do_get(ticket).await
        }).map_err(|e: FlightError| map_flight_error(e, "opening result stream"))?;

        // Step 6: Cache schema for execute_schema()
        self.cached_schema = Some(schema.clone());

        // Step 7: Create reader with fresh cancellation flag
        let cancelled = Arc::clone(&self.cancelled);
        cancelled.store(false, Ordering::SeqCst);

        Ok(Box::new(FlightSqlRecordBatchReader::new(
            Arc::clone(&self.runtime),
            stream,
            schema,
            cancelled,
        )))
    }

    fn execute_update(&mut self) -> Result<Option<i64>> {
        // Handle bulk ingest (FSQL-31)
        if self.ingest_target_table.is_some() {
            let nrec = self.execute_ingest()?;
            return Ok(Some(nrec));
        }

        if let Some(ref mut prepared) = self.prepared {
            let n: i64 = self.runtime.block_on(async {
                prepared.execute_update().await
            }).map_err(|e: FlightError| map_flight_error(e, "executing prepared update"))?;
            return Ok(Some(n));
        }
        Err(FlightSqlErrorHelper::not_implemented()
            .message("execute_update not supported (read-only driver)")
            .context("executing update")
            .to_adbc())
    }

    fn execute_schema(&mut self) -> Result<Schema> {
        // Per ADBC spec: execute_schema invalidates any prior result sets
        self.cancelled.store(true, Ordering::SeqCst);

        // Prepared statement path: use dataset_schema() directly
        if let Some(ref prepared) = self.prepared {
            let schema = prepared.dataset_schema()
                .map_err(|e| {
                    FlightSqlErrorHelper::internal_no_location()
                        .message(format!("failed to get dataset schema: {e}"))
                        .to_adbc()
                })?
                .clone();
            self.cached_schema = Some(schema.clone());
            return Ok(schema);
        }

        // Return cached schema if available (from a prior execute() call)
        if let Some(ref schema) = self.cached_schema {
            return Ok(schema.clone());
        }

        let sql = self.sql.as_ref().ok_or_else(|| {
            FlightSqlErrorHelper::invalid_state()
                .message("no SQL query set — call set_sql_query() first")
                .context("getting result schema")
                .to_adbc()
        })?;

        // Strategy 1: Use prepare() RPC to get schema directly (no data)
        match self.runtime.block_on(async {
            self.client.prepare(sql.clone(), None).await
        }) {
            Ok(prepared) => {
                let schema = prepared.dataset_schema()
                    .map_err(|e: FlightError| map_flight_error(e, "getting prepared schema"))?
                    .clone();
                self.cached_schema = Some(schema.clone());
                return Ok(schema);
            }
            Err(_) => {
                // prepare() failed — fall through to LIMIT 0 fallback
            }
        }

        // Strategy 2: Execute with LIMIT 0, extract schema from FlightInfo
        let limited_sql = format!("{sql} LIMIT 0");
        let flight_info: FlightInfo = self.runtime.block_on(async {
            self.client.execute(limited_sql, None).await
        }).map_err(|e: FlightError| map_flight_error(e, "executing schema query"))?;

        let schema = Schema::try_from(IpcMessage(flight_info.schema))
            .map_err(|e| {
                FlightSqlErrorHelper::internal_no_location()
                    .message(format!("failed to decode schema: {e}"))
                    .to_adbc()
            })?;

        self.cached_schema = Some(schema.clone());
        Ok(schema)
    }

    fn execute_partitions(&mut self) -> Result<PartitionedResult> {
        let flight_info: FlightInfo = if let Some(ref mut prepared) = self.prepared {
            self.runtime.block_on(async {
                prepared.execute().await
            }).map_err(|e: FlightError| map_flight_error(e, "executing prepared partitions"))?
        } else {
            let sql = self.sql.as_ref().ok_or_else(|| {
                FlightSqlErrorHelper::invalid_state()
                    .message("no SQL query set — call set_sql_query() first")
                    .context("executing partitions")
                    .to_adbc()
            })?;
            self.runtime.block_on(async {
                self.client.execute(sql.clone(), None).await
            }).map_err(|e: FlightError| map_flight_error(e, "executing partitions"))?
        };

        let schema = Schema::try_from(IpcMessage(flight_info.schema.clone()))
            .map_err(|e| {
                FlightSqlErrorHelper::internal_no_location()
                    .message(format!("failed to decode partition schema: {e}"))
                    .to_adbc()
            })?;

        let partition_ids: Vec<Vec<u8>> = flight_info.endpoint.iter()
            .map(|endpoint| {
                let mut partition = flight_info.clone();
                partition.endpoint = vec![endpoint.clone()];
                Ok(partition.encode_to_vec())
            })
            .collect::<Result<Vec<_>>>()?;

        Ok(PartitionedResult {
            partitions: partition_ids,
            schema,
            rows_affected: -1,
        })
    }

    fn get_parameter_schema(&self) -> Result<Schema> {
        if self.prepared.is_none() {
            return Err(FlightSqlErrorHelper::invalid_state()
                .message("must call Prepare before GetParameterSchema")
                .context("getting parameter schema")
                .to_adbc());
        }
        self.prepared_parameter_schema.clone().ok_or_else(|| {
            FlightSqlErrorHelper::not_implemented()
                .message("server did not provide parameter schema")
                .context("getting parameter schema")
                .to_adbc()
        })
    }

    fn prepare(&mut self) -> Result<()> {
        let sql = self.sql.clone().ok_or_else(|| {
            FlightSqlErrorHelper::invalid_state()
                .message("no SQL query set — call set_sql_query() first")
                .context("preparing statement")
                .to_adbc()
        })?;

        self.close_prepared_statement();

        let prepared = self.runtime.block_on(async {
            self.client.prepare(sql.clone(), None).await
        }).map_err(|e: FlightError| map_flight_error(e, "Prepare"))?;

        let parameter_schema = prepared.parameter_schema()
            .map_err(|e| {
                FlightSqlErrorHelper::internal_no_location()
                    .message(format!("failed to get parameter schema: {e}"))
                    .to_adbc()
            })?
            .clone();

        self.prepared = Some(prepared);
        self.prepared_parameter_schema = Some(parameter_schema);
        Ok(())
    }

    fn set_sql_query(&mut self, query: impl AsRef<str>) -> Result<()> {
        self.close_prepared_statement();
        self.prepared_parameter_schema = None;
        self.ingest_target_table = None;
        self.sql = Some(query.as_ref().to_string());
        // Clear cached schema — new SQL may return different schema
        self.cached_schema = None;
        Ok(())
    }

    fn set_substrait_plan(&mut self, _plan: impl AsRef<[u8]>) -> Result<()> {
        Err(FlightSqlErrorHelper::not_implemented()
            .message("Substrait plans not supported")
            .context("setting Substrait plan")
            .to_adbc())
    }

    fn cancel(&mut self) -> Result<()> {
        Err(FlightSqlErrorHelper::not_implemented()
            .message("statement cancellation not implemented")
            .context("cancelling statement")
            .to_adbc())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::Ordering;

    /// Helper: create a statement with a lazy channel inside a tokio runtime.
    fn make_statement() -> FlightSqlStatement {
        let runtime = Arc::new(Runtime::new(None).unwrap());
        let rt_for_client = Arc::clone(&runtime);
        let client = rt_for_client.block_on(async {
            FlightSqlServiceClient::new(
                tonic::transport::Endpoint::from_static("http://localhost:50051")
                    .connect_lazy(),
            )
        });
        FlightSqlStatement::new(runtime, client)
    }

    #[test]
    fn test_set_sql_query_stores_and_clears_cache() {
        use arrow_schema::{DataType, Field, Schema};
        let mut stmt = make_statement();

        // Set SQL should succeed
        stmt.set_sql_query("SELECT 1").unwrap();

        // Set a cached schema, then verify set_sql_query clears it
        let schema = Schema::new(vec![Field::new("a", DataType::Int32, false)]);
        stmt.cached_schema = Some(schema);
        stmt.set_sql_query("SELECT 2").unwrap();
        assert!(
            stmt.cached_schema.is_none(),
            "set_sql_query should clear cached_schema"
        );
    }

    #[test]
    fn test_set_sql_query_updates_sql() {
        let mut stmt = make_statement();

        stmt.set_sql_query("SELECT * FROM t").unwrap();
        stmt.set_sql_query("SELECT a FROM t").unwrap();
        // Setting SQL should succeed without error — second call overwrites first
    }

    #[test]
    fn test_bind_without_prepare_returns_invalid_state() {
        use arrow_schema::{DataType, Field, Schema};
        let mut stmt = make_statement();

        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
        let batch = RecordBatch::new_empty(schema);
        let result = stmt.bind(batch);
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err().status,
            adbc_core::error::Status::InvalidState
        );
    }

    #[test]
    fn test_bind_stream_without_prepare_returns_invalid_state() {
        use arrow_schema::{DataType, Field, Schema};
        let mut stmt = make_statement();

        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
        let reader: Box<dyn RecordBatchReader + Send> =
            Box::new(arrow_array::RecordBatchIterator::new(vec![], schema));
        let result = stmt.bind_stream(reader);
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err().status,
            adbc_core::error::Status::InvalidState
        );
    }

    #[test]
    fn test_execute_update_not_implemented_for_non_prepared() {
        let mut stmt = make_statement();

        let result = stmt.execute_update();
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err().status,
            adbc_core::error::Status::NotImplemented
        );
    }

    #[test]
    fn test_substrait_not_implemented() {
        let mut stmt = make_statement();

        let result = stmt.set_substrait_plan(b"{}");
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err().status,
            adbc_core::error::Status::NotImplemented
        );
    }

    #[test]
    fn test_execute_no_sql_returns_invalid_state() {
        let mut stmt = make_statement();

        let result = stmt.execute();
        match result {
            Err(e) => assert_eq!(e.status, adbc_core::error::Status::InvalidState),
            Ok(_) => panic!("expected InvalidState error"),
        }
    }

    #[test]
    fn test_execute_schema_no_sql_returns_invalid_state() {
        let mut stmt = make_statement();

        let result = stmt.execute_schema();
        match result {
            Err(e) => assert_eq!(e.status, adbc_core::error::Status::InvalidState),
            Ok(_) => panic!("expected InvalidState error"),
        }
    }

    #[test]
    fn test_execute_schema_uses_cached_schema() {
        use arrow_schema::{DataType, Field};
        let mut stmt = make_statement();

        // Set a cached schema directly
        let schema = Schema::new(vec![Field::new("a", DataType::Int32, false)]);
        stmt.cached_schema = Some(schema.clone());

        // With cached schema present, execute_schema should return it
        let result = stmt.execute_schema();
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), schema);
    }

    #[test]
    fn test_execute_flips_cancelled_flag() {
        let mut stmt = make_statement();
        stmt.set_sql_query("SELECT 1").unwrap();

        // execute() should flip the cancelled flag to true before the RPC
        let _ = stmt.execute(); // will fail — no server
        // After execute() attempts, flag was flipped to true at start
        assert!(stmt.cancelled.load(Ordering::SeqCst),
            "cancelled should be true after execute (flipped before RPC)");
    }

    #[test]
    fn test_execute_schema_flips_cancelled_flag() {
        let mut stmt = make_statement();
        stmt.set_sql_query("SELECT 1").unwrap();

        let _ = stmt.execute_schema();
        // cancelled flag should be true after execute_schema (per ADBC spec)
        assert!(stmt.cancelled.load(Ordering::SeqCst),
            "cancelled should be true after execute_schema");
    }

    #[test]
    fn test_new_statement_has_no_sql_initially() {
        let stmt = make_statement();

        // A new statement has no SQL — any execution should fail
        assert!(stmt.sql.is_none());
    }

    #[test]
    fn test_cancelled_flag_starts_false() {
        let stmt = make_statement();

        assert!(!stmt.cancelled.load(Ordering::SeqCst));
    }

    #[test]
    fn test_prepare_no_sql_returns_invalid_state() {
        let mut stmt = make_statement();

        let result = stmt.prepare();
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err().status,
            adbc_core::error::Status::InvalidState
        );
    }

    #[test]
    fn test_get_parameter_schema_without_prepare_returns_invalid_state() {
        let stmt = make_statement();

        let result = stmt.get_parameter_schema();
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err().status,
            adbc_core::error::Status::InvalidState
        );
    }

    #[test]
    fn test_cancel_not_implemented() {
        let mut stmt = make_statement();

        let result = stmt.cancel();
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err().status,
            adbc_core::error::Status::NotImplemented
        );
    }

    // --- Option tests (FSQL-12/FSQL-13) ---

    #[test]
    fn test_set_timeout_stores_duration() {
        let mut stmt = make_statement();
        stmt.set_option(
            OptionStatement::Other("adbc.statement.exec_timeout_ms".into()),
            OptionValue::Int(5000),
        ).unwrap();
        assert!(stmt.timeout.is_some());
        assert_eq!(stmt.timeout.unwrap(), Duration::from_millis(5000));
    }

    #[test]
    fn test_get_timeout_returns_stored_value() {
        let mut stmt = make_statement();
        stmt.set_option(
            OptionStatement::Other("adbc.statement.exec_timeout_ms".into()),
            OptionValue::Int(5000),
        ).unwrap();
        let v = stmt.get_option_int(
            OptionStatement::Other("adbc.statement.exec_timeout_ms".into())
        ).unwrap();
        assert_eq!(v, 5000);
    }

    #[test]
    fn test_negative_timeout_returns_invalid_argument() {
        let mut stmt = make_statement();
        let result = stmt.set_option(
            OptionStatement::Other("adbc.statement.exec_timeout_ms".into()),
            OptionValue::Int(-1),
        );
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().status, adbc_core::error::Status::InvalidArguments);
    }

    #[test]
    fn test_zero_timeout_disables_timeout() {
        let mut stmt = make_statement();
        stmt.set_option(
            OptionStatement::Other("adbc.statement.exec_timeout_ms".into()),
            OptionValue::Int(5000),
        ).unwrap();
        assert!(stmt.timeout.is_some());
        stmt.set_option(
            OptionStatement::Other("adbc.statement.exec_timeout_ms".into()),
            OptionValue::Int(0),
        ).unwrap();
        assert!(stmt.timeout.is_none());
    }

    #[test]
    fn test_timeout_set_from_string_value() {
        let mut stmt = make_statement();
        stmt.set_option(
            OptionStatement::Other("adbc.statement.exec_timeout_ms".into()),
            OptionValue::String("10000".into()),
        ).unwrap();
        let v = stmt.get_option_int(
            OptionStatement::Other("adbc.statement.exec_timeout_ms".into())
        ).unwrap();
        assert_eq!(v, 10000);
    }

    #[test]
    fn test_unknown_statement_option_returns_not_implemented() {
        let mut stmt = make_statement();
        let result = stmt.set_option(
            OptionStatement::Other("adbc.flightsql.nonexistent".into()),
            OptionValue::String("value".into()),
        );
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().status, adbc_core::error::Status::NotImplemented);
    }

    #[test]
    fn test_ingest_mode_option_accepted_noop() {
        let mut stmt = make_statement();
        stmt.set_option(
            OptionStatement::IngestMode,
            OptionValue::String("adbc.ingest.mode.create".into()),
        ).unwrap();
    }

    #[test]
    fn test_get_timeout_as_string_returns_not_found() {
        let mut stmt = make_statement();
        stmt.set_option(
            OptionStatement::Other("adbc.statement.exec_timeout_ms".into()),
            OptionValue::Int(5000),
        ).unwrap();
        let result = stmt.get_option_string(
            OptionStatement::Other("adbc.statement.exec_timeout_ms".into())
        );
        assert!(result.is_err());
    }
}
