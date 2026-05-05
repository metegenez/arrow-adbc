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

//! End-to-end integration test loading the FlightSQL driver .so via
//! the ADBC driver manager and executing a query against a test server.
//!
//! This verifies that the cdylib can be loaded by the C driver manager,
//! that the FFI function table is correctly populated, and that the
//! full query execution pipeline works across the FFI boundary.

use std::net::SocketAddr;
use std::sync::Arc;

use adbc_core::options::{AdbcVersion, OptionDatabase};
use adbc_core::{Connection, Database, Driver, Statement};
use adbc_driver_manager::ManagedDriver;
use arrow_array::{Int32Array, RecordBatch, RecordBatchReader};
use arrow_schema::ArrowError;
use arrow_flight::{
    flight_service_server::FlightServiceServer,
    sql::server::FlightSqlService,
    sql::{CommandStatementQuery, SqlInfo, TicketStatementQuery},
    FlightDescriptor, FlightEndpoint, FlightInfo, HandshakeRequest,
    HandshakeResponse, IpcMessage, SchemaAsIpc, Ticket,
};
use arrow_ipc::writer::IpcWriteOptions;
use arrow_schema::{DataType, Field, Schema};
use futures::{Stream, StreamExt};
use prost::Message;
use tokio::net::TcpListener;
use tokio::task::JoinHandle;
use tonic::transport::Server;
use tonic::{Request, Response, Status, Streaming};

/// A FlightSQL test server that supports handshake, get_flight_info_statement,
/// and do_get_statement for end-to-end driver manager integration tests.
#[derive(Debug, Clone)]
struct TestFlightSqlServer;

#[tonic::async_trait]
impl FlightSqlService for TestFlightSqlServer {
    type FlightService = TestFlightSqlServer;

    async fn do_handshake(
        &self,
        _request: Request<Streaming<HandshakeRequest>>,
    ) -> Result<
        Response<
            std::pin::Pin<
                Box<dyn Stream<Item = Result<HandshakeResponse, Status>> + Send + 'static>,
            >,
        >,
        Status,
    > {
        let result = HandshakeResponse {
            protocol_version: 1,
            payload: b"bearer"[..].into(),
        };
        let output: std::pin::Pin<
            Box<dyn Stream<Item = Result<HandshakeResponse, Status>> + Send>,
        > = Box::pin(futures::stream::iter(vec![Ok(result)]));
        Ok(Response::new(output))
    }

    async fn get_flight_info_statement(
        &self,
        _query: CommandStatementQuery,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let desc = request.into_inner();
        let cmd_bytes = desc.cmd;

        let schema = Schema::new(vec![Field::new("test_column", DataType::Int32, false)]);
        let options = IpcWriteOptions::default();
        let ipc_msg: IpcMessage = SchemaAsIpc::new(&schema, &options)
            .try_into()
            .map_err(|e: arrow_schema::ArrowError| {
                Status::internal(format!("schema serialization error: {e}"))
            })?;

        // Encode the command bytes as a protobuf TicketStatementQuery
        // so that the FlightSqlService do_get dispatch can decode it.
        let ticket_query = TicketStatementQuery {
            statement_handle: cmd_bytes.clone(),
        };
        let any = arrow_flight::sql::Any::pack(&ticket_query)
            .map_err(|e| Status::internal(format!("ticket encoding error: {e}")))?;
        let ticket_bytes = any.encode_to_vec().into();

        let flight_info = FlightInfo {
            schema: ipc_msg.0,
            flight_descriptor: Some(FlightDescriptor::new_cmd(cmd_bytes)),
            endpoint: vec![FlightEndpoint {
                ticket: Some(Ticket {
                    ticket: ticket_bytes,
                }),
                location: vec![],
                expiration_time: None,
                app_metadata: vec![].into(),
            }],
            total_records: 0,
            total_bytes: 0,
            ordered: false,
            app_metadata: vec![].into(),
        };

        Ok(Response::new(flight_info))
    }

    /// Serve query results based on the statement handle in the ticket.
    async fn do_get_statement(
        &self,
        ticket: TicketStatementQuery,
        _request: Request<Ticket>,
    ) -> Result<Response<<Self as arrow_flight::flight_service_server::FlightService>::DoGetStream>, Status>
    {
        let query = std::str::from_utf8(&ticket.statement_handle)
            .map_err(|_| Status::invalid_argument("invalid statement handle"))?;

        let schema = Schema::new(vec![Field::new("test_column", DataType::Int32, false)]);
        let schema_ref = Arc::new(schema);

        // Build a simple RecordBatch response based on the query
        let batch = if query.contains("SELECT 1") {
            RecordBatch::try_new(
                schema_ref.clone(),
                vec![Arc::new(Int32Array::from(vec![1]))],
            )
            .unwrap()
        } else if query.contains("SELECT 42") {
            RecordBatch::try_new(
                schema_ref.clone(),
                vec![Arc::new(Int32Array::from(vec![42]))],
            )
            .unwrap()
        } else {
            RecordBatch::try_new(
                schema_ref,
                vec![Arc::new(Int32Array::from(Vec::<i32>::new()))],
            )
            .unwrap()
        };

        // Encode the batch using FlightDataEncoderBuilder.
        // The builder emits the schema FlightData as its first message,
        // followed by the batch data, following the standard Flight protocol.
        let output = arrow_flight::encode::FlightDataEncoderBuilder::new()
            .build(futures::stream::once(
                async { Ok::<_, arrow_flight::error::FlightError>(batch) },
            ))
            .map(|r| r.map_err(tonic::Status::from));

        Ok(Response::new(Box::pin(output)))
    }

    async fn register_sql_info(&self, _id: i32, _result: &SqlInfo) {}
}

/// Start the FlightSQL test server on an OS-assigned port.
/// Returns the join handle for cleanup and the bound address.
async fn start_test_server() -> (JoinHandle<()>, SocketAddr) {
    let listener = TcpListener::bind("[::1]:0")
        .await
        .expect("Failed to bind test server");
    let addr = listener.local_addr().expect("Failed to get local addr");
    let svc = FlightServiceServer::new(TestFlightSqlServer);

    let handle = tokio::spawn(async move {
        Server::builder()
            .add_service(svc)
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
            .await
            .expect("Test server failed");
    });

    (handle, addr)
}

/// Resolve the path to libadbc_flightsql.so.
///
/// Uses `ADBC_FLIGHTSQL_SO_PATH` env var if set, otherwise derives from
/// the cargo target directory relative to the crate root.
fn resolve_so_path() -> String {
    std::env::var("ADBC_FLIGHTSQL_SO_PATH").unwrap_or_else(|_| {
        let manifest_dir =
            std::env::var("CARGO_MANIFEST_DIR").unwrap_or_else(|_| ".".to_string());
        let profile = if cfg!(debug_assertions) {
            "debug"
        } else {
            "release"
        };
        let so_name = if cfg!(target_os = "linux") {
            "libadbc_flightsql.so"
        } else if cfg!(target_os = "macos") {
            "libadbc_flightsql.dylib"
        } else {
            "adbc_flightsql.dll"
        };
        format!("{manifest_dir}/../../target/{profile}/{so_name}")
    })
}

/// End-to-end test: load the .so via driver manager, connect to test
/// server, execute `SELECT 1`, and verify the result.
#[tokio::test(flavor = "multi_thread")]
async fn test_load_driver_and_execute_query() {
    // 1. Start test server
    let (_server, addr) = start_test_server().await;
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;

    // 2. Resolve .so path
    let so_path = resolve_so_path();
    eprintln!("Loading driver from: {so_path}");
    assert!(
        std::path::Path::new(&so_path).exists(),
        "SO not found at {so_path}. Build with: cargo build --package adbc_flightsql --features ffi"
    );

    // 3. Load driver via driver manager
    // Use AdbcVersion::V110 because export_driver! validates version == V110
    let mut driver = ManagedDriver::load_dynamic_from_filename(
        &so_path,
        None, // uses default entrypoint: AdbcDriverInit -> AdbcFlightSqlDriverInit
        AdbcVersion::V110,
    )
    .expect("Failed to load libadbc_flightsql.so via driver manager");

    // 4. Create database with FlightSQL URI
    let uri = format!("grpc://{addr}");
    eprintln!("Test server at: {uri}");
    let database = driver
        .new_database_with_opts([(OptionDatabase::Uri, uri.into())])
        .expect("Failed to create database via FFI");

    // 5. Create connection (triggers handshake)
    let mut connection = database
        .new_connection()
        .expect("Failed to create connection via FFI");

    // 6. Create statement
    let mut statement = connection
        .new_statement()
        .expect("Failed to create statement via FFI");

    // 7. Set SQL query
    statement
        .set_sql_query("SELECT 1")
        .expect("Failed to set SQL query via FFI");

    // 8. Execute query
    let reader = statement
        .execute()
        .expect("Failed to execute query via FFI");

    // 9. Collect results
    let schema = reader.schema();
    let batches: Vec<RecordBatch> = reader
        .map(|r: Result<RecordBatch, ArrowError>| r.expect("Failed to read batch"))
        .collect();

    // 10. Verify schema
    assert_eq!(schema.fields().len(), 1, "Expected 1 column");
    assert_eq!(
        schema.field(0).name().as_str(),
        "test_column",
        "Expected column named test_column"
    );

    // 11. Verify data
    assert!(!batches.is_empty(), "Expected at least one batch");
    let total_rows: usize = batches.iter().map(|b: &RecordBatch| b.num_rows()).sum();
    assert_eq!(total_rows, 1, "Expected 1 row from SELECT 1");

    // Verify the value is 1
    let column = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int32Array>()
        .expect("Expected Int32Array");
    assert_eq!(column.value(0), 1, "Expected value 1 from SELECT 1");
}
