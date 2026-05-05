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

//! Integration tests requiring a running FlightSQL test server.
//!
//! The test server is started in-process as a background tokio task
//! before tests run and shut down after. It serves mock FlightSQL
//! responses for metadata and query execution.

use std::pin::Pin;
use std::net::SocketAddr;

use adbc_core::{
    options::{OptionDatabase, OptionValue},
    Connection, Database, Driver, Optionable,
};
use arrow_flight::{
    encode::FlightDataEncoderBuilder,
    sql::{
        server::FlightSqlService,
        metadata::SqlInfoDataBuilder,
        CommandGetCatalogs, CommandGetDbSchemas, CommandGetSqlInfo,
        CommandGetTableTypes, CommandGetTables, CommandStatementQuery,
        ProstMessageExt, SqlInfo,
    },
    flight_service_server::FlightServiceServer,
    FlightData, FlightDescriptor, FlightEndpoint, FlightInfo,
    HandshakeRequest, HandshakeResponse, Ticket,
};
use futures::stream;
use futures::{Stream, TryStreamExt};
use prost::Message;
use tokio::net::TcpListener;
use tokio::task::JoinHandle;
use tonic::transport::Server;
use tonic::{Request, Response, Status, Streaming};

use adbc_flightsql::{FlightSqlDatabase, FlightSqlDriver};

/// A minimal FlightSQL test server implementing the basic handshake,
/// metadata RPCs, and query execution for integration testing.
#[derive(Debug, Clone)]
struct TestFlightSqlServer;

/// Helper: build a FlightInfo from a query command, schema, and descriptor.
fn build_flight_info(
    schema: &arrow_schema::Schema,
    ticket_bytes: Vec<u8>,
    descriptor: FlightDescriptor,
) -> FlightInfo {
    FlightInfo::new()
        .try_with_schema(schema)
        .expect("schema IPC serialization should not fail")
        .with_descriptor(descriptor)
        .with_endpoint(FlightEndpoint::new().with_ticket(Ticket {
            ticket: ticket_bytes.into(),
        }))
}

/// Helper: encode a RecordBatch into a FlightData stream response.
fn encode_batch_stream(
    schema: arrow_schema::SchemaRef,
    batch: arrow_array::RecordBatch,
) -> Result<
    Response<
        Pin<Box<dyn Stream<Item = Result<FlightData, Status>> + Send + 'static>>,
    >,
    Status,
> {
    let stream = FlightDataEncoderBuilder::new()
        .with_schema(schema)
        .build(stream::once(async { Ok(batch) }))
        .map_err(Status::from);
    Ok(Response::new(Box::pin(stream)))
}

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

    // -- Statement RPCs --

    async fn get_flight_info_statement(
        &self,
        _query: CommandStatementQuery,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let desc = request.into_inner();
        let cmd_bytes = desc.cmd.clone();

        let schema = arrow_schema::Schema::new(vec![arrow_schema::Field::new(
            "test_column",
            arrow_schema::DataType::Int32,
            false,
        )]);

        let fi = FlightInfo::new()
            .try_with_schema(&schema)
            .map_err(|e| Status::internal(format!("schema serialization error: {e}")))?
            .with_descriptor(FlightDescriptor::new_cmd(cmd_bytes))
            .with_endpoint(FlightEndpoint::new().with_ticket(Ticket {
                ticket: b"test_ticket"[..].into(),
            }));

        Ok(Response::new(fi))
    }

    // -- Catalogs --

    async fn get_flight_info_catalogs(
        &self,
        query: CommandGetCatalogs,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let descriptor = request.into_inner();
        let ticket_bytes = query.as_any().encode_to_vec();
        let schema = query.into_builder().schema();
        let fi = build_flight_info(&schema, ticket_bytes, descriptor);
        Ok(Response::new(fi))
    }

    async fn do_get_catalogs(
        &self,
        query: CommandGetCatalogs,
        _request: Request<Ticket>,
    ) -> Result<
        Response<
            Pin<Box<dyn Stream<Item = Result<FlightData, Status>> + Send + 'static>>,
        >,
        Status,
    > {
        let mut builder = query.into_builder();
        builder.append("test_catalog");
        let schema = builder.schema();
        let batch = builder
            .build()
            .map_err(|e| Status::internal(format!("failed to build catalogs batch: {e}")))?;
        encode_batch_stream(schema, batch)
    }

    // -- Schemas --

    async fn get_flight_info_schemas(
        &self,
        query: CommandGetDbSchemas,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let descriptor = request.into_inner();
        let ticket_bytes = query.as_any().encode_to_vec();
        let schema = query.into_builder().schema();
        let fi = build_flight_info(&schema, ticket_bytes, descriptor);
        Ok(Response::new(fi))
    }

    async fn do_get_schemas(
        &self,
        query: CommandGetDbSchemas,
        _request: Request<Ticket>,
    ) -> Result<
        Response<
            Pin<Box<dyn Stream<Item = Result<FlightData, Status>> + Send + 'static>>,
        >,
        Status,
    > {
        let mut builder = query.into_builder();
        builder.append("test_catalog", "test_schema");
        let schema = builder.schema();
        let batch = builder
            .build()
            .map_err(|e| Status::internal(format!("failed to build schemas batch: {e}")))?;
        encode_batch_stream(schema, batch)
    }

    // -- Tables --

    async fn get_flight_info_tables(
        &self,
        query: CommandGetTables,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let descriptor = request.into_inner();
        let ticket_bytes = query.as_any().encode_to_vec();
        let schema = query.into_builder().schema();
        let fi = build_flight_info(&schema, ticket_bytes, descriptor);
        Ok(Response::new(fi))
    }

    async fn do_get_tables(
        &self,
        query: CommandGetTables,
        _request: Request<Ticket>,
    ) -> Result<
        Response<
            Pin<Box<dyn Stream<Item = Result<FlightData, Status>> + Send + 'static>>,
        >,
        Status,
    > {
        let mut builder = query.into_builder();
        let empty_schema = arrow_schema::Schema::empty();
        builder
            .append("test_catalog", "test_schema", "test_table", "TABLE", &empty_schema)
            .map_err(|e| Status::internal(format!("failed to append table: {e}")))?;
        let schema = builder.schema();
        let batch = builder
            .build()
            .map_err(|e| Status::internal(format!("failed to build tables batch: {e}")))?;
        encode_batch_stream(schema, batch)
    }

    // -- Table Types --

    async fn get_flight_info_table_types(
        &self,
        query: CommandGetTableTypes,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let descriptor = request.into_inner();
        let ticket_bytes = query.as_any().encode_to_vec();
        let schema = query.into_builder().schema();
        let fi = build_flight_info(&schema, ticket_bytes, descriptor);
        Ok(Response::new(fi))
    }

    async fn do_get_table_types(
        &self,
        query: CommandGetTableTypes,
        _request: Request<Ticket>,
    ) -> Result<
        Response<
            Pin<Box<dyn Stream<Item = Result<FlightData, Status>> + Send + 'static>>,
        >,
        Status,
    > {
        let mut builder = query.into_builder();
        builder.append("TABLE");
        builder.append("VIEW");
        let schema = builder.schema();
        let batch = builder
            .build()
            .map_err(|e| Status::internal(format!("failed to build table types batch: {e}")))?;
        encode_batch_stream(schema, batch)
    }

    // -- SQL Info --

    async fn get_flight_info_sql_info(
        &self,
        query: CommandGetSqlInfo,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let descriptor = request.into_inner();
        let ticket_bytes = query.as_any().encode_to_vec();
        let schema = SqlInfoDataBuilder::schema();
        let fi = build_flight_info(schema, ticket_bytes, descriptor);
        Ok(Response::new(fi))
    }

    async fn do_get_sql_info(
        &self,
        query: CommandGetSqlInfo,
        _request: Request<Ticket>,
    ) -> Result<
        Response<
            Pin<Box<dyn Stream<Item = Result<FlightData, Status>> + Send + 'static>>,
        >,
        Status,
    > {
        let mut data_builder = SqlInfoDataBuilder::new();
        data_builder.append(SqlInfo::FlightSqlServerName, "FlightSQL Test Server");
        let info_data = data_builder
            .build()
            .map_err(|e| Status::internal(format!("failed to build sql info data: {e}")))?;

        let schema = info_data.schema();
        let batch = info_data
            .record_batch(query.info)
            .map_err(|e| Status::internal(format!("failed to filter sql info batch: {e}")))?;

        encode_batch_stream(schema, batch)
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
            .serve_with_incoming(
                tokio_stream::wrappers::TcpListenerStream::new(listener),
            )
            .await
            .expect("Test server failed");
    });

    (handle, addr)
}

/// Create a FlightSqlDatabase connected to the test server.
/// Uses the current tokio runtime handle to avoid nested runtime issues.
fn create_test_db(addr: SocketAddr) -> FlightSqlDatabase {
    let handle = tokio::runtime::Handle::current();
    let mut driver = FlightSqlDriver::new(Some(handle));
    let mut db = driver.new_database().expect("Failed to create database");
    db.set_option(
        OptionDatabase::Uri,
        OptionValue::String(format!("grpc://{addr}")),
    )
    .expect("Failed to set URI option");
    db
}

#[tokio::test(flavor = "multi_thread")]
async fn test_connect_to_test_server() {
    let (_server, addr) = start_test_server().await;
    // Give server time to start
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;

    let db = create_test_db(addr);
    let conn = db.new_connection();
    assert!(
        conn.is_ok(),
        "Should connect to test server: {:?}",
        conn.err()
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_connection_get_table_types() {
    let (_server, addr) = start_test_server().await;
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;

    let db = create_test_db(addr);
    let conn = db.new_connection().expect("Failed to create connection");
    let result = conn.get_table_types();
    assert!(
        result.is_ok(),
        "get_table_types should succeed; got: {:?}",
        result.err()
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_connection_get_info() {
    let (_server, addr) = start_test_server().await;
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;

    let db = create_test_db(addr);
    let conn = db.new_connection().expect("Failed to create connection");
    let result = conn.get_info(None);
    assert!(
        result.is_ok(),
        "get_info should succeed; got: {:?}",
        result.err()
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_connection_get_objects() {
    let (_server, addr) = start_test_server().await;
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;

    let db = create_test_db(addr);
    let conn = db.new_connection().expect("Failed to create connection");
    let result = conn.get_objects(
        adbc_core::options::ObjectDepth::All,
        None,
        None,
        None,
        None,
        None,
    );
    assert!(
        result.is_ok(),
        "get_objects should succeed; got: {:?}",
        result.err()
    );
}
