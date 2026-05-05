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

//! Integration tests against a real SQLite-backed FlightSQL server.
//!
//! This mirrors the Go driver's validation strategy: start a FlightSQL
//! server with a SQLite backend, connect the ADBC driver, create tables,
//! insert data, and verify query results.

use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::{Arc, Mutex};

use adbc_core::{
    options::{OptionDatabase, OptionValue},
    Connection, Database, Driver, Optionable, Statement,
};
use arrow_array::{
    builder::{ArrayBuilder, BooleanBuilder, Float64Builder, Int32Builder, Int8Builder, StringBuilder, UInt32Builder},
    Int32Array, RecordBatch, RecordBatchIterator, RecordBatchReader, StringArray,
};
use arrow_flight::{
    flight_service_server::FlightServiceServer,
    sql::{
        server::FlightSqlService,
        ActionClosePreparedStatementRequest,
        ActionCreatePreparedStatementRequest,
        ActionCreatePreparedStatementResult,
        CommandGetCatalogs,
        CommandGetDbSchemas,
        CommandGetTables,
        CommandGetTableTypes,
        CommandGetSqlInfo,
        CommandStatementQuery,
        CommandPreparedStatementQuery,
        SqlInfo,
        ProstMessageExt,
        TicketStatementQuery,
    },
    FlightDescriptor, FlightEndpoint, FlightInfo,
    HandshakeRequest, HandshakeResponse,
    IpcMessage, SchemaAsIpc, Ticket,
};
use arrow_ipc::writer::IpcWriteOptions;
use arrow_schema::{DataType, Field, Schema};
use futures::{Stream, StreamExt};
use prost::Message;
use rusqlite::Connection as SqliteConnection;
use tokio::net::TcpListener;
use tokio::task::JoinHandle;
use tonic::transport::Server;
use tonic::{Request, Response, Status, Streaming};

use adbc_flightsql::{FlightSqlDatabase, FlightSqlDriver};

/// A FlightSQL server backed by a real SQLite database.
///
/// Parses SQL, executes queries via rusqlite, and returns Arrow results
/// matching the Go driver's `SQLiteFlightSQLServer` pattern.
#[derive(Clone)]
struct SqliteFlightServer {
    db: Arc<Mutex<SqliteConnection>>,
    prepared: Arc<Mutex<Vec<String>>>,
}

impl SqliteFlightServer {
    fn new() -> Self {
        let db = SqliteConnection::open_in_memory().expect("Failed to open SQLite");
        let db = Arc::new(Mutex::new(db));

        // Pre-populate with same tables as Go test server
        {
            let db = db.lock().unwrap();
            db.execute_batch(
                "CREATE TABLE foreignTable (
                    id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
                    foreignName varchar(100),
                    value int
                );
                INSERT INTO foreignTable (foreignName, value) VALUES ('keyOne', 1), ('keyTwo', 0), ('keyThree', -1);

                CREATE TABLE intTable (
                    id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
                    keyName varchar(100),
                    value int,
                    foreignId int references foreignTable(id)
                );
                INSERT INTO intTable (keyName, value, foreignId) VALUES
                    ('one', 1, 1),
                    ('zero', 0, 1),
                    ('negative one', -1, 1),
                    (NULL, NULL, NULL);",
            )
            .expect("Failed to create test tables");
        }

        Self {
            db,
            prepared: Arc::new(Mutex::new(Vec::new())),
        }
    }

    fn execute_sql(&self, sql: &str) -> Result<Box<dyn RecordBatchReader + Send>, Status> {
        let db = self.db.lock().map_err(|e| Status::internal(e.to_string()))?;

        // Check if it's a SELECT query
        let trimmed = sql.trim().to_uppercase();
        if trimmed.starts_with("SELECT") || trimmed.starts_with("PRAGMA") {
            let mut stmt = db
                .prepare(sql)
                .map_err(|e| Status::internal(format!("SQL error: {e}")))?;

            let col_count = stmt.column_count();
            let col_names: Vec<String> = (0..col_count)
                .map(|i| stmt.column_name(i).unwrap_or("?").to_string())
                .collect();

            // Collect all rows as owned rusqlite::types::Value so we
            // can inspect types and build Arrow arrays without borrow issues.
            let mut rows: Vec<Vec<rusqlite::types::Value>> = Vec::new();
            {
                let mut query_rows = stmt
                    .query(())
                    .map_err(|e| Status::internal(format!("Query error: {e}")))?;

                loop {
                    match query_rows.next() {
                        Ok(Some(row)) => {
                            let mut vals = Vec::with_capacity(col_count);
                            for i in 0..col_count {
                                vals.push(match row.get_ref(i) {
                                    Ok(rusqlite::types::ValueRef::Null) => {
                                        rusqlite::types::Value::Null
                                    }
                                    Ok(rusqlite::types::ValueRef::Integer(v)) => {
                                        rusqlite::types::Value::Integer(v)
                                    }
                                    Ok(rusqlite::types::ValueRef::Real(v)) => {
                                        rusqlite::types::Value::Real(v)
                                    }
                                    Ok(rusqlite::types::ValueRef::Text(v)) => {
                                        rusqlite::types::Value::Text(
                                            String::from_utf8_lossy(v).into_owned(),
                                        )
                                    }
                                    Ok(rusqlite::types::ValueRef::Blob(v)) => {
                                        rusqlite::types::Value::Blob(v.to_vec())
                                    }
                                    Err(_) => rusqlite::types::Value::Null,
                                });
                            }
                            rows.push(vals);
                        }
                        Ok(None) => break,
                        Err(e) => {
                            return Err(Status::internal(format!("Row error: {e}")));
                        }
                    }
                }
            }

            // Determine Arrow column types from actual Value data
            let mut fields: Vec<Field> = Vec::with_capacity(col_count);
            for i in 0..col_count {
                let name = &col_names[i];
                let mut is_integer = false;
                let mut is_real = false;
                for row in &rows {
                    match row[i] {
                        rusqlite::types::Value::Integer(_) => is_integer = true,
                        rusqlite::types::Value::Real(_) => is_real = true,
                        _ => {}
                    }
                }
                if is_integer {
                    fields.push(Field::new(name, DataType::Int32, true));
                } else if is_real {
                    fields.push(Field::new(name, DataType::Float64, true));
                } else {
                    fields.push(Field::new(name, DataType::Utf8, true));
                }
            }

            let schema = Arc::new(Schema::new(fields.clone()));

            // Build Arrow arrays from owned row data
            let mut builders: Vec<Box<dyn ArrayBuilder + Send>> = Vec::with_capacity(col_count);
            for f in &schema.fields {
                match f.data_type() {
                    DataType::Int32 => builders.push(Box::new(Int32Builder::new())),
                    DataType::Float64 => builders.push(Box::new(Float64Builder::new())),
                    _ => builders.push(Box::new(StringBuilder::new())),
                }
            }

            for row in &rows {
                for i in 0..col_count {
                    match &row[i] {
                        rusqlite::types::Value::Null => {
                            if let Some(b) =
                                builders[i].as_any_mut().downcast_mut::<Int32Builder>()
                            {
                                b.append_null();
                            } else if let Some(b) =
                                builders[i].as_any_mut().downcast_mut::<Float64Builder>()
                            {
                                b.append_null();
                            } else if let Some(b) =
                                builders[i].as_any_mut().downcast_mut::<StringBuilder>()
                            {
                                b.append_null();
                            }
                        }
                        rusqlite::types::Value::Integer(v) => {
                            if let Some(b) =
                                builders[i].as_any_mut().downcast_mut::<Int32Builder>()
                            {
                                b.append_value(*v as i32);
                            } else if let Some(b) =
                                builders[i].as_any_mut().downcast_mut::<Float64Builder>()
                            {
                                b.append_value(*v as f64);
                            } else if let Some(b) =
                                builders[i].as_any_mut().downcast_mut::<StringBuilder>()
                            {
                                b.append_value(v.to_string());
                            }
                        }
                        rusqlite::types::Value::Real(v) => {
                            if let Some(b) =
                                builders[i].as_any_mut().downcast_mut::<Float64Builder>()
                            {
                                b.append_value(*v);
                            } else if let Some(b) =
                                builders[i].as_any_mut().downcast_mut::<StringBuilder>()
                            {
                                b.append_value(v.to_string());
                            }
                        }
                        rusqlite::types::Value::Text(v) => {
                            if let Some(b) =
                                builders[i].as_any_mut().downcast_mut::<StringBuilder>()
                            {
                                b.append_value(v);
                            }
                        }
                        rusqlite::types::Value::Blob(_) => {
                            if let Some(b) =
                                builders[i].as_any_mut().downcast_mut::<StringBuilder>()
                            {
                                b.append_null();
                            } else if let Some(b) =
                                builders[i].as_any_mut().downcast_mut::<Int32Builder>()
                            {
                                b.append_null();
                            }
                        }
                    }
                }
            }

            let arrs: Vec<Arc<dyn arrow_array::Array>> = builders
                .into_iter()
                .map(|mut b| b.finish())
                .collect();

            let batch = RecordBatch::try_new(schema.clone(), arrs)
                .map_err(|e| Status::internal(format!("Batch error: {e}")))?;

            Ok(Box::new(RecordBatchIterator::new(
                vec![batch].into_iter().map(Ok),
                schema,
            )))
        } else {
            // Non-SELECT: execute directly
            db.execute_batch(sql)
                .map_err(|e| Status::internal(format!("SQL error: {e}")))?;
            let schema = Arc::new(Schema::new(vec![Field::new(
                "rows_affected",
                DataType::Int64,
                true,
            )]));
            let batch = RecordBatch::try_new(
                schema.clone(),
                vec![Arc::new(arrow_array::Int64Array::from(vec![0i64]))],
            )
            .map_err(|e| Status::internal(format!("Batch error: {e}")))?;
            Ok(Box::new(RecordBatchIterator::new(
                vec![batch].into_iter().map(Ok),
                schema,
            )))
        }
    }

    fn record_batch_to_flight_data(
        schema: std::sync::Arc<Schema>,
        reader: Box<dyn RecordBatchReader + Send>,
    ) -> Pin<
        Box<
            dyn Stream<Item = Result<arrow_flight::FlightData, Status>>
                + Send
                + 'static,
        >,
    > {
        use arrow_flight::encode::FlightDataEncoderBuilder;
        let options = IpcWriteOptions::default();
        let encoder = FlightDataEncoderBuilder::new()
            .with_schema(schema)
            .with_options(options)
            .build(futures::stream::iter(reader.into_iter().map(|r| {
                r.map_err(arrow_flight::error::FlightError::Arrow)
            })));
        Box::pin(encoder.map(|r| {
            r.map_err(|e| Status::internal(format!("Arrow error: {e}")))
        }))
    }
}

use arrow_flight::flight_descriptor::DescriptorType;

#[tonic::async_trait]
impl FlightSqlService for SqliteFlightServer {
    type FlightService = SqliteFlightServer;

    async fn do_handshake(
        &self,
        _request: Request<Streaming<HandshakeRequest>>,
    ) -> Result<
        Response<
            Pin<Box<dyn Stream<Item = Result<HandshakeResponse, Status>> + Send + 'static>>,
        >,
        Status,
    > {
        let result = HandshakeResponse {
            protocol_version: 1,
            payload: b"bearer"[..].into(),
        };
        let output: Pin<Box<dyn Stream<Item = Result<HandshakeResponse, Status>> + Send>> =
            Box::pin(futures::stream::iter(vec![Ok(result)]));
        Ok(Response::new(output))
    }

    async fn get_flight_info_statement(
        &self,
        query: CommandStatementQuery,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        // Execute the query to get the schema
        let reader = self.execute_sql(&query.query)?;
        let schema = reader.schema();
        let options = IpcWriteOptions::default();
        let ipc_msg: IpcMessage = SchemaAsIpc::new(&schema, &options)
            .try_into()
            .map_err(|e| Status::internal(format!("schema serialization error: {e}")))?;

        let any_ticket = prost_types::Any {
            type_url: "type.googleapis.com/arrow.flight.protocol.sql.TicketStatementQuery"
                .to_string(),
            value: query.encode_to_vec(),
        };

        let flight_info = FlightInfo {
            schema: ipc_msg.0,
            flight_descriptor: Some(FlightDescriptor {
                r#type: DescriptorType::Cmd.into(),
                cmd: query.query.as_bytes().to_vec().into(),
                ..Default::default()
            }),
            endpoint: vec![FlightEndpoint {
                ticket: Some(Ticket {
                    ticket: any_ticket.encode_to_vec().into(),
                }),
                location: vec![],
                expiration_time: None,
                app_metadata: vec![].into(),
            }],
            total_records: -1,
            total_bytes: -1,
            ordered: false,
            app_metadata: vec![].into(),
        };

        Ok(Response::new(flight_info))
    }

    async fn do_get_statement(
        &self,
        ticket: TicketStatementQuery,
        _request: Request<Ticket>,
    ) -> Result<
        Response<
            Pin<Box<dyn Stream<Item = Result<arrow_flight::FlightData, Status>> + Send + 'static>>,
        >,
        Status,
    > {
        let sql = std::str::from_utf8(&ticket.statement_handle)
            .map_err(|e| Status::internal(format!("invalid utf8: {e}")))?;
        let reader = self.execute_sql(sql)?;
        let schema = reader.schema();
        let stream = Self::record_batch_to_flight_data(schema, reader);
        Ok(Response::new(
            Box::pin(stream) as Pin<Box<dyn Stream<Item = Result<arrow_flight::FlightData, Status>> + Send>>
        ))
    }

    async fn get_flight_info_catalogs(
        &self,
        query: CommandGetCatalogs,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "catalog_name",
            DataType::Utf8,
            false,
        )]));
        let ticket_bytes = query.as_any().encode_to_vec();
        let info = FlightInfo {
            schema: IpcMessage::try_from(SchemaAsIpc::new(&schema, &IpcWriteOptions::default()))
                .unwrap()
                .0,
            endpoint: vec![FlightEndpoint {
                ticket: Some(Ticket {
                    ticket: ticket_bytes.into(),
                }),
                ..Default::default()
            }],
            ..Default::default()
        };
        Ok(Response::new(info))
    }

    async fn do_get_catalogs(
        &self,
        _query: CommandGetCatalogs,
        _request: Request<Ticket>,
    ) -> Result<
        Response<
            Pin<Box<dyn Stream<Item = Result<arrow_flight::FlightData, Status>> + Send + 'static>>,
        >,
        Status,
    > {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "catalog_name",
            DataType::Utf8,
            false,
        )]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(StringArray::from(vec!["main"]))],
        )
        .unwrap();
        let reader = RecordBatchIterator::new(vec![batch].into_iter().map(Ok), schema);
        let stream = Self::record_batch_to_flight_data(reader.schema(), Box::new(reader));
        Ok(Response::new(Box::pin(stream)))
    }

    async fn get_flight_info_schemas(
        &self,
        query: CommandGetDbSchemas,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("catalog_name", DataType::Utf8, true),
            Field::new("db_schema_name", DataType::Utf8, false),
        ]));
        let ticket_bytes = query.as_any().encode_to_vec();
        let info = FlightInfo {
            schema: IpcMessage::try_from(SchemaAsIpc::new(&schema, &IpcWriteOptions::default()))
                .unwrap()
                .0,
            endpoint: vec![FlightEndpoint {
                ticket: Some(Ticket {
                    ticket: ticket_bytes.into(),
                }),
                ..Default::default()
            }],
            ..Default::default()
        };
        Ok(Response::new(info))
    }

    async fn do_get_schemas(
        &self,
        _query: CommandGetDbSchemas,
        _request: Request<Ticket>,
    ) -> Result<
        Response<
            Pin<Box<dyn Stream<Item = Result<arrow_flight::FlightData, Status>> + Send + 'static>>,
        >,
        Status,
    > {
        let schema = Arc::new(Schema::new(vec![
            Field::new("catalog_name", DataType::Utf8, true),
            Field::new("db_schema_name", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(vec!["main"])),
                Arc::new(StringArray::from(vec!["main"])),
            ],
        )
        .unwrap();
        let reader = RecordBatchIterator::new(vec![batch].into_iter().map(Ok), schema);
        let stream = Self::record_batch_to_flight_data(reader.schema(), Box::new(reader));
        Ok(Response::new(Box::pin(stream)))
    }

    async fn get_flight_info_tables(
        &self,
        query: CommandGetTables,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("catalog_name", DataType::Utf8, true),
            Field::new("db_schema_name", DataType::Utf8, true),
            Field::new("table_name", DataType::Utf8, false),
            Field::new("table_type", DataType::Utf8, false),
            Field::new("table_schema", DataType::Binary, false),
        ]));
        let ticket_bytes = query.as_any().encode_to_vec();
        let info = FlightInfo {
            schema: IpcMessage::try_from(SchemaAsIpc::new(&schema, &IpcWriteOptions::default()))
                .unwrap()
                .0,
            endpoint: vec![FlightEndpoint {
                ticket: Some(Ticket {
                    ticket: ticket_bytes.into(),
                }),
                ..Default::default()
            }],
            ..Default::default()
        };
        Ok(Response::new(info))
    }

    async fn do_get_tables(
        &self,
        _query: CommandGetTables,
        _request: Request<Ticket>,
    ) -> Result<
        Response<
            Pin<Box<dyn Stream<Item = Result<arrow_flight::FlightData, Status>> + Send + 'static>>,
        >,
        Status,
    > {
        let schema = Arc::new(Schema::new(vec![
            Field::new("catalog_name", DataType::Utf8, true),
            Field::new("db_schema_name", DataType::Utf8, true),
            Field::new("table_name", DataType::Utf8, false),
            Field::new("table_type", DataType::Utf8, false),
            Field::new("table_schema", DataType::Binary, false),
        ]));
        use arrow_array::BinaryArray;
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(vec!["main", "main"])),
                Arc::new(StringArray::from(vec!["main", "main"])),
                Arc::new(StringArray::from(vec!["foreignTable", "intTable"])),
                Arc::new(StringArray::from(vec!["TABLE", "TABLE"])),
                Arc::new(BinaryArray::from_vec(vec![&b""[..], &b""[..]])),
            ],
        )
        .unwrap();
        let reader = RecordBatchIterator::new(vec![batch].into_iter().map(Ok), schema);
        let stream = Self::record_batch_to_flight_data(reader.schema(), Box::new(reader));
        Ok(Response::new(Box::pin(stream)))
    }

    async fn get_flight_info_table_types(
        &self,
        query: CommandGetTableTypes,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "table_type",
            DataType::Utf8,
            false,
        )]));
        let ticket_bytes = query.as_any().encode_to_vec();
        let info = FlightInfo {
            schema: IpcMessage::try_from(SchemaAsIpc::new(&schema, &IpcWriteOptions::default()))
                .unwrap()
                .0,
            endpoint: vec![FlightEndpoint {
                ticket: Some(Ticket {
                    ticket: ticket_bytes.into(),
                }),
                ..Default::default()
            }],
            ..Default::default()
        };
        Ok(Response::new(info))
    }

    async fn do_get_table_types(
        &self,
        _query: CommandGetTableTypes,
        _request: Request<Ticket>,
    ) -> Result<
        Response<
            Pin<Box<dyn Stream<Item = Result<arrow_flight::FlightData, Status>> + Send + 'static>>,
        >,
        Status,
    > {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "table_type",
            DataType::Utf8,
            false,
        )]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(StringArray::from(vec!["TABLE", "VIEW"]))],
        )
        .unwrap();
        let reader = RecordBatchIterator::new(vec![batch].into_iter().map(Ok), schema);
        let stream = Self::record_batch_to_flight_data(reader.schema(), Box::new(reader));
        Ok(Response::new(Box::pin(stream)))
    }

    async fn get_flight_info_sql_info(
        &self,
        query: CommandGetSqlInfo,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("info_name", DataType::UInt32, false),
            Field::new(
                "value",
                DataType::Union(
                    arrow_schema::UnionFields::try_new(
                        vec![0, 1],
                        vec![
                            Field::new("string_value", DataType::Utf8, true),
                            Field::new("bool_value", DataType::Boolean, true),
                        ],
                    )
                    .unwrap(),
                    arrow_schema::UnionMode::Dense,
                ),
                false,
            ),
        ]));
        let ticket_bytes = query.as_any().encode_to_vec();
        let info = FlightInfo {
            schema: IpcMessage::try_from(SchemaAsIpc::new(&schema, &IpcWriteOptions::default()))
                .unwrap()
                .0,
            endpoint: vec![FlightEndpoint {
                ticket: Some(Ticket {
                    ticket: ticket_bytes.into(),
                }),
                ..Default::default()
            }],
            ..Default::default()
        };
        Ok(Response::new(info))
    }

    async fn do_get_sql_info(
        &self,
        _query: CommandGetSqlInfo,
        _request: Request<Ticket>,
    ) -> Result<
        Response<
            Pin<Box<dyn Stream<Item = Result<arrow_flight::FlightData, Status>> + Send + 'static>>,
        >,
        Status,
    > {
        // Build SQL info response matching arrow-flight example format
        let mut info_name = UInt32Builder::new();
        let mut type_ids = Int8Builder::new();
        let mut offsets = Int32Builder::new();
        let mut string_builder = StringBuilder::new();
        let mut bool_builder = BooleanBuilder::new();

        // Server name (SQL_INFO_SERVER_NAME = 0)
        info_name.append_value(0);
        type_ids.append_value(0); // string type
        let offset = string_builder.len() as i32;
        offsets.append_value(offset);
        string_builder.append_value("SQLite FlightSQL Test Server");

        // Server version (SQL_INFO_SERVER_VERSION = 1)
        info_name.append_value(1);
        type_ids.append_value(0);
        let offset = string_builder.len() as i32;
        offsets.append_value(offset);
        string_builder.append_value("3.0.0");

        let string_arr: arrow_array::ArrayRef = Arc::new(string_builder.finish());
        let bool_arr: arrow_array::ArrayRef = Arc::new(bool_builder.finish());

        let type_ids = type_ids.finish();
        let offsets = offsets.finish();

        let union_fields = arrow_schema::UnionFields::try_new(
            vec![0, 1],
            vec![
                Field::new("string_value", DataType::Utf8, true),
                Field::new("bool_value", DataType::Boolean, true),
            ],
        )
        .unwrap();

        let type_ids_scalar = type_ids.values().clone();
        let offsets_scalar = offsets.values().clone();

        let value_arr = arrow_array::UnionArray::try_new(
            union_fields,
            type_ids_scalar,
            Some(offsets_scalar),
            vec![string_arr, bool_arr],
        )
        .unwrap();

        let schema = Arc::new(Schema::new(vec![
            Field::new("info_name", DataType::UInt32, false),
            Field::new(
                "value",
                DataType::Union(
                    arrow_schema::UnionFields::try_new(
                        vec![0, 1],
                        vec![
                            Field::new("string_value", DataType::Utf8, true),
                            Field::new("bool_value", DataType::Boolean, true),
                        ],
                    )
                    .unwrap(),
                    arrow_schema::UnionMode::Dense,
                ),
                false,
            ),
        ]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(info_name.finish()), Arc::new(value_arr)],
        )
        .unwrap();

        let reader = RecordBatchIterator::new(vec![batch].into_iter().map(Ok), schema);
        let stream = Self::record_batch_to_flight_data(reader.schema(), Box::new(reader));
        Ok(Response::new(Box::pin(stream)))
    }

    async fn register_sql_info(&self, _id: i32, _result: &SqlInfo) {}

    // Prepared statement support
    async fn do_action_create_prepared_statement(
        &self,
        query: ActionCreatePreparedStatementRequest,
        _request: Request<arrow_flight::Action>,
    ) -> Result<ActionCreatePreparedStatementResult, Status> {
        let mut prepared = self.prepared.lock().unwrap();
        let handle = prepared.len().to_string();
        prepared.push(query.query.clone());
        // Get schema via execute_sql
        let reader = self.execute_sql(&query.query)?;
        let schema = reader.schema();
        let options = IpcWriteOptions::default();
        let dataset_schema = IpcMessage::try_from(SchemaAsIpc::new(&schema, &options))
            .map_err(|e| Status::internal(format!("schema serialization error: {e}")))?;
        Ok(ActionCreatePreparedStatementResult {
            prepared_statement_handle: handle.into_bytes().into(),
            dataset_schema: dataset_schema.0,
            parameter_schema: vec![].into(),
        })
    }

    async fn do_action_close_prepared_statement(
        &self,
        _query: ActionClosePreparedStatementRequest,
        _request: Request<arrow_flight::Action>,
    ) -> Result<(), Status> {
        Ok(())
    }

    async fn get_flight_info_prepared_statement(
        &self,
        handle: CommandPreparedStatementQuery,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let query_str = {
            let prepared = self.prepared.lock().unwrap();
            let idx: usize = std::str::from_utf8(&handle.prepared_statement_handle)
                .unwrap_or("0")
                .parse()
                .unwrap_or(0);
            if idx >= prepared.len() {
                return Err(Status::not_found("prepared statement not found"));
            }
            prepared[idx].clone()
        };
        // Re-execute to get fresh flight info
        let cmd = CommandStatementQuery {
            query: query_str,
            transaction_id: None,
        };
        self.get_flight_info_statement(cmd, _request).await
    }

    async fn do_get_prepared_statement(
        &self,
        query: CommandPreparedStatementQuery,
        _request: Request<Ticket>,
    ) -> Result<
        Response<
            Pin<Box<dyn Stream<Item = Result<arrow_flight::FlightData, Status>> + Send + 'static>>,
        >,
        Status,
    > {
        let prepared = self.prepared.lock().unwrap();
        let idx: usize = std::str::from_utf8(&query.prepared_statement_handle)
            .unwrap_or("0")
            .parse()
            .unwrap_or(0);
        if idx >= prepared.len() {
            return Err(Status::not_found("prepared statement not found"));
        }
        let reader = self.execute_sql(&prepared[idx])?;
        let schema = reader.schema();
        let stream = Self::record_batch_to_flight_data(schema, reader);
        Ok(Response::new(Box::pin(stream)))
    }
}

async fn start_server() -> (JoinHandle<()>, SocketAddr) {
    let listener = TcpListener::bind("[::1]:0")
        .await
        .expect("Failed to bind test server");
    let addr = listener.local_addr().expect("Failed to get local addr");
    let svc = FlightServiceServer::new(SqliteFlightServer::new());

    let handle = tokio::spawn(async move {
        Server::builder()
            .add_service(svc)
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
            .await
            .expect("Test server failed");
    });

    (handle, addr)
}

fn create_test_db(addr: SocketAddr) -> FlightSqlDatabase {
    let handle = tokio::runtime::Handle::current();
    let mut driver = FlightSqlDriver::new(Some(handle));
    let mut db = driver.new_database().expect("Failed to create database");
    db.set_option(OptionDatabase::Uri, OptionValue::String(format!("grpc://{addr}")))
        .expect("Failed to set URI option");
    db
}

// ─── Real SQL Tests ───

#[tokio::test(flavor = "multi_thread")]
async fn test_execute_select_query() {
    let (_server, addr) = start_server().await;
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;

    let db = create_test_db(addr);
    let mut conn = db.new_connection().expect("connect");
    let mut stmt = conn.new_statement().expect("new statement");
    stmt.set_sql_query("SELECT id, keyName, value FROM intTable WHERE id = 1")
        .expect("set SQL");

    let reader = stmt.execute().expect("execute");
    let batches: Vec<RecordBatch> = reader.collect::<Result<Vec<_>, _>>().expect("collect");

    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].num_rows(), 1);
    assert_eq!(batches[0].num_columns(), 3);

    let id = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int32Array>()
        .expect("id column");
    assert_eq!(id.value(0), 1);

    let name = batches[0]
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("keyName column");
    assert_eq!(name.value(0), "one");

    let value = batches[0]
        .column(2)
        .as_any()
        .downcast_ref::<Int32Array>()
        .expect("value column");
    assert_eq!(value.value(0), 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_execute_schema() {
    let (_server, addr) = start_server().await;
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;

    let db = create_test_db(addr);
    let mut conn = db.new_connection().expect("connect");
    let mut stmt = conn.new_statement().expect("new statement");
    stmt.set_sql_query("SELECT id, keyName, value, foreignId FROM intTable")
        .expect("set SQL");

    let schema = stmt.execute_schema().expect("execute_schema");
    assert_eq!(schema.fields().len(), 4);
    assert_eq!(schema.field(0).name(), "id");
    assert_eq!(schema.field(1).name(), "keyName");
    assert_eq!(schema.field(2).name(), "value");
    assert_eq!(schema.field(3).name(), "foreignId");
}

#[tokio::test(flavor = "multi_thread")]
async fn test_execute_join_query() {
    let (_server, addr) = start_server().await;
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;

    let db = create_test_db(addr);
    let mut conn = db.new_connection().expect("connect");
    let mut stmt = conn.new_statement().expect("new statement");
    stmt.set_sql_query(
        "SELECT i.keyName, f.foreignName, i.value \
         FROM intTable i JOIN foreignTable f ON i.foreignId = f.id \
         WHERE i.id = 1",
    )
    .expect("set SQL");

    let reader = stmt.execute().expect("execute");
    let batches: Vec<RecordBatch> = reader.collect::<Result<Vec<_>, _>>().expect("collect");

    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].num_rows(), 1);

    let key = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("keyName");
    assert_eq!(key.value(0), "one");

    let foreign = batches[0]
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("foreignName");
    assert_eq!(foreign.value(0), "keyOne");
}

#[tokio::test(flavor = "multi_thread")]
async fn test_execute_null_handling() {
    let (_server, addr) = start_server().await;
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;

    let db = create_test_db(addr);
    let mut conn = db.new_connection().expect("connect");
    let mut stmt = conn.new_statement().expect("new statement");
    stmt.set_sql_query("SELECT keyName, value FROM intTable WHERE id = 4")
        .expect("set SQL");

    let reader = stmt.execute().expect("execute");
    let batches: Vec<RecordBatch> = reader.collect::<Result<Vec<_>, _>>().expect("collect");

    assert_eq!(batches[0].num_rows(), 1);
    assert!(batches[0].column(0).is_null(0)); // keyName is NULL
    assert!(batches[0].column(1).is_null(0)); // value is NULL
}

#[tokio::test(flavor = "multi_thread")]
async fn test_statement_reuse() {
    let (_server, addr) = start_server().await;
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;

    let db = create_test_db(addr);
    let mut conn = db.new_connection().expect("connect");
    let mut stmt = conn.new_statement().expect("new statement");

    // First query
    stmt.set_sql_query("SELECT keyName FROM intTable WHERE id = 1")
        .expect("set SQL 1");
    let r1 = stmt.execute().expect("execute 1");
    let b1: Vec<RecordBatch> = r1.collect::<Result<Vec<_>, _>>().expect("collect");
    assert_eq!(b1[0].num_rows(), 1);

    // Second query (invalidates first)
    stmt.set_sql_query("SELECT keyName FROM intTable WHERE id = 3")
        .expect("set SQL 2");
    let r2 = stmt.execute().expect("execute 2");
    let b2: Vec<RecordBatch> = r2.collect::<Result<Vec<_>, _>>().expect("collect");
    assert_eq!(b2[0].num_rows(), 1);

    let name = b2[0]
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("keyName");
    assert_eq!(name.value(0), "negative one");
}

#[tokio::test(flavor = "multi_thread")]
async fn test_metadata_against_live_server() {
    let (_server, addr) = start_server().await;
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;

    let db = create_test_db(addr);
    let conn = db.new_connection().expect("connect");

    // get_table_types
    let reader = conn.get_table_types().expect("get_table_types");
    let batches: Vec<RecordBatch> = reader.collect::<Result<Vec<_>, _>>().expect("collect");
    assert!(!batches.is_empty());

    // get_info
    let reader = conn.get_info(None).expect("get_info");
    let batches: Vec<RecordBatch> = reader.collect::<Result<Vec<_>, _>>().expect("collect");
    assert!(!batches.is_empty());

    // get_objects
    let reader = conn
        .get_objects(
            adbc_core::options::ObjectDepth::All,
            None,
            None,
            None,
            None,
            None,
        )
        .expect("get_objects");
    let batches: Vec<RecordBatch> = reader.collect::<Result<Vec<_>, _>>().expect("collect");
    assert!(!batches.is_empty());

    // get_table_schema
    let schema = conn
        .get_table_schema(None, None, "intTable")
        .expect("get_table_schema");
    assert!(schema.fields().len() >= 3);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_username_password_auth() {
    let (_server, addr) = start_server().await;
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;

    let handle = tokio::runtime::Handle::current();
    let mut driver = FlightSqlDriver::new(Some(handle));
    let mut db = driver.new_database().expect("new database");
    db.set_option(OptionDatabase::Uri, OptionValue::String(format!("grpc://{addr}")))
        .expect("set URI");
    db.set_option(OptionDatabase::Username, OptionValue::String("test_user".into()))
        .expect("set username");
    db.set_option(OptionDatabase::Password, OptionValue::String("test_pass".into()))
        .expect("set password");

    let conn = db.new_connection().expect("connect");
    // Connection succeeded with auth — test passes
    std::mem::drop(conn);
}
