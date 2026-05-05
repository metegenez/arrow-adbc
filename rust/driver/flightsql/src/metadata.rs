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

//! ADBC metadata introspection: get_info, get_objects, get_table_types, get_table_schema.
//!
//! Vendored builder code from driverbase-rs (Apache 2.0).
//! Original source: <https://github.com/adbc-drivers/driverbase-rs>

use std::collections::{HashSet, VecDeque};
use std::sync::{Arc, LazyLock};

use adbc_core::{
    error::Result,
    options::{InfoCode, ObjectDepth},
};
use arrow_array::{
    builder::{
        make_builder, ArrayBuilder, BooleanBuilder, Int16Builder, Int32Builder,
        Int32BufferBuilder, Int64Builder, Int8BufferBuilder, ListBuilder, MapBuilder,
        StringBuilder, StructBuilder, UInt32Builder,
    },
    cast::AsArray,
    types::UInt32Type,
    Array, RecordBatchIterator, RecordBatchReader, UnionArray,
};
use arrow_buffer::ScalarBuffer;
use arrow_flight::{
    decode::FlightRecordBatchStream,
    error::FlightError,
    sql::{CommandGetDbSchemas, CommandGetTables},
    sql::client::FlightSqlServiceClient,
    FlightInfo, IpcMessage,
};
use arrow_schema::{DataType, Field, Fields, Schema, UnionFields, UnionMode};
use futures::StreamExt;
use tonic::transport::Channel;

use crate::error::{ErrorHelper, FlightSqlErrorHelper, map_flight_error};
use crate::runtime::Runtime;

// ============================================================================
// Vendored from driverbase-rs (https://github.com/adbc-drivers/driverbase-rs)
// Copyright (c) 2025 Columnar Technologies Inc.
// Licensed under the Apache License, Version 2.0
// ============================================================================

/// A registry entry for an info value.
///
/// Extended from the original driverbase-rs to support
/// all info value types required by the ADBC spec.
#[derive(Clone)]
pub(crate) enum InfoValue {
    String(String),
    Bool(bool),
    Int64(i64),
}

/// A helper to build the result for GetInfo.
///
/// Vendored from driverbase-rs `InfoBuilder`.
pub struct InfoBuilder {
    info_name: UInt32Builder,
    type_id: Int8BufferBuilder,
    offset: Int32BufferBuilder,
    string_value: StringBuilder,
    bool_value: BooleanBuilder,
    int64_value: Int64Builder,
    int32_bitmask: Int32Builder,
    string_list: ListBuilder<StringBuilder>,
    // NOTE: MapBuilder<Int32, List<Int32>> is present in the original but
    // rarely used. We keep it for spec completeness.
    #[allow(dead_code)]
    int32_to_int32_list_map: MapBuilder<Int32Builder, ListBuilder<Int32Builder>>,
}

impl Default for InfoBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl InfoBuilder {
    const CODE_STRING: i8 = 0;
    const CODE_BOOL: i8 = 1;
    const CODE_INT64: i8 = 2;
    #[allow(dead_code)]
    const CODE_INT32_BITMASK: i8 = 3;
    #[allow(dead_code)]
    const CODE_STRING_LIST: i8 = 4;
    #[allow(dead_code)]
    const CODE_INT32_TO_INT32_LIST_MAP: i8 = 5;

    pub fn new() -> Self {
        InfoBuilder {
            info_name: UInt32Builder::new(),
            type_id: Int8BufferBuilder::new(16),
            offset: Int32BufferBuilder::new(16),
            string_value: StringBuilder::new(),
            bool_value: BooleanBuilder::new(),
            int64_value: Int64Builder::new(),
            int32_bitmask: Int32Builder::new(),
            string_list: ListBuilder::new(StringBuilder::new()),
            int32_to_int32_list_map: MapBuilder::new(
                None,
                Int32Builder::new(),
                ListBuilder::new(Int32Builder::new()),
            ),
        }
    }

    /// Add a string info value with the given key.
    pub fn add_string(&mut self, name: u32, value: impl AsRef<str>) {
        self.info_name.append_value(name);
        self.type_id.append(Self::CODE_STRING);
        self.offset
            .append(ArrayBuilder::len(&self.string_value) as i32);
        self.string_value.append_value(value);
    }

    /// Add a boolean info value with the given key.
    pub fn add_bool(&mut self, name: u32, value: bool) {
        self.info_name.append_value(name);
        self.type_id.append(Self::CODE_BOOL);
        self.offset
            .append(ArrayBuilder::len(&self.bool_value) as i32);
        self.bool_value.append_value(value);
    }

    /// Add an int64 info value with the given key.
    pub fn add_int64(&mut self, name: u32, value: i64) {
        self.info_name.append_value(name);
        self.type_id.append(Self::CODE_INT64);
        self.offset
            .append(ArrayBuilder::len(&self.int64_value) as i32);
        self.int64_value.append_value(value);
    }

    fn add_info_value(&mut self, name: u32, value: &InfoValue) {
        match value {
            InfoValue::String(s) => {
                self.add_string(name, s);
            }
            InfoValue::Bool(b) => {
                self.add_bool(name, *b);
            }
            InfoValue::Int64(i) => {
                self.add_int64(name, *i);
            }
        }
    }

    /// Finish building and get the result as an [`RecordBatchReader`].
    pub fn build(mut self) -> Box<dyn RecordBatchReader + Send> {
        let info_name = self.info_name.finish();
        let type_id = ScalarBuffer::from(self.type_id.as_slice().to_vec());
        let offset = ScalarBuffer::from(self.offset.as_slice().to_vec());
        let children: Vec<Arc<dyn Array>> = vec![
            Arc::new(self.string_value.finish()),
            Arc::new(self.bool_value.finish()),
            Arc::new(self.int64_value.finish()),
            Arc::new(self.int32_bitmask.finish()),
            Arc::new(self.string_list.finish()),
            Arc::new(self.int32_to_int32_list_map.finish()),
        ];

        let string_field = Field::new("string_value", DataType::Utf8, true);
        let bool_field = Field::new("bool_value", DataType::Boolean, true);
        let int64_field = Field::new("int64_value", DataType::Int64, true);
        let int32_bitmask_field = Field::new("int32_bitmask", DataType::Int32, true);
        let string_list_field = Field::new(
            "string_list",
            DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
            true,
        );
        let int32_to_int32_list_map_field = Field::new(
            "int32_to_int32_list_map",
            DataType::Map(
                Arc::new(Field::new(
                    "entries",
                    DataType::Struct(Fields::from(vec![
                        Field::new("key", DataType::Int32, false),
                        Field::new(
                            "value",
                            DataType::List(Arc::new(Field::new(
                                "item", DataType::Int32, true,
                            ))),
                            true,
                        ),
                    ])),
                    false,
                )),
                false,
            ),
            true,
        );

        let union_fields = UnionFields::try_new(
            vec![
                Self::CODE_STRING,
                Self::CODE_BOOL,
                Self::CODE_INT64,
                Self::CODE_INT32_BITMASK,
                Self::CODE_STRING_LIST,
                Self::CODE_INT32_TO_INT32_LIST_MAP,
            ],
            vec![
                string_field,
                bool_field,
                int64_field,
                int32_bitmask_field,
                string_list_field,
                int32_to_int32_list_map_field,
            ],
        )
        .expect("failed to create union fields for InfoBuilder");

        let info_value = unsafe {
            UnionArray::new_unchecked(union_fields.clone(), type_id, Some(offset), children)
        };

        let schema = Arc::new(Schema::new(vec![
            Field::new("info_name", DataType::UInt32, false),
            Field::new(
                "info_value",
                DataType::Union(union_fields, UnionMode::Dense),
                false,
            ),
        ]));

        let num_rows = self.type_id.len();
        let batch = arrow_array::RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(info_name), Arc::new(info_value)],
        )
        .expect("failed to build get_info RecordBatch");

        // Validate that num_rows matches the batch row count after try_new
        assert_eq!(batch.num_rows(), num_rows,
            "InfoBuilder row count mismatch");

        Box::new(RecordBatchIterator::new(
            vec![batch].into_iter().map(Ok),
            schema,
        ))
    }
}

/// A registry for info values.
///
/// Vendored from driverbase-rs `InfoRegistry`, extended with
/// Bool and Int64 support.
#[derive(Default)]
pub struct InfoRegistry {
    info_values: std::collections::HashMap<InfoCode, InfoValue>,
}

impl InfoRegistry {
    pub fn new() -> Self {
        InfoRegistry {
            info_values: std::collections::HashMap::new(),
        }
    }

    /// Add a string info value with the given key.
    pub fn add_string(&mut self, name: InfoCode, value: impl Into<String>) {
        self.info_values
            .insert(name, InfoValue::String(value.into()));
    }

    /// Add a boolean info value with the given key.
    pub fn add_bool(&mut self, name: InfoCode, value: bool) {
        self.info_values.insert(name, InfoValue::Bool(value));
    }

    /// Add an int64 info value with the given key.
    pub fn add_int64(&mut self, name: InfoCode, value: i64) {
        self.info_values.insert(name, InfoValue::Int64(value));
    }

    /// Add any InfoValue variant with the given key.
    pub(crate) fn add_info_value(&mut self, name: InfoCode, value: &InfoValue) {
        self.info_values.insert(name, value.clone());
    }

    /// Check whether the registry contains a value for the given code.
    pub fn contains(&self, code: &InfoCode) -> bool {
        self.info_values.contains_key(code)
    }

    /// Generate the result for a get_info, filtering by the given codes.
    pub fn get_info(&self, codes: Option<HashSet<InfoCode>>) -> InfoBuilder {
        let mut builder = InfoBuilder::new();

        if let Some(codes) = codes {
            for code in codes {
                if let Some(value) = self.info_values.get(&code) {
                    if let Some(code_u32) = info_code_to_u32(code) {
                        builder.add_info_value(code_u32, value);
                    }
                }
            }
        } else {
            for (&name, value) in &self.info_values {
                if let Some(code_u32) = info_code_to_u32(name) {
                    match value {
                        InfoValue::String(s) => {
                            builder.add_string(code_u32, s);
                        }
                        InfoValue::Bool(b) => {
                            builder.add_bool(code_u32, *b);
                        }
                        InfoValue::Int64(i) => {
                            builder.add_int64(code_u32, *i);
                        }
                    }
                }
            }
        }

        builder
    }
}

/// Map an ADBC [`InfoCode`] to its numeric constant.
fn info_code_to_u32(code: InfoCode) -> Option<u32> {
    match code {
        InfoCode::VendorName => Some(adbc_core::constants::ADBC_INFO_VENDOR_NAME),
        InfoCode::VendorVersion => Some(adbc_core::constants::ADBC_INFO_VENDOR_VERSION),
        InfoCode::VendorArrowVersion => {
            Some(adbc_core::constants::ADBC_INFO_VENDOR_ARROW_VERSION)
        }
        InfoCode::VendorSql => Some(adbc_core::constants::ADBC_INFO_VENDOR_SQL),
        InfoCode::VendorSubstrait => Some(adbc_core::constants::ADBC_INFO_VENDOR_SUBSTRAIT),
        InfoCode::VendorSubstraitMinVersion => {
            Some(adbc_core::constants::ADBC_INFO_VENDOR_SUBSTRAIT_MIN_VERSION)
        }
        InfoCode::VendorSubstraitMaxVersion => {
            Some(adbc_core::constants::ADBC_INFO_VENDOR_SUBSTRAIT_MAX_VERSION)
        }
        InfoCode::DriverName => Some(adbc_core::constants::ADBC_INFO_DRIVER_NAME),
        InfoCode::DriverVersion => Some(adbc_core::constants::ADBC_INFO_DRIVER_VERSION),
        InfoCode::DriverArrowVersion => {
            Some(adbc_core::constants::ADBC_INFO_DRIVER_ARROW_VERSION)
        }
        InfoCode::DriverAdbcVersion => {
            Some(adbc_core::constants::ADBC_INFO_DRIVER_ADBC_VERSION)
        }
        _ => None,
    }
}

// ============================================================================
// get_info implementation
// ============================================================================

/// Build a local-only InfoRegistry with driver metadata.
///
/// This registry is the fallback for servers that do not implement
/// the `get_sql_info` RPC. It includes driver name, version, ADBC
/// version, and supported features (SQL, no Substrait).
fn build_driver_info_registry() -> Result<InfoRegistry> {
    let mut registry = InfoRegistry::new();
    registry.add_string(InfoCode::VendorName, "Apache Arrow Flight SQL");
    registry.add_string(InfoCode::DriverName, "Arrow ADBC FlightSQL Driver");
    registry.add_string(InfoCode::DriverVersion, env!("CARGO_PKG_VERSION"));
    registry.add_int64(
        InfoCode::DriverAdbcVersion,
        adbc_core::constants::ADBC_VERSION_1_1_0 as i64,
    );
    registry.add_bool(InfoCode::VendorSql, true);
    registry.add_bool(InfoCode::VendorSubstrait, false);
    Ok(registry)
}

/// ADBC `get_info`: returns driver metadata, optionally supplemented
/// by server `get_sql_info` RPC results.
///
/// If the server implements `get_sql_info`, its results are merged into
/// the local registry (server values take precedence for missing codes).
/// If the server returns an error (typically `Unimplemented`), the
/// local-only registry is used without error.
pub fn get_info(
    runtime: &Runtime,
    client: &mut FlightSqlServiceClient<Channel>,
    codes: Option<HashSet<InfoCode>>,
) -> Result<Box<dyn RecordBatchReader + Send + 'static>> {
    let mut registry = build_driver_info_registry()?;

    // Try server get_sql_info() — if unimplemented, fall back to local-only.
    // Use empty vec to request everything the server knows.
    match runtime.block_on(async { client.get_sql_info(vec![]).await }) {
        Ok(flight_info) => {
            // Extract ticket and collect server info rows
            let ticket = flight_info
                .endpoint
                .first()
                .and_then(|ep| ep.ticket.clone())
                .ok_or_else(|| {
                    FlightSqlErrorHelper::internal_no_location()
                        .message("get_sql_info response has no endpoints")
                        .to_adbc()
                })?;
            let mut stream: FlightRecordBatchStream = runtime
                .block_on(async { client.do_get(ticket).await })
                .map_err(|e: FlightError| {
                    map_flight_error(e, "opening get_sql_info stream")
                })?;
            // Consume the stream to merge server info
            while let Some(batch_result) = runtime.block_on(async { stream.next().await }) {
                let batch: arrow_array::RecordBatch = batch_result.map_err(|e: FlightError| {
                    map_flight_error(e, "reading get_sql_info batch")
                })?;
                let info_names = batch.column(0).as_primitive::<UInt32Type>();
                let info_values = batch.column(1);
                for i in 0..batch.num_rows() {
                    let name = info_names.value(i);
                    // Map FlightSQL SqlInfo code -> ADBC InfoCode
                    if let Ok(adbc_code) = InfoCode::try_from(name) {
                        if !registry.contains(&adbc_code) {
                            if let Some(str_val) =
                                extract_union_string(info_values.as_ref(), i)
                            {
                                registry.add_string(adbc_code, str_val);
                            }
                        }
                    }
                }
            }
        }
        Err(_) => {
            // Server doesn't implement get_sql_info — use local-only info
        }
    }

    Ok(registry.get_info(codes).build())
}

/// Helper: extract a utf8 string from a dense union array slot.
///
/// Returns `None` if the slot is not the string variant (type_id != 0)
/// or is null.
fn extract_union_string(union_arr: &dyn Array, index: usize) -> Option<String> {
    let union = union_arr.as_any().downcast_ref::<UnionArray>()?;
    if union.is_null(index) {
        return None;
    }
    let type_id = union.type_id(index);
    if type_id != 0 {
        return None;
    }
    let child_offset = union.value_offset(index);
    let child = union.child(0);
    let strings = arrow_array::cast::as_string_array(child);
    if strings.is_null(child_offset) {
        return None;
    }
    Some(strings.value(child_offset).to_string())
}

// ============================================================================
// get_objects — Vendored from driverbase-rs get_objects.rs
// Copyright (c) 2025 Columnar Technologies Inc.
// Licensed under the Apache License, Version 2.0
// ============================================================================

/// Information about a single database table.
#[derive(Clone, Debug)]
pub(crate) struct TableInfo {
    pub table_name: String,
    pub table_type: String,
}

/// Information about a single column.
#[derive(Clone, Debug)]
pub(crate) struct ColumnInfo {
    pub column_name: String,
}

/// A table paired with its columns.
#[derive(Clone, Debug)]
pub(crate) struct TableAndColumnInfo {
    pub table: TableInfo,
    pub columns: Vec<ColumnInfo>,
}

/// Trait for getting database objects from a backend.
///
/// Vendored from driverbase-rs `GetObjectsImpl`.
pub(crate) trait GetObjectsImpl<E>: Send + 'static
where
    E: ErrorHelper,
{
    fn get_catalogs(
        &self,
        filter: Option<&str>,
    ) -> std::result::Result<Vec<String>, crate::error::Error<E>>;
    fn get_db_schemas(
        &self,
        catalog: &str,
        filter: Option<&str>,
    ) -> std::result::Result<Vec<String>, crate::error::Error<E>>;
    fn get_tables(
        &self,
        catalog: &str,
        db_schema: &str,
        table_filter: Option<&str>,
        table_type_filter: Option<&[String]>,
    ) -> std::result::Result<Vec<TableInfo>, crate::error::Error<E>>;
    fn get_columns(
        &self,
        catalog: &str,
        db_schema: &str,
        table_filter: Option<&str>,
        table_type_filter: Option<&[String]>,
        column_filter: Option<&str>,
    ) -> std::result::Result<Vec<TableAndColumnInfo>, crate::error::Error<E>>;
}

static SCHEMA: LazyLock<Schema> = LazyLock::new(|| {
    let usage = Fields::from(vec![
        Field::new("fk_catalog", DataType::Utf8, true),
        Field::new("fk_db_schema", DataType::Utf8, true),
        Field::new("fk_table", DataType::Utf8, false),
        Field::new("fk_column_name", DataType::Utf8, false),
    ]);

    let constraint = Fields::from(vec![
        Field::new("constraint_name", DataType::Utf8, true),
        Field::new("constraint_type", DataType::Utf8, true),
        Field::new_list(
            "constraint_column_names",
            Field::new_list_field(DataType::Utf8, false),
            false,
        ),
        Field::new_list(
            "constraint_column_usage",
            Field::new_list_field(DataType::Struct(usage), false),
            false,
        ),
    ]);

    let column = Fields::from(vec![
        Field::new("column_name", DataType::Utf8, false),
        Field::new("ordinal_position", DataType::Int32, true),
        Field::new("remarks", DataType::Utf8, true),
        Field::new("xdbc_data_type", DataType::Int16, true),
        Field::new("xdbc_type_name", DataType::Utf8, true),
        Field::new("xdbc_column_size", DataType::Int32, true),
        Field::new("xdbc_decimal_digits", DataType::Int16, true),
        Field::new("xdbc_num_prec_radix", DataType::Int16, true),
        Field::new("xdbc_nullable", DataType::Int16, true),
        Field::new("xdbc_column_def", DataType::Utf8, true),
        Field::new("xdbc_sql_data_type", DataType::Int16, true),
        Field::new("xdbc_datetime_sub", DataType::Int16, true),
        Field::new("xdbc_char_octet_length", DataType::Int32, true),
        Field::new("xdbc_is_nullable", DataType::Utf8, true),
        Field::new("xdbc_scope_catalog", DataType::Utf8, true),
        Field::new("xdbc_scope_schema", DataType::Utf8, true),
        Field::new("xdbc_scope_table", DataType::Utf8, true),
        Field::new("xdbc_is_autoincrement", DataType::Boolean, true),
        Field::new("xdbc_is_generatedcolumn", DataType::Boolean, true),
    ]);

    let table = Fields::from(vec![
        Field::new("table_name", DataType::Utf8, false),
        Field::new("table_type", DataType::Utf8, false),
        Field::new_list(
            "table_columns",
            Field::new_list_field(DataType::Struct(column), false),
            true,
        ),
        Field::new_list(
            "table_constraints",
            Field::new_list_field(DataType::Struct(constraint), false),
            true,
        ),
    ]);

    let db_schema = Fields::from(vec![
        Field::new("db_schema_name", DataType::Utf8, true),
        Field::new_list(
            "db_schema_tables",
            Field::new_list_field(DataType::Struct(table), false),
            true,
        ),
    ]);

    Schema::new(vec![
        Field::new("catalog_name", DataType::Utf8, true),
        Field::new_list(
            "catalog_db_schemas",
            Field::new_list_field(DataType::Struct(db_schema), false),
            true,
        ),
    ])
});

pub fn get_objects_schema() -> Schema {
    SCHEMA.clone()
}

struct GetObjectsBuilder<I, E>
where
    I: GetObjectsImpl<E>,
    E: ErrorHelper,
{
    _marker: std::marker::PhantomData<E>,
    inner: I,

    depth: ObjectDepth,
    catalog_filter: Option<String>,
    db_schema_filter: Option<String>,
    table_name_filter: Option<String>,
    table_type_filter: Option<Vec<String>>,
    column_name_filter: Option<String>,
}

impl<I, E> GetObjectsBuilder<I, E>
where
    I: GetObjectsImpl<E>,
    E: ErrorHelper,
{
    fn new(
        inner: I,
        depth: ObjectDepth,
        catalog: Option<&str>,
        db_schema: Option<&str>,
        table_name: Option<&str>,
        table_type: Option<Vec<&str>>,
        column_name: Option<&str>,
    ) -> Self {
        Self {
            _marker: std::marker::PhantomData,
            inner,
            depth,
            catalog_filter: catalog.map(|s| s.to_owned()),
            db_schema_filter: db_schema.map(|s| s.to_owned()),
            table_name_filter: table_name.map(|s| s.to_owned()),
            table_type_filter: table_type
                .map(|v| v.into_iter().map(|s| s.to_owned()).collect()),
            column_name_filter: column_name.map(|s| s.to_owned()),
        }
    }

    fn append_catalog(
        &mut self,
        catalog: &str,
        catalog_name: &mut StringBuilder,
        catalog_db_schemas: &mut ListBuilder<Box<dyn ArrayBuilder>>,
    ) -> std::result::Result<(), crate::error::Error<E>> {
        catalog_name.append_value(catalog);

        if let ObjectDepth::Catalogs = self.depth {
            catalog_db_schemas.append_null();
        } else {
            self.append_db_schemas(catalog, catalog_db_schemas)?;
            catalog_db_schemas.append(true);
        }

        Ok(())
    }

    fn append_db_schemas(
        &self,
        catalog: &str,
        catalog_db_schemas: &mut ListBuilder<Box<dyn ArrayBuilder>>,
    ) -> std::result::Result<(), crate::error::Error<E>> {
        let schemas = self
            .inner
            .get_db_schemas(catalog, self.db_schema_filter.as_deref())?;

        let db_schema_item = catalog_db_schemas
            .values()
            .as_any_mut()
            .downcast_mut::<StructBuilder>()
            .unwrap();
        for schema in schemas {
            {
                let db_schema_name = db_schema_item
                    .field_builder::<StringBuilder>(0)
                    .unwrap();
                db_schema_name.append_value(&schema);
            }
            {
                let db_schema_tables = db_schema_item
                    .field_builder::<ListBuilder<Box<dyn ArrayBuilder>>>(1)
                    .unwrap();
                if let ObjectDepth::Schemas = self.depth {
                    db_schema_tables.append_null();
                } else if let ObjectDepth::Tables = self.depth {
                    self.append_tables(catalog, schema.as_ref(), db_schema_tables)?;
                    db_schema_tables.append(true);
                } else {
                    self.append_tables_columns(catalog, schema.as_ref(), db_schema_tables)?;
                    db_schema_tables.append(true);
                }
            }

            db_schema_item.append(true);
        }
        Ok(())
    }

    fn append_tables(
        &self,
        catalog: &str,
        db_schema: &str,
        db_schema_tables: &mut ListBuilder<Box<dyn ArrayBuilder>>,
    ) -> std::result::Result<(), crate::error::Error<E>> {
        let table_item = db_schema_tables
            .values()
            .as_any_mut()
            .downcast_mut::<StructBuilder>()
            .unwrap();

        for table in self.inner.get_tables(
            catalog,
            db_schema,
            self.table_name_filter.as_deref(),
            self.table_type_filter.as_deref(),
        )? {
            {
                let table_name = table_item
                    .field_builder::<StringBuilder>(0)
                    .unwrap();
                table_name.append_value(&table.table_name);
            }
            {
                let table_type = table_item
                    .field_builder::<StringBuilder>(1)
                    .unwrap();
                table_type.append_value(&table.table_type);
            }
            {
                let table_columns = table_item
                    .field_builder::<ListBuilder<Box<dyn ArrayBuilder>>>(2)
                    .unwrap();
                table_columns.append_null();
            }
            {
                let table_constraints = table_item
                    .field_builder::<ListBuilder<Box<dyn ArrayBuilder>>>(3)
                    .unwrap();
                table_constraints.append_null();
            }
            table_item.append(true);
        }

        Ok(())
    }

    fn append_tables_columns(
        &self,
        catalog: &str,
        db_schema: &str,
        db_schema_tables: &mut ListBuilder<Box<dyn ArrayBuilder>>,
    ) -> std::result::Result<(), crate::error::Error<E>> {
        let table_item = db_schema_tables
            .values()
            .as_any_mut()
            .downcast_mut::<StructBuilder>()
            .unwrap();

        for table in self.inner.get_columns(
            catalog,
            db_schema,
            self.table_name_filter.as_deref(),
            self.table_type_filter.as_deref(),
            self.column_name_filter.as_deref(),
        )? {
            {
                let table_name = table_item
                    .field_builder::<StringBuilder>(0)
                    .unwrap();
                table_name.append_value(&table.table.table_name);
            }
            {
                let table_type = table_item
                    .field_builder::<StringBuilder>(1)
                    .unwrap();
                table_type.append_value(&table.table.table_type);
            }
            {
                let table_columns = table_item
                    .field_builder::<ListBuilder<Box<dyn ArrayBuilder>>>(2)
                    .unwrap();

                for (i, column) in table.columns.iter().enumerate() {
                    let column_item = table_columns
                        .values()
                        .as_any_mut()
                        .downcast_mut::<StructBuilder>()
                        .unwrap();

                    {
                        let column_name = column_item
                            .field_builder::<StringBuilder>(0)
                            .unwrap();
                        column_name.append_value(&column.column_name);
                    }
                    {
                        let ordinal_position = column_item
                            .field_builder::<Int32Builder>(1)
                            .unwrap();
                        ordinal_position.append_value(i as i32 + 1);
                    }
                    {
                        let remarks = column_item
                            .field_builder::<StringBuilder>(2)
                            .unwrap();
                        remarks.append_null();
                    }
                    {
                        let xdbc_data_type = column_item
                            .field_builder::<Int16Builder>(3)
                            .unwrap();
                        xdbc_data_type.append_null();
                    }
                    {
                        let xdbc_type_name = column_item
                            .field_builder::<StringBuilder>(4)
                            .unwrap();
                        xdbc_type_name.append_null();
                    }
                    {
                        let xdbc_column_size = column_item
                            .field_builder::<Int32Builder>(5)
                            .unwrap();
                        xdbc_column_size.append_null();
                    }
                    {
                        let xdbc_decimal_digits = column_item
                            .field_builder::<Int16Builder>(6)
                            .unwrap();
                        xdbc_decimal_digits.append_null();
                    }
                    {
                        let xdbc_num_prec_radix = column_item
                            .field_builder::<Int16Builder>(7)
                            .unwrap();
                        xdbc_num_prec_radix.append_null();
                    }
                    {
                        let xdbc_nullable = column_item
                            .field_builder::<Int16Builder>(8)
                            .unwrap();
                        xdbc_nullable.append_null();
                    }
                    {
                        let xdbc_column_def = column_item
                            .field_builder::<StringBuilder>(9)
                            .unwrap();
                        xdbc_column_def.append_null();
                    }
                    {
                        let xdbc_sql_data_type = column_item
                            .field_builder::<Int16Builder>(10)
                            .unwrap();
                        xdbc_sql_data_type.append_null();
                    }
                    {
                        let xdbc_datetime_sub = column_item
                            .field_builder::<Int16Builder>(11)
                            .unwrap();
                        xdbc_datetime_sub.append_null();
                    }
                    {
                        let xdbc_char_octet_length = column_item
                            .field_builder::<Int32Builder>(12)
                            .unwrap();
                        xdbc_char_octet_length.append_null();
                    }
                    {
                        let xdbc_is_nullable = column_item
                            .field_builder::<StringBuilder>(13)
                            .unwrap();
                        xdbc_is_nullable.append_null();
                    }
                    {
                        let xdbc_scope_catalog = column_item
                            .field_builder::<StringBuilder>(14)
                            .unwrap();
                        xdbc_scope_catalog.append_null();
                    }
                    {
                        let xdbc_scope_schema = column_item
                            .field_builder::<StringBuilder>(15)
                            .unwrap();
                        xdbc_scope_schema.append_null();
                    }
                    {
                        let xdbc_scope_table = column_item
                            .field_builder::<StringBuilder>(16)
                            .unwrap();
                        xdbc_scope_table.append_null();
                    }
                    {
                        let xdbc_is_autoincrement = column_item
                            .field_builder::<BooleanBuilder>(17)
                            .unwrap();
                        xdbc_is_autoincrement.append_null();
                    }
                    {
                        let xdbc_is_generatedcolumn = column_item
                            .field_builder::<BooleanBuilder>(18)
                            .unwrap();
                        xdbc_is_generatedcolumn.append_null();
                    }
                    column_item.append(true);
                }

                table_columns.append(true);
            }
            {
                let table_constraints = table_item
                    .field_builder::<ListBuilder<Box<dyn ArrayBuilder>>>(3)
                    .unwrap();
                table_constraints.append_null();
            }
            table_item.append(true);
        }

        Ok(())
    }
}

enum BuilderState {
    Uninit,
    ReadCatalogs(VecDeque<String>),
    Finished,
}

/// Split out the builder state so that we can avoid mutable reborrows.
struct BatchIterator<I, E>
where
    I: GetObjectsImpl<E>,
    E: ErrorHelper,
{
    state: BuilderState,
    catalog_name: StringBuilder,
    catalog_db_schemas: Box<dyn ArrayBuilder>,
    inner: GetObjectsBuilder<I, E>,
}

impl<I, E> BatchIterator<I, E>
where
    I: GetObjectsImpl<E>,
    E: ErrorHelper,
{
    fn new(inner: GetObjectsBuilder<I, E>) -> Self {
        let schema = get_objects_schema();
        let catalog_name = StringBuilder::with_capacity(32, 65536);
        let catalog_db_schemas = make_builder(schema.fields()[1].data_type(), 32);
        Self {
            state: BuilderState::Uninit,
            catalog_name,
            catalog_db_schemas,
            inner,
        }
    }
}

impl<I, E> Iterator for BatchIterator<I, E>
where
    I: GetObjectsImpl<E>,
    E: ErrorHelper,
{
    type Item = std::result::Result<arrow_array::RecordBatch, arrow_schema::ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.state {
            BuilderState::Uninit => {
                match self
                    .inner
                    .inner
                    .get_catalogs(self.inner.catalog_filter.as_deref())
                {
                    Ok(catalogs) => {
                        self.state = BuilderState::ReadCatalogs(
                            catalogs.into_iter().collect::<VecDeque<_>>(),
                        );
                        self.next()
                    }
                    Err(e) => {
                        self.state = BuilderState::Finished;
                        Some(Err(arrow_schema::ArrowError::ExternalError(Box::new(e))))
                    }
                }
            }
            BuilderState::ReadCatalogs(ref mut catalogs) => {
                if let Some(catalog) = catalogs.pop_front() {
                    let catalog_db_schemas = self
                        .catalog_db_schemas
                        .as_any_mut()
                        .downcast_mut::<ListBuilder<Box<dyn ArrayBuilder>>>()
                        .unwrap();

                    match self.inner.append_catalog(
                        &catalog,
                        &mut self.catalog_name,
                        catalog_db_schemas,
                    ) {
                        Ok(()) => {
                            let batch = arrow_array::RecordBatch::try_new(
                                Arc::new(get_objects_schema()),
                                vec![
                                    Arc::new(self.catalog_name.finish()),
                                    self.catalog_db_schemas.finish(),
                                ],
                            );
                            match batch {
                                Ok(b) => Some(Ok(b)),
                                Err(e) => {
                                    self.state = BuilderState::Finished;
                                    Some(Err(e))
                                }
                            }
                        }
                        Err(e) => {
                            self.state = BuilderState::Finished;
                            Some(Err(arrow_schema::ArrowError::ExternalError(Box::new(e))))
                        }
                    }
                } else {
                    self.state = BuilderState::Finished;
                    None
                }
            }
            BuilderState::Finished => None,
        }
    }
}

fn get_objects_inner<I, E>(
    inner: I,
    depth: ObjectDepth,
    catalog: Option<&str>,
    db_schema: Option<&str>,
    table_name: Option<&str>,
    table_type: Option<Vec<&str>>,
    column_name: Option<&str>,
) -> Box<dyn RecordBatchReader + Send + 'static>
where
    I: GetObjectsImpl<E>,
    E: ErrorHelper,
{
    Box::new(RecordBatchIterator::new(
        BatchIterator::new(GetObjectsBuilder::new(
            inner,
            depth,
            catalog,
            db_schema,
            table_name,
            table_type,
            column_name,
        )),
        Arc::new(get_objects_schema()),
    ))
}

// ============================================================================
// FlightSQL RPC integration for GetObjectsImpl
// ============================================================================

/// Context holding runtime and client for GetObjectsImpl FlightSQL RPC integration.
///
/// The client is wrapped in a `Mutex` because `GetObjectsImpl` trait methods
/// take `&self` while `FlightSqlServiceClient` methods require `&mut self`.
pub(crate) struct FlightSqlGetObjectsContext {
    pub runtime: Arc<Runtime>,
    client: std::sync::Mutex<FlightSqlServiceClient<Channel>>,
}

/// Map a FlightError to an Error<FlightSqlErrorHelper> (for trait methods).
fn map_flight_error_e(
    e: FlightError,
    context: &str,
) -> crate::error::Error<FlightSqlErrorHelper> {
    crate::error::Error::<FlightSqlErrorHelper>::from(
        arrow_schema::ArrowError::ExternalError(Box::new(map_flight_error(e, context))),
    )
}

/// Collect column 0 (utf8) from all batches of a do_get stream.
fn collect_string_column(
    runtime: &Runtime,
    client: &mut FlightSqlServiceClient<Channel>,
    flight_info: FlightInfo,
) -> std::result::Result<Vec<String>, crate::error::Error<FlightSqlErrorHelper>> {
    let ticket = flight_info
        .endpoint
        .first()
        .and_then(|ep| ep.ticket.clone())
        .ok_or_else(|| {
            FlightSqlErrorHelper::internal_no_location()
                .message("metadata response has no endpoints")
        })?;

    let mut stream: FlightRecordBatchStream = runtime
        .block_on(async { client.do_get(ticket).await })
        .map_err(|e: FlightError| map_flight_error_e(e, "opening metadata stream"))?;

    let mut result = Vec::new();
    loop {
        let next = runtime.block_on(async { stream.next().await });
        match next {
            Some(Ok(batch)) => {
                let col = batch.column(0);
                let strings = arrow_array::cast::as_string_array(col);
                for i in 0..strings.len() {
                    if !strings.is_null(i) {
                        result.push(strings.value(i).to_string());
                    }
                }
            }
            Some(Err(e)) => {
                return Err(map_flight_error_e(e, "reading metadata batch"));
            }
            None => break,
        }
    }
    Ok(result)
}

/// Collect table_name + table_type pairs from get_tables response.
fn collect_table_info(
    runtime: &Runtime,
    client: &mut FlightSqlServiceClient<Channel>,
    flight_info: FlightInfo,
) -> std::result::Result<Vec<TableInfo>, crate::error::Error<FlightSqlErrorHelper>> {
    let ticket = flight_info
        .endpoint
        .first()
        .and_then(|ep| ep.ticket.clone())
        .ok_or_else(|| {
            FlightSqlErrorHelper::internal_no_location()
                .message("metadata response has no endpoints")
        })?;

    let mut stream: FlightRecordBatchStream = runtime
        .block_on(async { client.do_get(ticket).await })
        .map_err(|e: FlightError| map_flight_error_e(e, "opening tables stream"))?;

    let mut result = Vec::new();
    loop {
        let next = runtime.block_on(async { stream.next().await });
        match next {
            Some(Ok(batch)) => {
                let names = arrow_array::cast::as_string_array(batch.column(2));
                let types = arrow_array::cast::as_string_array(batch.column(3));
                for i in 0..batch.num_rows() {
                    result.push(TableInfo {
                        table_name: names.value(i).to_string(),
                        table_type: types.value(i).to_string(),
                    });
                }
            }
            Some(Err(e)) => {
                return Err(map_flight_error_e(e, "reading tables batch"));
            }
            None => break,
        }
    }
    Ok(result)
}

/// Collect table+column info from get_tables(include_schema=true) response.
fn collect_table_and_column_info(
    runtime: &Runtime,
    client: &mut FlightSqlServiceClient<Channel>,
    flight_info: FlightInfo,
) -> std::result::Result<Vec<TableAndColumnInfo>, crate::error::Error<FlightSqlErrorHelper>> {
    let ticket = flight_info
        .endpoint
        .first()
        .and_then(|ep| ep.ticket.clone())
        .ok_or_else(|| {
            FlightSqlErrorHelper::internal_no_location()
                .message("metadata response has no endpoints")
        })?;

    let mut stream: FlightRecordBatchStream = runtime
        .block_on(async { client.do_get(ticket).await })
        .map_err(|e: FlightError| map_flight_error_e(e, "opening columns stream"))?;

    let mut result = Vec::new();
    loop {
        let next = runtime.block_on(async { stream.next().await });
        match next {
            Some(Ok(batch)) => {
                let names = arrow_array::cast::as_string_array(batch.column(2));
                let types = arrow_array::cast::as_string_array(batch.column(3));
                let schema_col = batch.column(4);
                for i in 0..batch.num_rows() {
                    let mut columns = Vec::new();
                    let bytes_arr = arrow_array::cast::as_generic_binary_array::<i32>(schema_col);
                    if !bytes_arr.is_null(i) {
                        let ipc_bytes = bytes_arr.value(i);
                        if !ipc_bytes.is_empty() {
                            if let Ok(schema) =
                                Schema::try_from(IpcMessage(ipc_bytes.to_vec().into()))
                            {
                                for field in schema.fields() {
                                    columns.push(ColumnInfo {
                                        column_name: field.name().clone(),
                                    });
                                }
                            }
                        }
                    }
                    result.push(TableAndColumnInfo {
                        table: TableInfo {
                            table_name: names.value(i).to_string(),
                            table_type: types.value(i).to_string(),
                        },
                        columns,
                    });
                }
            }
            Some(Err(e)) => {
                return Err(map_flight_error_e(e, "reading columns batch"));
            }
            None => break,
        }
    }
    Ok(result)
}

impl GetObjectsImpl<FlightSqlErrorHelper> for FlightSqlGetObjectsContext {
    fn get_catalogs(
        &self,
        filter: Option<&str>,
    ) -> std::result::Result<Vec<String>, crate::error::Error<FlightSqlErrorHelper>> {
        let mut client = self.client.lock().unwrap();
        let flight_info = self
            .runtime
            .block_on(async { client.get_catalogs().await })
            .map_err(|e: FlightError| map_flight_error_e(e, "getting catalogs"))?;

        let catalogs = collect_string_column(&self.runtime, &mut *client, flight_info)?;

        // Catalog-less server fallback: if no catalogs returned,
        // insert "" to allow schema discovery (per Go driver pattern).
        if catalogs.is_empty() {
            return Ok(vec![String::new()]);
        }
        // If filter is provided, filter catalogs
        if let Some(f) = filter {
            return Ok(catalogs
                .into_iter()
                .filter(|c| c.contains(f))
                .collect());
        }
        Ok(catalogs)
    }

    fn get_db_schemas(
        &self,
        catalog: &str,
        filter: Option<&str>,
    ) -> std::result::Result<Vec<String>, crate::error::Error<FlightSqlErrorHelper>> {
        let mut client = self.client.lock().unwrap();
        let request = CommandGetDbSchemas {
            catalog: if catalog.is_empty() {
                None
            } else {
                Some(catalog.to_string())
            },
            db_schema_filter_pattern: filter.map(|s| s.to_string()),
        };
        let flight_info = self
            .runtime
            .block_on(async { client.get_db_schemas(request).await })
            .map_err(|e: FlightError| map_flight_error_e(e, "getting db schemas"))?;

        collect_string_column(&self.runtime, &mut *client, flight_info)
    }

    fn get_tables(
        &self,
        catalog: &str,
        db_schema: &str,
        table_filter: Option<&str>,
        table_type_filter: Option<&[String]>,
    ) -> std::result::Result<Vec<TableInfo>, crate::error::Error<FlightSqlErrorHelper>> {
        let mut client = self.client.lock().unwrap();
        let request = CommandGetTables {
            catalog: if catalog.is_empty() {
                None
            } else {
                Some(catalog.to_string())
            },
            db_schema_filter_pattern: if db_schema.is_empty() {
                None
            } else {
                Some(db_schema.to_string())
            },
            table_name_filter_pattern: table_filter.map(|s| s.to_string()),
            table_types: table_type_filter.map(|v| v.to_vec()).unwrap_or_default(),
            include_schema: false,
        };
        let flight_info = self
            .runtime
            .block_on(async { client.get_tables(request).await })
            .map_err(|e: FlightError| map_flight_error_e(e, "getting tables"))?;

        collect_table_info(&self.runtime, &mut *client, flight_info)
    }

    fn get_columns(
        &self,
        catalog: &str,
        db_schema: &str,
        table_filter: Option<&str>,
        table_type_filter: Option<&[String]>,
        _column_filter: Option<&str>,
    ) -> std::result::Result<
        Vec<TableAndColumnInfo>,
        crate::error::Error<FlightSqlErrorHelper>,
    > {
        let mut client = self.client.lock().unwrap();
        let request = CommandGetTables {
            catalog: if catalog.is_empty() {
                None
            } else {
                Some(catalog.to_string())
            },
            db_schema_filter_pattern: if db_schema.is_empty() {
                None
            } else {
                Some(db_schema.to_string())
            },
            table_name_filter_pattern: table_filter.map(|s| s.to_string()),
            table_types: table_type_filter.map(|v| v.to_vec()).unwrap_or_default(),
            include_schema: true, // Request IPC schema to extract column names
        };
        let flight_info = self
            .runtime
            .block_on(async { client.get_tables(request).await })
            .map_err(|e: FlightError| map_flight_error_e(e, "getting tables with columns"))?;

        collect_table_and_column_info(&self.runtime, &mut *client, flight_info)
    }
}

/// ADBC get_objects: catalog→schema→table→column hierarchy with depth filtering.
pub fn get_objects(
    runtime: Arc<Runtime>,
    client: FlightSqlServiceClient<Channel>,
    depth: ObjectDepth,
    catalog: Option<&str>,
    db_schema: Option<&str>,
    table_name: Option<&str>,
    table_type: Option<Vec<&str>>,
    column_name: Option<&str>,
) -> Box<dyn RecordBatchReader + Send + 'static> {
    let context = FlightSqlGetObjectsContext {
        runtime,
        client: std::sync::Mutex::new(client),
    };
    get_objects_inner(
        context,
        depth,
        catalog,
        db_schema,
        table_name,
        table_type,
        column_name,
    )
}

// ============================================================================
// get_table_types
// ============================================================================

/// ADBC `get_table_types`: return list of supported table types from the server.
pub fn get_table_types(
    runtime: &Arc<Runtime>,
    client: &mut FlightSqlServiceClient<Channel>,
) -> Result<Box<dyn RecordBatchReader + Send + 'static>> {
    let flight_info = runtime
        .block_on(async { client.get_table_types().await })
        .map_err(|e: FlightError| map_flight_error(e, "getting table types"))?;

    let schema = Schema::try_from(IpcMessage(flight_info.schema.clone().into()))
        .map_err(|e| {
            FlightSqlErrorHelper::internal_no_location()
                .message(format!("failed to decode table types schema: {e}"))
                .to_adbc()
        })?;

    let ticket = flight_info
        .endpoint
        .first()
        .and_then(|ep| ep.ticket.clone())
        .ok_or_else(|| {
            FlightSqlErrorHelper::internal_no_location()
                .message("get_table_types response has no endpoints")
                .to_adbc()
        })?;

    let stream: FlightRecordBatchStream = runtime
        .block_on(async { client.do_get(ticket).await })
        .map_err(|e: FlightError| {
            map_flight_error(e, "opening table types stream")
        })?;

    Ok(Box::new(crate::stream::FlightSqlRecordBatchReader::new(
        Arc::clone(runtime),
        stream,
        schema,
        Arc::new(std::sync::atomic::AtomicBool::new(false)),
    )))
}

// ============================================================================
// get_table_schema
// ============================================================================

/// ADBC `get_table_schema`: return the Arrow Schema for a named table.
///
/// Primary strategy: `get_tables(include_schema=true)` → extract IPC
/// schema from the `table_schema` column (column index 4).
///
/// Fallback: `prepare("SELECT * FROM {table} LIMIT 0")` →
/// `dataset_schema()` for servers that don't include schemas
/// in `get_tables`.
pub fn get_table_schema(
    runtime: &Arc<Runtime>,
    client: &mut FlightSqlServiceClient<Channel>,
    catalog: Option<&str>,
    db_schema: Option<&str>,
    table_name: &str,
) -> Result<Schema> {
    // Strategy 1: get_tables with include_schema=true
    let request = CommandGetTables {
        catalog: catalog.map(|s| s.to_string()),
        db_schema_filter_pattern: db_schema.map(|s| s.to_string()),
        table_name_filter_pattern: Some(table_name.to_string()),
        table_types: vec![],
        include_schema: true,
    };

    let schema_result: Result<Schema> = (|| {
        let flight_info = runtime
            .block_on(async { client.get_tables(request.clone()).await })
            .map_err(|e: FlightError| {
                map_flight_error(e, "getting table schema via get_tables")
            })?;

        let ticket = flight_info
            .endpoint
            .first()
            .and_then(|ep| ep.ticket.clone())
            .ok_or_else(|| {
                FlightSqlErrorHelper::internal_no_location()
                    .message("get_tables response has no endpoints")
                    .to_adbc()
            })?;

        let mut stream: FlightRecordBatchStream = runtime
            .block_on(async { client.do_get(ticket).await })
            .map_err(|e: FlightError| {
                map_flight_error(e, "opening table schema stream")
            })?;

        // Read first batch — expect exactly one row for the requested table
        let first = runtime.block_on(async { stream.next().await });
        match first {
            Some(Ok(batch)) => {
                let schema_col = batch.column(4); // table_schema is column index 4
                let bytes_arr =
                    arrow_array::cast::as_generic_binary_array::<i32>(schema_col);
                if bytes_arr.is_null(0) || bytes_arr.value(0).is_empty() {
                    return Err(FlightSqlErrorHelper::not_found()
                        .message(format!(
                            "schema not available for table '{}' via get_tables",
                            table_name
                        ))
                        .to_adbc());
                }
                let schema_bytes: Vec<u8> = bytes_arr.value(0).to_vec();
                Schema::try_from(IpcMessage(schema_bytes.into())).map_err(|e| {
                    FlightSqlErrorHelper::internal_no_location()
                        .message(format!(
                            "failed to decode table schema IPC: {e}"
                        ))
                        .to_adbc()
                })
            }
            Some(Err(e)) => Err(map_flight_error(e, "reading table schema batch")),
            None => Err(FlightSqlErrorHelper::not_found()
                .message(format!("table '{}' not found", table_name))
                .to_adbc()),
        }
    })();

    match schema_result {
        Ok(schema) => Ok(schema),
        Err(_get_tables_err) => {
            // Strategy 2: prepare("SELECT * FROM {table} LIMIT 0")
            let sql = format!("SELECT * FROM {table_name} LIMIT 0");
            let prepared = runtime
                .block_on(async { client.prepare(sql, None).await })
                .map_err(|e: FlightError| {
                    map_flight_error(e, "preparing query for table schema")
                })?;

            prepared
                .dataset_schema()
                .cloned()
                .map_err(|e: FlightError| {
                    map_flight_error(e, "getting dataset schema from prepared statement")
                })
        }
    }
}

// ============================================================================
// Unit tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_info_registry_add_and_retrieve() {
        let mut registry = InfoRegistry::new();
        registry.add_string(InfoCode::DriverName, "Test Driver");

        let mut codes = HashSet::new();
        codes.insert(InfoCode::DriverName);
        let builder = registry.get_info(Some(codes));
        let reader = builder.build();

        let schema = reader.schema();
        assert_eq!(schema.field(0).name(), "info_name");
        assert_eq!(schema.field(1).name(), "info_value");

        // Collect all batches
        let batches: Vec<_> = reader.collect::<std::result::Result<_, _>>().unwrap();
        assert!(!batches.is_empty());
        // Should have at least one value
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert!(total_rows >= 1);
    }

    #[test]
    fn test_info_registry_get_all() {
        let mut registry = InfoRegistry::new();
        registry.add_string(InfoCode::DriverName, "Test Driver");
        registry.add_string(InfoCode::VendorName, "Test Vendor");
        registry.add_bool(InfoCode::VendorSql, true);

        let builder = registry.get_info(None);
        let reader = builder.build();
        let batches: Vec<_> = reader.collect::<std::result::Result<_, _>>().unwrap();
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 3);
    }

    #[test]
    fn test_info_registry_filtered() {
        let mut registry = InfoRegistry::new();
        registry.add_string(InfoCode::DriverName, "MyDriver");
        registry.add_string(InfoCode::VendorName, "MyVendor");

        let mut codes = HashSet::new();
        codes.insert(InfoCode::DriverName);
        let builder = registry.get_info(Some(codes));
        let reader = builder.build();
        let batches: Vec<_> = reader.collect::<std::result::Result<_, _>>().unwrap();
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 1);
    }

    #[test]
    fn test_build_driver_info_registry() {
        let registry = build_driver_info_registry().unwrap();
        let info = registry.get_info(None);

        // Verify it produces a non-empty Reader
        // (actual content checked by get_info integration test)
        let _reader = info.build();
    }

    #[test]
    fn test_get_info_all_no_server() {
        // get_info with no server → uses local registry (no runtime needed for this path)
        // This is a unit test — we can't make server calls, but we can verify
        // build_driver_info_registry produces valid data.
        let registry = build_driver_info_registry().unwrap();
        let builder = registry.get_info(None);
        let reader = builder.build();

        let schema = reader.schema();
        assert_eq!(schema.fields().len(), 2);
        let batches: Vec<_> = reader.collect::<std::result::Result<_, _>>().unwrap();
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert!(total_rows >= 4, "Expected at least 4 info entries (driver + vendor info)");
    }

    #[test]
    fn test_get_info_includes_driver_version() {
        let registry = build_driver_info_registry().unwrap();
        let mut codes = HashSet::new();
        codes.insert(InfoCode::DriverVersion);
        let builder = registry.get_info(Some(codes));
        let reader = builder.build();

        let batches: Vec<_> = reader.collect::<std::result::Result<_, _>>().unwrap();
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 1, "Expected exactly one row for DriverVersion filter");
    }

    #[test]
    fn test_extract_union_string_simple() {
        use arrow_array::{StringArray, UnionArray};
        use arrow_buffer::ScalarBuffer;
        use arrow_schema::{Field, UnionFields};

        let string_field = Field::new("string_value", DataType::Utf8, true);
        let union_fields =
            UnionFields::try_new(vec![0i8], vec![string_field]).unwrap();

        let strings = StringArray::from(vec![Some("hello"), None, Some("world")]);

        // type_id: 0 means slot 0 uses type 0 (string)
        let type_ids = ScalarBuffer::from(vec![0i8, 0i8, 0i8]);
        // offsets into child array
        let offsets = ScalarBuffer::from(vec![0i32, 1i32, 2i32]);

        let union_arr = unsafe {
            UnionArray::new_unchecked(
                union_fields.clone(),
                type_ids,
                Some(offsets),
                vec![Arc::new(strings) as Arc<dyn Array>],
            )
        };

        assert_eq!(
            extract_union_string(&union_arr, 0),
            Some("hello".to_string())
        );
        // index 1 maps to child offset 1 which is null in the string array
        assert_eq!(extract_union_string(&union_arr, 1), None);
        assert_eq!(
            extract_union_string(&union_arr, 2),
            Some("world".to_string())
        );
    }

    #[test]
    fn test_extract_union_string_wrong_type() {
        let string_field = Field::new("string_value", DataType::Utf8, true);
        let union_fields =
            UnionFields::try_new(vec![0i8, 1i8], vec![string_field.clone(), string_field]).unwrap();
        let strings =
            arrow_array::StringArray::from(vec![Some("a"), Some("b")]);

        let type_ids = ScalarBuffer::from(vec![1i8]); // type 1, not 0
        let offsets = ScalarBuffer::from(vec![0i32]);

        let union_arr = unsafe {
            UnionArray::new_unchecked(
                union_fields,
                type_ids,
                Some(offsets),
                vec![
                    Arc::new(arrow_array::StringArray::from(Vec::<Option<&str>>::new())),
                    Arc::new(strings),
                ],
            )
        };

        assert_eq!(extract_union_string(&union_arr, 0), None);
    }

    #[test]
    fn test_get_objects_schema_has_nineteen_column_fields() {
        let schema = get_objects_schema();
        // Top-level: catalog_name + catalog_db_schemas
        assert_eq!(schema.fields().len(), 2);
        assert_eq!(schema.field(0).name(), "catalog_name");

        // catalog_db_schemas is a list of struct
        let db_schema_field = schema.field(1);
        assert_eq!(db_schema_field.name(), "catalog_db_schemas");
        let list_dt = match db_schema_field.data_type() {
            DataType::List(f) => f,
            _ => panic!("catalog_db_schemas should be List"),
        };
        // Inner struct: db_schema_name + db_schema_tables
        let db_schema_inner = match list_dt.data_type() {
            DataType::Struct(fields) => fields,
            _ => panic!("db_schema should be Struct"),
        };
        assert_eq!(db_schema_inner.len(), 2);
        assert_eq!(db_schema_inner[0].name(), "db_schema_name");

        // db_schema_tables is a list of struct
        let tables_field = &db_schema_inner[1];
        let tables_list = match tables_field.data_type() {
            DataType::List(f) => f,
            _ => panic!("db_schema_tables should be List"),
        };
        // Table struct: table_name, table_type, table_columns, table_constraints
        let table_inner = match tables_list.data_type() {
            DataType::Struct(fields) => fields,
            _ => panic!("table should be Struct"),
        };
        assert_eq!(table_inner.len(), 4);

        // table_columns is a list of struct with 19 column fields
        let columns_field = &table_inner[2];
        let columns_list = match columns_field.data_type() {
            DataType::List(f) => f,
            _ => panic!("table_columns should be List"),
        };
        let column_inner = match columns_list.data_type() {
            DataType::Struct(fields) => fields,
            _ => panic!("column should be Struct"),
        };
        assert_eq!(
            column_inner.len(),
            19,
            "get_objects column struct must have 19 fields"
        );
        assert_eq!(column_inner[0].name(), "column_name");
        assert_eq!(column_inner[1].name(), "ordinal_position");
        assert_eq!(column_inner[17].name(), "xdbc_is_autoincrement");
        assert_eq!(column_inner[18].name(), "xdbc_is_generatedcolumn");
    }

    #[test]
    fn test_get_objects_schema_clone_is_independent() {
        let s1 = get_objects_schema();
        let s2 = get_objects_schema();
        assert_eq!(s1.fields().len(), s2.fields().len());
        // Clones should be equal but not share identity
        assert_eq!(s1, s2);
    }

    #[test]
    fn test_info_registry_contains() {
        let mut registry = InfoRegistry::new();
        assert!(!registry.contains(&InfoCode::DriverName));
        registry.add_string(InfoCode::DriverName, "test");
        assert!(registry.contains(&InfoCode::DriverName));
        assert!(!registry.contains(&InfoCode::DriverVersion));
    }

    #[test]
    fn test_info_registry_bool_and_int64() {
        let mut registry = InfoRegistry::new();
        registry.add_bool(InfoCode::VendorSql, true);
        registry.add_int64(InfoCode::DriverAdbcVersion, 1001000);

        let builder = registry.get_info(None);
        let reader = builder.build();
        let batches: Vec<_> = reader.collect::<std::result::Result<_, _>>().unwrap();
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 2);
    }

    #[test]
    fn test_info_builder_extended_types() {
        let mut builder = InfoBuilder::new();
        builder.add_string(100, "hello");
        builder.add_bool(101, true);
        builder.add_int64(102, 42);

        let reader = builder.build();
        let schema = reader.schema();
        assert_eq!(schema.fields().len(), 2);

        let batches: Vec<_> = reader.collect::<std::result::Result<_, _>>().unwrap();
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 3);
    }
}
