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

pub mod driver;
pub use driver::FlightSqlDriver;

pub mod database;
pub use database::FlightSqlDatabase;

pub mod connection;
pub use connection::FlightSqlConnection;

pub mod statement;
pub use statement::FlightSqlStatement;

pub mod stream;
pub use stream::FlightSqlRecordBatchReader;

pub mod runtime;
pub mod error;
pub mod metadata;
pub mod timeouts;

// FFI entry point for driver manager loading.
// When the `ffi` feature is enabled, this module exports
// `AdbcFlightSqlDriverInit` and `AdbcDriverInit` C symbols
// via the `adbc_ffi::export_driver!` macro, populating the
// ADBC 1.1.0 function table for C driver manager consumption.
#[cfg(feature = "ffi")]
pub mod ffi_entry;
