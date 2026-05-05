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

use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use adbc_core::{
    error::Result,
    options::{InfoCode, ObjectDepth, OptionConnection, OptionValue},
    Connection, Optionable,
};
use arrow_array::RecordBatchReader;
use arrow_flight::error::FlightError;
use arrow_flight::sql::client::FlightSqlServiceClient;
use arrow_schema::Schema;
use prost::Message;
use tonic::transport::{ClientTlsConfig, Endpoint};

use crate::database::{TlsOptions, Transport};
use crate::error::{ErrorHelper, FlightSqlErrorHelper, map_flight_error};
use crate::metadata;
use crate::runtime::Runtime;
use crate::statement::FlightSqlStatement;
use crate::stream::FlightSqlRecordBatchReader;
use crate::timeouts::TimeoutOption;

/// A handle to a FlightSQL connection.
///
/// Wraps a `FlightSqlServiceClient` over a gRPC channel. Supports
/// plaintext (`grpc://`), TLS (`grpc+tls://`), and Unix socket (`grpc+unix://`)
/// transports with bearer token, username/password handshake, and OAuth authentication.
pub struct FlightSqlConnection {
    runtime: Arc<Runtime>,
    client: FlightSqlServiceClient<tonic::transport::Channel>,
    // Option state (FSQL-12):
    read_only: Option<bool>,
    autocommit: Option<bool>,
    current_catalog: Option<String>,
    current_schema: Option<String>,
    isolation_level: Option<String>,
    // Per-operation timeouts (FSQL-22):
    timeouts: TimeoutOption,
    // Cookie jar (FSQL-33):
    cookies: Option<Arc<std::sync::Mutex<Vec<(String, String)>>>>,
    // Session options (FSQL-26):
    session_options: HashMap<String, String>,
}

impl FlightSqlConnection {
    pub(crate) fn new(
        runtime: Arc<Runtime>,
        transport: &Transport,
        tls_opts: TlsOptions,
        username: Option<String>,
        password: Option<String>,
        token: Option<String>,
        cookie_middleware: bool,
    ) -> Result<Self> {
        let channel = runtime.block_on(async {
            Self::create_channel(transport, &tls_opts)
        })?;
        let mut client = FlightSqlServiceClient::new(channel);

        if let (Some(username), Some(password)) = (&username, &password) {
            let _bearer_token = runtime.block_on(async {
                client.handshake(username, password).await
            }).map_err(|e: arrow_flight::error::FlightError| {
                FlightSqlErrorHelper::unauthenticated()
                    .message(format!("handshake failed: {e}"))
                    .context("authenticating to FlightSQL server")
                    .to_adbc()
            })?;
        } else if let Some(token) = &token {
            client.set_token(token.clone());
        }

        let cookies = if cookie_middleware {
            Some(Arc::new(std::sync::Mutex::new(Vec::new())))
        } else {
            None
        };

        // Forward cookies from jar as Cookie header if middleware is enabled
        if let Some(ref jar) = cookies {
            let cookie_header = Self::build_cookie_header(jar);
            if !cookie_header.is_empty() {
                client.set_header("cookie", cookie_header);
            }
        }

        Ok(Self {
            runtime,
            client,
            read_only: None,
            autocommit: Some(true),
            current_catalog: None,
            current_schema: None,
            isolation_level: None,
            timeouts: TimeoutOption::new(),
            cookies,
            session_options: HashMap::new(),
        })
    }

    fn create_channel(transport: &Transport, tls_opts: &TlsOptions) -> Result<tonic::transport::Channel> {
        match transport {
            Transport::Plaintext { host, port } => Self::create_plaintext_channel(host, *port),
            Transport::Tls { host, port } => Self::create_tls_channel(host, *port, tls_opts),
            Transport::Unix { path } => Self::create_unix_channel(path),
        }
    }

    fn create_plaintext_channel(host: &str, port: u16) -> Result<tonic::transport::Channel> {
        let addr = format!("http://{host}:{port}");
        let endpoint = Endpoint::from_shared(addr)
            .map_err(|e| {
                FlightSqlErrorHelper::invalid_argument()
                    .message(format!("invalid endpoint: {e}"))
                    .context("creating gRPC channel")
                    .to_adbc()
            })?
            .http2_keep_alive_interval(Duration::from_secs(30))
            .keep_alive_while_idle(true)
            .keep_alive_timeout(Duration::from_secs(10))
            .tcp_nodelay(true);

        Ok(endpoint.connect_lazy())
    }

    fn create_tls_channel(
        host: &str,
        port: u16,
        tls_opts: &TlsOptions,
    ) -> Result<tonic::transport::Channel> {
        use std::fs;

        let effective_host = tls_opts.authority.as_deref().unwrap_or(host);
        let addr = format!("https://{effective_host}:{port}");
        let mut tls_config = ClientTlsConfig::new();

        // Root CA certificates
        if let Some(ref ca_path) = tls_opts.ca_cert_path {
            let ca_pem = fs::read_to_string(ca_path).map_err(|e| {
                FlightSqlErrorHelper::invalid_argument()
                    .message(format!("failed to read CA cert: {e}"))
                    .context("creating TLS channel")
                    .to_adbc()
            })?;
            tls_config = tls_config.ca_certificate(tonic::transport::Certificate::from_pem(ca_pem));
        } else if !tls_opts.skip_verify {
            tls_config = tls_config.with_native_roots();
        }

        // Hostname verification
        match tls_opts.skip_verify {
            true => {
                tls_config = tls_config
                    .domain_name(host)
                    .with_native_roots();
            }
            false => {
                let hostname = tls_opts.override_hostname.as_deref().unwrap_or(host);
                tls_config = tls_config.domain_name(hostname);
            }
        }

        // mTLS client certificate
        if let (Some(ref cert_chain), Some(ref private_key)) =
            (&tls_opts.mtls_cert_chain, &tls_opts.mtls_private_key)
        {
            let cert_pem = fs::read_to_string(cert_chain).map_err(|e| {
                FlightSqlErrorHelper::invalid_argument()
                    .message(format!("failed to read mTLS cert chain: {e}"))
                    .context("creating TLS channel")
                    .to_adbc()
            })?;
            let key_pem = fs::read_to_string(private_key).map_err(|e| {
                FlightSqlErrorHelper::invalid_argument()
                    .message(format!("failed to read mTLS private key: {e}"))
                    .context("creating TLS channel")
                    .to_adbc()
            })?;
            let identity = tonic::transport::Identity::from_pem(cert_pem, key_pem);
            tls_config = tls_config.identity(identity);
        }

        let endpoint = Endpoint::from_shared(addr)
            .map_err(|e| {
                FlightSqlErrorHelper::invalid_argument()
                    .message(format!("invalid endpoint: {e}"))
                    .context("creating TLS channel")
                    .to_adbc()
            })?
            .tls_config(tls_config)
            .map_err(|e| {
                FlightSqlErrorHelper::internal_no_location()
                    .message(format!("TLS configuration error: {e}"))
                    .context("creating TLS channel")
                    .to_adbc()
            })?
            .http2_keep_alive_interval(Duration::from_secs(30))
            .keep_alive_while_idle(true)
            .keep_alive_timeout(Duration::from_secs(10))
            .tcp_nodelay(true);

        Ok(endpoint.connect_lazy())
    }

    fn create_unix_channel(path: &str) -> Result<tonic::transport::Channel> {
        let endpoint = Endpoint::try_from(format!("unix://{path}"))
            .map_err(|e| {
                FlightSqlErrorHelper::invalid_argument()
                    .message(format!("failed to create Unix socket endpoint: {e}"))
                    .context("creating Unix socket channel")
                    .to_adbc()
            })?
            .http2_keep_alive_interval(Duration::from_secs(30))
            .keep_alive_while_idle(true)
            .keep_alive_timeout(Duration::from_secs(10));

        Ok(endpoint.connect_lazy())
    }

    fn build_cookie_header(
        jar: &Arc<std::sync::Mutex<Vec<(String, String)>>>,
    ) -> String {
        let cookies = jar.lock().unwrap();
        cookies
            .iter()
            .map(|(k, v)| format!("{k}={v}"))
            .collect::<Vec<_>>()
            .join("; ")
    }

    fn set_session_option(&mut self, _name: &str, _value: &str) -> Result<()> {
        // Session options are stored locally and applied via SetSessionOptions RPC
        // when available. For now, store-only.
        Ok(())
    }
}

impl Optionable for FlightSqlConnection {
    type Option = OptionConnection;

    fn set_option(&mut self, key: Self::Option, value: OptionValue) -> Result<()> {
        match key {
            OptionConnection::AutoCommit => {
                let v = FlightSqlErrorHelper::option_as_bool(&key, &value)?;
                self.autocommit = Some(v);
                Ok(())
            }
            OptionConnection::ReadOnly => {
                let v = FlightSqlErrorHelper::option_as_bool(&key, &value)?;
                self.read_only = Some(v);
                Ok(())
            }
            OptionConnection::CurrentCatalog => {
                let v = FlightSqlErrorHelper::option_as_string(&key, &value)?.to_string();
                self.current_catalog = Some(v);
                Ok(())
            }
            OptionConnection::CurrentSchema => {
                let v = FlightSqlErrorHelper::option_as_string(&key, &value)?.to_string();
                self.current_schema = Some(v);
                Ok(())
            }
            OptionConnection::IsolationLevel => {
                let v = FlightSqlErrorHelper::option_as_string(&key, &value)?.to_string();
                self.isolation_level = Some(v);
                Ok(())
            }
            OptionConnection::Other(key) if key == "adbc.flightsql.token" => {
                Err(FlightSqlErrorHelper::invalid_state()
                    .message("Set token via database option before creating connection")
                    .to_adbc())
            }
            OptionConnection::Other(ref key) if key.starts_with("adbc.flight.sql.rpc.timeout_seconds.") => {
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
                self.timeouts.set_timeout_seconds(key, seconds)
            }
            OptionConnection::Other(ref key) if key.starts_with("adbc.flight.sql.session.option.") => {
                let name = key.strip_prefix("adbc.flight.sql.session.option.").unwrap_or(key);
                let val = FlightSqlErrorHelper::option_as_string(&key, &value)?.to_string();
                // Apply via SetSessionOptions RPC
                self.set_session_option(name, &val)?;
                self.session_options.insert(name.to_string(), val);
                Ok(())
            }
            OptionConnection::Other(key) => {
                Err(FlightSqlErrorHelper::set_unknown_option(&key).to_adbc())
            }
            _ => Err(FlightSqlErrorHelper::not_implemented()
                .message("unknown connection option").to_adbc()),
        }
    }

    fn get_option_string(&self, key: Self::Option) -> Result<String> {
        match key {
            OptionConnection::CurrentCatalog => {
                self.current_catalog.clone().ok_or_else(|| {
                    FlightSqlErrorHelper::not_found()
                        .message("CurrentCatalog not set").to_adbc()
                })
            }
            OptionConnection::CurrentSchema => {
                self.current_schema.clone().ok_or_else(|| {
                    FlightSqlErrorHelper::not_found()
                        .message("CurrentSchema not set").to_adbc()
                })
            }
            OptionConnection::IsolationLevel => {
                self.isolation_level.clone().ok_or_else(|| {
                    FlightSqlErrorHelper::not_found()
                        .message("IsolationLevel not set").to_adbc()
                })
            }
            OptionConnection::Other(ref key) if key.starts_with("adbc.flight.sql.session.option.") => {
                let name = key.strip_prefix("adbc.flight.sql.session.option.").unwrap_or(key);
                self.session_options.get(name).cloned().ok_or_else(|| {
                    FlightSqlErrorHelper::not_found()
                        .message(format!("Session option {name} not set"))
                        .to_adbc()
                })
            }
            _ => Err(FlightSqlErrorHelper::not_found()
                .message(format!("Option {key:?} not retrievable as string")).to_adbc()),
        }
    }

    fn get_option_int(&self, key: Self::Option) -> Result<i64> {
        match key {
            OptionConnection::AutoCommit => {
                match self.autocommit {
                    Some(true) => Ok(1),
                    Some(false) => Ok(0),
                    None => Err(FlightSqlErrorHelper::not_found()
                        .message("AutoCommit not set").to_adbc()),
                }
            }
            OptionConnection::ReadOnly => {
                match self.read_only {
                    Some(true) => Ok(1),
                    Some(false) => Ok(0),
                    None => Err(FlightSqlErrorHelper::not_found()
                        .message("ReadOnly not set").to_adbc()),
                }
            }
            _ => Err(FlightSqlErrorHelper::not_found()
                .message(format!("Option {key:?} not retrievable as int")).to_adbc()),
        }
    }

    fn get_option_bytes(&self, key: Self::Option) -> Result<Vec<u8>> {
        Err(FlightSqlErrorHelper::not_found()
            .message(format!("Option {key:?} not available")).to_adbc())
    }

    fn get_option_double(&self, key: Self::Option) -> Result<f64> {
        match key {
            OptionConnection::Other(ref k) if k.starts_with("adbc.flight.sql.rpc.timeout_seconds.") => {
                self.timeouts.get_timeout_seconds(k)
            }
            _ => Err(FlightSqlErrorHelper::not_found()
                .message(format!("Option {key:?} not available")).to_adbc()),
        }
    }
}

impl Connection for FlightSqlConnection {
    type StatementType = FlightSqlStatement;

    fn new_statement(&mut self) -> Result<Self::StatementType> {
        let client = self.client.clone();
        Ok(FlightSqlStatement::new(Arc::clone(&self.runtime), client))
    }

    fn cancel(&mut self) -> Result<()> {
        Err(FlightSqlErrorHelper::not_implemented()
            .message("cancel not implemented")
            .to_adbc())
    }

    fn get_info(
        &self,
        codes: Option<HashSet<InfoCode>>,
    ) -> Result<Box<dyn RecordBatchReader + Send + 'static>> {
        let mut client = self.client.clone();
        metadata::get_info(&self.runtime, &mut client, codes)
    }

    fn get_objects(
        &self,
        depth: ObjectDepth,
        catalog: Option<&str>,
        db_schema: Option<&str>,
        table_name: Option<&str>,
        table_type: Option<Vec<&str>>,
        column_name: Option<&str>,
    ) -> Result<Box<dyn RecordBatchReader + Send + 'static>> {
        Ok(metadata::get_objects(
            Arc::clone(&self.runtime),
            self.client.clone(),
            depth,
            catalog,
            db_schema,
            table_name,
            table_type,
            column_name,
        ))
    }

    fn get_table_schema(
        &self,
        catalog: Option<&str>,
        db_schema: Option<&str>,
        table_name: &str,
    ) -> Result<arrow_schema::Schema> {
        let mut client = self.client.clone();
        metadata::get_table_schema(
            &self.runtime,
            &mut client,
            catalog,
            db_schema,
            table_name,
        )
    }

    fn get_table_types(&self) -> Result<Box<dyn RecordBatchReader + Send + 'static>> {
        let mut client = self.client.clone();
        metadata::get_table_types(&self.runtime, &mut client)
    }

    fn get_statistic_names(&self) -> Result<Box<dyn RecordBatchReader + Send + 'static>> {
        Err(FlightSqlErrorHelper::not_implemented()
            .message("get_statistic_names not implemented")
            .to_adbc())
    }

    fn get_statistics(
        &self,
        _catalog: Option<&str>,
        _db_schema: Option<&str>,
        _table_name: Option<&str>,
        _approximate: bool,
    ) -> Result<Box<dyn RecordBatchReader + Send + 'static>> {
        Err(FlightSqlErrorHelper::not_implemented()
            .message("get_statistics not implemented")
            .to_adbc())
    }

    fn commit(&mut self) -> Result<()> {
        Err(FlightSqlErrorHelper::not_implemented()
            .message("commit not implemented")
            .to_adbc())
    }

    fn rollback(&mut self) -> Result<()> {
        Err(FlightSqlErrorHelper::not_implemented()
            .message("rollback not implemented")
            .to_adbc())
    }

    fn read_partition(
        &self,
        partition: impl AsRef<[u8]>,
    ) -> Result<Box<dyn RecordBatchReader + Send + 'static>> {
        use arrow_flight::{FlightInfo, IpcMessage};

        let info = FlightInfo::decode(partition.as_ref())
            .map_err(|e| {
                FlightSqlErrorHelper::invalid_argument()
                    .message(format!("failed to decode partition: {e}"))
                    .context("reading partition")
                    .to_adbc()
            })?;

        if info.endpoint.len() != 1 {
            return Err(FlightSqlErrorHelper::invalid_argument()
                .message(format!(
                    "invalid partition: expected 1 endpoint, got {}",
                    info.endpoint.len()
                ))
                .context("reading partition")
                .to_adbc());
        }

        let endpoint = &info.endpoint[0];
        let ticket = endpoint.ticket.clone().ok_or_else(|| {
            FlightSqlErrorHelper::internal_no_location()
                .message("partition endpoint has no ticket")
                .context("reading partition")
                .to_adbc()
        })?;

        let schema = Schema::try_from(IpcMessage(info.schema.clone()))
            .map_err(|e| {
                FlightSqlErrorHelper::internal_no_location()
                    .message(format!("failed to decode partition schema: {e}"))
                    .to_adbc()
            })?;

        let mut client = self.client.clone();
        let stream = self.runtime.block_on(async {
            client.do_get(ticket).await
        }).map_err(|e: FlightError| map_flight_error(e, "ReadPartition(DoGet)"))?;

        let cancelled = Arc::new(std::sync::atomic::AtomicBool::new(false));

        Ok(Box::new(FlightSqlRecordBatchReader::new(
            Arc::clone(&self.runtime),
            stream,
            schema,
            cancelled,
        )))
    }
}

#[cfg(test)]
mod option_tests {
    use super::*;
    use adbc_core::options::OptionValue;

    fn make_connection() -> FlightSqlConnection {
        let runtime = Arc::new(crate::runtime::Runtime::new(None).unwrap());
        let rt = Arc::clone(&runtime);
        let channel = rt.block_on(async {
            tonic::transport::Endpoint::from_static("http://localhost:50051")
                .connect_lazy()
        });
        let client = FlightSqlServiceClient::new(channel);
        FlightSqlConnection {
            runtime,
            client,
            read_only: None,
            autocommit: Some(true),
            current_catalog: None,
            current_schema: None,
            isolation_level: None,
            timeouts: TimeoutOption::new(),
            cookies: None,
            session_options: HashMap::new(),
        }
    }

    #[test]
    fn test_set_readonly_true_and_get_back() {
        let mut conn = make_connection();
        conn.set_option(OptionConnection::ReadOnly, OptionValue::String("true".into())).unwrap();
        let v = conn.get_option_int(OptionConnection::ReadOnly).unwrap();
        assert_eq!(v, 1);
    }

    #[test]
    fn test_set_readonly_false_and_get_back() {
        let mut conn = make_connection();
        conn.set_option(OptionConnection::ReadOnly, OptionValue::String("false".into())).unwrap();
        let v = conn.get_option_int(OptionConnection::ReadOnly).unwrap();
        assert_eq!(v, 0);
    }

    #[test]
    fn test_set_autocommit_false_and_get_back() {
        let mut conn = make_connection();
        conn.set_option(OptionConnection::AutoCommit, OptionValue::String("false".into())).unwrap();
        let v = conn.get_option_int(OptionConnection::AutoCommit).unwrap();
        assert_eq!(v, 0);
    }

    #[test]
    fn test_set_current_catalog_and_get_back() {
        let mut conn = make_connection();
        conn.set_option(OptionConnection::CurrentCatalog, OptionValue::String("my_catalog".into())).unwrap();
        let v = conn.get_option_string(OptionConnection::CurrentCatalog).unwrap();
        assert_eq!(v, "my_catalog");
    }

    #[test]
    fn test_set_current_schema_and_get_back() {
        let mut conn = make_connection();
        conn.set_option(OptionConnection::CurrentSchema, OptionValue::String("my_schema".into())).unwrap();
        let v = conn.get_option_string(OptionConnection::CurrentSchema).unwrap();
        assert_eq!(v, "my_schema");
    }

    #[test]
    fn test_set_isolation_level_and_get_back() {
        let mut conn = make_connection();
        conn.set_option(OptionConnection::IsolationLevel, OptionValue::String("adbc.connection.transaction.isolation.serializable".into())).unwrap();
        let v = conn.get_option_string(OptionConnection::IsolationLevel).unwrap();
        assert_eq!(v, "adbc.connection.transaction.isolation.serializable");
    }

    #[test]
    fn test_unknown_connection_option_returns_not_implemented() {
        let mut conn = make_connection();
        let result = conn.set_option(
            OptionConnection::Other("adbc.flightsql.nonexistent".into()),
            OptionValue::String("value".into()),
        );
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().status, adbc_core::error::Status::NotImplemented);
    }

    #[test]
    fn test_autocommit_defaults_true() {
        let conn = make_connection();
        assert_eq!(conn.autocommit, Some(true));
    }
}
