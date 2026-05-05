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

use std::sync::Arc;

use adbc_core::{
    error::{Error, Result},
    options::{OptionConnection, OptionDatabase, OptionValue},
    Database, Optionable,
};
use url::Url;

use crate::connection::FlightSqlConnection;
use crate::error::{ErrorHelper, FlightSqlErrorHelper};
use crate::runtime::Runtime;

/// Connection transport configuration.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum Transport {
    /// Plaintext gRPC (grpc://)
    Plaintext { host: String, port: u16 },
    /// TLS-secured gRPC (grpc+tls://)
    Tls {
        host: String,
        port: u16,
    },
    /// Unix domain socket (grpc+unix://)
    Unix { path: String },
}

/// TLS configuration for mTLS and custom certificate chains.
#[derive(Debug, Clone, Default)]
pub(crate) struct TlsOptions {
    pub ca_cert_path: Option<String>,
    pub ca_cert_pem: Option<String>,
    pub skip_verify: bool,
    pub override_hostname: Option<String>,
    pub mtls_cert_chain: Option<String>,
    pub mtls_private_key: Option<String>,
    pub authority: Option<String>,
}

/// OAuth2 authentication configuration.
#[derive(Debug, Clone, Default)]
pub(crate) struct OAuthOptions {
    pub flow: Option<String>,
    pub client_id: Option<String>,
    pub client_secret: Option<String>,
    pub token_url: Option<String>,
    pub scope: Option<String>,
    pub subject_token: Option<String>,
    pub actor_token: Option<String>,
}

/// A handle to a FlightSQL database.
///
/// Holds connection URI, authentication credentials, and the shared
/// async runtime. Database must be kept alive as long as any connections exist.
pub struct FlightSqlDatabase {
    pub(crate) uri: Option<String>,
    pub(crate) username: Option<String>,
    pub(crate) password: Option<String>,
    pub(crate) token: Option<String>,
    pub(crate) handle: Option<tokio::runtime::Handle>,
    // TLS/mTLS options (FSQL-23):
    pub(crate) tls_opts: TlsOptions,
    // OAuth options (FSQL-24):
    pub(crate) oauth: OAuthOptions,
    // Cookie middleware (FSQL-33):
    pub(crate) cookie_middleware: bool,
    // Parsed transport:
    pub(crate) transport: Transport,
    // Legacy fields (kept for backward compat, populated from Transport):
    pub(crate) tls: bool,
    pub(crate) host: String,
    pub(crate) port: u16,
    pub(crate) unix_socket_path: Option<String>,
}

impl FlightSqlDatabase {
    fn parse_uri(&mut self, uri_str: &str) -> Result<()> {
        let url = Url::parse(uri_str).map_err(|e| {
            FlightSqlErrorHelper::invalid_argument()
                .message(format!("invalid URI: {e}"))
                .context("parsing connection URI")
        })?;

        let scheme = url.scheme();
        match scheme {
            "grpc" => {
                self.transport = Transport::Plaintext {
                    host: url.host_str().unwrap_or("localhost").to_string(),
                    port: url.port().unwrap_or(50051),
                };
                self.tls = false;
            }
            "grpc+tls" => {
                self.transport = Transport::Tls {
                    host: url.host_str().unwrap_or("localhost").to_string(),
                    port: url.port().unwrap_or(443),
                };
                self.tls = true;
            }
            "grpc+unix" => {
                self.transport = Transport::Unix {
                    path: url.path().to_string(),
                };
                self.tls = false;
            }
            other => {
                return Err(FlightSqlErrorHelper::invalid_argument()
                    .message(format!("unsupported URI scheme: {other}"))
                    .context("parsing connection URI")
                    .to_adbc());
            }
        };

        let (ref host, port) = match &self.transport {
            Transport::Plaintext { host, port } | Transport::Tls { host, port } => {
                (host.clone(), *port)
            }
            Transport::Unix { .. } => ("localhost".to_string(), 0),
        };
        self.host = host.to_string();
        self.port = port;

        Ok(())
    }

    /// Perform OAuth token exchange and return the access token.
    fn exchange_oauth_token(&self) -> Result<String> {
        let flow = self.oauth.flow.as_deref().unwrap_or("");
        let client_id = self.oauth.client_id.as_deref().ok_or_else(|| {
            FlightSqlErrorHelper::invalid_argument()
                .message("OAuth client_id required")
                .to_adbc()
        })?;
        let client_secret = self.oauth.client_secret.as_deref().ok_or_else(|| {
            FlightSqlErrorHelper::invalid_argument()
                .message("OAuth client_secret required")
                .to_adbc()
        })?;
        let token_url = self.oauth.token_url.as_deref().ok_or_else(|| {
            FlightSqlErrorHelper::invalid_argument()
                .message("OAuth token_url required")
                .to_adbc()
        })?;

        match flow {
            "client_credentials" => {
                self.oauth_token_request(
                    token_url,
                    client_id,
                    client_secret,
                    &[("grant_type", "client_credentials")],
                )
            }
            "token_exchange" => {
                let subject_token = self.oauth.subject_token.as_deref().ok_or_else(|| {
                    FlightSqlErrorHelper::invalid_argument()
                        .message("OAuth subject_token required for token_exchange flow")
                        .to_adbc()
                })?;
                let mut params = vec![
                    ("grant_type", "urn:ietf:params:oauth:grant-type:token-exchange"),
                    ("subject_token", subject_token),
                    ("subject_token_type", "urn:ietf:params:oauth:token-type:access_token"),
                ];
                if let Some(ref actor) = self.oauth.actor_token {
                    params.push(("actor_token", actor.as_str()));
                    params.push(("actor_token_type", "urn:ietf:params:oauth:token-type:access_token"));
                }
                self.oauth_token_request(token_url, client_id, client_secret, &params)
            }
            _ => Err(FlightSqlErrorHelper::invalid_argument()
                .message(format!("unsupported OAuth flow: {flow}"))
                .context("specify 'client_credentials' or 'token_exchange'")
                .to_adbc()),
        }
    }

    fn oauth_token_request(
        &self,
        token_url: &str,
        client_id: &str,
        client_secret: &str,
        extra_params: &[(&str, &str)],
    ) -> Result<String> {

        let mut params: Vec<(&str, &str)> = extra_params.to_vec();
        if let Some(ref scope) = self.oauth.scope {
            params.push(("scope", scope.as_str()));
        }

        let client = reqwest::blocking::Client::new();
        let response = client
            .post(token_url)
            .basic_auth(client_id, Some(client_secret))
            .form(&params)
            .send()
            .map_err(|e| {
                FlightSqlErrorHelper::io()
                    .message(format!("OAuth token request failed: {e}"))
                    .context("exchanging OAuth token")
                    .to_adbc()
            })?;

        if !response.status().is_success() {
            return Err(FlightSqlErrorHelper::unauthenticated()
                .message(format!(
                    "OAuth token request returned {}: {}",
                    response.status(),
                    response.text().unwrap_or_default()
                ))
                .context("exchanging OAuth token")
                .to_adbc());
        }

        #[derive(serde::Deserialize)]
        struct TokenResponse {
            access_token: String,
        }

        let token_resp: TokenResponse = response.json().map_err(|e| {
            FlightSqlErrorHelper::io()
                .message(format!("failed to parse OAuth token response: {e}"))
                .context("exchanging OAuth token")
                .to_adbc()
        })?;

        Ok(token_resp.access_token)
    }
}

impl Optionable for FlightSqlDatabase {
    type Option = OptionDatabase;

    fn set_option(&mut self, key: Self::Option, value: OptionValue) -> Result<()> {
        match key {
            OptionDatabase::Uri => {
                let uri_str = match &value {
                    OptionValue::String(s) => s.clone(),
                    _ => {
                        return Err(FlightSqlErrorHelper::invalid_argument()
                            .message("URI option must be a string")
                            .to_adbc());
                    }
                };
                self.parse_uri(&uri_str)?;
                self.uri = Some(uri_str);
                Ok(())
            }
            OptionDatabase::Username => {
                let username = match &value {
                    OptionValue::String(s) => s.clone(),
                    _ => {
                        return Err(FlightSqlErrorHelper::invalid_argument()
                            .message("Username option must be a string")
                            .to_adbc());
                    }
                };
                self.username = Some(username);
                Ok(())
            }
            OptionDatabase::Password => {
                let password = match &value {
                    OptionValue::String(s) => s.clone(),
                    _ => {
                        return Err(FlightSqlErrorHelper::invalid_argument()
                            .message("Password option must be a string")
                            .to_adbc());
                    }
                };
                self.password = Some(password);
                Ok(())
            }
            OptionDatabase::Other(key) if key == "adbc.flightsql.token" => {
                let token = FlightSqlErrorHelper::option_as_string(&key, &value)?.to_string();
                self.token = Some(token);
                Ok(())
            }
            OptionDatabase::Other(key) if key == "adbc.flightsql.tls.ca_cert_path" => {
                let path = FlightSqlErrorHelper::option_as_string(&key, &value)?.to_string();
                self.tls_opts.ca_cert_path = Some(path);
                Ok(())
            }
            OptionDatabase::Other(key) if key == "adbc.flightsql.tls.skip_verify" => {
                let v = FlightSqlErrorHelper::option_as_string(&key, &value)?;
                self.tls_opts.skip_verify = v == "true";
                Ok(())
            }
            OptionDatabase::Other(key) if key == "adbc.flightsql.tls.override_hostname" => {
                let v = FlightSqlErrorHelper::option_as_string(&key, &value)?.to_string();
                self.tls_opts.override_hostname = Some(v);
                Ok(())
            }
            OptionDatabase::Other(key) if key == "adbc.flightsql.tls.mtls_cert_chain" => {
                let v = FlightSqlErrorHelper::option_as_string(&key, &value)?.to_string();
                self.tls_opts.mtls_cert_chain = Some(v);
                Ok(())
            }
            OptionDatabase::Other(key) if key == "adbc.flightsql.tls.mtls_private_key" => {
                let v = FlightSqlErrorHelper::option_as_string(&key, &value)?.to_string();
                self.tls_opts.mtls_private_key = Some(v);
                Ok(())
            }
            OptionDatabase::Other(key) if key == "adbc.connection.authority" => {
                let v = FlightSqlErrorHelper::option_as_string(&key, &value)?.to_string();
                self.tls_opts.authority = Some(v);
                Ok(())
            }
            OptionDatabase::Other(key) if key == "adbc.flightsql.oauth.flow" => {
                let flow = FlightSqlErrorHelper::option_as_string(&key, &value)?.to_string();
                match flow.as_str() {
                    "client_credentials" | "token_exchange" => {
                        self.oauth.flow = Some(flow);
                        Ok(())
                    }
                    _ => Err(FlightSqlErrorHelper::invalid_argument()
                        .message(format!("unsupported OAuth flow: {flow}"))
                        .to_adbc()),
                }
            }
            OptionDatabase::Other(key) if key == "adbc.flightsql.oauth.client_id" => {
                self.oauth.client_id = Some(FlightSqlErrorHelper::option_as_string(&key, &value)?.to_string());
                Ok(())
            }
            OptionDatabase::Other(key) if key == "adbc.flightsql.oauth.client_secret" => {
                self.oauth.client_secret = Some(FlightSqlErrorHelper::option_as_string(&key, &value)?.to_string());
                Ok(())
            }
            OptionDatabase::Other(key) if key == "adbc.flightsql.oauth.token_url" => {
                self.oauth.token_url = Some(FlightSqlErrorHelper::option_as_string(&key, &value)?.to_string());
                Ok(())
            }
            OptionDatabase::Other(key) if key == "adbc.flightsql.oauth.scope" => {
                self.oauth.scope = Some(FlightSqlErrorHelper::option_as_string(&key, &value)?.to_string());
                Ok(())
            }
            OptionDatabase::Other(key) if key == "adbc.flightsql.oauth.subject_token" => {
                self.oauth.subject_token = Some(FlightSqlErrorHelper::option_as_string(&key, &value)?.to_string());
                Ok(())
            }
            OptionDatabase::Other(key) if key == "adbc.flightsql.oauth.actor_token" => {
                self.oauth.actor_token = Some(FlightSqlErrorHelper::option_as_string(&key, &value)?.to_string());
                Ok(())
            }
            OptionDatabase::Other(key) if key == "adbc.flightsql.cookie.middleware" => {
                let v = FlightSqlErrorHelper::option_as_string(&key, &value)?;
                self.cookie_middleware = v == "true";
                Ok(())
            }
            OptionDatabase::Other(key) => {
                Err(FlightSqlErrorHelper::set_unknown_option(&key).to_adbc())
            }
            _ => Err(FlightSqlErrorHelper::not_implemented()
                .message("unknown database option")
                .to_adbc()),
        }
    }

    fn get_option_string(&self, key: Self::Option) -> Result<String> {
        match key {
            OptionDatabase::Uri => self.uri.clone().ok_or_else(|| {
                FlightSqlErrorHelper::not_found().message("URI not set").to_adbc()
            }),
            OptionDatabase::Username => self.username.clone().ok_or_else(|| {
                FlightSqlErrorHelper::not_found().message("Username not set").to_adbc()
            }),
            OptionDatabase::Password => Err(FlightSqlErrorHelper::not_found()
                .message("Password is write-only").to_adbc()),
            OptionDatabase::Other(ref key) if key == "adbc.flightsql.token" => {
                self.token.clone().ok_or_else(|| {
                    FlightSqlErrorHelper::not_found().message("Token not set").to_adbc()
                })
            }
            OptionDatabase::Other(ref key) if key == "adbc.flightsql.tls.ca_cert_path" => {
                self.tls_opts.ca_cert_path.clone().ok_or_else(|| {
                    FlightSqlErrorHelper::not_found().message("CA cert path not set").to_adbc()
                })
            }
            OptionDatabase::Other(ref key) if key == "adbc.flightsql.tls.override_hostname" => {
                self.tls_opts.override_hostname.clone().ok_or_else(|| {
                    FlightSqlErrorHelper::not_found().message("TLS hostname override not set").to_adbc()
                })
            }
            OptionDatabase::Other(ref key) if key == "adbc.flightsql.oauth.client_id" => {
                self.oauth.client_id.clone().ok_or_else(|| {
                    FlightSqlErrorHelper::not_found().message("OAuth client_id not set").to_adbc()
                })
            }
            OptionDatabase::Other(ref key) if key == "adbc.flightsql.oauth.flow" => {
                self.oauth.flow.clone().ok_or_else(|| {
                    FlightSqlErrorHelper::not_found().message("OAuth flow not set").to_adbc()
                })
            }
            _ => Err(FlightSqlErrorHelper::not_found()
                .message(format!("Unknown or unsupported option: {key:?}")).to_adbc()),
        }
    }

    fn get_option_bytes(&self, key: Self::Option) -> Result<Vec<u8>> {
        Err(Error::with_message_and_status(
            format!("Unrecognized option: {key:?}"),
            adbc_core::error::Status::NotFound,
        ))
    }

    fn get_option_int(&self, key: Self::Option) -> Result<i64> {
        Err(Error::with_message_and_status(
            format!("Unrecognized option: {key:?}"),
            adbc_core::error::Status::NotFound,
        ))
    }

    fn get_option_double(&self, key: Self::Option) -> Result<f64> {
        Err(Error::with_message_and_status(
            format!("Unrecognized option: {key:?}"),
            adbc_core::error::Status::NotFound,
        ))
    }
}

impl Database for FlightSqlDatabase {
    type ConnectionType = FlightSqlConnection;

    fn new_connection(&self) -> Result<Self::ConnectionType> {
        let runtime = Arc::new(Runtime::new(self.handle.clone()).map_err(|e| {
            FlightSqlErrorHelper::internal_no_location()
                .message(format!("failed to create tokio runtime: {e}"))
                .context("creating database connection")
                .to_adbc()
        })?);

        // Perform OAuth token exchange if configured
        let mut token = self.token.clone();
        if self.oauth.flow.is_some() {
            if self.username.is_some() || token.is_some() {
                return Err(FlightSqlErrorHelper::invalid_argument()
                    .message("cannot use both OAuth and username/password or token authentication")
                    .context("creating database connection")
                    .to_adbc());
            }
            token = Some(self.exchange_oauth_token()?);
        }

        // Validate mTLS options
        let has_cert_chain = self.tls_opts.mtls_cert_chain.is_some();
        let has_private_key = self.tls_opts.mtls_private_key.is_some();
        if has_cert_chain != has_private_key {
            return Err(FlightSqlErrorHelper::invalid_argument()
                .message("mTLS requires both cert_chain and private_key together")
                .context("creating database connection")
                .to_adbc());
        }

        FlightSqlConnection::new(
            runtime,
            &self.transport,
            self.tls_opts.clone(),
            self.username.clone(),
            self.password.clone(),
            token,
            self.cookie_middleware,
        )
    }

    fn new_connection_with_opts(
        &self,
        opts: impl IntoIterator<Item = (OptionConnection, OptionValue)>,
    ) -> Result<Self::ConnectionType> {
        let mut connection = self.new_connection()?;
        for (key, value) in opts {
            connection.set_option(key, value)?;
        }
        Ok(connection)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use adbc_core::options::OptionValue;

    fn make_db() -> FlightSqlDatabase {
        FlightSqlDatabase {
            uri: None,
            username: None,
            password: None,
            token: None,
            handle: None,
            tls_opts: TlsOptions::default(),
            oauth: OAuthOptions::default(),
            cookie_middleware: false,
            transport: Transport::Plaintext {
                host: "localhost".to_string(),
                port: 50051,
            },
            tls: false,
            host: "localhost".to_string(),
            port: 50051,
            unix_socket_path: None,
        }
    }

    #[test]
    fn test_set_and_get_uri_option() {
        let mut db = make_db();
        db.set_option(OptionDatabase::Uri, OptionValue::String("grpc://host:31337".into())).unwrap();
        let uri = db.get_option_string(OptionDatabase::Uri).unwrap();
        assert_eq!(uri, "grpc://host:31337");
    }

    #[test]
    fn test_set_and_get_username_option() {
        let mut db = make_db();
        db.set_option(OptionDatabase::Username, OptionValue::String("alice".into())).unwrap();
        let user = db.get_option_string(OptionDatabase::Username).unwrap();
        assert_eq!(user, "alice");
    }

    #[test]
    fn test_password_is_write_only() {
        let mut db = make_db();
        db.set_option(OptionDatabase::Password, OptionValue::String("secret".into())).unwrap();
        let result = db.get_option_string(OptionDatabase::Password);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().status, adbc_core::error::Status::NotFound);
    }

    #[test]
    fn test_set_and_get_token_option() {
        let mut db = make_db();
        db.set_option(
            OptionDatabase::Other("adbc.flightsql.token".into()),
            OptionValue::String("bearer-token-123".into()),
        ).unwrap();
        let token = db.get_option_string(OptionDatabase::Other("adbc.flightsql.token".into())).unwrap();
        assert_eq!(token, "bearer-token-123");
    }

    #[test]
    fn test_unknown_option_returns_not_implemented() {
        let mut db = make_db();
        let result = db.set_option(
            OptionDatabase::Other("adbc.flightsql.nonexistent".into()),
            OptionValue::String("value".into()),
        );
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().status, adbc_core::error::Status::NotImplemented);
    }

    #[test]
    fn test_set_ca_cert_path_option() {
        let mut db = make_db();
        db.set_option(
            OptionDatabase::Other("adbc.flightsql.tls.ca_cert_path".into()),
            OptionValue::String("/path/to/ca.pem".into()),
        ).unwrap();
        let path = db.get_option_string(OptionDatabase::Other("adbc.flightsql.tls.ca_cert_path".into())).unwrap();
        assert_eq!(path, "/path/to/ca.pem");
    }

    #[test]
    fn test_set_uri_parses_host_port_tls() {
        let mut db = make_db();
        db.set_option(OptionDatabase::Uri, OptionValue::String("grpc+tls://myserver:9999".into())).unwrap();
        assert!(matches!(db.transport, Transport::Tls { .. }));
        assert!(db.tls);
        assert_eq!(db.host, "myserver");
        assert_eq!(db.port, 9999);
    }

    #[test]
    fn test_uri_defaults_plaintext_port() {
        let mut db = make_db();
        db.set_option(OptionDatabase::Uri, OptionValue::String("grpc://server".into())).unwrap();
        assert!(!db.tls);
        assert_eq!(db.port, 50051);
    }

    #[test]
    fn test_unix_socket_uri() {
        let mut db = make_db();
        db.set_option(OptionDatabase::Uri, OptionValue::String("grpc+unix:///var/run/flightsql.sock".into())).unwrap();
        match db.transport {
            Transport::Unix { ref path } => assert_eq!(path, "/var/run/flightsql.sock"),
            _ => panic!("expected Unix transport"),
        }
    }

    #[test]
    fn test_oauth_flow_options() {
        let mut db = make_db();
        db.set_option(
            OptionDatabase::Other("adbc.flightsql.oauth.flow".into()),
            OptionValue::String("client_credentials".into()),
        ).unwrap();
        db.set_option(
            OptionDatabase::Other("adbc.flightsql.oauth.client_id".into()),
            OptionValue::String("my-client".into()),
        ).unwrap();
        assert_eq!(db.oauth.flow.as_deref(), Some("client_credentials"));
        assert_eq!(db.oauth.client_id.as_deref(), Some("my-client"));
    }

    #[test]
    fn test_oauth_invalid_flow() {
        let mut db = make_db();
        let result = db.set_option(
            OptionDatabase::Other("adbc.flightsql.oauth.flow".into()),
            OptionValue::String("invalid_flow".into()),
        );
        assert!(result.is_err());
    }

    #[test]
    fn test_mtls_options() {
        let mut db = make_db();
        db.set_option(
            OptionDatabase::Other("adbc.flightsql.tls.mtls_cert_chain".into()),
            OptionValue::String("CERT PEM...".into()),
        ).unwrap();
        assert_eq!(db.tls_opts.mtls_cert_chain.as_deref(), Some("CERT PEM..."));
    }

    #[test]
    fn test_tls_skip_verify() {
        let mut db = make_db();
        db.set_option(
            OptionDatabase::Other("adbc.flightsql.tls.skip_verify".into()),
            OptionValue::String("true".into()),
        ).unwrap();
        assert!(db.tls_opts.skip_verify);
    }

    #[test]
    fn test_cookie_middleware() {
        let mut db = make_db();
        db.set_option(
            OptionDatabase::Other("adbc.flightsql.cookie.middleware".into()),
            OptionValue::String("true".into()),
        ).unwrap();
        assert!(db.cookie_middleware);
    }
}
