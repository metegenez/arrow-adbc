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

use std::time::Duration;

use adbc_core::error::Result;

use crate::error::{ErrorHelper, FlightSqlErrorHelper};

pub const OPTION_TIMEOUT_FETCH: &str = "adbc.flight.sql.rpc.timeout_seconds.fetch";
pub const OPTION_TIMEOUT_QUERY: &str = "adbc.flight.sql.rpc.timeout_seconds.query";
pub const OPTION_TIMEOUT_UPDATE: &str = "adbc.flight.sql.rpc.timeout_seconds.update";
pub const OPTION_TIMEOUT_CONNECT: &str = "adbc.flight.sql.rpc.timeout_seconds.connect";

#[derive(Debug, Clone, Default)]
pub struct TimeoutOption {
    pub fetch_timeout: Option<Duration>,
    pub query_timeout: Option<Duration>,
    pub update_timeout: Option<Duration>,
    pub connect_timeout: Option<Duration>,
}

impl TimeoutOption {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn set_timeout_seconds(&mut self, key: &str, seconds: f64) -> Result<()> {
        if seconds.is_nan() || seconds.is_infinite() || seconds < 0.0 {
            return Err(FlightSqlErrorHelper::invalid_argument()
                .message(format!("invalid timeout seconds: {seconds}"))
                .context("setting timeout")
                .to_adbc());
        }
        let dur = if seconds == 0.0 {
            None
        } else {
            Some(Duration::from_secs_f64(seconds))
        };
        match key {
            OPTION_TIMEOUT_FETCH => self.fetch_timeout = dur,
            OPTION_TIMEOUT_QUERY => self.query_timeout = dur,
            OPTION_TIMEOUT_UPDATE => self.update_timeout = dur,
            OPTION_TIMEOUT_CONNECT => self.connect_timeout = dur,
            _ => {
                return Err(FlightSqlErrorHelper::invalid_argument()
                    .message(format!("unknown timeout key: {key}"))
                    .to_adbc());
            }
        }
        Ok(())
    }

    pub fn get_timeout_seconds(&self, key: &str) -> Result<f64> {
        let dur = match key {
            OPTION_TIMEOUT_FETCH => self.fetch_timeout,
            OPTION_TIMEOUT_QUERY => self.query_timeout,
            OPTION_TIMEOUT_UPDATE => self.update_timeout,
            OPTION_TIMEOUT_CONNECT => self.connect_timeout,
            _ => {
                return Err(FlightSqlErrorHelper::not_found()
                    .message(format!("unknown timeout key: {key}"))
                    .to_adbc());
            }
        };
        Ok(dur.map(|d| d.as_secs_f64()).unwrap_or(0.0))
    }

    pub fn timeout_for_method(&self, method: &str) -> Option<Duration> {
        if method.ends_with("DoGet") {
            self.fetch_timeout
        } else if method.ends_with("GetFlightInfo") {
            self.query_timeout
        } else if method.ends_with("DoPut") || method.ends_with("DoAction") {
            self.update_timeout
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use adbc_core::error::Status;

    #[test]
    fn test_set_timeout_valid_values() {
        let mut t = TimeoutOption::new();
        t.set_timeout_seconds(OPTION_TIMEOUT_FETCH, 30.0).unwrap();
        assert_eq!(t.fetch_timeout, Some(Duration::from_secs(30)));

        t.set_timeout_seconds(OPTION_TIMEOUT_FETCH, 0.0).unwrap();
        assert_eq!(t.fetch_timeout, None);

        t.set_timeout_seconds(OPTION_TIMEOUT_QUERY, 0.001).unwrap();
        assert!(t.query_timeout.is_some());
    }

    #[test]
    fn test_set_timeout_nan_returns_error() {
        let mut t = TimeoutOption::new();
        let result = t.set_timeout_seconds(OPTION_TIMEOUT_FETCH, f64::NAN);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().status, Status::InvalidArguments);
    }

    #[test]
    fn test_set_timeout_inf_returns_error() {
        let mut t = TimeoutOption::new();
        let result = t.set_timeout_seconds(OPTION_TIMEOUT_FETCH, f64::INFINITY);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().status, Status::InvalidArguments);
    }

    #[test]
    fn test_set_timeout_negative_returns_error() {
        let mut t = TimeoutOption::new();
        let result = t.set_timeout_seconds(OPTION_TIMEOUT_FETCH, -1.0);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().status, Status::InvalidArguments);
    }

    #[test]
    fn test_zero_timeout_removes_timeout() {
        let mut t = TimeoutOption::new();
        t.set_timeout_seconds(OPTION_TIMEOUT_QUERY, 30.0).unwrap();
        assert!(t.query_timeout.is_some());
        t.set_timeout_seconds(OPTION_TIMEOUT_QUERY, 0.0).unwrap();
        assert!(t.query_timeout.is_none());
    }

    #[test]
    fn test_timeout_for_method_do_get() {
        let mut t = TimeoutOption::new();
        t.set_timeout_seconds(OPTION_TIMEOUT_FETCH, 10.0).unwrap();
        let d = t.timeout_for_method("/arrow.flight.protocol.FlightService/DoGet");
        assert_eq!(d, Some(Duration::from_secs(10)));
    }

    #[test]
    fn test_timeout_for_method_get_flight_info() {
        let mut t = TimeoutOption::new();
        t.set_timeout_seconds(OPTION_TIMEOUT_QUERY, 20.0).unwrap();
        let d = t.timeout_for_method("/arrow.flight.protocol.FlightService/GetFlightInfo");
        assert_eq!(d, Some(Duration::from_secs(20)));
    }

    #[test]
    fn test_timeout_for_method_do_put() {
        let mut t = TimeoutOption::new();
        t.set_timeout_seconds(OPTION_TIMEOUT_UPDATE, 15.0).unwrap();
        let d = t.timeout_for_method("/arrow.flight.protocol.FlightService/DoPut");
        assert_eq!(d, Some(Duration::from_secs(15)));
    }

    #[test]
    fn test_timeout_for_method_do_action() {
        let mut t = TimeoutOption::new();
        t.set_timeout_seconds(OPTION_TIMEOUT_UPDATE, 15.0).unwrap();
        let d = t.timeout_for_method("/arrow.flight.protocol.FlightService/DoAction");
        assert_eq!(d, Some(Duration::from_secs(15)));
    }

    #[test]
    fn test_timeout_for_method_unknown() {
        let t = TimeoutOption::new();
        let d = t.timeout_for_method("/some/OtherService/OtherMethod");
        assert_eq!(d, None);
    }

    #[test]
    fn test_clone_preserves_timeouts() {
        let mut t = TimeoutOption::new();
        t.set_timeout_seconds(OPTION_TIMEOUT_QUERY, 5.0).unwrap();
        let cloned = t.clone();
        assert_eq!(cloned.query_timeout, Some(Duration::from_secs(5)));
    }

    #[test]
    fn test_default_all_none() {
        let t = TimeoutOption::default();
        assert_eq!(t.fetch_timeout, None);
        assert_eq!(t.query_timeout, None);
        assert_eq!(t.update_timeout, None);
        assert_eq!(t.connect_timeout, None);
    }

    #[test]
    fn test_get_timeout_seconds() {
        let mut t = TimeoutOption::new();
        t.set_timeout_seconds(OPTION_TIMEOUT_FETCH, 30.5).unwrap();
        assert_eq!(t.get_timeout_seconds(OPTION_TIMEOUT_FETCH).unwrap(), 30.5);
    }

    #[test]
    fn test_set_timeout_invalid_key() {
        let mut t = TimeoutOption::new();
        let result = t.set_timeout_seconds("adbc.bogus.key", 10.0);
        assert!(result.is_err());
    }
}
