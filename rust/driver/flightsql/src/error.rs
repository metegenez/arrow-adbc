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

//! Utilities for building legible, informative errors in ADBC drivers.
//!
//! Vendored and adapted from [driverbase-rs](https://github.com/adbc-drivers/driverbase-rs)
//! (Apache 2.0). Provides the `Error<E>` generic error type with builder-pattern
//! methods, the `ErrorHelper` trait with ADBC status code constructors, and
//! conversion to `adbc_core::error::Error`.

use std::fmt::Write;

use arrow_flight::error::FlightError;
use tonic::Code;

/// A lightweight error type.
#[derive(Clone)]
pub struct Error<E>
where
    E: ErrorHelper,
{
    // boxed so that the on-stack size is small
    inner: Box<ErrorImpl<E>>,
    _marker: std::marker::PhantomData<E>,
}

#[derive(Clone)]
struct ErrorImpl<E>
where
    E: ErrorHelper,
{
    status: adbc_core::error::Status,
    // Message will be "could not <CONTEXT_MESSAGE>: <ERROR_MESSAGE>"
    error_message: String,
    context_message: Option<String>,
    location: Option<String>,
    vendor_code: i32,
    sqlstate: [std::os::raw::c_char; 5],
    #[allow(
        clippy::vec_box,
        reason = "inner is Boxed - no need to move when combining"
    )]
    rest: Vec<Box<ErrorImpl<E>>>,
    _marker: std::marker::PhantomData<E>,
}

impl<E> Error<E>
where
    E: ErrorHelper,
{
    pub fn get_vendor_code(&self) -> i32 {
        self.inner.vendor_code
    }

    pub fn to_adbc(self) -> adbc_core::error::Error {
        self.into()
    }

    /// Add a new clause to the error message.
    pub fn message(mut self, message: impl AsRef<str>) -> Self {
        if !self.inner.error_message.is_empty() {
            self.inner.error_message.push_str("; ");
        }
        write!(&mut self.inner.error_message, "{}", message.as_ref()).unwrap();
        self
    }

    /// Add a new clause to the context message.
    pub fn context(mut self, message: impl AsRef<str>) -> Self {
        self.inner.context_message = match self.inner.context_message.take() {
            None => Some(message.as_ref().to_owned()),
            Some(ctx) => Some(format!("{}: could not {ctx}", message.as_ref())),
        };
        self
    }

    /// Add a source code location to the error.
    pub fn location(mut self, location: impl AsRef<str>) -> Self {
        self.inner.location = Some(location.as_ref().to_owned());
        self
    }

    /// Add a new clause to the error message.
    pub fn format(mut self, message: std::fmt::Arguments) -> Self {
        if !self.inner.error_message.is_empty() {
            self.inner.error_message.push_str("; ");
        }
        write!(&mut self.inner.error_message, "{message}").unwrap();
        self
    }

    /// Add a vendor-specific code to the error.
    pub fn vendor_code(mut self, code: i32) -> Self {
        self.inner.vendor_code = code;
        self
    }

    /// Add an ANSI SQL-style SQLSTATE code to the error.
    pub fn sqlstate(mut self, sqlstate: [std::os::raw::c_char; 5]) -> Self {
        self.inner.sqlstate = sqlstate;
        self
    }

    /// Merge two errors.
    pub fn and(mut self, mut other: Error<E>) -> Self {
        let rest = std::mem::take(&mut other.inner.rest);
        self.inner.rest.push(other.inner);
        self.inner.rest.extend(rest);
        self
    }

    /// Merge this error with a potential error from a fallible operation.
    pub fn and_also<T>(self, other: impl FnOnce() -> Result<T, Error<E>>) -> Self {
        match other() {
            Ok(_) => self,
            Err(other) => self.and(other),
        }
    }
}

impl<E> From<Error<E>> for adbc_core::error::Error
where
    E: ErrorHelper,
{
    fn from(value: Error<E>) -> Self {
        let mut message = match value.inner.context_message {
            Some(ctx) => format!(
                "[{}] could not {}: {}",
                E::NAME,
                ctx,
                value.inner.error_message
            ),
            None => format!("[{}] {}", E::NAME, value.inner.error_message),
        };

        if let Some(location) = value.inner.location {
            write!(&mut message, " (at {location})").unwrap();
        }

        for (i, err) in value.inner.rest.iter().enumerate() {
            if i == 0 {
                write!(&mut message, ". While handling error: ").unwrap();
            } else {
                write!(&mut message, "; ").unwrap();
            }
            match err.context_message {
                Some(ref ctx) => {
                    write!(&mut message, "could not {}: {}", ctx, err.error_message).unwrap()
                }
                None => write!(&mut message, "{}", err.error_message).unwrap(),
            }

            if let Some(ref location) = err.location {
                write!(&mut message, " (at {location})").unwrap();
            }
        }

        adbc_core::error::Error {
            message,
            status: value.inner.status,
            vendor_code: value.inner.vendor_code,
            sqlstate: value.inner.sqlstate,
            details: None,
        }
    }
}

impl<E> From<arrow_schema::ArrowError> for Error<E>
where
    E: ErrorHelper,
{
    fn from(value: arrow_schema::ArrowError) -> Self {
        E::from_arrow(value)
    }
}

impl<E> std::fmt::Debug for Error<E>
where
    E: ErrorHelper,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self.inner)
    }
}

impl<E> std::fmt::Display for Error<E>
where
    E: ErrorHelper,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}: [{}] ", self.inner.status, E::NAME)?;
        if let Some(ref ctx) = self.inner.context_message {
            write!(f, "could not {ctx}: ")?;
        }
        write!(f, "{}", self.inner.error_message)?;

        if let Some(ref location) = self.inner.location {
            write!(f, ". Location: {location}").unwrap();
        }

        if self.inner.vendor_code != 0 {
            write!(f, ". Vendor code: {}", self.inner.vendor_code)?;
        }
        Ok(())
    }
}

impl<E> std::fmt::Debug for ErrorImpl<E>
where
    E: ErrorHelper,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Error {\n")?;
        writeln!(f, "  status: {:?},", self.status)?;
        writeln!(f, "  error_message: {:?},", self.error_message)?;
        writeln!(f, "  context_message: {:?},", self.context_message)?;
        writeln!(f, "  vendor_code: {},", self.vendor_code)?;
        writeln!(f, "  sqlstate: {:?},", self.sqlstate)?;
        write!(f, "  rest: [")?;
        if !self.rest.is_empty() {
            writeln!(f)?;
            for err in &self.rest {
                writeln!(f, "    {err:?},")?;
            }
        }
        writeln!(f, "],")?;
        f.write_str("}")?;
        Ok(())
    }
}

impl<E> std::error::Error for Error<E> where E: ErrorHelper {}

/// A factory for errors with consistent formatting and messaging.
pub trait ErrorHelper: Clone + Send + Sync + Sized + 'static {
    const NAME: &'static str;

    /// Error for when a user requests an unknown option.
    fn get_unknown_option<K: std::fmt::Debug>(key: &K) -> Error<Self> {
        Self::not_found().format(format_args!("unknown option {key:?}"))
    }

    /// Error for when when a user sets an option to an invalid value.
    fn set_invalid_option<K: std::fmt::Debug>(
        key: &K,
        value: &adbc_core::options::OptionValue,
    ) -> Error<Self> {
        Self::invalid_argument().context(format!("invalid option {key:?}={value:?}"))
    }

    /// Error for when a user sets an unknown option.
    fn set_unknown_option<K: std::fmt::Debug>(key: &K) -> Error<Self> {
        Self::not_implemented().format(format_args!("unknown option {key:?}"))
    }

    /// Unwrap an option value as an integer.
    fn option_as_int<K: std::fmt::Debug>(
        k: &K,
        v: &adbc_core::options::OptionValue,
    ) -> Result<i64, Error<Self>> {
        match v {
            adbc_core::options::OptionValue::String(s) => s
                .parse::<i64>()
                .map_err(|_| Self::set_invalid_option(k, v).message("must be an integer")),
            adbc_core::options::OptionValue::Int(v) => Ok(*v),
            v => Err(Self::set_invalid_option(k, v).message("must be an integer")),
        }
    }

    /// Unwrap an option value as a string.
    fn option_as_string<'v, K: std::fmt::Debug>(
        k: &K,
        v: &'v adbc_core::options::OptionValue,
    ) -> Result<&'v str, Error<Self>> {
        match v {
            adbc_core::options::OptionValue::String(s) => Ok(s.as_ref()),
            v => Err(Self::set_invalid_option(k, v).message("must be a string")),
        }
    }

    /// Unwrap an option value as a string.
    fn option_as_bool<K: std::fmt::Debug>(
        k: &K,
        v: &adbc_core::options::OptionValue,
    ) -> Result<bool, Error<Self>> {
        match v {
            adbc_core::options::OptionValue::String(s) => {
                if s == "true" {
                    Ok(true)
                } else if s == "false" {
                    Ok(false)
                } else {
                    Err(Self::set_invalid_option(k, v).message("must be 'true' or 'false'"))
                }
            }
            v => Err(Self::set_invalid_option(k, v).message("must be a string")),
        }
    }

    /// An error with the given status and no message.
    fn status(status: adbc_core::error::Status) -> Error<Self> {
        Error::<Self> {
            inner: Box::new(ErrorImpl::<Self> {
                status,
                error_message: String::new(),
                context_message: None,
                location: None,
                vendor_code: 0,
                sqlstate: Default::default(),
                rest: Vec::new(),
                _marker: std::marker::PhantomData,
            }),
            _marker: std::marker::PhantomData,
        }
    }

    fn already_exists() -> Error<Self> {
        Self::status(adbc_core::error::Status::AlreadyExists)
    }

    fn cancelled() -> Error<Self> {
        Self::status(adbc_core::error::Status::Cancelled)
    }

    fn integrity() -> Error<Self> {
        Self::status(adbc_core::error::Status::Integrity)
    }

    fn internal(location: impl AsRef<str>) -> Error<Self> {
        Self::status(adbc_core::error::Status::Internal).location(location)
    }

    fn internal_no_location() -> Error<Self> {
        Self::status(adbc_core::error::Status::Internal)
    }

    fn invalid_argument() -> Error<Self> {
        Self::status(adbc_core::error::Status::InvalidArguments)
    }

    fn invalid_data() -> Error<Self> {
        Self::status(adbc_core::error::Status::InvalidData)
    }

    fn invalid_state() -> Error<Self> {
        Self::status(adbc_core::error::Status::InvalidState)
    }

    fn io() -> Error<Self> {
        Self::status(adbc_core::error::Status::IO)
    }

    fn not_found() -> Error<Self> {
        Self::status(adbc_core::error::Status::NotFound)
    }

    fn not_implemented() -> Error<Self> {
        Self::status(adbc_core::error::Status::NotImplemented)
    }

    fn timeout() -> Error<Self> {
        Self::status(adbc_core::error::Status::Timeout)
    }

    fn unauthenticated() -> Error<Self> {
        Self::status(adbc_core::error::Status::Unauthenticated)
    }

    fn unauthorized() -> Error<Self> {
        Self::status(adbc_core::error::Status::Unauthorized)
    }

    fn unknown() -> Error<Self> {
        Self::status(adbc_core::error::Status::Unknown)
    }

    /// Box an ADBC error as an Arrow error.
    fn to_arrow(err: adbc_core::error::Error) -> arrow_schema::ArrowError {
        arrow_schema::ArrowError::ExternalError(Box::new(err))
    }

    /// Convert an Arrow error into this error, mapping status appropriately.
    fn from_arrow(err: arrow_schema::ArrowError) -> Error<Self> {
        match err {
            arrow_schema::ArrowError::NotYetImplemented(msg) => {
                Self::not_implemented().format(format_args!("{msg}"))
            }
            arrow_schema::ArrowError::ExternalError(error) => {
                Self::unknown().format(format_args!("{error}"))
            }
            arrow_schema::ArrowError::CastError(msg) => {
                Self::internal_no_location().format(format_args!("cast error: {msg}"))
            }
            arrow_schema::ArrowError::MemoryError(msg) => {
                Self::internal_no_location().format(format_args!("memory error: {msg}"))
            }
            arrow_schema::ArrowError::ParseError(msg) => {
                Self::internal_no_location().format(format_args!("parse error: {msg}"))
            }
            arrow_schema::ArrowError::SchemaError(msg) => {
                Self::internal_no_location().format(format_args!("schema error: {msg}"))
            }
            arrow_schema::ArrowError::ComputeError(msg) => {
                Self::internal_no_location().format(format_args!("compute error: {msg}"))
            }
            arrow_schema::ArrowError::DivideByZero => {
                Self::invalid_data().format(format_args!("divide by zero",))
            }
            arrow_schema::ArrowError::ArithmeticOverflow(msg) => {
                Self::invalid_data().format(format_args!("arithmetic overflow: {msg}"))
            }
            arrow_schema::ArrowError::CsvError(msg) => {
                Self::invalid_argument().format(format_args!("CSV error: {msg}"))
            }
            arrow_schema::ArrowError::JsonError(msg) => {
                Self::invalid_argument().format(format_args!("JSON error: {msg}"))
            }
            arrow_schema::ArrowError::IoError(msg, error) => {
                Self::io().format(format_args!("I/O error: {msg}: {error}"))
            }
            arrow_schema::ArrowError::IpcError(msg) => {
                Self::io().format(format_args!("IPC error: {msg}"))
            }
            arrow_schema::ArrowError::InvalidArgumentError(msg) => {
                Self::invalid_argument().message(msg)
            }
            arrow_schema::ArrowError::ParquetError(msg) => {
                Self::io().format(format_args!("Parquet error: {msg}"))
            }
            arrow_schema::ArrowError::CDataInterface(msg) => {
                Self::io().format(format_args!("C Data interface error: {msg}"))
            }
            arrow_schema::ArrowError::DictionaryKeyOverflowError => {
                Self::invalid_data().message("dictionary key overflowed")
            }
            arrow_schema::ArrowError::RunEndIndexOverflowError => {
                Self::invalid_data().message("run end index overflowed")
            }
            arrow_schema::ArrowError::OffsetOverflowError(size) => {
                Self::invalid_data().format(format_args!("offset overflowed at size {size}"))
            }
            arrow_schema::ArrowError::AvroError(err) => {
                Self::io().format(format_args!("Avro error: {err}"))
            }
        }
    }

    /// Convert an iterator of errors into a single error.
    fn from_all(errors: impl IntoIterator<Item = Error<Self>>) -> Option<Error<Self>> {
        let mut iter = errors.into_iter();
        let first = iter.next()?;
        Some(iter.fold(first, |acc, err| acc.and(err)))
    }
}

#[macro_export]
macro_rules! location {
    () => {
        format!("{}:{}", file!(), line!())
    };
}
pub use location;

/// The concrete error helper for the FlightSQL driver.
#[derive(Clone)]
pub struct FlightSqlErrorHelper;

impl ErrorHelper for FlightSqlErrorHelper {
    const NAME: &'static str = "adbc_flightsql";
}

/// Map a gRPC/tonic StatusCode to an ADBC Status.
///
/// Follows the Go FlightSQL driver's canonical `adbcFromFlightStatus` mapping
/// with all 17 tonic::Code variants covered. Unknown/unexpected integer codes
/// fall through to `Status::Unknown`.
///
/// Three non-obvious mappings per the Go driver:
/// - `PermissionDenied` → `Unauthorized` (authZ failure, not authN)
/// - `Unavailable` → `IO` (transient network issue)
/// - `DataLoss` → `IO` (data corruption during transfer)
pub fn grpc_code_to_adbc_status(code: Code) -> adbc_core::error::Status {
    match code {
        Code::Ok => adbc_core::error::Status::Ok,
        Code::Cancelled => adbc_core::error::Status::Cancelled,
        Code::Unknown => adbc_core::error::Status::Unknown,
        Code::InvalidArgument => adbc_core::error::Status::InvalidArguments,
        Code::DeadlineExceeded => adbc_core::error::Status::Timeout,
        Code::NotFound => adbc_core::error::Status::NotFound,
        Code::AlreadyExists => adbc_core::error::Status::AlreadyExists,
        Code::PermissionDenied => adbc_core::error::Status::Unauthorized,
        Code::ResourceExhausted => adbc_core::error::Status::Internal,
        Code::FailedPrecondition => adbc_core::error::Status::Unknown,
        Code::Aborted => adbc_core::error::Status::Unknown,
        Code::OutOfRange => adbc_core::error::Status::Unknown,
        Code::Unimplemented => adbc_core::error::Status::NotImplemented,
        Code::Internal => adbc_core::error::Status::Internal,
        Code::Unavailable => adbc_core::error::Status::IO,
        Code::DataLoss => adbc_core::error::Status::IO,
        Code::Unauthenticated => adbc_core::error::Status::Unauthenticated,
    }
}

/// Map an arrow_flight::FlightError to an adbc_core::error::Error.
///
/// When the FlightError wraps a tonic::Status (the `Tonic` variant),
/// extracts the gRPC status code and maps it to an ADBC status via
/// `grpc_code_to_adbc_status()`. Includes the gRPC message, code name,
/// and ADBC operation context in the error message.
///
/// Non-Tonic FlightErrors (Arrow, ProtocolError, DecodeError, ExternalError)
/// are mapped to `Status::IO` with the error's Display representation.
pub fn map_flight_error(
    err: FlightError,
    context: &str,
) -> adbc_core::error::Error {
    match err {
        FlightError::Tonic(status) => {
            let code = status.code();
            let adbc_status = grpc_code_to_adbc_status(code);
            let message = format!(
                "[FlightSQL] {} (gRPC {}; {context})",
                status.message(),
                code,
            );
            adbc_core::error::Error {
                message,
                status: adbc_status,
                vendor_code: code as i32,
                sqlstate: Default::default(),
                details: None,
            }
        }
        other => {
            // Non-gRPC errors (IPC decode, protocol errors, Arrow errors, etc.)
            FlightSqlErrorHelper::io()
                .message(format!("{other}"))
                .context(context.to_string())
                .to_adbc()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tonic::Code;

    #[test]
    fn test_map_grpc_invalid_argument() {
        let result = grpc_code_to_adbc_status(Code::InvalidArgument);
        assert_eq!(result, adbc_core::error::Status::InvalidArguments);
    }

    #[test]
    fn test_map_grpc_unauthenticated() {
        let result = grpc_code_to_adbc_status(Code::Unauthenticated);
        assert_eq!(result, adbc_core::error::Status::Unauthenticated);
    }

    #[test]
    fn test_map_grpc_unavailable() {
        let result = grpc_code_to_adbc_status(Code::Unavailable);
        assert_eq!(result, adbc_core::error::Status::IO);
    }

    #[test]
    fn test_map_grpc_not_found() {
        let result = grpc_code_to_adbc_status(Code::NotFound);
        assert_eq!(result, adbc_core::error::Status::NotFound);
    }

    #[test]
    fn test_map_grpc_ok() {
        let result = grpc_code_to_adbc_status(Code::Ok);
        assert_eq!(result, adbc_core::error::Status::Ok);
    }

    #[test]
    fn test_map_grpc_permission_denied() {
        let result = grpc_code_to_adbc_status(Code::PermissionDenied);
        assert_eq!(result, adbc_core::error::Status::Unauthorized);
    }

    #[test]
    fn test_map_grpc_deadline_exceeded() {
        let result = grpc_code_to_adbc_status(Code::DeadlineExceeded);
        assert_eq!(result, adbc_core::error::Status::Timeout);
    }

    #[test]
    fn test_map_flight_error_tonic_invalid_argument() {
        let status = tonic::Status::new(Code::InvalidArgument, "bad query syntax");
        let err = FlightError::Tonic(Box::new(status));
        let adbc_err = map_flight_error(err, "executing query");
        assert_eq!(adbc_err.status, adbc_core::error::Status::InvalidArguments);
        assert!(adbc_err.message.contains("executing query"));
        assert!(adbc_err.message.contains("bad query syntax"));
        assert_eq!(adbc_err.vendor_code, Code::InvalidArgument as i32);
    }

    #[test]
    fn test_map_flight_error_tonic_unauthenticated() {
        let status = tonic::Status::new(Code::Unauthenticated, "token expired");
        let err = FlightError::Tonic(Box::new(status));
        let adbc_err = map_flight_error(err, "connecting");
        assert_eq!(adbc_err.status, adbc_core::error::Status::Unauthenticated);
        assert!(adbc_err.message.contains("connecting"));
        assert!(adbc_err.message.contains("token expired"));
        assert_eq!(adbc_err.vendor_code, Code::Unauthenticated as i32);
    }

    #[test]
    fn test_map_flight_error_non_tonic() {
        let arrow_err = arrow_schema::ArrowError::ParseError("test parse error".into());
        let flight_err = FlightError::Arrow(arrow_err);
        let adbc_err = map_flight_error(flight_err, "decoding schema");
        assert_eq!(adbc_err.status, adbc_core::error::Status::IO);
        assert!(adbc_err.message.contains("decoding schema"));
    }

    #[test]
    fn test_map_grpc_all_codes_covered() {
        let codes = [
            Code::Ok,
            Code::Cancelled,
            Code::Unknown,
            Code::InvalidArgument,
            Code::DeadlineExceeded,
            Code::NotFound,
            Code::AlreadyExists,
            Code::PermissionDenied,
            Code::ResourceExhausted,
            Code::FailedPrecondition,
            Code::Aborted,
            Code::OutOfRange,
            Code::Unimplemented,
            Code::Internal,
            Code::Unavailable,
            Code::DataLoss,
            Code::Unauthenticated,
        ];
        for code in &codes {
            let _ = grpc_code_to_adbc_status(*code);
        }
    }
}
