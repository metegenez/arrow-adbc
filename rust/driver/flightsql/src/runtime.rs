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

use std::future::Future;

/// Bridges synchronous ADBC trait methods to the async tokio/tonic stack.
///
/// Two variants handle both standalone use (creates its own runtime)
/// and embedded use (shares an existing runtime handle).
///
/// Variant naming follows the DataFusion driver convention: `Handle` + `Tokio`.
pub enum Runtime {
    /// Uses an existing tokio runtime handle (e.g., when embedded in an async application).
    /// Uses `tokio::task::block_in_place` to avoid deadlocks.
    Handle(tokio::runtime::Handle),
    /// Creates and owns a new multi-threaded tokio runtime.
    Tokio(tokio::runtime::Runtime),
}

impl Runtime {
    /// Create a new Runtime, either reusing an existing handle or creating a new runtime.
    pub fn new(handle: Option<tokio::runtime::Handle>) -> std::io::Result<Self> {
        if let Some(handle) = handle {
            Ok(Self::Handle(handle))
        } else {
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()?;
            Ok(Self::Tokio(runtime))
        }
    }

    /// Execute a future synchronously, bridging to the async world.
    ///
    /// CRITICAL: For the Handle variant, uses `tokio::task::block_in_place`
    /// to prevent deadlocks when called from within an existing tokio runtime.
    pub fn block_on<F: Future>(&self, future: F) -> F::Output {
        match self {
            Runtime::Handle(handle) => tokio::task::block_in_place(|| handle.block_on(future)),
            Runtime::Tokio(runtime) => runtime.block_on(future),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tokio_block_on_works_standalone() {
        let runtime = Runtime::new(None).unwrap();
        let result = runtime.block_on(async { 42 });
        assert_eq!(result, 42);
    }

    #[test]
    fn test_handle_block_on_from_within_tokio() {
        // This test verifies that block_in_place prevents deadlocks
        // when called from within an existing tokio runtime.
        let outer = tokio::runtime::Runtime::new().unwrap();
        outer.block_on(async {
            let handle = tokio::runtime::Handle::current();
            let runtime = Runtime::new(Some(handle)).unwrap();
            let result = runtime.block_on(async { 42 });
            assert_eq!(result, 42);
        });
    }
}
