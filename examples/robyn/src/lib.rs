// Vendored from Robyn (https://github.com/sparckles/Robyn) commit 1170748e — BSD 2-Clause (c) 2021 Sanskar Jethi.
// Ported from actix-web to the Aethonan rlib (Aethonan: Fidus Writer fork of Granian).
//
// The crate builds a Python extension module providing the `robyn.robyn`
// native module (the ported Robyn core) *and* the `aethonan._aethonan`
// module: both names are backed by the same shared object, so the native
// route registry and every other process-global of the embedded Aethonan
// rlib are shared between the Robyn side and Aethonan's own server classes.

mod asyncio;
mod callbacks;
mod conversion;
mod executors;
mod io_helpers;
mod routers;
mod server;
mod types;
mod websockets;

use server::Server;
use types::{
    cookie::{Cookie, Cookies, CookiesIter},
    function_info::{FunctionInfo, MiddlewareType},
    headers::Headers,
    identity::Identity,
    multimap::QueryParams,
    request::PyRequest,
    response::{PyResponse, PyStreamingResponse},
    HttpMethod, Url,
};

use websockets::{WebSocketChannel, WebSocketConnector};
use pyo3::prelude::*;

#[pyfunction]
fn get_version() -> String {
    env!("CARGO_PKG_VERSION").into()
}

/// Register the ported Robyn classes and functions onto a module that has
/// already been initialised with Aethonan's own machinery (`init_py_module`).
fn register_robyn_classes(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_function(wrap_pyfunction!(get_version, module)?)?;

    module.add_class::<Server>()?;
    module.add_class::<Headers>()?;
    module.add_class::<Cookie>()?;
    module.add_class::<Cookies>()?;
    module.add_class::<CookiesIter>()?;
    module.add_class::<WebSocketConnector>()?;
    module.add_class::<WebSocketChannel>()?;
    module.add_class::<FunctionInfo>()?;
    module.add_class::<Identity>()?;
    module.add_class::<PyRequest>()?;
    module.add_class::<PyResponse>()?;
    module.add_class::<PyStreamingResponse>()?;
    module.add_class::<Url>()?;
    module.add_class::<QueryParams>()?;
    module.add_class::<MiddlewareType>()?;
    module.add_class::<HttpMethod>()?;

    // Register awaitable types
    module.add_class::<callbacks::PyEmptyAwaitable>()?;
    module.add_class::<callbacks::PyDoneAwaitable>()?;
    module.add_class::<callbacks::PyErrAwaitable>()?;
    module.add_class::<callbacks::PyIterAwaitable>()?;
    module.add_class::<callbacks::PyFutureAwaitable>()?;

    Ok(())
}

#[pyo3::pymodule(gil_used = false)]
fn robyn(py: Python, module: &Bound<'_, PyModule>) -> PyResult<()> {
    // Initialise the Aethonan module: this provides the workers, the callback
    // scheduler and everything else Aethonan's Python side expects to find.
    aethonan::init_py_module(py, module)?;
    register_robyn_classes(module)?;
    Ok(())
}

#[pyo3::pymodule(gil_used = false)]
fn _aethonan(py: Python, module: &Bound<'_, PyModule>) -> PyResult<()> {
    // The same module under the name `aethonan._aethonan`: when this shared
    // object is installed over the Aethonan package (see install.py), the
    // whole Python side (Aethonan's server classes and the ported Robyn core)
    // runs on a single copy of the embedded rlib.
    aethonan::init_py_module(py, module)?;
    register_robyn_classes(module)?;
    Ok(())
}
