//! Example of a crate embedding Aethonan as a library in order to serve
//! *native routes*: paths handled directly by the Rust side, without involving
//! the Python application.
//!
//! The crate builds a Python extension module named `aethonan._aethonan`, which
//! replaces the one shipped with the Aethonan package: the Python side is
//! entirely stock Aethonan, while the native routes are provided here.

use pyo3::prelude::*;

mod consumers;
mod views;

#[pyo3::pymodule(gil_used = false)]
fn _aethonan(py: Python, module: &Bound<PyModule>) -> PyResult<()> {
    // Initialise the Aethonan module: this provides the workers, the callback
    // scheduler and everything else the Python side expects to find.
    aethonan::init_py_module(py, module)?;

    // Register the native routes: they are served by this crate, without ever
    // reaching the Python application.
    aethonan::register_native_route(
        "/native_view",
        aethonan::NativeRouteKind::Http,
        views::info_view,
    );
    aethonan::register_native_route(
        "/native_ws",
        aethonan::NativeRouteKind::Websocket,
        consumers::broadcast_consumer(),
    );

    Ok(())
}
