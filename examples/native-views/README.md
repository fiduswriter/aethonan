# Aethonan native views

An example of a crate embedding [Aethonan](https://github.com/fiduswriter/aethonan) as a
Rust library in order to serve *native routes*: paths handled directly by the Rust side,
without ever reaching the Python application.

The crate builds a Python extension module named `aethonan._aethonan`, which **replaces** the
one shipped with the Aethonan package: the Python side is entirely stock Aethonan, while the
native routes are provided here.

## Usage

The extension module must be built against the same Python interpreter the Aethonan package
is installed for. In a working copy, the dependencies are usually installed in a local
virtual environment (the `.venv` directory created by `uv`, which is not tracked by the
repository):

```bash
# build the extension module
make build PYTHON=.venv/bin/python

# install it over the stock Aethonan one
make install PYTHON=.venv/bin/python
```

When the environment is already the active one, the default (`python3`) is enough:

```bash
make build
make install
```

The extension module is then served by the stock Aethonan server:

```bash
aethonan --interface rsgi --port 8000 package:app
```

```
$ curl http://127.0.0.1:8000/native_view
{"path":"/native_view","query":"","remote":"127.0.0.1:55568","secure":false,"source":"native-views"}
```

## How it works

The crate depends on Aethonan as a library and provides the `aethonan._aethonan` module itself:

```rust
use pyo3::prelude::*;

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
```

Since the routes are registered by this crate, the dependency direction is the one that
matters: **Aethonan is a library dependency, this crate is the artifact**. Changing a handler
here recompiles this crate only, Aethonan is not recompiled at all.

> **Note:** the `default-features = false` in the `Cargo.toml` is required: the
> `python-module` feature provides the `aethonan._aethonan` module, which is provided by this
> crate instead.

## Handlers

A handler is an async callable receiving the request and a context, and returning the
response:

```rust
use futures::future::BoxFuture;
use aethonan::{HTTPRequest, HTTPResponse, NativeContext};

pub fn info_view(req: HTTPRequest, ctx: NativeContext) -> BoxFuture<'static, HTTPResponse> {
    Box::pin(async move {
        // ...
    })
}
```

The websockets handshake is performed by Aethonan, the handler is responsible for the
upgraded stream:

```rust
let mut req = req;
match aethonan::ws::upgrade_intent(&mut req, None) {
    Ok((builder, ws)) => {
        ctx.rt.spawn(async move {
            if let Ok(stream) = ws.await {
                // handle the stream
            }
        });
        builder.body(aethonan::http::empty_body()).unwrap()
    }
    Err(_) => bad_request(),
}
```
