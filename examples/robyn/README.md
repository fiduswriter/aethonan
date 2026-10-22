# Aethonan — Robyn example

This directory is a **major example of embedding [Aethonan](https://github.com/fiduswriter/aethonan)**
(the Fidus Writer fork of [Granian](https://github.com/emmett-framework/granian)) as a Rust
library: it contains a vendored copy of the [Robyn](https://github.com/sparckles/Robyn) Python
web framework (upstream commit `1170748e`, version 0.88.0) whose Rust performance core has been
ported from **actix-web** to the **Aethonan rlib**.

A Robyn application served end-to-end by Aethonan: the HTTP and websockets pipelines run as
**native routes** — requests are answered directly in Rust through Aethonan's
`register_native_route` API, and Aethonan owns processes, workers, the event loop and the
socket.

> This is a demonstration fork-port, not a supported Robyn distribution. It exists to show
> what the Aethonan embedding API looks like for a real, non-trivial framework.

## What was ported

| Stock Robyn (actix-web) | This port (Aethonan rlib) |
| --- | --- |
| `src/server.rs` actix `HttpServer` bootstrap | One catch-all native route (`NativeRouteKind::Http`) + one `NativeRouteKind::Websocket` route per websocket endpoint; `Server.start()` registers them and hands serving over to Aethonan |
| `Request::from_actix_request` | `Request::from_hyper_request` over `aethonan::HTTPRequest` |
| `Responder` impls (actix `HttpResponse`) | `ResponseType::to_hyper_response` building `aethonan::HTTPResponse` |
| `HttpMethod::from_actix_method` | `HttpMethod::from_hyper_method` |
| actix `Files` directory services | static directory serving inside the native handler (GET/HEAD only, OPTIONS falls through to the router like stock, index-file + redirect-to-slash + listing) |
| actix-web-actors websockets (~680 lines actor code) | `aethonan::ws::{is_upgrade_request, upgrade_intent, WSStream}` + a per-endpoint client registry; the `WebSocketConnector`/`WebSocketChannel` Python contract is preserved |
| `actix_multipart` | the `multer` crate over the collected hyper body |
| `actix_web::cookie` | the standalone `cookie` crate (the same crate actix builds on) |
| Robyn's own process pool (`processpool.py` + `SocketHeld`) | Aethonan's `MPServer`: `run_processes()` registers everything on the native `Server`, then starts Aethonan; each worker process replays the registration and serves the native routes |

The reusable, actix-free Rust files (executors, routers, types, asyncio/callbacks/conversion
helpers) were migrated from pyo3 0.27 / pyo3-async-runtimes 0.27 to **pyo3 0.30** (Bound API,
`cast` instead of `downcast`, `Python::attach`, manual `FromPyObject`/`Clone` for pyclasses)
and from pyo3-asyncio's task-locals model to `pyo3_async_runtimes` 0.30.

## Layout

- `src/` — the ported Rust crate `robyn-aethonan` (cdylib + rlib, pyo3 module `robyn.robyn`,
  which also exports `aethonan._aethonan`, see below).
- `robyn/` — the vendored Robyn Python package. Unmodified except:
  - `robyn/robyn.py` replaces the compiled extension module with a shim re-exporting
    `aethonan._aethonan` (a single shared object backs both module names, so the native route
    registry of the embedded rlib is shared between the Robyn core and Aethonan's server
    classes);
  - `robyn/processpool.py` rewritten to hand over to Aethonan (see above);
  - `robyn/_aethonan_target.py` added: a dummy RSGI target (never invoked — the catch-all
    native route answers everything).
- `integration_tests/` — Robyn's own integration tests (vendored, see the attribution note in
  `NOTICE`), so the example is self-verifying.
- `install.py` — installs the built extension over the `aethonan` package (atomic rename; never
  overwrite a loaded `.so` in place).
- `NOTICE`, `LICENSE` — attribution and licensing.

## Building

Requirements: a Python 3.14 venv with `maturin`, plus the Python test dependencies
(`pytest requests websocket-client websockets orjson jinja2 pydantic multipart multiprocess`;
`orjson` and `multiprocess` are runtime requirements of the vendored Robyn).

```bash
python3.14 -m venv /tmp/robyn-venv
/tmp/robyn-venv/bin/pip install maturin pytest requests websocket-client websockets \
    orjson jinja2 pydantic multipart multiprocess

# Aethonan itself (Python package + stock _aethonan extension):
/tmp/robyn-venv/bin/pip install /home/johannes/src/granian

# The ported Robyn core + vendored Python package:
cd /home/johannes/src/granian/examples/robyn
/tmp/robyn-venv/bin/maturin build -i python --out dist
/tmp/robyn-venv/bin/pip install --force-reinstall dist/robyn_aethonan-*.whl

# Move the extension over the aethonan package (single shared object, see above):
/tmp/robyn-venv/bin/python install.py \
    "$(ls /tmp/robyn-venv/lib/python3.14/site-packages/robyn/robyn*.so)"
```

## Running the tests

```bash
cd /home/johannes/src/granian/examples/robyn
PYTHONPATH=$PWD /tmp/robyn-venv/bin/python -m pytest integration_tests/test_get_requests.py -q
PYTHONPATH=$PWD /tmp/robyn-venv/bin/python -m pytest integration_tests -q   # the whole suite
```

The test harness (`integration_tests/conftest.py`, vendored) spawns
`python3 integration_tests/base_routes.py`, waits for the port and exercises the server over
HTTP with `requests` (and `websocket-client` for the websockets tests).

Two environment notes:

- `PYTHONPATH=$PWD` (the example root) is required so the spawned `base_routes.py` can
  import the `integration_tests` package.
- The suite defaults to port 8080 (upstream's hardcoded value); set `ROBYN_TEST_PORT` to
  use another port. (If the venv's `bin` is not on `PATH`, prefix the commands with
  `PATH=/tmp/robyn-venv/bin:$PATH` so the spawned `python3` is the venv interpreter.)

Current status: **the full vendored suite passes — 373 passed, 0 failed** (HTTP, websockets,
sessions, SSE/streaming, multipart, static files, subrouters, lifecycle events).

## Serving an app manually

```python
from robyn import Robyn

app = Robyn(__file__)

@app.get("/")
def hello():
    return "hello, world"

app.start(port=8080)
```

`app.start()` registers routes/middlewares/websockets with the native `Server` and starts
Aethonan (`aethonan.Aethonan`), which spawns the worker processes. Each worker registers the
catch-all native route and serves the Robyn pipeline in Rust; Python is only invoked for
handlers and middlewares.

### Mapping of Robyn's process model

Robyn's `--processes` map to Aethonan **workers** (separate Python processes, each with its
own event loop — this matches Robyn's one-event-loop-per-process model), and Robyn's
`--workers` map to Aethonan's **runtime threads** per worker. The port requires the `fork`
start method: route state is inherited by the workers rather than pickled (Aethonan's stock
spawn/forkserver methods cannot carry the registered Python handlers into the children; see
the note in `robyn/processpool.py`).

## Known limitations

- HTTP/2 websocket upgrades (RFC 8441) are not answered by the websocket routes (same
  limitation as Aethonan's reference consumer); HTTP/1.1 upgrades are fully supported.
- The port requires the `fork` multiprocessing start method (see above), so it does not
  run on Windows.
- Aethonan's HTTP-layer knobs (keep-alive timeouts, header read timeout) replace actix's;
  Robyn's `client_timeout`/`keep_alive_timeout` arguments are accepted by `app.start()` but
  not forwarded.
- Stock-parity quirk: Robyn 0.88.0 on actix accidentally extends incoming request headers
  with the *global response headers* (its `web::Data<Arc<Headers>>` injections collide on
  the same type). The tests encode that behavior (handlers observe `server: robyn` on the
  request), so the port deliberately reproduces it; see the note in `src/server.rs`.
- Everything else is on par with stock Robyn 0.88.0; `integration_tests/` is the arbiter.

## Library gaps worked around (nothing changed in the aethonan rlib)

- **pyo3-async-runtimes 0.30.0 drops `Py` values on unattached runtime threads** in
  `future_into_py_with_locals` when pyo3 is built with `--cfg pyo3_disable_reference_pool`
  (which the aethonan repository sets in its root `.cargo/config.toml`, inherited by every
  crate inside the repo). The example carries a minimal, clearly marked patch of the crate
  (`patches/pyo3-async-runtimes`, applied via `[patch.crates-io] in `Cargo.toml`). With the
  reference pool enabled (pyo3's default, as used by e.g. fiduswriter-native) the patch is
  a no-op equivalent.
- All other pyo3 0.30 strictness (drops of Python heap pointers on attached threads only)
  is handled inside the ported code with explicit `Python::attach` disposal.

## Licensing and attribution

Robyn is © 2021 Sanskar Jethi and the sparckles project, licensed under the BSD 2-Clause
license (see `LICENSE`). `src/runtime.rs`/`src/asyncio.rs`-derived helpers carry a secondary
BSD 3-Clause attribution to Giovanni Barillari / Granian; those headers are preserved. New
files added by the port note the Aethonan fork in their headers. The example's `Cargo.toml`
declares `license = "BSD-2-Clause"`. See `NOTICE` for the full attribution.
