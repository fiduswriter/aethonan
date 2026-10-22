// Vendored from Robyn (https://github.com/sparckles/Robyn) commit 1170748e — BSD 2-Clause (c) 2021 Sanskar Jethi.
// Ported from actix-web (actix actor futures) to Aethonan's native websockets
// handles (Aethonan: Fidus Writer fork of Granian).

use pyo3::prelude::*;

use crate::types::function_info::FunctionInfo;
use crate::websockets::WebSocketConnector;

/// Execute a websocket lifecycle handler (`connect`/`close`). Async handlers
/// are awaited through the worker's event loop (the task locals scoped around
/// the session by the caller); sync handlers run under the GIL directly. The
/// handler's return value, when a string, is sent to the client as a text
/// frame — the behavior of stock Robyn's actix `ctx.text(...)` calls.
pub async fn execute_ws_function(
    function: &FunctionInfo,
    ws: &Py<WebSocketConnector>,
) -> Option<String> {
    if function.is_async {
        let fut = match Python::attach(|py| {
            let handler = function.handler.bind(py);
            let result = handler.call1((ws.clone_ref(py),))?;
            pyo3_async_runtimes::tokio::into_future(result)
        }) {
            Ok(fut) => fut,
            Err(e) => {
                log::error!("Failed to call async WebSocket handler: {}", e);
                return None;
            }
        };
        match fut.await {
            Ok(output) => {
                let result = Python::attach(|py| match output.extract::<Option<String>>(py) {
                    Ok(msg) => msg,
                    Err(e) => {
                        log::error!("Failed to extract WebSocket handler result: {}", e);
                        None
                    }
                });
                // pyo3 0.30 (with the reference pool disabled) requires drops
                // of Python heap pointers on attached threads.
                Python::attach(|_| drop(output));
                result
            }
            Err(e) => {
                let msg = Python::attach(|py| e.value(py).to_string());
                Python::attach(|_| drop(e));
                log::error!("Async WebSocket handler failed: {}", msg);
                None
            }
        }
    } else {
        Python::attach(|py| {
            let handler = match function.handler.bind(py).cast::<PyAny>() {
                Ok(h) => h,
                Err(e) => {
                    log::error!("Failed to get sync WebSocket handler: {}", e);
                    return None;
                }
            };
            match handler.call1((ws.clone_ref(py),)) {
                Ok(result) => match result.extract::<Option<String>>() {
                    Ok(op) => op,
                    Err(e) => {
                        log::error!("Failed to extract WebSocket handler result: {}", e);
                        None
                    }
                },
                Err(e) => {
                    log::error!("Sync WebSocket handler call failed: {}", e);
                    None
                }
            }
        })
    }
}
