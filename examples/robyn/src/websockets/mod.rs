// Vendored from Robyn (https://github.com/sparckles/Robyn) commit 1170748e — BSD 2-Clause (c) 2021 Sanskar Jethi.
// Ported from actix-web (actix-web-actors) to Aethonan's native websockets
// handles (Aethonan: Fidus Writer fork of Granian).

pub mod registry;

use crate::executors::web_socket_executors::execute_ws_function;
use crate::routers::web_socket_router::WebSocketRouter;
use crate::types::function_info::FunctionInfo;
use crate::types::multimap::QueryParams;
use registry::{WsCommand, registry_for};

use futures::{future::BoxFuture, SinkExt, StreamExt};
use http_body_util::BodyExt;
use log::debug;
use pyo3::prelude::*;
use pyo3::IntoPyObject;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::Message;
use uuid::Uuid;

use aethonan::ws::{WSStream, is_upgrade_request, upgrade_intent};
use aethonan::{HTTPRequest, HTTPResponse, NativeContext, NativeRouteKind};

/// A single WebSocket payload — UTF-8 text or raw binary bytes.
pub enum WsPayload {
    Text(String),
    Binary(Vec<u8>),
}

/// A Rust-backed channel receiver exposed to Python.
/// Python handlers call `await channel.receive()` to get the next message.
/// Returns a `str` for text frames, `bytes` for binary frames, or None when
/// the connection is closed.
#[pyclass]
pub struct WebSocketChannel {
    receiver: Arc<tokio::sync::Mutex<mpsc::UnboundedReceiver<Option<WsPayload>>>>,
}

#[pymethods]
impl WebSocketChannel {
    /// Await the next message from the WebSocket.
    /// Returns a `str` (text frame) or `bytes` (binary frame), or None if the
    /// connection was closed.
    fn receive<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let receiver = self.receiver.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut rx = receiver.lock().await;
            let next = rx.recv().await;
            Python::attach(|py| match next {
                Some(Some(WsPayload::Text(text))) => {
                    Ok(Some(text.into_pyobject(py)?.into_any().unbind()))
                }
                Some(Some(WsPayload::Binary(bytes))) => Ok(Some(
                    pyo3::types::PyBytes::new(py, &bytes).into_any().unbind(),
                )),
                Some(None) | None => Ok(None::<Py<PyAny>>),
            })
        })
    }
}

/// The Python-visible handle of a websocket connection. Replaces the actix
/// `WebSocketConnector` actor: instead of an actor address it holds the
/// per-endpoint registry and its own command channel towards the session
/// task that owns the websocket sink.
#[pyclass]
pub struct WebSocketConnector {
    pub id: Uuid,
    pub router: HashMap<String, FunctionInfo>,
    pub registry: Arc<registry::WebSocketRegistry>,
    pub query_params: QueryParams,
    /// Sender side of the message channel (feeds the Python `WebSocketChannel`).
    pub message_sender: Option<mpsc::UnboundedSender<Option<WsPayload>>>,
    /// Receiver side exposed to Python via WebSocketChannel.
    pub message_channel: Option<Py<WebSocketChannel>>,
}

impl Clone for WebSocketConnector {
    fn clone(&self) -> Self {
        Self {
            id: self.id,
            router: self.router.clone(),
            registry: Arc::clone(&self.registry),
            query_params: self.query_params.clone(),
            message_sender: self.message_sender.clone(),
            message_channel: Python::attach(|py| {
                self.message_channel.as_ref().map(|c| c.clone_ref(py))
            }),
        }
    }
}

#[pymethods]
impl WebSocketConnector {
    pub fn sync_send_to(&self, recipient_id: String, message: String) {
        let recipient_id = match Uuid::parse_str(&recipient_id) {
            Ok(id) => id,
            Err(e) => {
                log::error!("Invalid recipient_id '{}': {}", recipient_id, e);
                return;
            }
        };

        self.registry.send_text_to(recipient_id, message);
        log::debug!("Message sent successfully");
    }

    pub fn async_send_to(
        &self,
        py: Python,
        recipient_id: String,
        message: String,
    ) -> PyResult<Py<PyAny>> {
        let recipient_id = match Uuid::parse_str(&recipient_id) {
            Ok(id) => id,
            Err(e) => {
                return Err(pyo3::exceptions::PyValueError::new_err(format!(
                    "Invalid recipient_id '{}': {}",
                    recipient_id, e
                )));
            }
        };
        let registry = Arc::clone(&self.registry);

        let awaitable = pyo3_async_runtimes::tokio::future_into_py(py, async move {
            registry.send_text_to(recipient_id, message);
            Ok(())
        })?;

        Ok(awaitable.into_pyobject(py)?.into_any().into())
    }

    pub fn sync_broadcast(&self, message: String) {
        self.registry.broadcast_text(message);
    }

    pub fn async_broadcast(&self, py: Python, message: String) -> PyResult<Py<PyAny>> {
        let registry = Arc::clone(&self.registry);

        let awaitable = pyo3_async_runtimes::tokio::future_into_py(py, async move {
            registry.broadcast_text(message);
            Ok(())
        })?;

        Ok(awaitable.into_pyobject(py)?.into_any().into())
    }

    pub fn sync_send_bytes_to(&self, recipient_id: String, data: Vec<u8>) {
        let recipient_id = match Uuid::parse_str(&recipient_id) {
            Ok(id) => id,
            Err(e) => {
                log::error!("Invalid recipient_id '{}': {}", recipient_id, e);
                return;
            }
        };

        self.registry.send_binary_to(recipient_id, data);
    }

    pub fn async_send_bytes_to(
        &self,
        py: Python,
        recipient_id: String,
        data: Vec<u8>,
    ) -> PyResult<Py<PyAny>> {
        let recipient_id = match Uuid::parse_str(&recipient_id) {
            Ok(id) => id,
            Err(e) => {
                return Err(pyo3::exceptions::PyValueError::new_err(format!(
                    "Invalid recipient_id '{}': {}",
                    recipient_id, e
                )));
            }
        };
        let registry = Arc::clone(&self.registry);

        let awaitable = pyo3_async_runtimes::tokio::future_into_py(py, async move {
            registry.send_binary_to(recipient_id, data);
            Ok(())
        })?;

        Ok(awaitable.into_pyobject(py)?.into_any().into())
    }

    pub fn sync_broadcast_bytes(&self, data: Vec<u8>) {
        self.registry.broadcast_binary(data);
    }

    pub fn async_broadcast_bytes(&self, py: Python, data: Vec<u8>) -> PyResult<Py<PyAny>> {
        let registry = Arc::clone(&self.registry);

        let awaitable = pyo3_async_runtimes::tokio::future_into_py(py, async move {
            registry.broadcast_binary(data);
            Ok(())
        })?;

        Ok(awaitable.into_pyobject(py)?.into_any().into())
    }

    pub fn close(&self) {
        // Stock behavior (registry `Close` message): deliver a
        // "Connection closed" text frame, which the session task treats as
        // the signal to shut the connection down.
        self.registry
            .send_text_to(self.id, "Connection closed".to_string());
    }

    #[getter]
    pub fn get_id(&self) -> String {
        self.id.to_string()
    }

    #[getter]
    pub fn get_query_params(&self) -> QueryParams {
        self.query_params.clone()
    }

    /// Get the message channel for WebSocket handlers.
    #[getter]
    pub fn get_message_channel(&self, py: Python) -> Option<Py<WebSocketChannel>> {
        self.message_channel.as_ref().map(|c| c.clone_ref(py))
    }
}

/// Register a native websocket route for the given endpoint. The handler is
/// responsible for the handshake, like Aethonan's reference websocket
/// consumer; once upgraded, `ws_session` drives the connection.
pub fn register_web_socket_route(endpoint: &str, router: &Arc<WebSocketRouter>) {
    let endpoint = endpoint.to_string();
    let endpoint_for_session = endpoint.clone();
    let router = Arc::clone(router);

    aethonan::register_native_route(
        &endpoint,
        NativeRouteKind::Websocket,
        move |mut req: HTTPRequest, ctx: NativeContext| -> BoxFuture<'static, HTTPResponse> {
            let router = Arc::clone(&router);
            let endpoint = endpoint_for_session.clone();

            Box::pin(async move {
                // Prefix matching only buys us the first segments; the
                // endpoint must match exactly ("/web_socket" must not swallow
                // "/web_socket2").
                if req.uri().path() != endpoint {
                    return text_response(404, "Not found");
                }
                if !is_upgrade_request(&req) {
                    // HTTP/2 has no "Connection: upgrade" header, hence websockets
                    // over HTTP/2 (RFC 8441) are not supported.
                    if req.version() == hyper::Version::HTTP_2 {
                        return text_response(501, "Not implemented");
                    }
                    return text_response(400, "Bad request");
                }

                let query_string = req.uri().query().unwrap_or_default().to_string();

                match upgrade_intent(&mut req, None) {
                    Ok((builder, ws)) => {
                        let task_locals = crate::server::task_locals().cloned();
                        ctx.rt.spawn(async move {
                            match ws.await {
                                Ok(stream) => {
                                    let session =
                                        ws_session(endpoint, stream, router, query_string);
                                    if let Some(task_locals) = task_locals {
                                        pyo3_async_runtimes::tokio::scope(task_locals, session)
                                            .await;
                                    } else {
                                        session.await;
                                    }
                                }
                                Err(err) => {
                                    log::warn!("Websockets upgrade failed with {err:?}");
                                }
                            }
                        });

                        builder.body(aethonan::http::empty_body()).unwrap()
                    }
                    Err(err) => {
                        log::info!("Websockets handshake failed with {err:?}");
                        text_response(400, "Bad request")
                    }
                }
            })
        },
    );
}

fn text_response(status: u16, body: &str) -> HTTPResponse {
    let mut builder = hyper::Response::builder().status(status);
    let headers = builder.headers_mut().unwrap();
    headers.insert(
        hyper::header::CONTENT_TYPE,
        hyper::http::HeaderValue::from_static("text/plain; charset=utf-8"),
    );
    builder
        .body(
            http_body_util::Full::new(hyper::body::Bytes::copy_from_slice(body.as_bytes()))
                .map_err(|e| match e {})
                .boxed(),
        )
        .unwrap()
}

/// Drive an upgraded websocket connection: forwards incoming frames to the
/// Python message channel, executes the connect/close handlers, and applies
/// the commands coming from the Python-visible connector.
async fn ws_session(
    endpoint: String,
    stream: WSStream,
    router: Arc<WebSocketRouter>,
    query_string: String,
) {
    let registry = registry_for(&endpoint);
    let router_map = router.get_web_socket_map().read().clone();

    let (mut sink, mut source) = stream.split();
    let (cmd_tx, mut cmd_rx) = mpsc::unbounded_channel::<WsCommand>();
    let (msg_tx, msg_rx) = mpsc::unbounded_channel::<Option<WsPayload>>();

    let handlers = router_map.get(&endpoint).cloned().unwrap_or_default();

    let id = Uuid::new_v4();
    registry.clients.insert(id, cmd_tx);

    let mut query_params = QueryParams::new();
    if !query_string.is_empty() {
        for pair in query_string.split('&') {
            let (key, value) = pair.split_once('=').unwrap_or((pair, ""));
            query_params.set(key.to_string(), value.to_string());
        }
    }

    let connector = Python::attach(|py| {
        let channel = Py::new(
            py,
            WebSocketChannel {
                receiver: Arc::new(tokio::sync::Mutex::new(msg_rx)),
            },
        )
        .unwrap();
        let connector = WebSocketConnector {
            id,
            router: handlers.clone(),
            registry: Arc::clone(&registry),
            query_params: query_params.clone(),
            message_sender: Some(msg_tx.clone()),
            message_channel: Some(channel),
        };
        Py::new(py, connector).unwrap()
    });

    // `connect` handler (stock: `Actor::started`).
    match handlers.get("connect") {
        Some(function) => {
            if let Some(msg) = execute_ws_function(function, &connector).await {
                let _ = sink.send(Message::Text(msg.into())).await;
            }
        }
        None => log::error!("No 'connect' handler registered for WebSocket"),
    }

    debug!("Actor is alive");

    loop {
        tokio::select! {
            msg = source.next() => {
                match msg {
                    Some(Ok(Message::Text(text))) => {
                        debug!("Text message received {:?}", text);
                        if msg_tx.send(Some(WsPayload::Text(text.to_string()))).is_err() {
                            break;
                        }
                    }
                    Some(Ok(Message::Binary(bin))) => {
                        debug!("Binary message received ({} bytes)", bin.len());
                        // Forward the raw bytes to the Python handler as-is, so
                        // arbitrary (non-UTF-8) binary frames are delivered intact.
                        if msg_tx.send(Some(WsPayload::Binary(bin.to_vec()))).is_err() {
                            break;
                        }
                    }
                    Some(Ok(Message::Ping(payload))) => {
                        debug!("Ping message {:?}", payload);
                        if sink.send(Message::Pong(payload)).await.is_err() {
                            break;
                        }
                    }
                    Some(Ok(Message::Pong(_))) => {
                        debug!("Pong message");
                    }
                    Some(Ok(Message::Close(_))) | None => {
                        debug!("Socket was closed");
                        break;
                    }
                    Some(Ok(_)) => {}
                    Some(Err(err)) => {
                        log::debug!("Websocket client error: {err:?}");
                        break;
                    }
                }
            }
            cmd = cmd_rx.recv() => {
                match cmd {
                    Some(WsCommand::SendText(message)) => {
                        let close = message == "Connection closed";
                        if sink.send(Message::Text(message.into())).await.is_err() {
                            break;
                        }
                        if close {
                            break;
                        }
                    }
                    Some(WsCommand::SendBinary(data)) => {
                        if sink.send(Message::Binary(data.into())).await.is_err() {
                            break;
                        }
                    }
                    None => break,
                }
            }
        }
    }

    // Signal end-of-stream to the Python channel and run the `close` handler
    // (stock: `Actor::stopped`).
    let _ = msg_tx.send(None);

    match handlers.get("close") {
        Some(function) => {
            if let Some(msg) = execute_ws_function(function, &connector).await {
                let _ = sink.send(Message::Text(msg.into())).await;
            }
        }
        None => log::error!("No 'close' handler registered for WebSocket"),
    }

    registry.clients.remove(&id);
    debug!("Actor is dead");

    // pyo3 0.30 requires drops of Python heap pointers to happen on attached
    // threads: dispose of the connector and the handler maps here.
    Python::attach(|_| {
        drop(connector);
        drop(handlers);
        drop(router_map);
    });
}
