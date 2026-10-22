// Vendored from Robyn (https://github.com/sparckles/Robyn) commit 1170748e — BSD 2-Clause (c) 2021 Sanskar Jethi.
// Ported from actix-web to the Aethonan rlib (Aethonan: Fidus Writer fork of Granian).

use crate::executors::{
    execute_after_middleware_function, execute_http_function, execute_middleware_function,
};

use crate::routers::const_router::ConstRouter;
use crate::routers::Router;

use crate::routers::http_router::HttpRouter;
use crate::routers::{middleware_router::MiddlewareRouter, web_socket_router::WebSocketRouter};
use crate::types::cookie::Cookies;
use crate::types::function_info::{FunctionInfo, MiddlewareType};
use crate::types::headers::Headers;
use crate::types::request::Request;
use crate::types::response::{Response, ResponseType};
use crate::types::HttpMethod;
use crate::types::MiddlewareReturn;
use crate::websockets::register_web_socket_route;

use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering::SeqCst;
use std::sync::{Arc, OnceLock, RwLock};

use std::env;

use futures::future::BoxFuture;
use http_body_util::BodyExt;
use log::error;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3_async_runtimes::TaskLocals;

use aethonan::{HTTPRequest, HTTPResponse, NativeContext, NativeRouteKind};

const MAX_PAYLOAD_SIZE: &str = "ROBYN_MAX_PAYLOAD_SIZE";
const DEFAULT_MAX_PAYLOAD_SIZE: usize = 1_000_000; // 1Mb

static STARTED: AtomicBool = AtomicBool::new(false);
static TASK_LOCALS: OnceLock<TaskLocals> = OnceLock::new();

/// The task locals (worker event loop + context) shared by every native
/// handler of this process.
pub fn task_locals() -> Option<&'static TaskLocals> {
    TASK_LOCALS.get()
}

#[derive(Clone)]
struct Directory {
    route: String,
    directory_path: String,
    show_files_listing: bool,
    index_file: Option<String>,
}

/// The subset of the `Server` state the native request handlers need access
/// to: everything behind an `Arc`, cloned at `start()` time and moved into the
/// registered native route closures.
#[derive(Clone)]
struct ServerInner {
    router: Arc<HttpRouter>,
    const_router: Arc<ConstRouter>,
    websocket_router: Arc<WebSocketRouter>,
    middleware_router: Arc<MiddlewareRouter>,
    global_request_headers: Arc<Headers>,
    global_response_headers: Arc<Headers>,
    directories: Arc<RwLock<Vec<Directory>>>,
    excluded_response_headers_paths: Option<Vec<String>>,
}

#[pyclass]
pub struct Server {
    router: Arc<HttpRouter>,
    const_router: Arc<ConstRouter>,
    websocket_router: Arc<WebSocketRouter>,
    middleware_router: Arc<MiddlewareRouter>,
    global_request_headers: Arc<Headers>,
    global_response_headers: Arc<Headers>,
    directories: Arc<RwLock<Vec<Directory>>>,
    startup_handler: Option<Arc<FunctionInfo>>,
    shutdown_handler: Option<Arc<FunctionInfo>>,
    excluded_response_headers_paths: Option<Vec<String>>,
}

#[pymethods]
impl Server {
    #[new]
    pub fn new() -> Self {
        Self {
            router: Arc::new(HttpRouter::new()),
            const_router: Arc::new(ConstRouter::new()),
            websocket_router: Arc::new(WebSocketRouter::new()),
            middleware_router: Arc::new(MiddlewareRouter::new()),
            global_request_headers: Arc::new(Headers::new(None)),
            global_response_headers: Arc::new(Headers::new(None)),
            directories: Arc::new(RwLock::new(Vec::new())),
            startup_handler: None,
            shutdown_handler: None,
            excluded_response_headers_paths: None,
        }
    }

    /// Port of `Server::start`: where stock Robyn booted actix-web here, the
    /// aethonan port registers the native routes (a per-endpoint websocket
    /// route plus a catch-all HTTP route) dispatching into the ported Robyn
    /// pipeline. The actual serving is then handed over to Aethonan by the
    /// Python side (`robyn.processpool`), which owns processes and workers.
    ///
    /// Must be called once per worker process, with that worker's event loop,
    /// before Aethonan's worker starts accepting connections.
    pub fn start(&self, py: Python, event_loop: Bound<'_, PyAny>) -> PyResult<()> {
        if STARTED
            .compare_exchange(false, true, SeqCst, SeqCst)
            .is_err()
        {
            return Ok(());
        }

        let task_locals = TaskLocals::new(event_loop).copy_context(py)?;
        let _ = TASK_LOCALS.get_or_init(|| task_locals);

        if !self.middleware_router.has_any_middleware() {
            self.const_router
                .bake_global_headers(&self.global_response_headers);
        }

        let inner = ServerInner {
            router: Arc::clone(&self.router),
            const_router: Arc::clone(&self.const_router),
            websocket_router: Arc::clone(&self.websocket_router),
            middleware_router: Arc::clone(&self.middleware_router),
            global_request_headers: Arc::clone(&self.global_request_headers),
            global_response_headers: Arc::clone(&self.global_response_headers),
            directories: Arc::clone(&self.directories),
            excluded_response_headers_paths: self.excluded_response_headers_paths.clone(),
        };

        // One native websocket route per registered websocket endpoint. They
        // are more specific prefixes than the catch-all below, hence win the
        // router's longest-prefix matching; their kind also tells Aethonan to
        // enable connection upgrades.
        let endpoints: Vec<String> = inner
            .websocket_router
            .get_web_socket_map()
            .read()
            .keys()
            .cloned()
            .collect();
        for endpoint in endpoints {
            register_web_socket_route(&endpoint, &inner.websocket_router);
        }

        // The catch-all: every non-websocket request is dispatched into the
        // ported Robyn pipeline without ever reaching a Python protocol app.
        aethonan::register_native_route(
            "/",
            NativeRouteKind::Http,
            move |req: HTTPRequest, ctx: NativeContext| -> BoxFuture<'static, HTTPResponse> {
                let inner = inner.clone();
                Box::pin(async move { native_dispatch(inner, req, ctx).await })
            },
        );

        Ok(())
    }

    /// Run the registered shutdown handler (sync or async) on the current
    /// thread's loop. Called by the Python worker glue after Aethonan's serve
    /// loop has been interrupted, mirroring stock Robyn's shutdown behavior.
    pub fn run_shutdown_handler(&self, py: Python) -> PyResult<()> {
        let Some(function) = self.shutdown_handler.clone() else {
            return Ok(());
        };
        if function.is_async {
            let Some(task_locals) = TASK_LOCALS.get() else {
                return Err(PyValueError::new_err(
                    "shutdown handler requires the worker event loop",
                ));
            };
            let fut = pyo3_async_runtimes::into_future_with_locals(
                task_locals,
                function.handler.bind(py).call0()?,
            )?;
            pyo3_async_runtimes::tokio::get_runtime().block_on(fut)?;
            Ok(())
        } else {
            function.handler.call0(py)?;
            Ok(())
        }
    }

    pub fn add_directory(
        &mut self,
        route: String,
        directory_path: String,
        show_files_listing: bool,
        index_file: Option<String>,
    ) {
        self.directories.write().unwrap().push(Directory {
            route,
            directory_path,
            show_files_listing,
            index_file,
        });
    }

    /// Removes a new request header to our concurrent hashmap
    /// this can be called after the server has started.
    pub fn remove_header(&self, key: &str) {
        self.global_request_headers.headers.remove(key);
    }

    /// Removes a new response header to our concurrent hashmap
    /// this can be called after the server has started.
    pub fn remove_response_header(&self, key: &str) {
        self.global_response_headers.headers.remove(key);
    }

    pub fn apply_request_headers(&mut self, headers: &Headers) {
        self.global_request_headers = Arc::new(headers.clone());
    }

    pub fn apply_response_headers(&mut self, headers: &Headers) {
        self.global_response_headers = Arc::new(headers.clone());
    }

    pub fn set_response_headers_exclude_paths(
        &mut self,
        excluded_response_headers_paths: Option<Vec<String>>,
    ) {
        self.excluded_response_headers_paths = excluded_response_headers_paths;
    }

    /// Add a new route to the routing tables
    /// can be called after the server has been started
    pub fn add_route(
        &self,
        py: Python,
        route_type: &HttpMethod,
        route: &str,
        function: PyRef<FunctionInfo>,
        is_const: bool,
    ) {
        self._add_route(py, route_type, route, &function, is_const);
    }

    fn _add_route(
        &self,
        py: Python,
        route_type: &HttpMethod,
        route: &str,
        function: &FunctionInfo,
        is_const: bool,
    ) {
        let asyncio = py.import("asyncio").unwrap();
        let event_loop = asyncio.call_method0("get_event_loop").unwrap();

        if is_const {
            match self.const_router.add_route(
                py,
                route_type,
                route,
                function,
                Some(event_loop),
            ) {
                Ok(_) => (),
                Err(e) => {
                    log::debug!("Error adding const route {}", e);
                }
            }
        } else {
            match self
                .router
                .add_route(py, route_type, route, function, None)
            {
                Ok(_) => (),
                Err(e) => {
                    log::debug!("Error adding route {}", e);
                }
            }
        }
    }

    /// Add a new global middleware
    /// can be called after the server has been started
    pub fn add_global_middleware(
        &self,
        middleware_type: &MiddlewareType,
        function: PyRef<FunctionInfo>,
    ) {
        self.middleware_router
            .add_global_middleware(middleware_type, &function)
            .unwrap();
    }

    /// Add a new route to the routing tables
    /// can be called after the server has been started
    pub fn add_middleware_route(
        &self,
        middleware_type: &MiddlewareType,
        route: &str,
        function: PyRef<FunctionInfo>,
        http_method: PyRef<HttpMethod>,
    ) {
        let mut endpoint_prefixed_with_method = http_method.to_string();

        if !route.starts_with('/') {
            endpoint_prefixed_with_method.push('/');
        }

        endpoint_prefixed_with_method.push_str(route);

        Python::attach(|py| {
            self.middleware_router
                .add_route(
                    py,
                    middleware_type,
                    &endpoint_prefixed_with_method,
                    &function,
                    None,
                )
                .unwrap()
        });
    }

    /// Add a new web socket route to the routing tables
    /// can be called after the server has been started
    pub fn add_web_socket_route(
        &mut self,
        route: &str,
        connect_route: PyRef<FunctionInfo>,
        close_route: PyRef<FunctionInfo>,
        message_route: PyRef<FunctionInfo>,
    ) {
        self.websocket_router
            .add_websocket_route(route, &connect_route, &close_route, &message_route);
    }

    /// Add a new startup handler
    pub fn add_startup_handler(&mut self, function: PyRef<FunctionInfo>) {
        self.startup_handler = Some(Arc::new(function.clone()));
    }

    /// Add a new shutdown handler
    pub fn add_shutdown_handler(&mut self, function: PyRef<FunctionInfo>) {
        self.shutdown_handler = Some(Arc::new(function.clone()));
    }
}

impl Default for Server {
    fn default() -> Self {
        Self::new()
    }
}

fn max_payload_size() -> usize {
    env::var(MAX_PAYLOAD_SIZE)
        .unwrap_or(DEFAULT_MAX_PAYLOAD_SIZE.to_string())
        .trim()
        .parse::<usize>()
        .unwrap_or(DEFAULT_MAX_PAYLOAD_SIZE)
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

/// The native route handler: the aethonan counterpart of actix-web's
/// `default_service`, dispatching every non-websocket request into the ported
/// Robyn pipeline.
async fn native_dispatch(inner: ServerInner, req: HTTPRequest, ctx: NativeContext) -> HTTPResponse {
    // Connection upgrades only happen on the dedicated websocket routes; an
    // upgrade request reaching the catch-all matches no websocket endpoint.
    if aethonan::ws::is_upgrade_request(&req) {
        return text_response(400, "Bad request");
    }

    let (parts, incoming) = req.into_parts();

    // Static directories: like the actix `Files` services, this only answers
    // GET/HEAD, so OPTIONS and other methods (and CORS preflights, #1130) fall
    // through to the router below.
    if parts.method == hyper::Method::GET || parts.method == hyper::Method::HEAD {
        if let Some(response) = serve_directory(&inner, &parts).await {
            return response;
        }

        // Fast path: const routes bypass request parsing, Python, and middleware
        // Only safe when no middlewares are registered (checked dynamically via AtomicBool)
        if !inner.middleware_router.has_any_middleware() {
            if let Ok(http_method) = HttpMethod::from_hyper_method(&parts.method) {
                let path = parts.uri.path();
                if let Some(cached) = inner.const_router.get_cached_route(&http_method, path) {
                    if let Some(ref excluded) = inner.excluded_response_headers_paths {
                        if excluded.contains(&path.to_owned()) {
                            return cached.to_http_response_without_global_headers();
                        }
                    }
                    return cached.to_http_response();
                }
            }
        }
    }

    // Normal path: dynamic routes (and const routes when middlewares exist) require Python
    let limit = max_payload_size();
    let collected = match incoming
        .collect()
        .await
        .map(|collected| collected.to_bytes().to_vec())
    {
        Ok(body) if body.len() <= limit => body,
        _ => {
            return text_response(413, "Payload too large");
        }
    };

    let Some(task_locals) = TASK_LOCALS.get() else {
        return text_response(500, "Internal server error");
    };

    let scoped_locals = task_locals.clone();
    let response =
        pyo3_async_runtimes::tokio::scope(scoped_locals.clone(), index(inner, parts, collected, ctx))
            .await;
    // pyo3 0.30 requires drops of Python heap pointers to happen on attached
    // threads: dispose of the task locals held by this future explicitly.
    Python::attach(|_| drop(scoped_locals));

    response.to_hyper_response()
}

/// Serve a request from a configured static directory. Returns `None` when the
/// request does not match any directory (or must fall through to the router,
/// mirroring the actix guards in stock Robyn).
async fn serve_directory(
    inner: &ServerInner,
    parts: &hyper::http::request::Parts,
) -> Option<HTTPResponse> {
    let path = parts.uri.path();
    let directories = inner.directories.read().unwrap();

    for directory in directories.iter() {
        let rel = if path == directory.route {
            ""
        } else if let Some(rest) = path.strip_prefix(directory.route.as_str()) {
            if rest.is_empty() || !rest.starts_with('/') {
                continue;
            }
            rest.trim_start_matches('/')
        } else {
            continue;
        };

        let full_path = std::path::Path::new(&directory.directory_path).join(rel);
        let metadata = std::fs::metadata(&full_path).ok();

        let is_file_request = metadata.as_ref().is_some_and(|md| md.is_file());

        // Stock behavior: without an index file or a listing, the actix guard
        // only lets regular files through; anything else falls to the router.
        if !is_file_request
            && directory.index_file.is_none()
            && !directory.show_files_listing
        {
            continue;
        }

        if is_file_request {
            return Some(file_response(&full_path, parts.method == hyper::Method::HEAD));
        }

        // Directory request: redirect to the slash-terminated path (like
        // actix `redirect_to_slash_directory`), then serve the index or a
        // listing.
        if directory.index_file.is_some() && !path.ends_with('/') {
            let location = format!("{}/", path);
            let mut builder = hyper::Response::builder().status(302);
            builder.headers_mut().unwrap().insert(
                hyper::header::LOCATION,
                hyper::http::HeaderValue::from_str(&location).unwrap(),
            );
            return Some(builder.body(aethonan::http::empty_body()).unwrap());
        }

        if let Some(index_file) = &directory.index_file {
            let index_path = full_path.join(index_file);
            if index_path.is_file() {
                return Some(file_response(&index_path, parts.method == hyper::Method::HEAD));
            }
            return Some(text_response(404, "Not found"));
        }

        if directory.show_files_listing {
            return Some(listing_response(&full_path, path));
        }
    }

    None
}

fn file_response(path: &std::path::Path, head_only: bool) -> HTTPResponse {
    match crate::io_helpers::read_file(&path.to_string_lossy()) {
        Ok(content) => {
            let mut builder = hyper::Response::builder().status(200);
            {
                let headers = builder.headers_mut().unwrap();
                if let Some(mime) = mime_guess::from_path(path).first() {
                    if let Ok(value) = hyper::http::HeaderValue::from_str(mime.as_ref()) {
                        headers.insert(hyper::header::CONTENT_TYPE, value);
                    }
                }
                headers.insert(
                    hyper::header::CONTENT_LENGTH,
                    hyper::http::HeaderValue::from_str(&content.len().to_string()).unwrap(),
                );
            }
            let body = if head_only { Vec::new() } else { content };
            builder
                .body(
                    http_body_util::Full::new(hyper::body::Bytes::from(body))
                        .map_err(|e| match e {})
                        .boxed(),
                )
                .unwrap()
        }
        Err(_) => text_response(404, "Not found"),
    }
}

fn listing_response(path: &std::path::Path, request_path: &str) -> HTTPResponse {
    let mut body = String::from("<html><body>");
    if let Ok(entries) = std::fs::read_dir(path) {
        let mut entries: Vec<_> = entries.flatten().collect();
        entries.sort_by_key(|e| e.file_name());
        for entry in entries {
            let name = entry.file_name().to_string_lossy().to_string();
            body.push_str(&format!(
                "<p><a href=\"{}/{}\">{}</a></p>",
                request_path.trim_end_matches('/'),
                name,
                name
            ));
        }
    }
    body.push_str("</body></html>");
    text_response(200, &body)
}

/// Owner of the Python heap pointers bound to a request lifecycle (the shared
/// contextvars context and the session-carrying request). Dropping it on an
/// attached thread keeps pyo3 0.30's strict drop checker (reference pool
/// disabled) satisfied on every exit path of the pipeline.
struct RequestLifecycle {
    context: Option<Py<PyAny>>,
    request: Option<Request>,
}

impl Drop for RequestLifecycle {
    fn drop(&mut self) {
        Python::attach(|_| {
            drop(self.context.take());
            drop(self.request.take());
        });
    }
}

/// Port of actix's `default_service` handler: the full Robyn request pipeline.
async fn index(
    inner: ServerInner,
    parts: hyper::http::request::Parts,
    body: Vec<u8>,
    ctx: NativeContext,
) -> ResponseType {
    let ServerInner {
        router,
        const_router,
        middleware_router,
        global_request_headers: _,
        global_response_headers,
        excluded_response_headers_paths,
        ..
    } = inner;

    if !HttpMethod::is_supported(&parts.method) {
        return ResponseType::Standard(Response::method_not_allowed(None));
    }

    let http_method = match HttpMethod::from_hyper_method(&parts.method) {
        Ok(method) => method,
        Err(_) => return ResponseType::Standard(Response::method_not_allowed(None)),
    };

    let scheme = if ctx.secure { "https" } else { "http" };
    let host = parts
        .headers
        .get(hyper::header::HOST)
        .and_then(|value| value.to_str().ok())
        .map(|value| value.to_string())
        .unwrap_or_else(|| ctx.addr_local.to_string());
    let ip_addr = Some(ctx.addr_remote.ip().to_string());

    // Stock-parity note: in stock Robyn 0.88.0 the actix `web::Data<Arc<Headers>>`
    // injections for the global request and response headers collide on the same
    // type, so the request is actually built extending the request headers with
    // the *global response headers*. The integration tests encode that behavior
    // (handlers observe `server: robyn` on the request), so the port reproduces
    // it deliberately.
    let request: Request = match Request::from_hyper_request(
        &parts,
        body,
        &global_response_headers,
        scheme,
        &host,
        ip_addr,
    )
    .await
    {
        Ok(r) => r,
        Err(e) => {
            error!("Failed to parse request for `{}`: {}", parts.uri.path(), e);
            return ResponseType::Standard(Response::internal_server_error(None));
        }
    };

    let route = format!("{}{}", parts.method, request.url.path);

    // Allocate a single `contextvars.Context` for the full request lifecycle so
    // that writes made by a `before_request` hook are visible to the route
    // handler and to `after_request` hooks. asyncio copies the current context
    // for each task it creates, so without this shared object each phase would
    // see its own isolated snapshot (see issue #1380).
    //
    // This also provides per-request ContextVar isolation for sync handlers
    // (which run via `ctx.run(...)`): without a fresh context, a ContextVar
    // written inside a handler would persist in the worker thread's current
    // context and leak into the next request on that thread.
    let request_context: Py<PyAny> = match Python::attach(crate::asyncio::new_context) {
        Ok(ctx) => ctx,
        Err(e) => {
            let msg = get_traceback(e);
            error!("Failed to create request contextvars context: {}", msg);
            return ResponseType::Standard(Response::internal_server_error(None));
        }
    };
    let mut lifecycle = RequestLifecycle {
        context: Some(request_context),
        request: Some(request),
    };
    let ctx_ref = lifecycle.context.as_ref();
    let request = lifecycle.request.as_mut().expect("request is set");

    // Before middleware
    let before_middlewares =
        middleware_router.get_global_middlewares(&MiddlewareType::BeforeRequest);
    let route_before = middleware_router.get_route(&MiddlewareType::BeforeRequest, &route);

    let mut early_response: Option<Response> = None;
    if !before_middlewares.is_empty() || route_before.is_some() {
        let mut all_before = before_middlewares;
        if let Some((functions, route_params)) = route_before {
            all_before.extend(functions);
            request.path_params = route_params;
        }
        let mut all_before = all_before.into_iter();
        while let Some(before_middleware) = all_before.next() {
            let middleware_result =
                execute_middleware_function(&*request, &before_middleware, ctx_ref).await;
            // pyo3 0.30 (with the reference pool disabled) requires drops of
            // Python heap pointers on attached threads.
            Python::attach(|_| drop(before_middleware));
            match middleware_result {
                Ok(MiddlewareReturn::Request(r)) => {
                    let old = std::mem::replace(&mut *request, r);
                    // Dispose of the replaced request (which may carry a
                    // session object) on an attached thread.
                    Python::attach(|_| drop(old));
                }
                Ok(MiddlewareReturn::Response(r)) => {
                    early_response = Some(r);
                    break;
                }
                Err(e) => {
                    let msg = executor_error_message(e);
                    error!(
                        "Error executing before middleware for `{}`: {}",
                        request.url.path, msg
                    );
                    let fallback = ResponseType::Standard(Response::internal_server_error(None));
                    Python::attach(|_| drop(all_before));
                    return fallback;
                }
            };
        }
        Python::attach(|_| drop(all_before));
    }

    let mut response = if let Some(r) = early_response {
        ResponseType::Standard(r)
    } else if let Some(cached) = const_router.get_cached_route(&http_method, &request.url.path) {
        let mut resp = Response {
            status_code: cached.status.as_u16(),
            response_type: "text".to_string(),
            headers: Headers::new(None),
            description: cached.body.to_vec(),
            file_path: None,
            cookies: Cookies::new(),
        };
        for (k, v) in cached.headers.as_ref() {
            resp.headers.set(k.clone(), v.clone());
        }
        ResponseType::Standard(resp)
    } else if let Some((function, route_params)) = router.get_route(&http_method, &request.url.path)
    {
        request.path_params = route_params;
        let handler_result = execute_http_function(&*request, &function, ctx_ref).await;
        Python::attach(|_| drop(function));
        match handler_result {
            Ok(r) => r,
            Err(e) => {
                let msg = get_traceback(e);
                error!(
                    "Error executing route function for `{}`: {}",
                    request.url.path, msg
                );
                ResponseType::Standard(Response::internal_server_error(None))
            }
        }
    } else {
        ResponseType::Standard(Response::not_found(None))
    };

    let is_excluded = excluded_response_headers_paths
        .as_ref()
        .is_some_and(|paths| paths.contains(&request.url.path));

    if !is_excluded {
        response.headers_mut().set_missing(&global_response_headers);
    }

    // After middleware
    let after_middlewares =
        middleware_router.get_global_middlewares(&MiddlewareType::AfterRequest);
    let route_after = middleware_router.get_route(&MiddlewareType::AfterRequest, &route);

    if !after_middlewares.is_empty() || route_after.is_some() {
        let mut all_after = after_middlewares;
        if let Some((functions, _)) = route_after {
            all_after.extend(functions);
        }
        let mut all_after = all_after.into_iter();
        while let Some(after_middleware) = all_after.next() {
            if let ResponseType::Standard(std_response) = response {
                let middleware_result = execute_after_middleware_function(
                    &*request,
                    &std_response,
                    &after_middleware,
                    ctx_ref,
                )
                .await;
                // pyo3 0.30 (with the reference pool disabled) requires drops
                // of Python heap pointers on attached threads.
                Python::attach(|_| drop(after_middleware));
                response = match middleware_result {
                    Ok(MiddlewareReturn::Request(_)) => {
                        error!("After middleware returned a request");
                        Python::attach(|_| drop(all_after));
                        return ResponseType::Standard(Response::internal_server_error(None));
                    }
                    Ok(MiddlewareReturn::Response(r)) => ResponseType::Standard(r),
                    Err(e) => {
                        let msg = executor_error_message(e);
                        error!(
                            "Error executing after middleware for `{}`: {}",
                            request.url.path, msg
                        );
                        let fallback =
                            ResponseType::Standard(Response::internal_server_error(Some(
                                &std_response.headers,
                            )));
                        Python::attach(|_| drop(all_after));
                        return fallback;
                    }
                };
            } else {
                // Streaming responses skip after-middleware (stock
                // behavior), but the middleware handle must still be
                // disposed of on an attached thread.
                Python::attach(|_| drop(after_middleware));
            }
        }
        Python::attach(|_| drop(all_after));
    }

    response
}

fn get_traceback(error: PyErr) -> String {
    Python::attach(|py| -> String {
        let msg = if let Some(traceback) = error.traceback(py) {
            match traceback.format() {
                Ok(msg) => format!("\n{msg} {error}"),
                Err(e) => e.to_string(),
            }
        } else {
            error.value(py).to_string()
        };
        // Dispose of the error on this attached thread.
        drop(error);
        msg
    })
}

/// Format an error returned by the executor layer for logging, consuming it:
/// a `PyErr` becomes a traceback; anything else is debug-formatted. Disposal
/// happens on an attached thread (see `get_traceback`).
fn executor_error_message(error: anyhow::Error) -> String {
    match error.downcast::<PyErr>() {
        Ok(py_err) => get_traceback(py_err),
        Err(other) => {
            let msg = format!("{other:?}");
            Python::attach(|_| drop(other));
            msg
        }
    }
}
