# Vendored from Robyn (https://github.com/sparckles/Robyn) commit 1170748e — BSD 2-Clause (c) 2021 Sanskar Jethi.
# Ported from actix-web to the Aethonan rlib (Aethonan: Fidus Writer fork of Granian).
#
# In stock Robyn this module owns the process orchestration: it creates a
# shared socket, spawns `processes` x `workers` via multiprocess and runs an
# actix server in each process. In the aethonan port Aethonan owns processes
# and workers, so this module registers everything with the native (ported)
# `Server` and then hands over to Aethonan's own server classes. Each Aethonan
# worker process replays the registration into the native Server, registers
# the catch-all native route and serves the Robyn pipeline natively.

import asyncio
import multiprocessing
import webbrowser

from robyn.events import Events
from robyn.logger import logger
from robyn.robyn import FunctionInfo, Headers, Server
from robyn.router import GlobalMiddleware, Route, RouteMiddleware
from robyn.types import Directory

# The route/middleware configuration, set by `run_processes` in the parent and
# inherited by every Aethonan worker process through the fork.
_server_config = None


def _build_server(
    directories: list[Directory],
    request_headers: Headers,
    routes: list[Route],
    global_middlewares: list[GlobalMiddleware],
    route_middlewares: list[RouteMiddleware],
    web_sockets: dict[str, dict],
    event_handlers: dict[Events, FunctionInfo],
    response_headers: Headers,
    excluded_response_headers_paths: list[str] | None,
) -> Server:
    server = Server()

    if Events.STARTUP in event_handlers:
        server.add_startup_handler(event_handlers[Events.STARTUP])

    if Events.SHUTDOWN in event_handlers:
        server.add_shutdown_handler(event_handlers[Events.SHUTDOWN])

    for directory in directories:
        server.add_directory(*directory.as_list())

    server.apply_request_headers(request_headers)
    server.apply_response_headers(response_headers)
    server.set_response_headers_exclude_paths(excluded_response_headers_paths)

    for route in routes:
        server.add_route(route.route_type, route.route, route.function, route.is_const)

    for middleware_type, middleware_function in global_middlewares:
        server.add_global_middleware(middleware_type, middleware_function)

    for middleware_type, endpoint, function, route_type in route_middlewares:
        server.add_middleware_route(middleware_type, endpoint, function, route_type)

    for endpoint in web_sockets:
        web_socket = web_sockets[endpoint]
        server.add_web_socket_route(
            endpoint,
            web_socket["connect"],
            web_socket["close"],
            web_socket["message"],
        )

    return server


def _run_event_handlers(server: Server, event_handlers: dict[Events, FunctionInfo], loop, event: Events) -> None:
    if event not in event_handlers:
        return
    handler = event_handlers[event]
    if handler.is_async:
        loop.run_until_complete(handler.handler())
    else:
        handler.handler()


def _load_target():
    # Aethonan requires a Python RSGI target, but every request is served by
    # the catch-all native route registered by `Server.start()`: the target is
    # never invoked. It exists only to satisfy the worker bootstrap.
    from robyn._aethonan_target import app

    return app


def _spawn_rsgi_worker(
    worker_id,
    callback,
    sock,
    ipc,
    loop,
    runtime_mode,
    runtime_threads,
    runtime_blocking_threads,
    blocking_threads,
    blocking_threads_idle_timeout,
    backpressure,
    task_impl,
    http_mode,
    http1_settings,
    http2_settings,
    websockets,
    static_path,
    log_access_fmt,
    ssl_ctx,
    scope_opts,
    metrics,
):
    """The Aethonan worker target (mirror of aethonan.server.mp's rsgi worker,
    plus the Robyn glue: native route registration and lifecycle handlers)."""
    from aethonan._aethonan import RSGIWorker
    from aethonan._futures import _future_watcher_wrapper, _new_cbscheduler
    from aethonan._signals import set_loop_signals
    from aethonan.constants import TaskImpl
    from aethonan.rsgi import _callback_wrapper as _rsgi_call_wrap

    from aethonan.server.common import WORKERS_METHODS

    config = _server_config
    assert config is not None, "robyn worker started without a server configuration"

    (directories, request_headers, routes, global_middlewares, route_middlewares, web_sockets, event_handlers, response_headers, excluded_response_headers_paths) = config

    server = _build_server(
        directories,
        request_headers,
        routes,
        global_middlewares,
        route_middlewares,
        web_sockets,
        event_handlers,
        response_headers,
        excluded_response_headers_paths,
    )

    # Register the native routes (catch-all + websocket endpoints) with this
    # worker's event loop, and bake const routes/global headers.
    server.start(loop)

    # Startup handlers run per worker process, like stock Robyn's spawn_process.
    _run_event_handlers(server, event_handlers, loop, Events.STARTUP)

    wcallback = _future_watcher_wrapper(_rsgi_call_wrap(callback, log_access_fmt))
    shutdown_event = set_loop_signals(loop)
    evp = asyncio.Event()

    async def _main():
        await evp.wait()

    def shutdown_glue():
        def _inner():
            evp.set()

        loop.call_soon_threadsafe(_inner)

    shutdown_event.add_cb(shutdown_glue)

    worker = RSGIWorker(
        worker_id,
        sock,
        ipc,
        runtime_threads,
        runtime_blocking_threads,
        blocking_threads,
        blocking_threads_idle_timeout,
        backpressure,
        http_mode,
        http1_settings,
        http2_settings,
        websockets,
        static_path,
        *ssl_ctx,
        metrics,
    )
    serve = getattr(worker, WORKERS_METHODS[runtime_mode][(sock[0] or sock[1]).is_uds()])
    scheduler = _new_cbscheduler(loop, wcallback, impl_asyncio=task_impl == TaskImpl.asyncio)
    serve(scheduler, loop, shutdown_event)
    loop.run_until_complete(_main())

    # Shutdown handlers run per worker process once the serve loop is
    # interrupted (graceful SIGINT/SIGTERM), like stock Robyn.
    server.run_shutdown_handler()


def run_processes(
    url: str,
    port: int,
    directories: list[Directory],
    request_headers: Headers,
    routes: list[Route],
    global_middlewares: list[GlobalMiddleware],
    route_middlewares: list[RouteMiddleware],
    web_sockets: dict[str, dict],
    event_handlers: dict[Events, FunctionInfo],
    workers: int,
    processes: int,
    response_headers: Headers,
    excluded_response_headers_paths: list[str] | None,
    open_browser: bool,
    client_timeout: int = 30,
    keep_alive_timeout: int = 20,
) -> None:
    """Hand over to Aethonan: it owns the processes (robyn `processes` map to
    aethonan workers, robyn `workers` map to aethonan runtime threads) and
    serves every request through the native route registered by Server.start().
    """
    global _server_config
    _server_config = (
        directories,
        request_headers,
        routes,
        global_middlewares,
        route_middlewares,
        web_sockets,
        event_handlers,
        response_headers,
        excluded_response_headers_paths,
    )

    # Aethonan's MPServer derives the multiprocessing start method from the
    # global default (spawn/forkserver on modern Python, which cannot carry the
    # registered routes into the workers); the port requires fork, which hands
    # the whole process state (routes, middlewares, websocket handlers) to each
    # worker without pickling.
    multiprocessing.set_start_method("fork", force=True)

    if open_browser:
        logger.info("Opening browser...")
        webbrowser.open_new_tab(f"http://{url}:{port}/")

    from aethonan import Aethonan
    from aethonan.server.mp import WorkerProcess

    logger.info("Press Ctrl + C to stop \n")

    server = Aethonan(
        target="robyn._aethonan_target:app",
        address=url,
        port=port,
        interface="rsgi",
        workers=max(1, processes),
        runtime_threads=max(1, workers),
        runtime_mode="mt" if workers > 1 else "st",
        websockets=True,
        loop="auto",
    )
    server.serve(
        spawn_target=WorkerProcess.wrap_target(_spawn_rsgi_worker),
        target_loader=_load_target,
        wrap_loader=False,
    )
