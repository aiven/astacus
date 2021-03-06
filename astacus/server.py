"""
Copyright (c) 2020 Aiven Ltd
See LICENSE for details

It is responsible for setting up the FastAPI app, with the sub-routers
mapped ( coordinator + node) and configured (by loading configuration
entries from both JSON file, as well as accepting configuration
entries from command line (later part TBD).

Note that 'app' may be initialized based on ASTACUS_CONFIG and SENTRY_DSN
options, or within main() which handles parameters. While not super elegant,
it allows for nice in-place-reloading.

"""

from astacus import config
from astacus.coordinator.api import router as coordinator_router
from astacus.coordinator.state import app_coordinator_state
from astacus.node.api import router as node_router
from fastapi import FastAPI
from sentry_sdk.integrations.asgi import SentryAsgiMiddleware

import logging
import os
import sentry_sdk
import subprocess
import uvicorn  # type:ignore

logger = logging.getLogger(__name__)


def init_app():
    """Initialize the FastAPI app

    Note that we return both the wrapped one, as well as 'astacus'
    app.  The wrapped one may not provide access to e.g. app.state,
    which may be needed by something outside this function.
    """
    config_path = os.environ.get("ASTACUS_CONFIG")
    if not config_path:
        return None, None
    api = FastAPI()
    api.include_router(coordinator_router, tags=["coordinator"])
    api.include_router(node_router, prefix="/node", tags=["node"])

    @api.on_event("shutdown")
    async def _shutdown_event():
        state = await app_coordinator_state(app=app)
        state.shutting_down = True

    gconfig = config.set_global_config_from_path(api, config_path)
    sentry_dsn = os.environ.get("SENTRY_DSN", gconfig.sentry_dsn)
    sentry_api = api
    if sentry_dsn:
        sentry_sdk.init(dsn=sentry_dsn)
        sentry_api = SentryAsgiMiddleware(api)
    return sentry_api, api


app, _ = init_app()


def _systemd_notify_ready():
    if not os.environ.get("NOTIFY_SOCKET"):
        return
    try:
        from systemd import daemon  # pylint: disable=no-name-in-module,disable=import-outside-toplevel
        daemon.notify("READY=1")
    except ImportError:
        logger.warning("Running under systemd but python-systemd not available, attempting systemd notify via utility")
        subprocess.run(["systemd-notify", "--ready"], check=True)


def _run_server(args):
    # On reload (and following init_app), the app is configured based on this
    os.environ["ASTACUS_CONFIG"] = args.config

    global app  # pylint: disable=global-statement
    app, app_nosentry = init_app()
    uconfig = app_nosentry.state.global_config.uvicorn
    _systemd_notify_ready()
    uvicorn.run(
        "astacus.server:app",
        host=uconfig.host,
        port=uconfig.port,
        reload=uconfig.reload,
        log_level=uconfig.log_level,
        http=uconfig.http
    )


def create_server_parser(subparsers):
    # TBD: Add overrides for configuration file entries that may be
    # relevant to update in more human-friendly way
    server = subparsers.add_parser("server", help="Run REST server")
    server.add_argument(
        "-c",
        "--config",
        type=str,
        help="YAML configuration file to use",
        required=True,
        default=os.environ.get("ASTACUS_CONFIG")
    )
    server.set_defaults(func=_run_server)
