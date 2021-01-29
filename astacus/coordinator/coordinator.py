"""
Copyright (c) 2020 Aiven Ltd
See LICENSE for details
"""

from .config import coordinator_config, CoordinatorConfig
from .state import coordinator_state, CoordinatorState
from astacus.common import asyncstorage, exceptions, ipc, magic, op, statsd, utils
from astacus.common.cachingjsonstorage import MultiCachingJsonStorage
from astacus.common.magic import LockCall
from astacus.common.progress import Progress
from astacus.common.rohmustorage import MultiRohmuStorage
from astacus.common.storage import HexDigestStorage, JsonStorage, MultiFileStorage, MultiStorage
from datetime import datetime
from enum import Enum
from fastapi import BackgroundTasks, Depends, HTTPException, Request
from typing import Dict, List, Optional
from urllib.parse import urlunsplit

import asyncio
import json
import logging
import socket
import threading
import time

logger = logging.getLogger(__name__)


class LockResult(Enum):
    ok = "ok"
    failure = "failure"
    exception = "exception"


class CoordinatorOp(op.Op):
    attempt = -1  # try_run iteration number
    attempt_start: Optional[datetime] = None  # try_run iteration start time
    node_progress: Dict[str, Progress]

    def __init__(self, *, c: "Coordinator"):
        super().__init__(info=c.state.op_info)
        self.nodes = c.config.nodes
        self.request_url = c.request.url
        self.config = c.config
        self.state = c.state
        self.hexdigest_mstorage = c.hexdigest_mstorage
        self.json_mstorage = c.json_mstorage
        self.set_storage_name(self.default_storage_name)
        self.subresult_received_event = asyncio.Event()
        self.progress = Progress()

    @property
    def subresult_url(self):
        url = self.request_url
        parts = [url.scheme, url.netloc, f"{url.path}/{self.op_id}/sub-result", "", ""]
        return urlunsplit(parts)

    hexdigest_storage: Optional[HexDigestStorage] = None
    json_storage: Optional[JsonStorage] = None

    def set_storage_name(self, storage_name):
        self.hexdigest_storage = asyncstorage.AsyncHexDigestStorage(self.hexdigest_mstorage.get_storage(storage_name))
        self.json_storage = asyncstorage.AsyncJsonStorage(self.json_mstorage.get_storage(storage_name))

    @property
    def default_storage_name(self):
        return self.json_mstorage.get_default_storage_name()

    async def request_from_nodes(self, url, *, caller, req=None, nodes=None, **kw):
        if nodes is None:
            nodes = self.nodes
        if req is not None:
            assert isinstance(req, ipc.NodeRequest)
            req.result_url = self.subresult_url
            kw["data"] = req.json()
        urls = [f"{node.url}/{url}" for node in nodes]
        aws = [utils.httpx_request(url, caller=caller, **kw) for url in urls]
        results = await asyncio.gather(*aws, return_exceptions=True)
        logger.info("request_from_nodes %r => %r", urls, results)
        return results

    async def request_lock_call_from_nodes(self, *, call: LockCall, locker: str, ttl: int = 0, nodes=None) -> LockResult:
        if nodes is None:
            nodes = self.nodes
        results = await self.request_from_nodes(
            f"{call}?locker={locker}&ttl={ttl}",
            method="post",
            ignore_status_code=True,
            json=False,
            nodes=nodes,
            caller="CoordinatorOp.request_lock_op_from_nodes"
        )
        logger.debug("%s results: %r", call, results)
        if call in [LockCall.lock, LockCall.relock]:
            expected_result = {"locked": True}
        elif call in [LockCall.unlock]:
            expected_result = {"locked": False}
        else:
            raise NotImplementedError(f"Unknown lock call: {call!r}")
        rv = LockResult.ok
        for node, result in zip(nodes, results):
            if result is None or isinstance(result, Exception):
                logger.info("Exception occurred when talking with node %r: %r", node, result)
                if rv != LockResult.failure:
                    # failures mean that we're done, so don't override them
                    rv = LockResult.exception
            elif result.is_error:
                logger.info("%s of %s failed - unexpected result %r %r", call, node, result.status_code, result)
                rv = LockResult.failure
            else:
                try:
                    decoded_result = result.json()
                except json.JSONDecodeError:
                    decoded_result = None
                if decoded_result != expected_result:
                    logger.info("%s of %s failed - unexpected result %r", call, node, decoded_result)
                    rv = LockResult.failure
        if rv == LockResult.failure and self.stats is not None:
            self.stats.increase("astacus_lock_call_failure", tags={
                "call": call,
                "locker": locker,
            })
        return rv

    async def request_lock_from_nodes(self, *, locker: str, ttl: int) -> bool:
        return await self.request_lock_call_from_nodes(call=LockCall.lock, locker=locker, ttl=ttl) == LockResult.ok

    async def request_unlock_from_nodes(self, *, locker: str) -> bool:
        return await self.request_lock_call_from_nodes(call=LockCall.unlock, locker=locker) == LockResult.ok

    async def run_attempts(self, attempts):
        name = self.__class__.__name__
        try:
            for attempt in range(1, attempts + 1):
                logger.debug("%s - attempt #%d/%d", name, attempt, attempts)
                self.attempt = attempt
                self.attempt_start = utils.now()
                async with self.stats.async_timing_manager(
                    "astacus_attempt_duration", {
                        "op": name,
                        "attempt": str(attempt)
                    }
                ):
                    try:
                        if await self.try_run():
                            return
                    except exceptions.TransientException as ex:
                        logger.info("%s - trasient failure: %r", name, ex)
        except exceptions.PermanentException as ex:
            logger.info("%s - permanent failure: %r", name, ex)
        self.set_status_fail()

    async def wait_successful_results(
        self, start_results, *, result_class, all_nodes=True
    ):  # pylint: disable-msg=too-many-locals
        urls = []
        self.node_progress = {}
        self.progress = Progress()
        self.progress.start(0)

        for i, result in enumerate(start_results, 1):
            if not result or isinstance(result, Exception):
                logger.info("wait_successful_results: Incorrect start result for #%d/%d: %r", i, len(start_results), result)
                return []
            parsed_result = op.Op.StartResult.parse_obj(result)
            urls.append(parsed_result.status_url)
        if all_nodes and len(urls) != len(self.nodes):
            return []
        delay = self.config.poll.delay_start
        results = [None] * len(urls)
        # Note that we don't have timeout mechanism here as such,
        # however, if re-locking times out, we will bail out. TBD if
        # we need timeout mechanism here anyway.
        failures = {}

        def _event_awaitable_factory():
            return self.subresult_received_event.wait()

        async for _ in utils.exponential_backoff(
            initial=delay,
            multiplier=self.config.poll.delay_multiplier,
            maximum=self.config.poll.delay_max,
            duration=self.config.poll.duration,
            event_awaitable_factory=_event_awaitable_factory,
        ):
            self.subresult_received_event.clear()
            for i, (url, result) in enumerate(zip(urls, results)):
                # TBD: This could be done in parallel too
                if result is not None and result.progress.final:
                    continue
                r = await utils.httpx_request(
                    url, caller="CoordinatorOp.wait_successful_results", timeout=self.config.poll.result_timeout
                )
                if r is None:
                    failures[i] = failures.get(i, 0) + 1
                    if failures[i] >= self.config.poll.maximum_failures:
                        return []
                    continue
                # We got something -> decode the result
                result = result_class.parse_obj(r)
                results[i] = result
                failures[i] = 0
                if result.progress.finished_failed:
                    return []
                self.node_progress[result.hostname] = result.progress
                for progress in self.node_progress.values():
                    self.progress.add_total(progress.total)
                    if progress.handled:
                        self.progress.add_success(progress.handled)
                    if progress.failed:
                        self.progress.add_fail(progress.failed)
            if not any(True for result in results if result is None or not result.progress.final):
                break
        else:
            logger.debug("wait_successful_results timed out")
            return []
        self.progress.done()
        return results

    async def download_backup_manifest(self, backup_name: str) -> ipc.BackupManifest:
        assert self.json_storage
        d = await self.json_storage.download_json(backup_name)
        manifest = ipc.BackupManifest.parse_obj(d)
        assert not manifest.filename or manifest.filename == backup_name
        manifest.filename = backup_name
        return manifest


class CoordinatorOpWithClusterLock(CoordinatorOp):
    op_started: Optional[float]  # set when op_info.status is set to starting

    def __init__(self, *, c: "Coordinator"):
        super().__init__(c=c)
        self.ttl = self.config.default_lock_ttl
        self.initial_lock_start = time.monotonic()
        self.locker = self.get_locker()
        self.relock_tasks: List[asyncio.Task] = []

    def get_locker(self):
        return f"{socket.gethostname()}-{id(self)}"

    async def acquire_cluster_lock(self):
        # Acquire initial locks
        r = await self.request_lock_from_nodes(locker=self.locker, ttl=self.ttl)
        if not r:
            # Ensure we don't wind up holding partial lock on the cluster
            await self.request_unlock_from_nodes(locker=self.locker)
            raise HTTPException(
                409, {
                    "code": magic.ErrorCode.cluster_lock_unavailable,
                    "message": "Unable to acquire cluster lock to create operation"
                }
            )
        self.relock_tasks = await self._create_relock_tasks()

    async def run(self):
        assert self.relock_tasks
        try:
            await self.run_with_lock()
        finally:
            for task in self.relock_tasks:
                task.cancel()
            await asyncio.gather(*self.relock_tasks, return_exceptions=True)
            await self.request_unlock_from_nodes(locker=self.locker)

    def set_status(self, status: op.Op.Status, *, from_status: Optional[op.Op.Status] = None) -> bool:
        changed = super().set_status(status=status, from_status=from_status)
        if status == op.Op.Status.starting and changed:
            self.op_started = utils.monotonic_time()
        self._update_running_stats()
        return changed

    def _update_running_stats(self):
        if self.stats is not None:
            if self.info.op_status in {op.Op.Status.done, op.Op.Status.fail}:
                op_running_for = 0
            else:
                op_running_for = int(utils.monotonic_time() - self.op_started)
            logger.debug("Sending op_running_for metric. value=%d", op_running_for)
            self.stats.gauge("astacus_op_running_for", op_running_for, tags={"op": self.info.op_name, "id": self.info.op_id})

    async def _create_relock_tasks(self):
        current_task = asyncio.current_task()
        return [asyncio.create_task(self._node_relock_loop(current_task, node)) for node in self.nodes]

    async def _node_relock_loop(self, main_task, node):
        lock_eol = self.initial_lock_start + self.ttl
        next_lock = self.initial_lock_start + self.ttl / 2
        while True:
            self._update_running_stats()
            t = time.monotonic()
            if t > lock_eol:
                logger.info("Lock of node %r expired, canceling operation", node)
                main_task.cancel()
                return
            while t < next_lock:
                left = next_lock - t + 0.01
                logger.debug("_node_relock_loop sleeping %r", left)
                await asyncio.sleep(left)
                t = time.monotonic()

            # Attempt to reacquire lock
            r = await self.request_lock_call_from_nodes(
                call=magic.LockCall.relock, locker=self.locker, ttl=self.ttl, nodes=[node]
            )
            if r == LockResult.ok:
                lock_eol = t + self.ttl
                next_lock = t + self.ttl / 2
            elif r == LockResult.failure:
                logger.info("Relock of node %r failed, canceling operation", node)
                main_task.cancel()
                return
            elif r == LockResult.exception:
                # We attempt ~4-5 times until giving up
                await asyncio.sleep(self.ttl / 10)
            else:
                raise NotImplementedError(f"Unknown result from request_lock_call_from_nodes:{r!r}")


class Coordinator(op.OpMixin):
    """ Convenience dependency which contains sub-dependencies most API endpoints need """
    def __init__(
        self,
        *,
        request: Request,
        background_tasks: BackgroundTasks,
        config: CoordinatorConfig = Depends(coordinator_config),
        state: CoordinatorState = Depends(coordinator_state)
    ):
        self.request = request
        self.background_tasks = background_tasks
        self.config = config
        self.state = state
        self.stats = statsd.StatsClient(config=config.statsd)

        assert self.config.object_storage
        mstorage = MultiRohmuStorage(config=self.config.object_storage)
        self.hexdigest_mstorage = mstorage
        json_mstorage: MultiStorage = mstorage
        if self.config.object_storage_cache:
            file_mstorage = MultiFileStorage(self.config.object_storage_cache)
            json_mstorage = MultiCachingJsonStorage(backend_mstorage=mstorage, cache_mstorage=file_mstorage)
        self.json_mstorage = json_mstorage
        self.sync_lock = utils.get_or_create_state(app=request.app, key="sync_lock", factory=threading.RLock)

    async def start_op_async(self, *, op, op_name, fun):  # pylint: disable=redefined-outer-name
        if isinstance(op, CoordinatorOpWithClusterLock):
            await op.acquire_cluster_lock()
        return super().start_op(op=op, op_name=op_name, fun=fun)
