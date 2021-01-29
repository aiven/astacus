"""
Copyright (c) 2020 Aiven Ltd
See LICENSE for details
"""

from .cleanup import CleanupOp
from .coordinator import Coordinator
from .list import list_backups
from .lockops import LockOps
from .plugins import base, get_plugin_backup_class, get_plugin_restore_class
from .state import CachedListResponse
from astacus.common import ipc
from astacus.common.op import Op
from enum import Enum
from fastapi import APIRouter, Depends, HTTPException
from urllib.parse import urljoin

import logging
import time

router = APIRouter()

logger = logging.getLogger(__name__)


class OpName(str, Enum):
    """ (Long-running) operations defined in this API (for coordinator) """
    backup = "backup"
    lock = "lock"
    restore = "restore"
    unlock = "unlock"
    cleanup = "cleanup"


@router.get("/{op_name}/{op_id}")
def op_status(*, op_name: OpName, op_id: int, c: Coordinator = Depends()):
    op, op_info = c.get_op_and_op_info(op_id=op_id, op_name=op_name)
    result = {"state": op_info.op_status}
    if isinstance(op, (base.RestoreOpBase, base.BackupOpBase)):
        result["progress"] = op.progress
    return result


class LockStartResult(Op.StartResult):
    unlock_url: str


@router.get("/")
def root():
    # Root is no-op, just useful for testing that Astacus is actually running
    return {}


@router.post("/lock")
async def lock(*, locker: str, ttl: int = 60, c: Coordinator = Depends()):
    op = LockOps(c=c, ttl=ttl, locker=locker)
    result = c.start_op(op_name=OpName.lock, op=op, fun=op.lock)
    return LockStartResult(unlock_url=urljoin(str(c.request.url), f"../unlock?locker={locker}"), **result.dict())


@router.post("/unlock")
def unlock(*, locker: str, c: Coordinator = Depends()):
    op = LockOps(c=c, locker=locker)
    return c.start_op(op_name=OpName.unlock, op=op, fun=op.unlock)


@router.post("/backup")
async def backup(*, c: Coordinator = Depends()):
    op_class = get_plugin_backup_class(c.config.plugin)
    op = op_class(c=c)
    return await c.start_op_async(op_name=OpName.backup, op=op, fun=op.run)


@router.post("/restore")
async def restore(*, req: ipc.RestoreRequest = ipc.RestoreRequest(), c: Coordinator = Depends()):
    op_class = get_plugin_restore_class(c.config.plugin)
    op = op_class(c=c, req=req)
    return await c.start_op_async(op_name=OpName.restore, op=op, fun=op.run)


@router.get("/list")
def _list_backups(*, req: ipc.ListRequest = ipc.ListRequest(), c: Coordinator = Depends()):
    with c.sync_lock:
        cached_list_response = c.state.cached_list_response
        if cached_list_response is not None:
            age = time.monotonic() - cached_list_response.timestamp
            if age < c.config.list_ttl and cached_list_response.list_request == req:
                return cached_list_response.list_response
        if c.state.cached_list_running:
            raise HTTPException(status_code=429, detail="Already caching list result")
        c.state.cached_list_running = True
    list_response = list_backups(req=req, json_mstorage=c.json_mstorage)
    with c.sync_lock:
        c.state.cached_list_response = CachedListResponse(list_request=req, list_response=list_response)
        c.state.cached_list_running = False
    return list_response


@router.post("/cleanup")
async def cleanup(*, req: ipc.CleanupRequest = ipc.CleanupRequest(), c: Coordinator = Depends()):
    op = CleanupOp(c=c, req=req)
    return await c.start_op_async(op_name=OpName.cleanup, op=op, fun=op.run)


@router.put("/{op_name}/{op_id}/sub-result")
async def op_sub_result(*, op_name: OpName, op_id: int, c: Coordinator = Depends()):
    op, _ = c.get_op_and_op_info(op_id=op_id, op_name=op_name)
    # Someday, we might want to actually store results. This is sort
    # of spoofable endpoint though, so just triggering subsequent
    # result fetching faster. In case of terminal results, this
    # results only in one extra fetch per node, so not big deal.
    if not op.subresult_received_event:
        return
    op.subresult_received_event.set()
