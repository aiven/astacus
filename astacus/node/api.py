"""
Copyright (c) 2020 Aiven Ltd
See LICENSE for details
"""

from .clear import ClearOp
from .download import DownloadOp
from .node import Node
from .snapshot import SnapshotOp, UploadOp
from .state import node_state, NodeState
from astacus.common import ipc
from enum import Enum
from fastapi import APIRouter, Depends, HTTPException

router = APIRouter()


class OpName(str, Enum):
    """ (Long-running) operations defined in this API (for node) """
    clear = "clear"
    download = "download"
    snapshot = "snapshot"
    upload = "upload"


@router.post("/lock")
def lock(locker: str, ttl: int, state: NodeState = Depends(node_state)):
    with state.mutate_lock:
        if state.is_locked:
            raise HTTPException(status_code=409, detail="Already locked")
        state.lock(locker=locker, ttl=ttl)
    return {"locked": True}


@router.post("/relock")
def relock(locker: str, ttl: int, state: NodeState = Depends(node_state)):
    with state.mutate_lock:
        if not state.is_locked:
            raise HTTPException(status_code=409, detail="Not locked")
        if state.is_locked != locker:
            raise HTTPException(status_code=403, detail="Locked by someone else")
        state.lock(locker=locker, ttl=ttl)
    return {"locked": True}


@router.post("/unlock")
def unlock(locker: str, state: NodeState = Depends(node_state)):
    with state.mutate_lock:
        if not state.is_locked:
            raise HTTPException(status_code=409, detail="Already unlocked")
        if state.is_locked != locker:
            raise HTTPException(status_code=403, detail="Locked by someone else")
        state.unlock()
    return {"locked": False}


@router.post("/snapshot")
def snapshot(req: ipc.SnapshotRequest, n: Node = Depends()):
    if not n.state.is_locked:
        raise HTTPException(status_code=409, detail="Not locked")
    return SnapshotOp(n=n).start(req=req)


@router.get("/snapshot/{op_id}")
def snapshot_result(*, op_id: int, n: Node = Depends()):
    op, _ = n.get_op_and_op_info(op_id=op_id, op_name=OpName.snapshot)
    return op.result


@router.post("/upload")
def upload(req: ipc.SnapshotUploadRequest, n: Node = Depends()):
    if not n.state.is_locked:
        raise HTTPException(status_code=409, detail="Not locked")
    return UploadOp(n=n).start(req=req)


@router.get("/upload/{op_id}")
def upload_result(*, op_id: int, n: Node = Depends()):
    op, _ = n.get_op_and_op_info(op_id=op_id, op_name=OpName.upload)
    return op.result


@router.post("/download")
def download(req: ipc.SnapshotDownloadRequest, n: Node = Depends()):
    if not n.state.is_locked:
        raise HTTPException(status_code=409, detail="Not locked")
    return DownloadOp(n=n).start(req=req)


@router.get("/download/{op_id}")
def download_result(*, op_id: int, n: Node = Depends()):
    op, _ = n.get_op_and_op_info(op_id=op_id, op_name=OpName.download)
    return op.result


@router.post("/clear")
def clear(req: ipc.SnapshotClearRequest, n: Node = Depends()):
    if not n.state.is_locked:
        raise HTTPException(status_code=409, detail="Not locked")
    return ClearOp(n=n).start(req=req)


@router.get("/clear/{op_id}")
def clear_result(*, op_id: int, n: Node = Depends()):
    op, _ = n.get_op_and_op_info(op_id=op_id, op_name=OpName.clear)
    return op.result
