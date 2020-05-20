"""
Copyright (c) 2020 Aiven Ltd
See LICENSE for details

Test that the coordinator backup endpoint works.

"""

from astacus.common import ipc
from astacus.common.ipc import SnapshotHash
from astacus.coordinator.backup import BackupOp, NodeIndexData
from astacus.coordinator.config import CoordinatorConfig
from astacus.coordinator.coordinator import CoordinatorOpWithClusterLock

import pytest
import respx

_CCNode = CoordinatorConfig.Node

FAILS = [1, 2, 3, 4, 5, None]


@pytest.mark.parametrize("fail_at", FAILS)
def test_backup(fail_at, app, client, mocker, sleepless):  # pylint: disable=unused-argument
    mocker.patch.object(CoordinatorOpWithClusterLock, 'get_locker', return_value='x')
    mocker.patch.object(BackupOp, 'storage')
    nodes = [
        _CCNode(url="http://localhost:12345/asdf"),
        _CCNode(url="http://localhost:12346/asdf"),
    ]
    app.state.coordinator_config.nodes = nodes
    with respx.mock:
        for node in nodes:
            respx.post(f"{node.url}/unlock?locker=x&ttl=0", content={"locked": False})
            # Failure point 1: Lock fails
            respx.post(f"{node.url}/lock?locker=x&ttl=60", content={"locked": fail_at != 1})
            # Failure point 2: snapshot call fails
            if fail_at != 2:
                respx.post(f"{node.url}/snapshot", content={"op_id": 42, "status_url": f"{node.url}/snapshot/result"})
            # Failure point 3: snapshot result call fails
            if fail_at != 3:
                respx.get(
                    f"{node.url}/snapshot/result",
                    content={
                        "progress": {
                            "final": True
                        },
                        "hashes": [{
                            "hexdigest": "HASH",
                            "size": 42
                        }]
                    }
                )
            # Failure point 4: upload call fails
            if fail_at != 4:
                respx.post(f"{node.url}/upload", content={"op_id": 43, "status_url": f"{node.url}/upload/result"})

            # Failure point 5: upload result call fails
            if fail_at != 5:
                respx.get(f"{node.url}/upload/result", content={"progress": {"final": True}})

        response = client.post("/backup")
        assert response.status_code == 200, response.json()

        response = client.get(response.json()["status_url"])
        assert response.status_code == 200, response.json()
        if fail_at:
            assert response.json() == {"state": "fail"}
        else:
            assert response.json() == {"state": "done"}

        assert app.state.coordinator_state.op_info.op_id == 1


class DummyBackupOp(BackupOp):
    def __init__(self):
        # pylint: disable=super-init-not-called
        # NOP __init__, we mock whatever we care about
        self.nodes = [0, 1, 2, 3]

    def assert_upload_of_snapshot_is(self, hexdigests, snapshot, upload):
        got_upload = self._snapshot_results_to_upload_node_index_datas(snapshot_results=snapshot, hexdigests=hexdigests)
        assert got_upload == upload


_progress_done = ipc.Progress(final=True)


@pytest.mark.parametrize(
    "hexdigests,snapshots,uploads",
    [
        ([], [
            ipc.SnapshotResult(progress=_progress_done),
            ipc.SnapshotResult(progress=_progress_done),
            ipc.SnapshotResult(progress=_progress_done),
            ipc.SnapshotResult(progress=_progress_done),
        ], []),
        (
            ["2-1"],
            [
                ipc.SnapshotResult(progress=_progress_done),  # node 0 is empty
                ipc.SnapshotResult(
                    hashes=[
                        SnapshotHash(hexdigest="1-1", size=1),
                        SnapshotHash(hexdigest="12-2", size=2),
                        SnapshotHash(hexdigest="123-3", size=3),
                    ],
                    progress=_progress_done
                ),
                ipc.SnapshotResult(
                    hashes=[
                        SnapshotHash(hexdigest="2-1", size=1),
                        SnapshotHash(hexdigest="12-2", size=2),
                        SnapshotHash(hexdigest="23-2", size=2),
                        SnapshotHash(hexdigest="123-3", size=3),
                    ],
                    progress=_progress_done
                ),
                ipc.SnapshotResult(
                    hashes=[
                        SnapshotHash(hexdigest="3-1", size=1),
                        SnapshotHash(hexdigest="23-2", size=2),
                        SnapshotHash(hexdigest="123-3", size=3),
                    ],
                    progress=_progress_done
                ),
            ],
            [
                NodeIndexData(
                    node_index=1,
                    sshashes=[
                        SnapshotHash(hexdigest='1-1', size=1),
                        SnapshotHash(hexdigest='123-3', size=3),
                    ],
                    total_size=4
                ),
                NodeIndexData(node_index=2, sshashes=[SnapshotHash(hexdigest='12-2', size=2)], total_size=2),
                NodeIndexData(
                    node_index=3,
                    sshashes=[SnapshotHash(hexdigest='3-1', size=1),
                              SnapshotHash(hexdigest='23-2', size=2)],
                    total_size=3
                ),
            ]
        ),
    ]
)
def test_upload_optimization(hexdigests, snapshots, uploads):
    op = DummyBackupOp()
    op.assert_upload_of_snapshot_is(hexdigests, snapshots, uploads)
