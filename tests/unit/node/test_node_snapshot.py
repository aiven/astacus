"""
Copyright (c) 2020 Aiven Ltd
See LICENSE for details
"""

from astacus.common import ipc, utils
from astacus.common.progress import Progress

import pytest


@pytest.mark.timeout(2)
def test_snapshot(snapshotter, uploader):
    # Start with empty
    assert snapshotter.snapshot(progress=Progress()) == 0
    src = snapshotter.src
    dst = snapshotter.dst
    assert not (dst / "foo").is_file()

    # Create files in src, run snapshot
    snapshotter.create_4foobar()
    ss2 = snapshotter.get_snapshot_state()

    assert (dst / "foo").is_file()
    assert (dst / "foo").read_text() == "foobar"
    assert (dst / "foo2").read_text() == "foobar"

    hashes = snapshotter.get_snapshot_hashes()
    assert len(hashes) == 1
    assert hashes == [
        ipc.SnapshotHash(hexdigest='326827fe6fd23503bf16eed91861766df522748794814a1bf46d479d9feae1a0', size=600)
    ]

    while True:
        (src / "foo").write_text("barfoo")  # same length
        if snapshotter.snapshot(progress=Progress()) > 0:
            # Sometimes fails on first iteration(s) due to same mtime
            # (inaccurate timestamps)
            break
    ss3 = snapshotter.get_snapshot_state()
    assert ss2 != ss3
    assert snapshotter.snapshot(progress=Progress()) == 0
    assert (dst / "foo").is_file()
    assert (dst / "foo").read_text() == "barfoo"

    uploader.write_hashes_to_storage(snapshotter=snapshotter, hashes=hashes, parallel=1, progress=Progress())

    # Remove file from src, run snapshot
    for filename in ["foo", "foo2", "foobig", "foobig2"]:
        (src / filename).unlink()
        assert snapshotter.snapshot(progress=Progress()) > 0
        assert snapshotter.snapshot(progress=Progress()) == 0
        assert not (dst / filename).is_file()

    # Now shouldn't have any data hashes
    hashes_empty = snapshotter.get_snapshot_hashes()
    assert not hashes_empty


def test_api_snapshot_and_upload(client, mocker):
    url = "http://addr/result"
    m = mocker.patch.object(utils, "http_request")
    response = client.post("/node/snapshot")
    assert response.status_code == 422, response.json()

    req_json = {"root_globs": ["*"]}
    response = client.post("/node/snapshot", json=req_json)
    assert response.status_code == 409, response.json()

    response = client.post("/node/lock?locker=x&ttl=10")
    assert response.status_code == 200, response.json()
    response = client.post("/node/snapshot", json=req_json)
    assert response.status_code == 200, response.json()

    req_json["result_url"] = url
    response = client.post("/node/snapshot", json=req_json)
    assert response.status_code == 200, response.json()

    # Decode the (result endpoint) response using the model
    response = m.call_args[1]["data"]
    result = ipc.SnapshotResult.parse_raw(response)
    assert result.progress.finished_successfully
    assert result.hashes
    assert result.files
    assert result.total_size

    # Ask it to be uploaded
    response = client.post("/node/upload")
    assert response.status_code == 422, response.json()
    response = client.post(
        "/node/upload", json={
            "storage": "x",
            "hashes": [x.dict() for x in result.hashes],
            "result_url": url
        }
    )
    assert response.status_code == 200, response.json()
    response = m.call_args[1]["data"]
    result = ipc.SnapshotUploadResult.parse_raw(response)
    assert result.progress.finished_successfully
