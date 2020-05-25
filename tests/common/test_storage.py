"""
Copyright (c) 2020 Aiven Ltd
See LICENSE for details

Test that RohmuStorage works as advertised.

TBD: Test with something else than local files?

"""

from astacus.common.rohmustorage import RohmuConfig, RohmuStorage
from astacus.common.storage import FileStorage
from pathlib import Path

import pytest

TEST_HEXDIGEST = "deadbeef"
TEXT_HEXDIGEST_DATA = b"data" * 15

TEST_JSON = "jsonblob"
TEST_JSON_DATA = {"foo": 7, "array": [1, 2, 3], "true": True}


def create_storage(*, tmpdir, engine):
    if engine == "rohmu":
        rohmu_path = Path(tmpdir) / "test-storage-rohmu"
        rohmu_path.mkdir()
        tmp_path = Path(tmpdir) / "test-storage-rohmu-tmp"
        tmp_path.mkdir()
        config = RohmuConfig.parse_obj({
            "temporary_directory": str(tmp_path),
            "default_storage": "x",
            "compression": {
                "algorithm": "zstd"
            },
            "storages": {
                "x": {
                    "storage_type": "local",
                    "directory": str(rohmu_path),
                }
            }
        })
        return RohmuStorage(config=config)
    if engine == "file":
        path = Path(tmpdir / "test-storage-file")
        return FileStorage(path)
    raise NotImplementedError(f"unknown storage {engine}")


def _test_hexdigeststorage(storage):
    storage.upload_hexdigest_bytes(TEST_HEXDIGEST, TEXT_HEXDIGEST_DATA)
    assert storage.download_hexdigest_bytes(TEST_HEXDIGEST) == TEXT_HEXDIGEST_DATA
    assert storage.list_hexdigests() == [TEST_HEXDIGEST]
    storage.delete_hexdigest(TEST_HEXDIGEST)
    assert storage.list_hexdigests() == []


def _test_jsonstorage(storage):
    assert storage.list_jsons() == []
    storage.upload_json(TEST_JSON, TEST_JSON_DATA)
    assert storage.download_json(TEST_JSON) == TEST_JSON_DATA
    assert storage.list_jsons() == [TEST_JSON]
    storage.delete_json(TEST_JSON)
    assert storage.list_jsons() == []


@pytest.mark.parametrize("engine", ["file", "rohmu"])
def test_storage(tmpdir, engine):
    storage = create_storage(tmpdir=tmpdir, engine=engine)
    _test_hexdigeststorage(storage)
    _test_jsonstorage(storage)