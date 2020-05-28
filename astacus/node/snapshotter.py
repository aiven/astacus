"""

Copyright (c) 2020 Aiven Ltd
See LICENSE for details

"""

from astacus.common.ipc import SnapshotFile, SnapshotHash, SnapshotState
from astacus.common.progress import Progress
from pathlib import Path
from typing import Optional

import hashlib
import logging
import os

logger = logging.getLogger(__name__)

_hash = hashlib.blake2s


def hash_hexdigest_readable(f, *, read_buffer=1_000_000):
    h = _hash()
    while True:
        data = f.read(read_buffer)
        if not data:
            break
        h.update(data)
    return h.hexdigest()


class Snapshotter:
    """Snapshotter keeps track of files on disk, and their hashes.

    The hash on disk MAY change, which may require subsequent
    incremential snapshot and-or ignoring the files which have changed.

    The output to outside is just root object's hash, as well as list
    of other hashes which correspond to files referred to within the
    file list contained in root object.

    For actual products, Snapshotter should be subclassed and
    e.g. file_path_filter should be overridden.
    """
    def __init__(self, *, src, dst, globs, file_path_filter=None):
        self.src = Path(src)
        self.dst = Path(dst)
        self.globs = globs
        self.relative_path_to_snapshotfile = {}
        self.hexdigest_to_snapshotfiles = {}
        self.file_path_filter = file_path_filter

    def _list_files(self, basepath: Path):
        result_files = set()
        for glob in self.globs:
            for path in basepath.glob(glob):
                if not path.is_file() or path.is_symlink():
                    continue
                relpath = path.relative_to(basepath)
                result_files.add(relpath)
        result = sorted(result_files)
        if self.file_path_filter:
            result = self.file_path_filter(result)
        return result

    def _list_dirs_and_files(self, basepath: Path):
        files = self._list_files(basepath)
        dirs = {p.parent for p in files}
        return sorted(dirs), files

    def _add_snapshotfile(self, snapshotfile: SnapshotFile):
        old_snapshotfile = self.relative_path_to_snapshotfile.get(snapshotfile.relative_path, None)
        if old_snapshotfile:
            self._remove_snapshotfile(old_snapshotfile)
        self.relative_path_to_snapshotfile[snapshotfile.relative_path] = snapshotfile
        self.hexdigest_to_snapshotfiles.setdefault(snapshotfile.hexdigest, []).append(snapshotfile)

    def _remove_snapshotfile(self, snapshotfile: SnapshotFile):
        assert self.relative_path_to_snapshotfile[snapshotfile.relative_path] == snapshotfile
        del self.relative_path_to_snapshotfile[snapshotfile.relative_path]
        self.hexdigest_to_snapshotfiles[snapshotfile.hexdigest].remove(snapshotfile)

    def _snapshotfile_from_path(self, relative_path):
        src_path = self.src / relative_path
        st = src_path.stat()
        return SnapshotFile(relative_path=relative_path, mtime_ns=st.st_mtime_ns, file_size=st.st_size, hexdigest="")

    def _get_snapshot_hash_list(self, relfilenames):
        for relfilename in relfilenames:
            old_snapshotfile = self.relative_path_to_snapshotfile.get(relfilename)
            snapshotfile = self._snapshotfile_from_path(relfilename)
            if old_snapshotfile:
                snapshotfile.hexdigest = old_snapshotfile.hexdigest
                if old_snapshotfile == snapshotfile:
                    logger.debug("%r in %s is same", old_snapshotfile, relfilename)
                    continue
            yield snapshotfile

    def get_snapshot_hashes(self):
        return [
            SnapshotHash(hexdigest=dig, size=sf[0].file_size) for dig, sf in self.hexdigest_to_snapshotfiles.items() if sf
        ]

    def get_snapshot_state(self):
        return SnapshotState(files=sorted(self.relative_path_to_snapshotfile.values()))

    def snapshot(self, *, progress: Optional[Progress] = None):
        if progress is None:
            progress = Progress()
        progress.start(3)

        src_dirs, src_files = self._list_dirs_and_files(self.src)
        dst_dirs, dst_files = self._list_dirs_and_files(self.dst)

        # Create missing directories
        changes = 0
        for reldirname in set(src_dirs).difference(dst_dirs):
            dst_path = self.dst / reldirname
            dst_path.mkdir(parents=True, exist_ok=True)
            changes += 1

        progress.add_success()

        # Remove extra files
        for relfilename in set(dst_files).difference(src_files):
            dst_path = self.dst / relfilename
            snapshotfile = self.relative_path_to_snapshotfile.get(relfilename)
            if snapshotfile:
                self._remove_snapshotfile(snapshotfile)
            dst_path.unlink()
            changes += 1
        progress.add_success()

        # Add missing files
        for relfilename in set(src_files).difference(dst_files):
            src_path = self.src / relfilename
            dst_path = self.dst / relfilename
            os.link(src=src_path, dst=dst_path, follow_symlinks=False)
            changes += 1
        progress.add_success()

        # We COULD also remove extra directories, but it is not
        # probably really worth it and due to ignored files it
        # actually might not even work.

        # Then, create/update corresponding snapshotfile objects (old
        # ones were already removed)
        snapshotfiles = list(self._get_snapshot_hash_list(src_files))
        progress.add_total(len(snapshotfiles))
        # TBD: This could be done via e.g. multiprocessing too some day.
        for snapshotfile in snapshotfiles:
            # src may or may not be present; dst is present as it is in snapshot
            dst_path = self.dst / snapshotfile.relative_path
            snapshotfile.hexdigest = hash_hexdigest_readable(dst_path.open(mode="rb"))
            self._add_snapshotfile(snapshotfile)
            changes += 1
            progress.add_success()
        progress.done()
        return changes

    def write_hashes_to_storage(self, *, hashes, storage, progress: Progress, still_running_callback=lambda: True):
        todo = set(hash.hexdigest for hash in hashes)
        progress.start(len(todo))
        for hexdigest in todo:
            if not still_running_callback():
                break
            files = self.hexdigest_to_snapshotfiles.get(hexdigest, [])
            for snapshotfile in files:
                path = self.dst / snapshotfile.relative_path
                if not path.is_file():
                    logger.warning("%s disappeared post-snapshot", path)
                    continue
                current_hexdigest = hash_hexdigest_readable(open(path, "rb"))
                if current_hexdigest != snapshotfile.hexdigest:
                    logger.info("Hash of %s changed before upload", snapshotfile.relative_path)
                    continue
                if storage.upload_hexdigest_from_path(hexdigest, path):
                    current_hexdigest = hash_hexdigest_readable(open(path, "rb"))
                    if current_hexdigest != snapshotfile.hexdigest:
                        logger.info("Hash of %s changed after upload", snapshotfile.relative_path)
                        storage.delete_hexdigest(hexdigest)
                        continue
                    progress.upload_success(hexdigest)
                else:
                    progress.upload_failure(hexdigest)
                # We found and processed one file with the particular
                # hexdigest; even if sending it failed, we won't try
                # subsequent files and instead break out of iterating
                # through candidate files with same hexdigest.
                break
            else:
                # We didn't find single file with the matching hexdigest.
                # Report it as missing but keep uploading other files.
                progress.upload_missing(hexdigest)

        # This operation is done. It may or may not have been a success.
        progress.done()