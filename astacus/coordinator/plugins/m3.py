"""

Copyright (c) 2020 Aiven Ltd
See LICENSE for details

M3 backup plugin

All of the actual heavy lifting is done using the base file
snapshot/restore functionality. M3 plugin will simply ensure etcd
state is consistent.
"""

from .etcd import ETCDBackupOpBase, ETCDConfiguration, ETCDDump, ETCDRestoreOpBase
from astacus.common import ipc
from astacus.common.utils import AstacusModel
from typing import Optional


class M3Configuration(ETCDConfiguration):
    pass


class M3Manifest(AstacusModel):
    etcd: ETCDDump


class M3BackupOp(ETCDBackupOpBase):
    # upload backup manifest only after we've retrieved again etcd
    # contents and found it consistent
    steps = [
        "retrieve_etcd",  # local
        "snapshot",  # base -->
        "list_hexdigests",
        "upload_blocks",
        "retrieve_etcd_again",  # local -->
        "create_m3_manifest",
        "upload_manifest",  # base
    ]

    plugin = ipc.Plugin.m3

    result_retrieve_etcd: Optional[ETCDDump] = None

    etcd_prefixes = [b"_kv/", b"_sd.placement/"]
    snapshot_root_globs = ["**/*.db"]

    async def step_retrieve_etcd(self):
        return await self.get_etcd_dump(self.etcd_prefixes)

    async def step_retrieve_etcd_again(self):
        etcd_now = await self.get_etcd_dump(self.etcd_prefixes)
        return etcd_now == self.result_retrieve_etcd

    async def step_create_m3_manifest(self):
        m3manifest = M3Manifest(etcd=self.result_retrieve_etcd)
        self.plugin_data = m3manifest.dict()
        return m3manifest


class M3RestoreOp(ETCDRestoreOpBase):
    plugin = ipc.Plugin.m3
    steps = [
        "backup_name",  # base -->
        "backup_manifest",
        "restore_etcd",  # local
        "restore",  # base
    ]
    plugin = ipc.Plugin.m3

    async def step_restore_etcd(self):
        return await self.restore_etcd_dump(self.plugin_manifest.etcd)


plugin_info = {"backup": M3BackupOp, "manifest": M3Manifest, "restore": M3RestoreOp, "config": M3Configuration}