#!/usr/bin/python
# -*- coding: utf-8 -*-
#
# Copyright (c) 2022 Seagate Technology LLC and/or its Affiliates
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published
# by the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU Affero General Public License for more details.
# You should have received a copy of the GNU Affero General Public License
# along with this program. If not, see <https://www.gnu.org/licenses/>.
#
# For any questions about this software or licensing,
# please email opensource@seagate.com or cortx-questions@seagate.com.

"""Module to generate support bundle."""

import logging
import os
import time

from config import CLUSTER_CFG
from config import CORIO_CFG
from src.commons.constants import MOUNT_DIR
from src.commons.utils.corio_utils import rotate_logs
from src.commons.utils.cluster_utils import ClusterServices
from src.commons.constants import ROOT

LOGGER = logging.getLogger(ROOT)


def collect_upload_sb_to_nfs_server(mount_path: str, run_id: str, max_sb: int = 0) -> tuple:
    """
    Collect support bundle log and copy to NFS/LOCAL server and keep SB logs as per max_sb count.

    :param mount_path: Path of mounted directory.
    :param run_id: Unique id for each run.
    :param max_sb: maximum sb count to keep on nfs server.
    :param interval: Time interval to wait to generate next support bundle.
    """
    try:
        sb_dir = os.path.join(mount_path, "CorIO-Execution", str(run_id), "Support_Bundles")
        if not os.path.exists(sb_dir):
            os.makedirs(sb_dir)
        nodes = CLUSTER_CFG["nodes"]
        host, user, password = None, None, None
        for node in nodes:
            if node["node_type"] == "master":
                host = node["hostname"]
                user = node["username"]
                password = node["password"]
                break
        if not host:
            LOGGER.critical(
                "Failed to collect support bundles as cluster details are missing: %s", nodes)
            return False, f"Failed to collect support bundles for {nodes}."
        cluster_obj = ClusterServices(host, user, password)
        cluster_obj.collect_support_bundles(sb_dir)
        rotate_logs(sb_dir, max_sb)
        sb_files = os.listdir(sb_dir)
        LOGGER.debug("SB list: %s", sb_files)
        return bool(sb_files), sb_files
    except IOError as error:
        LOGGER.error(error)
        return False, error


def support_bundle_process(interval, sb_identifier):
    """Support bundle wrapper.

    :param interval: Interval in Seconds.
    :param sb_identifier: Support Bundle Identifier.
    """
    while True:
        time.sleep(interval)
        resp = collect_upload_sb_to_nfs_server(MOUNT_DIR, sb_identifier,
                                               max_sb=CORIO_CFG["max_no_of_sb"])
        LOGGER.debug(resp)
