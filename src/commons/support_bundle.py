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
#
#
"""Module to generate support bundle."""

import logging
import os
import time

from config import CLUSTER_CFG
from config import CORIO_CFG
from src.commons.constants import CMN_LOG_DIR
from src.commons.constants import ROOT
from src.commons.utils.cluster_utils import ClusterServices
from src.commons.utils.corio_utils import rotate_logs

LOGGER = logging.getLogger(ROOT)


def collect_upload_rotate_support_bundles(dir_path: str, max_sb: int = 0) -> None:
    """Collect support bundle log and copy to NFS/LOCAL server and keep SB logs as per max_sb count.

    :param dir_path: Path of common log directory.
    :param max_sb: maximum sb count to keep on nfs server.
    """
    try:
        sb_dir = os.path.join(dir_path, os.getenv("run_id"), "support_bundles")
        max_sb = max_sb if max_sb else CORIO_CFG["max_no_of_sb"]
        if not os.path.exists(sb_dir):
            os.makedirs(sb_dir)
        master = [node for node in CLUSTER_CFG["nodes"] if node["node_type"] == "master"][-1]
        host, user, password = master["hostname"], master["username"], master["password"]
        if not host:
            raise AssertionError(f"Failed to collect support bundles as cluster details are"
                                 f" missing: {CLUSTER_CFG['nodes']}")
        cluster_obj = ClusterServices(host, user, password)
        status, response = cluster_obj.collect_support_bundles(sb_dir)
        if not status:
            raise AssertionError(f"Failed to generate support bundles. Response: {response}")
        rotate_logs(sb_dir, max_sb)
        sb_files = os.listdir(sb_dir)
        LOGGER.debug("SB list: %s", sb_files)
    except (IOError, AssertionError) as error:
        LOGGER.error(error)


def support_bundle_process(interval: int) -> None:
    """
    Support bundle wrapper.

    :param interval: Interval in Seconds.
    """
    while True:
        time.sleep(interval)
        collect_upload_rotate_support_bundles(CMN_LOG_DIR, max_sb=CORIO_CFG["max_no_of_sb"])
