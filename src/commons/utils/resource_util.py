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

"""Module to collect resource utilisation utils."""

import glob
import logging
import os
import shutil

from config import CLUSTER_CFG
from src.commons import constants as cm_cmd
from src.commons.constants import MOUNT_DIR
from src.commons.constants import ROOT
from src.commons.utils.cluster_utils import RemoteHost
from src.commons.utils.corio_utils import run_local_cmd

LOGGER = logging.getLogger(ROOT)


# pylint: disable-msg=too-many-statements
def collect_resource_utilisation(action: str):
    """
    start/stop collect resource utilisation.

    :param action: start/stop collection resource_utilisation
    """
    cluster_obj = None
    host, user, passwd = None, None, None
    server_nodes = []
    for node in CLUSTER_CFG["nodes"]:
        if node["node_type"] == "master":
            if not node.get("hostname", None):
                LOGGER.critical("failed to get master details so will not able to collect system"
                                " stats for cluster. Nodes: '%s'", CLUSTER_CFG["nodes"])
                continue
            host, user, passwd = node["hostname"], node["username"], node["password"]
            server_nodes.extend(host)
            cluster_obj = RemoteHost(host, user, passwd)
            resp = cluster_obj.execute_command(cm_cmd.K8S_WORKER_NODES)
            LOGGER.debug("response is: %s", str(resp))
            worker_node = resp[1].strip().split("\n")[1:]
            LOGGER.info("worker nodes: %s", str(worker_node))
            server_nodes.extend(worker_node)
    if action == "start":
        resp = run_local_cmd(cm_cmd.YUM_UNZIP)
        LOGGER.debug("Local response: %s", str(resp))
        resp = run_local_cmd(cm_cmd.CMD_WGET_NIMON)
        LOGGER.debug("Local response: %s", str(resp))
        resp = run_local_cmd(cm_cmd.UNZIP_NIMON)
        LOGGER.debug("Local response: %s", str(resp))
        resp = run_local_cmd(cm_cmd.CMD_CHMOD)
        LOGGER.debug("Local response: %s", str(resp))
        resp = run_local_cmd(cm_cmd.CMD_NINSTALL)
        LOGGER.debug("Local response: %s", str(resp))
        resp = run_local_cmd(cm_cmd.CMD_RUN_NIMON)
        # resp = run_local_cmd(cm_cmd.CMD_RUN_NMON)
        LOGGER.debug("Local response: %s", str(resp))
        if not cluster_obj:
            LOGGER.critical("Will not able to collect system stats for cluster as details not "
                            "provided in cluster config.")
            return
        for server in server_nodes:
            worker_obj = RemoteHost(server, user, passwd)
            resp = worker_obj.execute_command(cm_cmd.YUM_UNZIP)
            LOGGER.debug("worker response: %s", str(resp))
            resp = worker_obj.execute_command(cm_cmd.CMD_WGET_NIMON)
            LOGGER.debug("worker response: %s", str(resp))
            resp = worker_obj.execute_command(cm_cmd.UNZIP_NIMON)
            LOGGER.debug("worker response: %s", str(resp))
            resp = worker_obj.execute_command(cm_cmd.CMD_CHMOD)
            LOGGER.debug("worker response: %s", str(resp))
            resp = worker_obj.execute_command(cm_cmd.CMD_NINSTALL)
            LOGGER.debug("worker response: %s", str(resp))
            resp = worker_obj.execute_command(cm_cmd.CMD_RUN_NIMON)
            LOGGER.debug("worker response: %s", str(resp))
    else:
        resp = run_local_cmd(cm_cmd.CMD_KILL_NIMON)
        LOGGER.debug(resp)
        for server in server_nodes:
            worker_obj = RemoteHost(server, user, passwd)
            resp = worker_obj.execute_command(cm_cmd.CMD_KILL_NIMON)
            LOGGER.debug("worker response: %s", str(resp))
