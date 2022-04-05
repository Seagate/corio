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

import os
import logging
import shutil
from src.commons.utils.support_bundle_utils import execute_remote_command
from src.commons.utils.support_bundle_utils import copy_file_from_remote
from src.commons.utils.corio_utils import run_local_cmd
from src.commons import constants as cm_cmd
from src.commons.constants import MOUNT_DIR
from config import CLUSTER_CFG

LOGGER = logging.getLogger("corio")


def collect_resource_utilisation(action: str) -> None:
    """
    start/stop collect resource utilisation.

    :param action: start or stop collection resource_utilisation
    """
    host, user, passwd = None, None, None
    worker_node = []
    for node in CLUSTER_CFG["nodes"]:
        if node["node_type"] == "master":
            host, user, passwd = node["hostname"], node["username"], node["password"]
            resp = execute_remote_command(cm_cmd.K8S_WORKER_NODES, host, user, passwd)
            LOGGER.debug("response is: %s", str(resp))
            resp = resp[1].read(-1).decode()
            LOGGER.debug("response[1] is: %s", str(resp))
            worker_node = resp.strip().split("\n")[1:]
            LOGGER.info("worker nodes: %s", str(worker_node))
    if action == "start":
        resp = run_local_cmd(cm_cmd.CMD_YUM_NMON)
        LOGGER.debug("Local response: %s", str(resp))
        resp = run_local_cmd(cm_cmd.CMD_RUN_NMON)
        LOGGER.debug("Local response: %s", str(resp))
        resp = execute_remote_command(cm_cmd.CMD_YUM_NMON, host, user, passwd)
        LOGGER.debug("master response: %s", str(resp))
        resp = execute_remote_command(cm_cmd.CMD_RUN_NMON, host, user, passwd)
        LOGGER.debug("master response: %s", str(resp))
        for worker in worker_node:
            host = worker
            resp = execute_remote_command(cm_cmd.CMD_YUM_NMON, host, user, passwd)
            LOGGER.debug("worker response: %s", str(resp))
            resp = execute_remote_command(cm_cmd.CMD_RUN_NMON, host, user, passwd)
            LOGGER.debug("worker response: %s", str(resp))
    else:
        resp = run_local_cmd(cm_cmd.CMD_KILL_NMON)
        LOGGER.debug("Local response: %s", str(resp))
        resp = run_local_cmd(cm_cmd.CMD_NMON_FILE)
        resp = resp[1].read(-1).decode()
        filename = str([x.strip("./") for x in resp.strip().split("\n")][0])
        LOGGER.info("Filename is: %s", filename)
        dest = os.path.join(MOUNT_DIR, "system_stats", "client")
        if not os.path.exists(dest):
            os.makedirs(dest)
        shutil.move(filename, dest)
        resp = execute_remote_command(cm_cmd.CMD_KILL_NMON, host, user, passwd)
        LOGGER.debug("master response: %s", str(resp))
        resp = execute_remote_command(cm_cmd.CMD_NMON_FILE, host, user, passwd)
        resp = resp[1].read(-1).decode()
        filename = str([x.strip("./") for x in resp.strip().split("\n")][0])
        LOGGER.info("Filename is: %s", filename)
        client_path = os.path.join(MOUNT_DIR, "system_stats", "server")
        if not os.path.exists(client_path):
            os.makedirs(client_path)
        copy_file_from_remote(host, user, passwd, client_path, f"/root/{filename}")
        resp = execute_remote_command(cm_cmd.CMD_RM_NMON.format(filename), host, user, passwd)
        LOGGER.debug("file removed: %s", resp)
        for worker in worker_node:
            host = worker
            resp = execute_remote_command(cm_cmd.CMD_KILL_NMON, host, user, passwd)
            LOGGER.debug("worker response: %s", str(resp))
            resp = execute_remote_command(cm_cmd.CMD_NMON_FILE, host, user, passwd)
            resp = resp[1].read(-1).decode()
            filename = str([x.strip("./") for x in resp.strip().split("\n")][0])
            LOGGER.info("Filename is: %s", filename)
            copy_file_from_remote(host, user, passwd, client_path, f"/root/{filename}")
            resp = execute_remote_command(cm_cmd.CMD_RM_NMON.format(filename), host, user, passwd)
            LOGGER.debug("file removed: %s", resp)


