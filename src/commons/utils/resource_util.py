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

import logging
from src.commons.utils import support_bundle_utils as remote_cmd
from src.commons import constants as cm_cmd
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
            resp = remote_cmd.execute_remote_command(cm_cmd.K8S_WORKER_NODES, host, user, passwd)
            LOGGER.debug("response is: %s", str(resp))
            resp = resp[1].read(-1).decode()
            LOGGER.debug("response[1] is: %s", str(resp))
            worker_node = resp.strip().split("\n")[1:]
            LOGGER.info("worker nodes: %s", str(worker_node))
    if action == "start":
        remote_cmd.execute_remote_command(cm_cmd.CMD_YUM_NMON, "localhost", user, passwd)
        LOGGER.debug("Local response: %s", str(resp))
        remote_cmd.execute_remote_command(cm_cmd.CMD_RUN_NMON, "localhost", user, passwd)
        LOGGER.debug("master response: %s", str(resp))
        resp = remote_cmd.execute_remote_command(cm_cmd.CMD_YUM_NMON, host, user, passwd)
        LOGGER.debug("master response: %s", str(resp))
        resp = remote_cmd.execute_remote_command(cm_cmd.CMD_RUN_NMON, host, user, passwd)
        LOGGER.debug("master response: %s", str(resp))
        for worker in worker_node:
            host = worker
            resp = remote_cmd.execute_remote_command(cm_cmd.CMD_YUM_NMON, host, user, passwd)
            LOGGER.debug("worker response: %s", str(resp))
            resp = remote_cmd.execute_remote_command(cm_cmd.CMD_RUN_NMON, host, user, passwd)
            LOGGER.debug("worker response: %s", str(resp))
    else:
        resp = remote_cmd.execute_remote_command(cm_cmd.CMD_KILL_NMON, "localhost", user, passwd)
        LOGGER.debug("Local response: %s", str(resp))
        resp = remote_cmd.execute_remote_command(cm_cmd.CMD_KILL_NMON, host, user, passwd)
        LOGGER.debug("master response: %s", str(resp))
        for worker in worker_node:
            host = worker
            resp = remote_cmd.execute_remote_command(cm_cmd.CMD_KILL_NMON, host, user, passwd)
            LOGGER.debug("worker response: %s", str(resp))
