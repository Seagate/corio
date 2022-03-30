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
from src.commons.utils import corio_utils as cu
from src.commons.utils import support_bundle_utils as remote_cmd
from src.commons.constants import CMD_YUM_NMON, CMD_RUN_NMON
from src.commons.constants import CMD_KILL_NMON, K8S_WORKER_NODES
from config import CLUSTER_CFG

LOGGER = logging.getLogger(__name__)


def collect_resource_utilisation(action: str):
    """start collect resource utilisation

    :param action: start or stop collection resource_utilisation
    """
    host, user, passwd = None, None, None
    worker_node = []
    for node in CLUSTER_CFG["nodes"]:
        if node["node_type"] == "master":
            host, user, passwd = node["hostname"], node["username"], node["password"]
            resp = remote_cmd.execute_remote_command(K8S_WORKER_NODES, host, user, passwd)
            LOGGER.info("response is: ", resp)
            resp = resp[1].read(-1).decode()
            LOGGER.info("response[1] is: ", resp)
            worker_node = resp.strip().split("\n")[1:]
            LOGGER.info("worker nodes: ", worker_node)
    if action == "start":
        resp = cu.run_local_cmd(cmd=CMD_YUM_NMON)
        LOGGER.info(resp)
        resp = remote_cmd.execute_remote_command(CMD_YUM_NMON, host, user, passwd)
        LOGGER.info(resp)
        for worker in worker_node:
            host = worker
            resp = remote_cmd.execute_remote_command(CMD_YUM_NMON, host, user, passwd)
            LOGGER.info(resp)
            resp = remote_cmd.execute_remote_command(CMD_RUN_NMON, host, user, passwd)
            LOGGER.info(resp)
    else:
        resp = cu.run_local_cmd(cmd=CMD_KILL_NMON)
        LOGGER.info(resp)
        resp = remote_cmd.execute_remote_command(CMD_YUM_NMON, host, user, passwd)
        LOGGER.info(resp)
        for worker in worker_node:
            host = worker
            resp = remote_cmd.execute_remote_command(CMD_KILL_NMON, host, user, passwd)
            LOGGER.info(resp)


