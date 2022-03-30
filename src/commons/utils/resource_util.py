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

from src.commons.utils import corio_utils as cu
from src.commons.utils import support_bundle_utils as remote_cmd
from src.commons.constants import CMD_YUM_NMON, CMD_RUN_NMON
from src.commons.constants import CMD_KILL_NMON, K8S_WORKER_NODES
from config import CLUSTER_CFG


def start_resource_utilisation():
    """start collect resource utilisation
    """
    # install nmon on client & all master, nodes
    resp = cu.run_local_cmd(cmd=CMD_YUM_NMON)
    host, user, passwd = None, None, None
    resp = m_node_obj.execute_cmd(cmd=cm_cmd.K8S_WORKER_NODES, read_lines=True)
    pods_list = m_node_obj.get_all_pods(pod_prefix=cm_const.SERVER_POD_NAME_PREFIX)
    worker_node = {resp[index].strip("\n"): dict() for index in range(1, len(resp))}
    for node in CLUSTER_CFG["nodes"]:
        host, user, passwd = node["hostname"], node["username"], node["password"]
        resp = remote_cmd.execute_remote_command(CMD_YUM_NMON, host, user, passwd)
        resp = remote_cmd.execute_remote_command(CMD_RUN_NMON, host, user, passwd)


def stop_resource_utilisation():
    """stop collection
    """
    resp = cu.run_local_cmd(cmd=CMD_KILL_NMON)
    host, user, passwd = None, None, None
    for node in CLUSTER_CFG["nodes"]:
        host, user, passwd = node["hostname"], node["username"], node["password"]
        resp = remote_cmd.execute_remote_command(CMD_KILL_NMON, host, user, passwd)