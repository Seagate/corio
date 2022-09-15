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

from src.commons import commands as cmd
from src.commons import constants as const
from src.commons.utils.k8s import ClusterServices

LOGGER = logging.getLogger(const.ROOT)


def collect_resource_utilisation(action: str):
    """
    Start/stop collect resource utilisation.

    :param action: start/stop collection resource_utilisation
    """
    if action == "start":
        start_client_resource_utilisation()
        start_server_resource_utilization()
    if action == "stop":
        stop_store_client_resource_utilization()
        stop_store_server_resource_utilization()


def start_client_resource_utilisation():
    """Start resource utilization on client."""
    resp = corio_utils.install_package(const.NMON)
    if resp[0]:
        resp = corio_utils.run_local_cmd(cmd.CMD_RUN_NMON)
    LOGGER.info(resp)


def stop_store_client_resource_utilization():
    """Stop resource utilization from client and copy to NFS/LOCAL server."""
    resp = corio_utils.run_local_cmd(cmd.CMD_KILL_NMON)
    LOGGER.debug(resp)
    stat_fpath = sorted(glob.glob(os.getcwd() + "/*.nmon"), key=os.path.getctime, reverse=True)[-1]
    LOGGER.info(stat_fpath)
    dpath = os.path.join(const.CMN_LOG_DIR, os.getenv("run_id"), "system_stats", "client")
    if not os.path.exists(dpath):
        os.makedirs(dpath)
    shutil.move(stat_fpath, os.path.join(dpath, os.path.basename(stat_fpath)))
    if os.path.exists(stat_fpath):
        os.remove(stat_fpath)
    # corio_utils.remove_package(const.NMON)
    # collect journalctl logs from client.
    journalctl_filepath = os.path.join("/root", "client_journalctl.log")
    resp = corio_utils.run_local_cmd(cmd.CMD_JOURNALCTL.format(journalctl_filepath))
    if resp[0]:
        shutil.move(
            journalctl_filepath,
            os.path.join(dpath, os.path.basename(journalctl_filepath)),
        )
    # collect dmesg logs from client.
    dmesg_filepath = os.path.join("/root", "client_dmesg.log")
    resp = corio_utils.run_local_cmd(cmd.CMD_JOURNALCTL.format(dmesg_filepath))
    if resp[0]:
        shutil.move(dmesg_filepath, os.path.join(dpath, os.path.basename(dmesg_filepath)))


def start_server_resource_utilization() -> None:
    """Start resource utilization on master and workers."""
    user, passwd, cluster_nodes = get_server_details()
    for node in cluster_nodes:
        cluster_obj = ClusterServices(node, user, passwd)
        resp = cluster_obj.install_package(const.NMON)
        LOGGER.info(resp)
        resp = cluster_obj.execute_command(cmd.CMD_RUN_NMON)
        LOGGER.info(resp)


def get_server_details() -> tuple:
    """Get K8s based Server details."""
    cluster_nodes = []
    host, user, passwd = corio_utils.get_master_details()
    if not host:
        LOGGER.critical("Will not able to collect system stats for cluster as detail is missing.")
        return None, None, cluster_nodes
    cluster_nodes.append(host)
    cluster_obj = ClusterServices(host, user, passwd)
    worker_node = cluster_obj.get_all_workers_details()
    LOGGER.debug("Workers list is: %s", worker_node)
    cluster_nodes.extend(worker_node)
    return user, passwd, cluster_nodes


def stop_store_server_resource_utilization():
    """Stop resource utilization on master, workers and copy to NFS/LOCAL server."""
    user, passwd, cluster_nodes = get_server_details()
    for node in cluster_nodes:
        cluster_obj = ClusterServices(node, user, passwd)
        resp = cluster_obj.execute_command(cmd.CMD_KILL_NMON)
        LOGGER.info(resp)
        resp = cluster_obj.execute_command(cmd.CMD_SEARCH_FILE.format("'*.nmon'"))
        filename = str([x.strip("./") for x in resp[1].strip().split("\n")][0])
        LOGGER.info("Filename is: %s", filename)
        stat_path = os.path.join(const.CMN_LOG_DIR, os.getenv("run_id"), "system_stats", "server")
        cl_path = os.path.join(stat_path, filename)
        remote_path = os.path.join("/root", filename)
        if not os.path.exists(stat_path):
            os.makedirs(stat_path)
        cluster_obj.download_file(cl_path, remote_path)
        resp = cluster_obj.execute_command(cmd.CMD_RM_NMON.format(remote_path))
        LOGGER.debug("file removed: %s", resp)
        # resp = cluster_obj.remove_package(const.NMON)
        LOGGER.info(resp)
        # collect journalctl
        journalctl_path = os.path.join("/root", f"{node}_journalctl.log")
        resp = cluster_obj.execute_command(cmd.CMD_JOURNALCTL.format(journalctl_path))
        if resp[0]:
            cluster_obj.download_file(
                os.path.join(stat_path, os.path.basename(journalctl_path)),
                journalctl_path,
            )
            cluster_obj.delete_file(journalctl_path)
        # collect dmesg
        dmesg_path = os.path.join("/root", f"{node}_dmesg.log")
        resp = cluster_obj.execute_command(cmd.CMD_DMESG.format(dmesg_path))
        if resp[0]:
            cluster_obj.download_file(
                os.path.join(stat_path, os.path.basename(dmesg_path)), dmesg_path
            )
            cluster_obj.delete_file(dmesg_path)
