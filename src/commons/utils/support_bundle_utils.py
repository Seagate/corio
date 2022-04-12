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

"""Module to maintain support bundle utils."""

import logging
import os

import paramiko

from config import CLUSTER_CFG
from src.commons.constants import CLSTR_LOGS_CMD, K8S_SCRIPTS_PATH
from src.commons.constants import ROOT

LOGGER = logging.getLogger(ROOT)


def execute_remote_command(command, host, user, passwd, timeout=20 * 60):
    """Execute command on remote host.

    :param command: Command to execute
    :param host: Hostname
    :param user: Username
    :param passwd: Password
    :param timeout: Timeout for the connection
    """
    host_obj = paramiko.SSHClient()
    host_obj.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    LOGGER.info("Connecting to host: %s", host)
    host_obj.connect(hostname=host, username=user, password=passwd, timeout=timeout)
    _, stdout, _ = host_obj.exec_command(command, timeout=timeout)
    exit_status = stdout.channel.recv_exit_status()
    return exit_status, stdout


def copy_file_from_remote(host, user, passwd, local_path, remote_path):
    """Copy file from remote host to local host.

    :param host: Hostname
    :param user: Username
    :param passwd: Password
    :param local_path: Local path of the file
    :param remote_path: Remote path of the file
    """
    host_obj = paramiko.SSHClient()
    host_obj.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    host_obj.connect(hostname=host, username=user, password=passwd)
    sftp = host_obj.open_sftp()
    sftp.get(remote_path, local_path)
    sftp.close()
    host_obj.close()


def remove_remote_file(host, user, passwd, remote_path):
    """Remove file from remote host.

    :param host: Hostname
    :param user: Username
    :param passwd: Password
    :param remote_path: Remote path of the file
    """
    host_obj = paramiko.SSHClient()
    host_obj.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    host_obj.connect(hostname=host, username=user, password=passwd)
    sftp = host_obj.open_sftp()
    sftp.remove(remote_path)
    sftp.close()
    host_obj.close()


def collect_support_bundle_k8s(local_dir_path: str, scripts_path: str = K8S_SCRIPTS_PATH):
    """Utility function to get the support bundle created with services script and copied to client.

    :param local_dir_path: local dir path on client
    :param scripts_path: services scripts path on master node
    :return: Boolean
    """
    host, user, passwd = None, None, None
    for node in CLUSTER_CFG["nodes"]:
        if node["node_type"] == "master":
            host, user, passwd = node["hostname"], node["username"], node["password"]
            break
    resp = execute_remote_command(CLSTR_LOGS_CMD.format(scripts_path), host, user, passwd)
    for line in resp[1]:
        if ".tar" in line:
            out = line.split()[1]
            file = out.strip('\"')
            LOGGER.info("Support bundle generated: %s", file)
            remote_path = os.path.join(scripts_path, file)
            local_path = os.path.join(local_dir_path, file)
            copy_file_from_remote(host, user, passwd, local_path, remote_path)
            remove_remote_file(host, user, passwd, remote_path)
            LOGGER.info("Support bundle %s generated and copied to %s path.", file, local_dir_path)
            return True, local_path
    LOGGER.info("Support Bundle not generated; response: %s", resp)
    return False, f"Support bundles not generated. Response: {resp}"
