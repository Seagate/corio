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
# For any questions about this software or licensing,
# please email opensource@seagate.com or cortx-questions@seagate.com.
#

"""IO cluster services module."""

import glob
import logging
import os
import json
import shutil
import paramiko
from paramiko import SSHException

from config import CORIO_CFG, CLUSTER_CFG
from src.commons.constants import CMD_MOUNT
from src.commons import commands as cmd
from src.commons import constants as const
from src.commons.utils import support_bundle_utils as sb
from src.commons.utils.corio_utils import copy_file_from_remote
from src.commons.utils.corio_utils import remove_remote_file
from src.commons.constants import ROOT

LOGGER = logging.getLogger(ROOT)


class ClusterServices:
    """Cluster services class to perform service related operations."""

    def __init__(self, host, user, password, timeout=20 * 60):
        """Initializer for cluster services."""
        self.host = host
        self.user = user
        self.password = password
        self.timeout = timeout

    def exec_k8s_command(self, command, read_lines=False):
        """Execute command on remote k8s master."""
        hobj = paramiko.SSHClient()
        hobj.set_missing_host_key_policy(paramiko.AutoAddPolicy)
        try:
            hobj.connect(hostname=self.host, username=self.user, password=self.password,
                         timeout=self.timeout)
            _, stdout, stderr = hobj.exec_command(command=command, timeout=self.timeout)
            exit_status = stdout.channel.recv_exit_status()
            LOGGER.info("Execution status: %s", exit_status)
            error = list(map(lambda x: x.decode("utf-8"), stderr.readlines()
                             )) if read_lines else stderr.read().decode("utf-8")
            output = list(map(lambda x: x.decode("utf-8"), stderr.readlines()
                              )) if read_lines else stdout.read().decode("utf-8")
            LOGGER.debug("Error: %s", error)
            LOGGER.debug("Output: %s", output)
            return True, output
        except SSHException as error:
            LOGGER.error(error)
            return False, str(error)
        finally:
            hobj.close()

    def get_pod_name(self, pod_prefix: str = const.POD_NAME_PREFIX):
        """Get pod name with given prefix."""
        output = self.exec_k8s_command(cmd.CMD_K8S_PODS_NAME, read_lines=True)
        for lines in output:
            if pod_prefix in lines:
                return True, lines.strip()
        return False, f"pod with prefix \"{pod_prefix}\" not found"

    def get_hctl_status(self):
        """Get hctl status from master node."""
        status, pod_name = self.get_pod_name()
        if status:
            status, output = self.exec_k8s_command(cmd.CMD_K8S_CLUSTER_HEALTH.format(
                pod_name, const.HAX_CONTAINER_NAME, cmd.CMD_HCTL_STATUS))
            LOGGER.debug("Response of %s:\n %s ", cmd.CMD_HCTL_STATUS, output)
            if status:
                return True, json.loads(output)
        LOGGER.warning("Failed to get %s", cmd.CMD_HCTL_STATUS)
        return False, {"error": f"Failed to get {cmd.CMD_HCTL_STATUS}"}

    def check_cluster_health(self):
        """Check the cluster health."""
        status, response = self.get_hctl_status()
        if status:
            for node in response["nodes"]:
                pod_name = node["name"]
                services = node["svcs"]
                for service in services:
                    if service["status"] != "started":
                        LOGGER.error("%s service not started on pod %s", service["name"], pod_name)
                        return False, f"Cluster is not healthy: {response}"
                if not services:
                    LOGGER.critical("No service found on pod %s", pod_name)
                    return False, f"Cluster is not healthy: {response}"
        return True, "Cluster is not healthy."

    def check_cluster_storage(self):
        """Check the cluster storage."""
        status, response = self.get_hctl_status()
        if status:
            avail_capacity = response['filesystem']['stats']['fs_avail_disk']
            LOGGER.info("Available Capacity : %s", avail_capacity)
            total_capacity = response['filesystem']['stats']['fs_total_disk']
            LOGGER.info("Total Capacity : %s", total_capacity)
            used_capacity = total_capacity - avail_capacity
            LOGGER.info("Used Capacity : %s", used_capacity)
            return True, (total_capacity, avail_capacity, used_capacity)
        return False, "Failed to get cluster storage stat."

    def collect_support_bundles(self, dir_path: str):
        """
        Collect support bundles from various components using support bundle cmd.

        :param dir_path: local directory path to copy support bundles.
        """
        LOGGER.info("Support bundle collection is started.")
        resp = self.exec_k8s_command(cmd.CMD_GENERATE_CLSTR_LOGS)
        for line in resp[1]:
            if ".tar" in line:
                out = line.split()[1]
                file = out.strip('\"')
                remote_path = os.path.join(
                    const.K8S_SCRIPTS_PATH.format(
                        const.K8S_SCRIPTS_PATH), file)
                local_path = os.path.join(dir_path, file)
                copy_file_from_remote(self.host, self.user, self.password, local_path, remote_path)
                remove_remote_file(self.host, self.user, self.password, remote_path)
                LOGGER.info("Support bundle '%s' generated and copied to %s.", file, local_path)
                return True, local_path
        LOGGER.error("Support Bundle not generated; response: %s", resp)
        return False, f"Support bundles not generated. Response: {resp}"


def mount_nfs_server(host_dir: str, mnt_dir: str) -> bool:
    """
    Mount nfs server on mount directory.

    :param: host_dir: Link of NFS server with path.
    :param: mnt_dir: Path of directory to be mounted.
    """
    try:
        if not os.path.exists(mnt_dir):
            os.makedirs(mnt_dir)
            LOGGER.debug("Created directory: %s", mnt_dir)
        if host_dir:
            if not os.path.ismount(mnt_dir):
                resp = os.system(CMD_MOUNT.format(host_dir, mnt_dir))
                if resp:
                    raise IOError(f"Failed to mount server: {host_dir} on {mnt_dir}")
                LOGGER.debug("NFS Server: %s, mount on %s successfully.", host_dir, mnt_dir)
            else:
                LOGGER.debug("NFS Server already mounted.")
            return os.path.ismount(mnt_dir)
        LOGGER.debug("NFS Server not provided, Storing logs locally at %s", mnt_dir)
        return os.path.isdir(mnt_dir)
    except OSError as error:
        LOGGER.error(error)
        return False


def collect_support_bundle():
    """Collect support bundles from various components using support bundle cmd."""
    try:
        bundle_dir = os.path.join(os.getcwd(), "support_bundle")
        if os.path.exists(bundle_dir):
            LOGGER.debug("Removing existing directory %s", bundle_dir)
            shutil.rmtree(bundle_dir)
        os.mkdir(bundle_dir)
        if CLUSTER_CFG["product_family"] == "LC":
            status, bundle_fpath = sb.collect_support_bundle_k8s(local_dir_path=bundle_dir)
            if not status:
                raise IOError(f"Failed to generated SB. Response:{bundle_fpath}")
        else:
            raise Exception(
                f"Support bundle collection unsupported for {CLUSTER_CFG['product_family']}")
    except OSError as error:
        LOGGER.error("An error occurred in collect_support_bundle: %s", error)
        return False, error

    return os.path.exists(bundle_fpath), bundle_fpath


def rotate_logs(dpath: str, max_count: int = 0):
    """
    Remove old logs based on creation time and keep as per max log count, default is 5.

    :param: dpath: Directory path of log files.
    :param: max_count: Maximum count of log files to keep.
    """
    max_count = max_count if max_count else CORIO_CFG.get("max_sb", 5)
    if not os.path.exists(dpath):
        raise IOError(f"Directory '{dpath}' path does not exists.")
    files = sorted(glob.glob(dpath + '/**'), key=os.path.getctime, reverse=True)
    LOGGER.debug(files)
    if len(files) > max_count:
        for fpath in files[max_count:]:
            if os.path.exists(fpath):
                if os.path.isfile(fpath):
                    os.remove(fpath)
                    LOGGER.debug("Removed: Old log file: %s", fpath)
                if os.path.isdir(fpath):
                    shutil.rmtree(fpath)
                    LOGGER.debug("Removed: Old log directory: %s", fpath)

    if len(os.listdir(dpath)) > max_count:
        raise IOError(f"Failed to rotate SB logs: {os.listdir(dpath)}")


def collect_upload_sb_to_nfs_server(mount_path: str, run_id: str, max_sb: int = 0):
    """
    Collect SB and copy to NFS server log and keep SB logs as per max_sb count.

    :param mount_path: Path of mounted directory.
    :param run_id: Unique id for each run.
    :param max_sb: maximum sb count to keep on nfs server.
    """
    try:
        sb_files = []
        sb_dir = os.path.join(mount_path, "CorIO-Execution", str(run_id), "Support_Bundles")
        if not os.path.exists(sb_dir):
            os.makedirs(sb_dir)
        status, fpath = collect_support_bundle()
        if status:
            shutil.copy2(fpath, sb_dir)
            LOGGER.info("Support bundle path: %s", os.path.join(sb_dir, os.path.basename(fpath)))
            rotate_logs(sb_dir, max_sb)
            sb_files = os.listdir(sb_dir)
            LOGGER.debug("SB list: %s", sb_files)
    except IOError as error:
        LOGGER.error(error)
        return False, error

    return status, sb_files
