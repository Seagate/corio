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

import json
import logging
import os

from src.commons import commands as cmd
from src.commons import constants as const
from src.commons.constants import ROOT
from src.commons.utils.corio_utils import RemoteHost
from src.commons.utils.corio_utils import convert_size

LOGGER = logging.getLogger(ROOT)


class ClusterServices(RemoteHost):
    """Cluster services class to perform service related operations."""

    def exec_k8s_command(self, command, read_lines=False):
        """Execute command on remote k8s master."""
        status, output = self.execute_command(command, read_lines)
        return status, output

    def get_pod_name(self, pod_prefix: str = const.POD_NAME_PREFIX):
        """Get pod name with given prefix."""
        status, output = self.exec_k8s_command(cmd.CMD_K8S_PODS_NAME, read_lines=True)
        if status:
            for lines in output:
                if pod_prefix in lines:
                    return True, lines.strip()
        return False, f"pod with prefix \"{pod_prefix}\" not found"

    def get_hctl_status(self):
        """Get hctl status from master node."""
        status, pod_name = self.get_pod_name()
        if status:
            LOGGER.info("POD Name: %s", pod_name)
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
        LOGGER.info(response["nodes"])
        return True, "Cluster is healthy."

    def check_cluster_storage(self):
        """Check the cluster storage."""
        status, response = self.get_hctl_status()
        if status:
            LOGGER.debug(response['filesystem'])
            if response['filesystem']['stats']:
                avail_capacity = response['filesystem']['stats']['fs_avail_disk']
                LOGGER.debug("Available Capacity : %s", convert_size(avail_capacity))
                total_capacity = response['filesystem']['stats']['fs_total_disk']
                LOGGER.debug("Total Capacity : %s", convert_size(total_capacity))
                used_capacity = total_capacity - avail_capacity
                LOGGER.debug("Used Capacity : %s", convert_size(used_capacity))
                return True, {"total_capacity": total_capacity,
                              "avail_capacity": avail_capacity,
                              "used_capacity": used_capacity}
            LOGGER.warning("Cluster stat is not available: %s", response['filesystem']['stats'])
        return False, "Failed to get cluster storage stat."

    def collect_support_bundles(self, dir_path: str) -> tuple:
        """Collect support bundles from various components using support bundle cmd.

        :param dir_path: local directory path to copy support bundles.
        """
        LOGGER.info("Support bundle collection is started.")
        # Check service script path exists.
        script_path, file_name = None, None
        if (self.path_exists(const.K8S_CFT_SCRIPTS_PATH) and const.K8S_SB_SCRIPT in
                self.list_dirs(const.K8S_CFT_SCRIPTS_PATH)):
            script_path = const.K8S_CFT_SCRIPTS_PATH
        elif (self.path_exists(const.K8S_RE_SCRIPTS_PATH) and const.K8S_SB_SCRIPT in
              self.list_dirs(const.K8S_RE_SCRIPTS_PATH)):
            script_path = const.K8S_RE_SCRIPTS_PATH
        else:
            assert script_path, f"Script {const.K8S_SB_SCRIPT} missing to collect SB log's in: " \
                                f"'CFT:{const.K8S_CFT_SCRIPTS_PATH}/RE:{const.K8S_RE_SCRIPTS_PATH}"
        status, response = self.exec_k8s_command(cmd.CMD_GENERATE_CLSTR_LOGS.format(
            script_path, const.K8S_SB_SCRIPT), read_lines=True)
        assert status, f"Failed to generate support bundle: {response}"
        for line in response:
            if ".tar" in line:
                file_name = line.split()[1].strip('\"')
        assert file_name, f"Failed to generate support bundles. Response: {response}"
        remote_path = os.path.join(script_path, file_name)
        local_path = os.path.join(dir_path, file_name)
        self.download_file(local_path, remote_path)
        self.delete_file(remote_path)
        LOGGER.info("Support bundle '%s' generated and copied to %s.", file_name, local_path)
        return os.path.exists(local_path), local_path
