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
import random
import time

from src.commons import commands as cmd
from src.commons import constants as const
from src.commons.constants import DATA_POD_NAME_PREFIX
from src.commons.constants import ROOT
from src.commons.exception import K8sDeploymentRecoverError, DeploymentBackupException
from src.commons.exception import PodReplicaError, DeployReplicasetError, NumReplicaError
from src.commons.utils.corio_utils import RemoteHost
from src.commons.utils.corio_utils import convert_size

LOGGER = logging.getLogger(ROOT)


class ClusterServices(RemoteHost):
    """Cluster services class to perform service related operations."""
    kube_commands = ('create', 'apply', 'config', 'get', 'explain',
                     'autoscale', 'patch', 'scale', 'exec')

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
            status, output = self.exec_k8s_command(cmd.CMD_K8S_CLUSTER_HEALTH.format_map(
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

    def send_k8s_cmd(
            self,
            operation: str,
            pod: str,
            namespace: str,
            command_suffix: str,
            decode=False,
            **kwargs) -> bytes:
        """send/execute command on logical node/pods."""
        if operation not in ClusterServices.kube_commands:
            raise ValueError(
                f"command parameter must be one of {ClusterServices.kube_commands}.")
        LOGGER.debug("Performing %s on service %s in namespace %s...", operation, pod, namespace)
        k8s_cmd = cmd.KUBECTL_CMD.format_map(operation, pod, namespace, command_suffix)
        status, resp = self.execute_command(k8s_cmd, **kwargs)
        if decode and status:
            resp = (resp.decode("utf8")).strip()
        return resp

    def send_sync_command(self, pod_prefix):
        """
        Helper function to send sync command to all containers of given pod category.

        :param pod_prefix: Prefix to define the pod category
        :return: Bool
        """
        LOGGER.info("Run sync command on all containers of pods %s", pod_prefix)
        pod_dict = self.get_all_pods_containers(pod_prefix=pod_prefix)
        if pod_dict:
            for pod, containers in pod_dict.items():
                for cnt in containers:
                    res = self.send_k8s_cmd(
                        operation="exec", pod=pod, namespace=const.NAMESPACE,
                        command_suffix=f"-c {cnt} -- sync", decode=True)
                    LOGGER.info("Response for pod %s container %s: %s", pod, cnt, res)

        return True

    def get_all_pods_containers(self, pod_prefix, pod_list=None):
        """
        Helper function to get all pods with containers of given pod_prefix.

        :param pod_prefix: Prefix to define the pod category
        :param pod_list: List of pods
        :return: Dict
        """
        pod_containers = {}
        if not pod_list:
            LOGGER.info("Get all data pod names of %s", pod_prefix)
            output = self.execute_command(cmd.CMD_POD_STATUS +
                                          " -o=custom-columns=NAME:.metadata.name", read_lines=True)
            for lines in output:
                if pod_prefix in lines:
                    pod_list.append(lines.strip())

        for pod in pod_list:
            k8s_cmd = cmd.KUBECTL_GET_POD_CONTAINERS.format_map(pod)
            status, output = self.execute_command(command=k8s_cmd, read_lines=True)
            if status:
                pod_containers[pod] = output

        return pod_containers

    def create_pod_replicas(self, num_replica, deploy=None, pod_name=None):
        """
        Helper function to delete/remove/create pod by changing number of replicas.

        :param num_replica: Number of replicas to be scaled
        :param deploy: Name of the deployment of pod
        :param pod_name: Name of the pod
        :return: Bool, string (status, deployment name)
        """

        if pod_name:
            LOGGER.info("Getting deploy and replicaset of pod %s", pod_name)
            resp = self.get_deploy_replicaset(pod_name)
            deploy = resp[1]
        LOGGER.info("Scaling %s replicas for deployment %s", num_replica, deploy)
        k8s_cmd = cmd.KUBECTL_CREATE_REPLICA.format_map(num_replica, deploy)
        output = self.execute_command(command=k8s_cmd, read_lines=True)
        LOGGER.info("Response: %s", output)
        time.sleep(60)
        LOGGER.info("Check if pod of deployment %s exists", deploy)
        k8s_cmd = cmd.KUBECTL_GET_POD_DETAILS.format_map(deploy)
        status, output = self.execute_command(command=k8s_cmd, read_lines=True)
        LOGGER.info("Deployment %s after POD %s Replica", status, deploy)
        if not status:
            raise PodReplicaError(output)

    def get_deploy_replicaset(self, pod_name):
        """
        Helper function to get deployment name and replicaset name of the given pod.

        :param pod_name: Name of the pod
        :return: Bool, str, str (status, deployment name, replicaset name)
        """
        try:
            LOGGER.info("Getting details of pod %s", pod_name)
            k8s_cmd = cmd.KUBECTL_GET_POD_DETAILS.format_map(pod_name)
            status, output = self.execute_command(command=k8s_cmd, read_lines=True)
            LOGGER.info("Status %s,Response: %s", status, output)
            output = output[-1].split(',')
            deploy = output.split('=')[-1]
            replicaset = deploy + "-" + output[-1].split('=')[-1]
            return True, deploy, replicaset
        except DeployReplicasetError as error:
            LOGGER.error("*ERROR* An exception occurred in %s: %s",
                         ClusterServices.get_deploy_replicaset.__name__, error)
            return False, error

    def get_num_replicas(self, replicaset):
        """
        Helper function to get number of desired, current and ready replicas for given replica set.

        :param replicaset: Name of the replica set
        :return: Bool, str, str, str (Status, Desired replicas, Current replicas, Ready replicas)
        """
        try:
            LOGGER.info("Getting details of replicaset %s", replicaset)
            k8s_cmd = cmd.KUBECTL_GET_REPLICASET.format_map(replicaset)
            status, output = self.execute_command(command=k8s_cmd, read_lines=True)
            LOGGER.info("Status %s, Response: %s", status, output)
            LOGGER.info("Desired replicas: %s \nCurrent replicas: %s \nReady replicas: %s",
                        output[1], output[2], output[3])
            return True, output[1], output[2], output[3]
        except NumReplicaError as error:
            LOGGER.error("*ERROR* An exception occurred in %s: %s",
                         ClusterServices.get_num_replicas.__name__, error)
            return False, error

    def recover_deployment_k8s(self, backup_path, deployment_name):
        """
        Helper function to recover the deleted deployment using kubectl.

        :param deployment_name: Name of the deployment to be recovered
        :param backup_path: Path of the backup taken for given deployment
        :return: Bool, str (status, output)
        """
        try:
            LOGGER.info("Recovering deployment using kubectl")
            k8s_cmd = cmd.KUBECTL_RECOVER_DEPLOY.format_map(backup_path)
            status, output = self.execute_command(command=k8s_cmd, read_lines=True)
            LOGGER.info("Status %s, Response: %s", status, output)
            time.sleep(60)
            LOGGER.info("Check if pod of deployment %s exists", deployment_name)
            k8s_cmd = cmd.KUBECTL_GET_POD_DETAILS.format_map(deployment_name)
            status, output = self.execute_command(command=k8s_cmd, read_lines=True)
            return status, output
        except K8sDeploymentRecoverError as error:
            LOGGER.error("*ERROR* An exception occurred in %s: %s",
                         ClusterServices.recover_deployment_k8s.__name__, error)
            return False, error

    def backup_deployment(self, deployment_name):
        """
        Helper function to take backup of the given deployment.

        :param deployment_name: Name of the deployment
        :return: Bool, str (status, path of the backup)
        """
        try:
            filename = deployment_name + "_backup.yaml"
            backup_path = os.path.join("/root", filename)
            LOGGER.info("Taking backup for deployment %s", deployment_name)
            k8s_cmd = cmd.KUBECTL_DEPLOY_BACKUP.format_map(deployment_name, backup_path)
            status, output = self.execute_command(command=k8s_cmd, read_lines=True)
            LOGGER.debug("Backup for %s is stored at %s", deployment_name, backup_path)
            LOGGER.info("Status %s: Response: %s", status, output)
            return True, backup_path
        except DeploymentBackupException as error:
            LOGGER.error("*ERROR* An exception occurred in %s: %s",
                         ClusterServices.backup_deployment.__name__, error)
            return False, error

    def get_all_pods_and_ips(self, pod_prefix) -> dict:
        """
        Helper function to get pods name with pod_prefix and their IPs.

        :param: pod_prefix: Prefix to define the pod category
        :return: dict
        """
        pod_dict = {}
        _, output = self.execute_command(command=cmd.KUBECTL_GET_POD_IPS, read_lines=True)
        for lines in output:
            if pod_prefix in lines:
                data = lines.strip()
                pod_name = data.split()[0]
                pod_ip = data.split()[1].replace("\n", "")
                pod_dict[pod_name.strip()] = pod_ip.strip()
        return pod_dict

    def get_container_of_pod(self, pod_name, container_prefix):
        """
        Get containers with container_prefix (str) from the specified pod_name.

        :param: pod_name : Pod name to query container of
        :param: container_prefix: Prefix to define container category
        :return: list
        """
        k8s_cmd = cmd.KUBECTL_GET_POD_CONTAINERS.format_map(pod_name)
        _, output = self.execute_command(command=k8s_cmd, read_lines=True)
        output = output.split()
        container_list = []
        for each in output:
            if container_prefix in each:
                container_list.append(each)

        return container_list

    def get_all_pods(self, pod_prefix=None) -> list:
        """
        Helper function to get all pods name with pod_prefix.

        :param: pod_prefix: Prefix to define the pod category
        :return: list
        """
        pods_list = []
        _, output = self.execute_command(command=cmd.KUBECTL_GET_POD_NAMES, read_lines=True)
        pods = [line.strip().replace("\n", "") for line in output]
        if pod_prefix is not None:
            for each in pods:
                if pod_prefix in each:
                    pods_list.append(each)
        else:
            pods_list = pods
        LOGGER.debug("Pods list : %s", pods_list)
        return pods_list

    def get_pod_hostname(self, pod_name):
        """
        Helper function to get pod hostname.

        :param pod_name: name of the pod
        :return: str
        """
        LOGGER.info("Getting pod hostname for pod %s", pod_name)
        k8s_cmd = cmd.KUBECTL_GET_POD_HOSTNAME.format_map(pod_name)
        _, output = self.execute_command(command=k8s_cmd, read_lines=True)
        hostname = output.strip()
        return hostname

    def degrade_nodes(self, pod_prefix=DATA_POD_NAME_PREFIX) -> None:
        """
        degrade nodes as needed for execution by shutdown/deleting pod
        :param pod_prefix : pod type which needs to be degraded.
        """
        LOGGER.info("Setting the current namespace")
        k8s_cmd = cmd.KUBECTL_SET_CONTEXT.format_map(const.NAMESPACE)
        status, resp_ns = self.execute_command(command=k8s_cmd,
                                               read_lines=True)
        if status:
            LOGGER.info(resp_ns)
        LOGGER.info("Shutdown the pods safely by making replicas=0")
        pod_list = self.get_all_pods(pod_prefix=pod_prefix)
        for _ in range(int(os.environ['DEGRADED_PODS_CNT'])):
            LOGGER.info("Get pod name to be deleted")
            pod_name = random.sample(pod_list, 1)[0]
            pod_list.remove(pod_name)
            hostname = self.get_pod_hostname(pod_name=pod_name)
            LOGGER.info("Deleting pod %s", pod_name)
            self.create_pod_replicas(num_replica=0, pod_name=pod_name)
            LOGGER.info("Shutdown/deleted pod %s for host %s with replicas=0", pod_name, hostname)
        LOGGER.info("All pods shutdown successfully")
        return True
