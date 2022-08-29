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
from src.commons.constants import ROOT
from src.commons.exception import K8sDeploymentRecoverError, DeploymentBackupException
from src.commons.exception import PodReplicaError, DeployReplicasetError, NumReplicaError
from src.commons.utils.corio_utils import convert_size
from src.commons.utils.system_utils import RemoteHost
from src.commons.yaml_parser import read_yaml

LOGGER = logging.getLogger(ROOT)


class BaseClusterServices(RemoteHost):
    """Base class for cluster services class to perform service related operations."""

    kube_commands = ('create', 'apply', 'config', 'get', 'explain',
                     'autoscale', 'patch', 'scale', 'exec')

    def exec_k8s_command(self, command, read_lines=False):
        """Execute command on remote k8s master."""
        status, output = self.execute_command(command, read_lines)
        return status, output

    def send_k8s_cmd(self, operation: str, pod: str, namespace: str, command_suffix: str,
                     **kwargs) -> bytes:
        """Send/execute command on logical node/pods."""
        if operation not in ClusterServices.kube_commands:
            raise ValueError(f"command parameter must be one of {ClusterServices.kube_commands}.")
        LOGGER.debug("Performing %s on service %s in namespace %s...", operation, pod, namespace)
        k8s_cmd = cmd.KUBECTL_CMD.format(operation, pod, namespace, command_suffix)
        status, resp = self.execute_command(k8s_cmd, **kwargs)
        if kwargs.get("decode", False) and status:
            resp = (resp.decode("utf8")).strip()
        return resp

    def send_sync_command(self, pod_prefix):
        """
        Send sync command to all containers of given pod category.

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
        Get all pods with containers of given pod_prefix.

        :param pod_prefix: Prefix to define the pod category
        :param pod_list: List of pods
        :return: Dict
        """
        pod_containers = {}
        if not pod_list:
            LOGGER.info("Get all data pod names of %s", pod_prefix)
            output = self.execute_command(
                cmd.CMD_POD_STATUS + " -o=custom-columns=NAME:.metadata.name", read_lines=True)
            for lines in output:
                if pod_prefix in lines:
                    pod_list.append(lines.strip())
        for pod in pod_list:
            k8s_cmd = cmd.KUBECTL_GET_POD_CONTAINERS.format(pod)
            status, output = self.execute_command(command=k8s_cmd, read_lines=True)
            if status:
                pod_containers[pod] = output
        return pod_containers


class ClusterServices(BaseClusterServices):
    """Cluster services class to perform service related operations."""
    # pylint: disable=E1101
    def get_pod_name(self, pod_prefix: str = const.SERVER_POD_NAME_PREFIX):
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
                pod = pod_name[:10] + pod_name[23:]
                if pod in os.getenv('DEGRADED_PODS', '').split(','):
                    LOGGER.info(
                        "Skipping Check for Pod %s as system is in degraded mode",
                        pod_name)
                    continue
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
        """
        Collect support bundles from various components using support bundle cmd.

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
            if not script_path:
                raise AssertionError(
                    f"Script {const.K8S_SB_SCRIPT} missing to collect SB log's in: CFT"
                    f":{const.K8S_CFT_SCRIPTS_PATH}/RE:{const.K8S_RE_SCRIPTS_PATH}")
        status, response = self.exec_k8s_command(cmd.CMD_GENERATE_CLSTR_LOGS.format(
            script_path, const.K8S_SB_SCRIPT), read_lines=True)
        if not status:
            raise AssertionError(f"Failed to generate support bundle: {response}")
        file_list = self.list_dirs(remote_path=script_path)
        LOGGER.debug(file_list)
        for f_name in file_list:
            if f_name in str(response):
                file_name = f_name
                break
        if not file_name:
            raise AssertionError(f"Failed to generate support bundles. Response: {response}")
        remote_path = os.path.join(script_path, file_name)
        local_path = os.path.join(dir_path, file_name)
        self.download_file(local_path, remote_path)
        self.delete_file(remote_path)
        LOGGER.info("Support bundle '%s' generated and copied to %s.", file_name, local_path)
        return os.path.exists(local_path), local_path

    def create_pod_replicas(self, num_replica, deploy=None, pod_name=None):
        """
        Delete/remove/create pod by changing number of replicas.

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
        k8s_cmd = cmd.KUBECTL_CREATE_REPLICA.format(num_replica, deploy)
        output = self.execute_command(command=k8s_cmd, read_lines=True)
        LOGGER.info("Response: %s", output)
        time.sleep(60)
        LOGGER.info("Check if deployment pod %s exists", deploy)
        k8s_cmd = cmd.KUBECTL_GET_POD_DETAILS.format(deploy)
        status, output = self.execute_command(command=k8s_cmd, read_lines=True)
        LOGGER.info("Deployment exists is: %s after POD: %s Replica set to %s",
                    status, deploy, num_replica)
        if (num_replica == 0 and status) or (num_replica == 1 and not status):
            raise PodReplicaError(output)
        os.environ['DEGRADED_PODS'] = deploy
        return True, deploy

    def get_deploy_replicaset(self, pod_name):
        """
        Get deployment name and replicaset name of the given pod.

        :param pod_name: Name of the pod
        :return: Bool, str, str (status, deployment name, replicaset name)
        """
        try:
            LOGGER.info("Getting details of pod %s", pod_name)
            k8s_cmd = cmd.KUBECTL_GET_POD_DETAILS.format(pod_name)
            status, output = self.execute_command(command=k8s_cmd, read_lines=True)
            LOGGER.info("Status %s,Response: %s", status, output)
            pod_template_hash = output[-1].split('=')[-1].rstrip('\n')
            deploy = output[0].split(' ')[0].split(pod_template_hash)[0].rstrip('-')
            replicaset = deploy + "-" + output[-1].split('=')[-1]
            return True, deploy, replicaset
        except DeployReplicasetError as error:
            LOGGER.error("*ERROR* An exception occurred in %s: %s",
                         ClusterServices.get_deploy_replicaset.__name__, error)
            return False, error

    def get_num_replicas(self, replicaset):
        """
        Get number of desired, current and ready replicas for given replica set.

        :param replicaset: Name of the replica set
        :return: Bool, str, str, str (Status, Desired replicas, Current replicas, Ready replicas)
        """
        try:
            LOGGER.info("Getting details of replicaset %s", replicaset)
            k8s_cmd = cmd.KUBECTL_GET_REPLICASET.format(replicaset)
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
        Recover the deleted deployment using kubectl.

        :param deployment_name: Name of the deployment to be recovered
        :param backup_path: Path of the backup taken for given deployment
        :return: Bool, str (status, output)
        """
        try:
            LOGGER.info("Recovering deployment using kubectl")
            k8s_cmd = cmd.KUBECTL_RECOVER_DEPLOY.format(backup_path)
            status, output = self.execute_command(command=k8s_cmd, read_lines=True)
            LOGGER.info("Status %s, Response: %s", status, output)
            time.sleep(60)
            LOGGER.info("Check if pod of deployment %s exists", deployment_name)
            k8s_cmd = cmd.KUBECTL_GET_POD_DETAILS.format(deployment_name)
            status, output = self.execute_command(command=k8s_cmd, read_lines=True)
            return status, output
        except K8sDeploymentRecoverError as error:
            LOGGER.error("*ERROR* An exception occurred in %s: %s",
                         ClusterServices.recover_deployment_k8s.__name__, error)
            return False, error

    def backup_deployment(self, deployment_name):
        """
        Take backup of the given deployment.

        :param deployment_name: Name of the deployment
        :return: Bool, str (status, path of the backup)
        """
        try:
            filename = deployment_name + "_backup.yaml"
            backup_path = os.path.join("/root", filename)
            LOGGER.info("Taking backup for deployment %s", deployment_name)
            k8s_cmd = cmd.KUBECTL_DEPLOY_BACKUP.format(deployment_name, backup_path)
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
        Get pods name with pod_prefix and their IPs.

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
        k8s_cmd = cmd.KUBECTL_GET_POD_CONTAINERS.format(pod_name)
        _, output = self.execute_command(command=k8s_cmd, read_lines=True)
        output = output.split()
        container_list = []
        for each in output:
            if container_prefix in each:
                container_list.append(each)

        return container_list

    def get_all_pods(self, pod_prefix=None) -> list:
        """
        Get all pods name with pod_prefix.

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
        Get pod hostname.

        :param pod_name: name of the pod
        :return: str
        """
        LOGGER.info("Getting pod hostname for pod %s", pod_name)
        k8s_cmd = cmd.KUBECTL_GET_POD_HOSTNAME.format(pod_name)
        _, output = self.execute_command(command=k8s_cmd, read_lines=True)
        hostname = output[0]
        return hostname

    def degrade_nodes(self, pod_prefix=const.DATA_POD_NAME_PREFIX) -> tuple:
        """
        Degrade nodes as needed for execution by shutdown/deleting pod.

        :param pod_prefix : pod type which needs to be degraded.
        :return: Bool, List of down nodes
        """
        LOGGER.info("Setting the current namespace")
        k8s_cmd = cmd.KUBECTL_SET_CONTEXT.format(const.NAMESPACE)
        status, resp_ns = self.execute_command(command=k8s_cmd,
                                               read_lines=True)
        if status:
            LOGGER.info(resp_ns)
        LOGGER.info("Shutdown the pods safely by making replicas=0")
        pod_list = self.get_all_pods(pod_prefix=pod_prefix)
        deleted_pod = []
        for _ in range(int(os.environ['DEGRADED_PODS_CNT'])):
            LOGGER.info("Get pod name to be deleted")
            pod_name = random.sample(pod_list, 1)[0]
            pod_list.remove(pod_name)
            hostname = self.get_pod_hostname(pod_name=pod_name)
            LOGGER.info("Deleting pod %s", pod_name)
            ret = self.create_pod_replicas(num_replica=0, pod_name=pod_name)
            deleted_pod.append(ret[1])
            LOGGER.info("Shutdown/deleted pod %s for host %s with replicas=0", pod_name, hostname)
        LOGGER.info("%s pods shutdown successfully", deleted_pod)
        return True, deleted_pod

    def get_all_workers_details(self, names: bool = True) -> list:
        """Get all worker name from master node if names else all details."""
        resp = self.execute_command(cmd.KUBECTL_GET_WORKERS_NAME)
        LOGGER.debug("response is: %s", resp)
        if names:
            resp = resp[1].strip().split("\n")[1:]
            LOGGER.info("worker nodes: %s", resp)
        return resp

    def get_cluster_config(self, pod_list=None) -> dict:
        """
        Function to fetch data from file (e.g. conf files).

        :param pod_list: Data pod name list to get the cluster.conf File.
        """
        if pod_list is None:
            pod_list = self.get_all_pods(pod_prefix=const.DATA_POD_NAME_PREFIX)
        pod_name = random.sample(pod_list, 1)[0]
        conf_cp = cmd.K8S_CP_TO_LOCAL_CMD.format(pod_name, const.CLUSTER_CONF_PATH,
                                                 const.LOCAL_CONF_PATH, const.HAX_CONTAINER_NAME)
        resp_node = self.execute_command(command=conf_cp)
        LOGGER.debug("%s response %s ", conf_cp, resp_node)
        local_conf = os.path.join(os.getcwd(), "cluster.conf")
        if os.path.exists(local_conf):
            os.remove(local_conf)
        self.download_file(local_path=local_conf, remote_path=const.LOCAL_CONF_PATH)
        return read_yaml(local_conf)

    def retrieve_durability_values(self, dur_type: str = 'sns') -> dict:
        """
        Return the durability Configuration for Data/Metadata (SNS/DIX) for the cluster.

        :param dur_type: sns/dix
        :return : dict of format { 'data': '1','parity': '4','spare': '0'}
        """
        resp = self.get_cluster_config()['cluster']['storage_set'][0]['durability'][
            dur_type.lower()]
        LOGGER.info(resp)
        return resp

    def get_user_quota_in_bytes(self):
        """Get available user quota in bytes"""
        status, cluster_stat = self.check_cluster_storage()
        if not status:
            raise AssertionError(f"Failed to get cluster storage details: {cluster_stat}")
        total_capacity = cluster_stat.get("total_capacity")
        avail_capacity = cluster_stat.get("avail_capacity")
        used_capacity = cluster_stat.get("used_capacity")
        LOGGER.info("Total Capacity: %s Available Capacity: %s Used Capacity: %s", total_capacity,
                    avail_capacity, used_capacity)
        # Get SNS configuration to retrieve available user data space
        durability_values = self.retrieve_durability_values('sns')
        sns_values = {key: int(value) for key, value in durability_values.items()}
        LOGGER.debug("Durability Values (SNS) %s", sns_values)
        user_quota_to_writes = int(sns_values['data'] / sum(sns_values.values()) * avail_capacity)
        LOGGER.info("User writes to be performed %s bytes to attain the disk full space",
                    user_quota_to_writes)
        return True, user_quota_to_writes
