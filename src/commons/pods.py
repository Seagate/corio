#!/usr/bin/python
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

"""Pods impl.

Command builder should not be part of this class.
However validation of sub commands and options can be done in command issuing functions
like send_k8s_cmd.
"""

import logging
import os
import time

from src.commons import commands
from src.commons import constants as const
from src.commons.utils.corio_utils import RemoteHost as Host

log = logging.getLogger(__name__)

namespace_map = {}


class LogicalNode(Host):
    """
    Pods helper class.

    The Command builder should be written separately and will be
    using this class.
    """

    kube_commands = ('create', 'apply', 'config', 'get', 'explain',
                     'autoscale', 'patch', 'scale', 'exec')

    def send_k8s_cmd(
            self,
            operation: str,
            pod: str,
            namespace: str,
            command_suffix: str,
            decode=False,
            **kwargs) -> bytes:
        """send/execute command on logical node/pods."""
        if operation not in LogicalNode.kube_commands:
            raise ValueError(
                "command parameter must be one of %r." % str(LogicalNode.kube_commands))
        log.debug("Performing %s on service %s in namespace %s...", operation, pod, namespace)
        cmd = commands.KUBECTL_CMD.format_map(operation, pod, namespace, command_suffix)
        resp = self.execute_command(cmd, **kwargs)
        if decode:
            resp = (resp.decode("utf8")).strip()
        return resp

    def send_sync_command(self, pod_prefix):
        """
        Helper function to send sync command to all containers of given pod category.

        :param pod_prefix: Prefix to define the pod category
        :return: Bool
        """
        log.info("Run sync command on all containers of pods %s", pod_prefix)
        pod_dict = self.get_all_pods_containers(pod_prefix=pod_prefix)
        if pod_dict:
            for pod, containers in pod_dict.items():
                for cnt in containers:
                    res = self.send_k8s_cmd(
                        operation="exec", pod=pod, namespace=const.NAMESPACE,
                        command_suffix=f"-c {cnt} -- sync", decode=True)
                    log.info("Response for pod %s container %s: %s", pod, cnt, res)

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
            log.info("Get all data pod names of %s", pod_prefix)
            output = self.execute_command(commands.CMD_POD_STATUS +
                                          " -o=custom-columns=NAME:.metadata.name", read_lines=True)
            for lines in output:
                if pod_prefix in lines:
                    pod_list.append(lines.strip())

        for pod in pod_list:
            cmd = commands.KUBECTL_GET_POD_CONTAINERS.format_map(pod)
            output = self.execute_command(command=cmd, read_lines=True)
            output = output[0].split()
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
        try:
            if pod_name:
                log.info("Getting deploy and replicaset of pod %s", pod_name)
                resp = self.get_deploy_replicaset(pod_name)
                deploy = resp[1]
            log.info("Scaling %s replicas for deployment %s", num_replica, deploy)
            cmd = commands.KUBECTL_CREATE_REPLICA.format_map(num_replica, deploy)
            output = self.execute_command(command=cmd, read_lines=True)
            log.info("Response: %s", output)
            time.sleep(60)
            log.info("Check if pod of deployment %s exists", deploy)
            cmd = commands.KUBECTL_GET_POD_DETAILS.format_map(deploy)
            output = self.execute_command(command=cmd, read_lines=True)
            status = bool(output)
            return status, deploy
        except Exception as error:
            log.error("*ERROR* An exception occurred in %s: %s",
                      LogicalNode.create_pod_replicas.__name__, error)
            return False, error

    def get_deploy_replicaset(self, pod_name):
        """
        Helper function to get deployment name and replicaset name of the given pod.

        :param pod_name: Name of the pod
        :return: Bool, str, str (status, deployment name, replicaset name)
        """
        try:
            log.info("Getting details of pod %s", pod_name)
            cmd = commands.KUBECTL_GET_POD_DETAILS.format(pod_name)
            output = self.execute_command(command=cmd, read_lines=True)
            log.info("Response: %s", output)
            output = (output[0].split())[-1].split(',')
            deploy = output[0].split('=')[-1]
            replicaset = deploy + "-" + output[-1].split('=')[-1]
            return True, deploy, replicaset
        except Exception as error:
            log.error("*ERROR* An exception occurred in %s: %s",
                      LogicalNode.get_deploy_replicaset.__name__, error)
            return False, error

    def get_num_replicas(self, replicaset):
        """
        Helper function to get number of desired, current and ready replicas for given replica set.

        :param replicaset: Name of the replica set
        :return: Bool, str, str, str (Status, Desired replicas, Current replicas, Ready replicas)
        """
        try:
            log.info("Getting details of replicaset %s", replicaset)
            cmd = commands.KUBECTL_GET_REPLICASET.format_map(replicaset)
            output = self.execute_command(command=cmd, read_lines=True)
            log.info("Response: %s", output)
            output = output[0].split()
            log.info("Desired replicas: %s \nCurrent replicas: %s \nReady replicas: %s",
                     output[1], output[2], output[3])
            return True, output[1], output[2], output[3]
        except Exception as error:
            log.error("*ERROR* An exception occurred in %s: %s",
                      LogicalNode.get_num_replicas.__name__, error)
            return False, error

    def recover_deployment_k8s(self, backup_path, deployment_name):
        """
        Helper function to recover the deleted deployment using kubectl.

        :param deployment_name: Name of the deployment to be recovered
        :param backup_path: Path of the backup taken for given deployment
        :return: Bool, str (status, output)
        """
        try:
            log.info("Recovering deployment using kubectl")
            cmd = commands.KUBECTL_RECOVER_DEPLOY.format_map(backup_path)
            output = self.execute_command(command=cmd, read_lines=True)
            log.info("Response: %s", output)
            time.sleep(60)
            log.info("Check if pod of deployment %s exists", deployment_name)
            cmd = commands.KUBECTL_GET_POD_DETAILS.format_map(deployment_name)
            output = self.execute_command(command=cmd, read_lines=True)
            status = bool(output)
            return status, output
        except Exception as error:
            log.error("*ERROR* An exception occurred in %s: %s",
                      LogicalNode.recover_deployment_k8s.__name__, error)
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
            log.info("Taking backup for deployment %s", deployment_name)
            cmd = commands.KUBECTL_DEPLOY_BACKUP.format_map(deployment_name, backup_path)
            output = self.execute_command(command=cmd, read_lines=True)
            log.debug("Backup for %s is stored at %s", deployment_name, backup_path)
            log.info("Response: %s", output)
            return True, backup_path
        except Exception as error:
            log.error("*ERROR* An exception occurred in %s: %s",
                      LogicalNode.backup_deployment.__name__, error)
            return False, error

    def get_all_pods_and_ips(self, pod_prefix) -> dict:
        """
        Helper function to get pods name with pod_prefix and their IPs.

        :param: pod_prefix: Prefix to define the pod category
        :return: dict
        """
        pod_dict = {}
        output = self.execute_command(command=commands.KUBECTL_GET_POD_IPS, read_lines=True)
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
        cmd = commands.KUBECTL_GET_POD_CONTAINERS.format_map(pod_name)
        output = self.execute_command(command=cmd, read_lines=True)
        output = output[0].split()
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
        output = self.execute_command(command=commands.KUBECTL_GET_POD_NAMES, read_lines=True)
        pods = [line.strip().replace("\n", "") for line in output]
        if pod_prefix is not None:
            for each in pods:
                if pod_prefix in each:
                    pods_list.append(each)
        else:
            pods_list = pods
        log.debug("Pods list : %s", pods_list)
        return pods_list

    def get_pod_hostname(self, pod_name):
        """
        Helper function to get pod hostname.

        :param pod_name: name of the pod
        :return: str
        """
        log.info("Getting pod hostname for pod %s", pod_name)
        cmd = commands.KUBECTL_GET_POD_HOSTNAME.format_map(pod_name)
        output = self.execute_command(command=cmd, read_lines=True)
        hostname = output[0].strip()
        return hostname
